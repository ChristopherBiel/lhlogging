"""
Fleet simulation — P(a tail flies a flight) under the fleet's physics.

booking_model.p_target judges every flight on its own. This plays the coming
days forward many times instead, one fleet type at a time, flight by flight in
departure order, keeping every tail's position:

  * a tail can only take a flight that leaves from where it is, once it has
    landed there (MIN_TURN) — 99.5% of real consecutive legs chain like that;
  * having taken it, it is away until it lands at the other end. Outstation
    departures therefore go to whoever flew in (the return inherits the
    outbound, 97% of hub departures are out-and-back trips);
  * the tail published on a flight keeps it with the model's hold rate
    (booking_model.p_target, clash checks included), corrected for how often
    the simulation still has it at that airport: keep = hold / availability,
    so the marginal stays the hold rate;
  * otherwise a tail is drawn from those on the ground there, weighted by its
    history share of the flight (booking_model.share) and a bonus when it came
    in on the flight's usual inbound connection (Lufthansa's "lines": the
    inbound predicts the outbound 24% / 54% / 38% for 747-8 / A380 / A350).

P = the share of samples in which the tail took the flight. Every flight's
probabilities sum to 1 and no tail is on two overlapping flights in any sample.
The served number (blend_hard) mixes it with p_target (`mix`) as a safety net
for what the simulation gets wrong (stale looks, chain swaps, legs it does not
know) — with the odds of tails that cannot be at the airport in time at all
cut to a tenth.

Stdlib only, no database: runs in the dashboard (model warmer) and in
tools/benchmark_fleet.py, which scores it walk-forward against p_target.
"""
from __future__ import annotations

import random
from collections import Counter, defaultdict
from datetime import timedelta

import booking_model as bm

HUBS = frozenset({"FRA", "MUC"})
MIN_TURN = bm.TURN
# How far back the start state looks for a tail's last flight.
LOOKBACK = timedelta(days=7)
# A tail the simulation leaves at an outstation this long after it could have
# left (its return is not in the schedule) counts as back at the hubs.
STUCK = timedelta(hours=36)
# Weight of a tail whose position is unknown (or stuck) at a hub departure.
UNKNOWN_W = 0.5
# Connection history: inbound -> next outbound pairs with at most this ground time.
CONN_MAX_GROUND = timedelta(hours=24)
N_SAMPLES = 200
# How far ahead the simulation is worth running: it sharpens the published
# window; ten days out it only adds sampling noise to p_target's even shares.
HORIZON = timedelta(hours=144)
# Knobs per fleet type (tools/benchmark_fleet.py picks them on the walk-forward):
#   mix   weight of p_target (location-weighted, see blend) in the served number
#   conn  bonus for the usual inbound connection: w x (1 + conn x P(this out | inbound))
DEFAULTS = {"mix": 0.5, "conn": 3.0}
# Fleets the dashboard runs it for: those whose walk-forward it beat
# (tools/benchmark_fleet.py, docs/booking_model.md). The 787 joined once
# multi-stop flights were stored whole (LH568/569 FRA-LOS-SSG broke its chains:
# the tail that flew was "unreachable" 5-8% of the time, now 1.2-2.2%).
SIM_TYPES = ("B748", "A388", "A359", "B789")
# blend_hard(): share of p_target's weight a tail keeps when no path through
# the schedule brings it to the airport in time — the plan it rests on can be
# stale (the tail that flew was "unreachable" for 0-1.2% of legs, most of them
# inside 12h: last-minute swaps after our last look).
HARD_FLOOR = 0.1
KNOBS = {}


def connections(legs, now, weeks=bm.HISTORY_WEEKS):
    """{inbound flight number: Counter(next outbound flight number)} from the
    truth chains of legs settled in the last `weeks` before `now`."""
    lo = now - timedelta(weeks=weeks)
    by_tail = defaultdict(list)
    for leg in legs:
        dep = leg.get("dep_utc")
        if leg.get("truth_tail") and not leg.get("cancelled") and dep and lo <= dep < now:
            by_tail[leg["truth_tail"]].append(leg)
    out = defaultdict(Counter)
    for chain in by_tail.values():
        chain.sort(key=lambda l: l["dep_utc"])
        for a, b in zip(chain, chain[1:]):
            arr = bm.arr_utc(a)
            if (a.get("arr") in HUBS and b.get("dep") == a.get("arr") and arr is not None
                    and timedelta(0) <= b["dep_utc"] - arr <= CONN_MAX_GROUND):
                out[a["flight_number"]][b["flight_number"]] += 1
    return out


class SimResult:
    def __init__(self, ftype, now, n):
        self.ftype, self.now, self.n = ftype, now, n
        self.counts = {}                  # leg key -> Counter(tail): samples it flew
        self.reach = {}                   # leg key -> tails that can be there in time at all
        self.diag = Counter()             # fallbacks taken, for the benchmark
        self.gaps = Counter()             # (fallback, airport, flight number) -> samples

    def p(self, leg, tail):
        c = self.counts.get(bm.leg_key(leg))
        return c[tail] / self.n if c is not None else None

    def dist(self, leg):
        c = self.counts.get(bm.leg_key(leg))
        return {t: k / self.n for t, k in c.items()} if c is not None else None

    def reachable(self, leg, tail):
        """Whether any path through the schedule can bring `tail` from where
        it is now to the departure airport in time (unknown position: yes)."""
        r = self.reach.get(bm.leg_key(leg))
        return r is None or tail in r

    def __contains__(self, leg):
        return bm.leg_key(leg) in self.counts


def start_state(st, tails, now):
    """tail -> (airport, free from, inbound flight number) as of `now`, from
    the last flight the plan has it on that left before `now` (still airborne
    counts: free once it lands). (None, None, None) when unknown."""
    state = {}
    for t in tails:
        last = None
        for leg in st.plan.around(t, now - LOOKBACK, now):
            if bm.tail_as_of(leg, now) == t and (last is None or leg["dep_utc"] > last["dep_utc"]):
                last = leg
        if last is None:
            state[t] = (None, None, None)
        else:
            state[t] = (last.get("arr"), (bm.arr_utc(last) or last["dep_utc"]) + MIN_TURN,
                        last["flight_number"])
    return state


def simulate(st, legs, now, ftype, horizon, n=N_SAMPLES, knobs=None, conn=None, seed=0,
             hist_knobs=None):
    """Play `ftype`'s flights departing in (now, now + horizon] forward `n`
    times. `legs` is the schedule (anything; only this type's are flown);
    `st` a booking_model Stats with its plan index; `conn` from connections()."""
    k = dict(DEFAULTS, **(KNOBS.get(ftype) or {}), **(knobs or {}))
    tails = sorted(t for t, ty in st.tail_type.items() if ty == ftype)
    res = SimResult(ftype, now, n)
    if not tails or st.plan is None:
        return res
    sched = sorted((l for l in legs
                    if l.get("fleet_type") == ftype and not l.get("cancelled")
                    and l.get("dep_utc") is not None and now < l["dep_utc"] <= now + horizon),
                   key=lambda l: l["dep_utc"])
    conn = conn if conn is not None else {}
    rng = random.Random(seed)
    init = start_state(st, tails, now)
    states = [dict(init) for _ in range(n)]
    tail_set = set(tails)
    # earliest time each tail can be at each airport, over any path through
    # the schedule (relaxed leg by leg in departure order)
    earliest = {}
    for t, (loc, free, _) in init.items():
        if loc is None:
            earliest[t] = None              # unknown: anywhere
        else:
            earliest[t] = {loc: free}
            if loc not in HUBS:
                for h in HUBS:              # its return may not be in the schedule
                    earliest[t][h] = free + STUCK

    for leg in sched:
        T, A = leg["dep_utc"], leg.get("dep")
        hub = A in HUBS
        lead_d = (T - now).total_seconds() / 86400.0
        route = bm.route_key(leg)
        base = {t: bm.share(st, t, leg["flight_number"], route, ftype, hist_knobs, lead_d)[0]
                for t in tails}
        pub = bm.tail_as_of(leg, now)
        pub = pub if pub in tail_set else None
        hold = (bm.p_target(st, pub, leg, published=pub, now=now)["p"] if pub else None)
        dur = bm.arr_utc(leg)
        free_after = (dur or T + timedelta(hours=8)) + MIN_TURN
        reach = {t for t, e in earliest.items() if e is None or e.get(A, T + MIN_TURN) <= T}
        res.reach[bm.leg_key(leg)] = reach
        for t in reach:
            e = earliest[t]
            if e is not None and free_after < e.get(leg.get("arr"), free_after + MIN_TURN):
                e[leg.get("arr")] = free_after

        # candidates per sample: tails on the ground at A by T
        cands = []
        for s in states:
            here = [t for t, (loc, free, _) in s.items() if loc == A and free <= T]
            if hub:
                here += [t for t, (loc, free, _) in s.items()
                         if loc is None or (loc not in HUBS and free + STUCK <= T)]
            elif not here:
                here = [t for t, (loc, _, _) in s.items() if loc == A]  # tight turn
            cands.append(here)
        keep = 0.0
        if pub is not None and hold is not None:
            avail = sum(pub in c for c in cands) / n
            keep = min(1.0, hold / avail) if avail > 0 else 0.0

        counts = Counter()
        for s, here in zip(states, cands):
            if pub is not None and pub in here and rng.random() < keep:
                pick = pub
            else:
                pool = [t for t in here if t != pub]
                if not pool and pub is not None and pub in here:
                    pool = [pub]  # nobody else there: it goes anyway
                elif not pool:
                    if pub is not None and not hub:
                        # nobody the simulation has there: the leg that brought
                        # the tail is one it does not know (a multi-stop flight's
                        # later leg, a stale look) — trust the plan's tail
                        pool = [pub]
                        kind = "outstation: published tail"
                    else:
                        pool = tails
                        kind = "fleet-wide draw" if hub else "outstation draw"
                    res.diag[kind] += 1
                    res.gaps[(kind, A, leg["flight_number"])] += 1
                pick = _draw(rng, pool, s, base, conn, leg, k, hub)
            counts[pick] += 1
            s[pick] = (leg.get("arr"), free_after, leg["flight_number"])
        res.counts[bm.leg_key(leg)] = counts
    return res


def _draw(rng, pool, s, base, conn, leg, k, hub):
    if len(pool) == 1:
        return pool[0]
    weights = []
    for t in pool:
        loc, _, inbound = s[t]
        w = base.get(t, 0.0) or 1e-6
        if hub and (loc is None or loc not in HUBS):
            w *= UNKNOWN_W
        nxt = conn.get(inbound) if inbound else None
        if nxt:
            w *= 1.0 + k["conn"] * nxt[leg["flight_number"]] / sum(nxt.values())
        weights.append(w)
    x = rng.random() * sum(weights)
    for t, w in zip(pool, weights):
        x -= w
        if x <= 0:
            return t
    return pool[-1]


def blend_hard(res, leg, model, mix, floor=None):
    """Served distribution: (1 - mix) x the simulation + mix x p_target's
    distribution (`model`: tail -> p) with the tails that cannot reach the
    airport in time at all (res.reachable) cut to `floor` of their weight,
    renormalised. p_target is the safety net for the simulation's blind spots
    (stale looks, chain swaps); cutting it on the certain rule only — not on
    the sampled positions, which are not reliable enough to cut on (scored
    worse: the tail that flew was often one the samples had elsewhere) — keeps
    it from handing swap odds to a tail on another continent."""
    floor = HARD_FLOOR if floor is None else floor
    sim = res.dist(leg) or {}
    w = {x: p * (1.0 if res.reachable(leg, x) else floor) for x, p in model.items()}
    z = sum(w.values())
    return {x: (1.0 - mix) * sim.get(x, 0.0) + (mix * w[x] / z if z > 0 else mix * model[x])
            for x in model}


def p_fleet(res, st, target, leg, published=None, now=None, fleet=None):
    """The served P(target flies leg): blend_hard() at the type's knobs, over
    `fleet` (the type's tails; default: those the simulation knows). Falls back
    to p_target alone for a flight the simulation did not fly."""
    base = bm.p_target(st, target, leg, published=published, now=now)
    if res is None or leg not in res or base["regime"] in ("cancelled", "departed"):
        return dict(base, fleet=None)
    k = dict(DEFAULTS, **(KNOBS.get(res.ftype) or {}))
    tails = set(fleet or res.dist(leg) or ()) | {target}
    model = {x: (base["p"] if x == target else
                 bm.p_target(st, x, leg, published=published, now=now)["p"]) for x in tails}
    ps = res.p(leg, target)
    p = blend_hard(res, leg, model, k["mix"], k.get("floor"))[target]
    why = base["why"] + (" Playing the fleet forward (which tails can be at %s by then): %d%%."
                         % (leg.get("dep") or "the airport", round(ps * 100)))
    return dict(base, p=p, fleet=ps, why=why)
