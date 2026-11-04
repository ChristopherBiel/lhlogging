"""
Booking model — P(a target tail operates a given flight), for the /book planner.

Pure and stdlib-only, so the same code runs in the dashboard (over `fis_legs`
rows) and in tools/benchmark_booking.py (walk-forward over the offline leg
export): the number a visitor sees is the number the benchmark scored.

Two regimes, both built from settled legs (truth = the tail FIS reports as
having flown it) over the last HISTORY_WEEKS:

  published  a tail is currently published on the flight.
             target published      -> P = hold rate at this lead time
                                      (route -> type -> overall, first cell
                                      with enough support; between two bands
                                      the two rates interpolated); from FAR_BAND_H
                                      out, one rate per type over all far
                                      bands, barely pulled toward the other
                                      types (747-8 ~13% vs A380 ~48% there);
                                      a published tail that clashes with its
                                      own plan (also on an overlapping flight,
                                      or its previous flight lands elsewhere)
                                      takes the much lower clash rate
             other tail published  -> P = (1 - hold) x the target's share of
                                      the flight among the tails that are not
                                      the published one. 98.6% of changes are
                                      swaps, so "who replaces it" is drawn
                                      from who usually flies it.
  history    nothing published yet (beyond the feed's horizon).
             P = the target's share of that flight number's legs, shrunk
                 toward its route share and then its type share (m pseudo-legs
                 per level), so a thin flight number borrows strength instead
                 of reporting 0% or 100%.

The bottom of that chain is not "how much the tail flew lately" but "is it
flying at all": every active widebody does ~9-11 legs a week, so a tail's recent
share mostly reflects whether it spent part of the window in maintenance. The
type level is therefore the recent share shrunk toward an even split over the
fleet, in which a tail idle for more than ACTIVE_DAYS counts only an idle
weight — one for flights inside NEAR_DAYS, one beyond: an idle tail is usually
still down tomorrow, but often back from maintenance ten days on.

How much each level is worth differs by fleet: A350/787 sub-fleets stick to
routes, while 747-8 and A380 tails rotate through the daily slots (which tail
flies LH716 today says nothing about next week). So the knobs — m (flight and
route shrinkage), m_type (type share toward the even split) and the two idle
weights — are fitted per fleet type on every build: trained on the window minus
its last VALIDATION_DAYS, scored on those days (log-likelihood of the tail that
flew). tools/benchmark_booking.py scores the result walk-forward.

A leg is a dict with:
    flight_date, flight_number, dep, arr, fleet_type, dep_utc (aware datetime)
    duration_min        gate to gate, stops included (None when FIS gave none)
    segments            the legs of a flight of several (optional; see stops())
    truth_tail          '' until the flight has operated
    truth_seen          when the truth was first seen (optional; the dashboard
                        only has truths it has seen)
    timeline            [[lead_h, tail], ...] one entry per published change
    first_lead_h        lead of our first pre-departure look (None if none)
    cancelled           bool
"""
from __future__ import annotations

import math
from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

# Lead-time bands (hours before departure) the hold rate is measured at.
# Mirrors flightstatus/legs.BANDS.
BANDS_H = (216, 192, 168, 144, 120, 96, 72, 48, 36, 24, 18, 12, 6, 3)
# A cell says how often the tail published at exactly that band flew. A flight
# stands between two bands nearly always, and the rate falls steeply with the
# lead: read from the nearer band alone (False, what the model did until
# 2026-10-13; tools/benchmark_booking.py --step-hold), a 747-8 71h out was
# quoted the 48h rate, 55% where tails published 71h out hold 39%.
HOLD_INTERPOLATE = True
# A band only counts the legs we looked at that far out, and how far out the
# collector looks has changed (D+5..D+9 since 2026-09-24), so the far bands
# held only recent legs while the near ones held all eight weeks: an A380 tail
# published 5 days out read 66%, 4 days out 57%, where the same flights held
# 66% and 72%. Chained, a band's rate is corrected by how differently the
# legs seen that far out did one band in, against all the legs there, so
# every band speaks for the same legs (corrected_rate). /insights draws its
# curve so; the model keeps each band's own legs (False): walk-forward
# (tools/benchmark_booking.py --chain, 2026-10-16) it moved no lead by more
# than 0.02 nats of log-loss, and what the far A380 quote misses (about 41%
# quoted where 56-73% held 5-7 days out) is the fleet growing steadier, which
# neither reading sees.
HOLD_CHAIN = False
HISTORY_WEEKS = 8
# Share knobs, fitted per fleet type on a temporal holdout of the last
# VALIDATION_DAYS (defaults when a type has too little holdout to judge):
#   m       pseudo-legs of the route share in a flight's, and of the type share
#           in a route's
#   m_type  pseudo-legs of the even fleet split in the type share
#   idle_near / idle_far
#           weight of a tail idle > ACTIVE_DAYS in that even split, for a
#           flight less / at least NEAR_DAYS away
M_GRID = (5.0, 20.0, 80.0, 320.0, 1e6)
M_TYPE_GRID = (0.0, 20.0, 80.0, 320.0, 1e6)
IDLE_W_GRID = (0.05, 0.2, 0.5, 1.0)
DEFAULTS = {"m": 20.0, "m_type": 80.0, "idle_near": 0.2, "idle_far": 0.5}
NEAR_DAYS = 5
VALIDATION_DAYS = 14
MIN_VALIDATION_LEGS = 30
# Among grid points scoring within this many nats per holdout leg of the best,
# take the most conservative (most shrinkage): two weeks of one fleet is a few
# hundred legs, and a flight-specific setting can win that by luck.
FIT_TOLERANCE = 0.01
# A fitted m at least this large means the fleet's flights carry no history
# signal (the grid's top value is "use the fleet share").
NO_SIGNAL_M = 1e5
ACTIVE_DAYS = 3
# Same for hold rates: pseudo-legs of the type (and overall) rate mixed into a
# route's hold rate, so a thin route can't report 0% or 100%.
HOLD_M = 20.0
# Who replaces a swapped-out tail: this weight on the flight's own history
# (who usually flies it), the rest on the type-wide share (who is flying at all).
SWAP_MIX = 0.5
# From this lead out (the D+5..D+9 probe) a publication holds about equally
# often at every band, but very differently per type, so the far bands share
# one cell per type (each leg counted once, its far bands averaged) and the
# type is pulled only FAR_TYPE_M pseudo-legs toward the cross-type rate.
FAR_BAND_H = 120
FAR_TYPE_M = 5.0
# the band just inside the far ones, which the far cells chain onto
_FAR_INSIDE = BANDS_H[BANDS_H.index(FAR_BAND_H) + 1]
# A settled leg's truth lands with the D-1/D-2 truth pass; until then the plan
# knows it by its last pre-departure publication.
TRUTH_LAG = timedelta(days=2)
# Plan clash: minimum turn between two flights of one tail (0.13% of real
# consecutive legs turn faster), and how far around a flight the tail's other
# flights are searched.
TURN = timedelta(minutes=45)
PLAN_BACK = timedelta(hours=48)
PLAN_AHEAD = timedelta(hours=24)
# Clash hold rates are rare cells, so they pool bands into three groups
# (labelled by their farthest band): inside a day, 36-72h, 96h and beyond.
CLASH_GROUPS = (24, 72, 216)


def tail_at(timeline, first_lead_h, lead_h):
    """Tail published at `lead_h` hours before departure, from a collapsed
    timeline (descending lead). None when our first look came later than that.
    Mirrors flightstatus/legs.tail_at."""
    if first_lead_h is None or first_lead_h == "" or float(first_lead_h) < lead_h:
        return None
    tail = None
    for lead, reg in timeline:
        if lead >= lead_h:
            tail = reg
        else:
            break
    return tail


def band_for(lead_h):
    """The band whose hold rate applies to a tail published `lead_h` hours out:
    the largest band not beyond it (inside 3h, the 3h band)."""
    for b in BANDS_H:
        if lead_h >= b:
            return b
    return BANDS_H[-1]


def bands_around(lead_h):
    """(near, far, w): the bands `lead_h` lies between and how far it is from
    the near one toward the far one (0..1). Beyond the outermost band, inside
    the innermost and on a band, that band alone (w = 0)."""
    near = band_for(lead_h)
    i = BANDS_H.index(near)
    if i == 0 or lead_h <= near:
        return near, near, 0.0
    far = BANDS_H[i - 1]
    return near, far, (lead_h - near) / float(far - near)


def clash_group(band):
    for g in CLASH_GROUPS:
        if band <= g:
            return g
    return CLASH_GROUPS[-1]


def arr_utc(leg):
    dep, dur = leg.get("dep_utc"), leg.get("duration_min")
    return dep + timedelta(minutes=int(dur)) if dep is not None and dur else None


def leg_key(leg):
    return (leg.get("flight_date"), leg.get("flight_number"))


def tail_as_of(leg, now):
    """The tail the plan had on `leg` at `now`: the truth once it was seen
    (`truth_seen`, the first terminal look; without one, TRUTH_LAG after
    departure), else the last publication taken before `now` (before
    departure, for a flight that has left)."""
    dep = leg.get("dep_utc")
    if dep is None or leg.get("cancelled"):
        return None
    if leg.get("truth_tail") and dep <= now:
        seen = leg.get("truth_seen")
        if (seen <= now) if seen is not None else (dep <= now - TRUTH_LAG):
            return leg["truth_tail"]
    lead = (dep - now).total_seconds() / 3600.0
    return tail_at(leg.get("timeline") or [], leg.get("first_lead_h"), max(lead, 0.0))


class PlanIndex:
    """Every leg a tail was ever published on or flew, by tail and departure,
    so a tail's plan around a flight is a bisect away. Built once over the
    history and upcoming legs; which of those legs the tail held at a given
    moment is tail_as_of's question."""

    def __init__(self, legs):
        by = defaultdict(list)
        for leg in legs:
            if leg.get("cancelled") or leg.get("dep_utc") is None:
                continue
            for tail in {t for _, t in leg.get("timeline") or ()} | {leg.get("truth_tail")}:
                if tail:
                    by[tail].append(leg)
        self.legs = {t: sorted(ls, key=lambda l: l["dep_utc"]) for t, ls in by.items()}
        self.deps = {t: [l["dep_utc"] for l in ls] for t, ls in self.legs.items()}
        self.memo = {}

    def around(self, tail, lo, hi):
        deps = self.deps.get(tail)
        if not deps:
            return []
        return self.legs[tail][bisect_left(deps, lo):bisect_left(deps, hi)]


def plan_conflict(plan, leg, tail, now):
    """Why `tail`, published on `leg` as of `now`, cannot fly it as planned:
    ('overlap', other leg) when the plan also has it on a flight that overlaps
    (turn included), ('airport', previous leg) when its previous planned or
    flown leg lands elsewhere; None when the plan is consistent (or unknown)."""
    dep = leg.get("dep_utc")
    if plan is None or dep is None or not tail:
        return None
    arr = arr_utc(leg) or dep
    key = leg_key(leg)
    prev = None
    for other in plan.around(tail, dep - PLAN_BACK, arr + PLAN_AHEAD):
        if leg_key(other) == key or tail_as_of(other, now) != tail:
            continue
        o_arr = arr_utc(other) or other["dep_utc"]
        if other["dep_utc"] < arr + TURN and dep < o_arr + TURN:
            return ("overlap", other)
        if o_arr <= dep and (prev is None or other["dep_utc"] > prev["dep_utc"]):
            prev = other
    if prev is not None and prev.get("arr") and leg.get("dep") and prev["arr"] != leg["dep"]:
        return ("airport", prev)
    return None


def conflict_at_band(plan, leg, tail, band):
    """plan_conflict as of `band` hours before departure — memoised on the
    index, since every build over the same index asks it again."""
    k = (leg_key(leg), tail, band)
    if k not in plan.memo:
        plan.memo[k] = plan_conflict(plan, leg, tail,
                                     leg["dep_utc"] - timedelta(hours=band)) is not None
    return plan.memo[k]


def unpublished(leg):
    """`leg` as if nothing were published on it yet — for a flight beyond the
    horizon a page shows tails to, so that every part of the number (the fleet
    simulation reads the plan through tail_as_of) agrees with the card."""
    return dict(leg, timeline=[], first_lead_h=None, latest_tail="")


def stops(leg):
    """The airports a flight of several legs stops at (LH568 FRA-SSG: ['LOS']).
    A leg row is the whole itinerary (dep / arr its ends); `segments`, when
    present, lists each of its legs."""
    segs = leg.get("segments") or ()
    return [s.get("arr") for s in segs[:-1]] if len(segs) > 1 else []


def itinerary(leg):
    """['FRA', 'LOS', 'SSG']: every airport the flight touches, in order."""
    return [leg.get("dep")] + stops(leg) + [leg.get("arr")]


def serves(leg, deps=None, arrs=None):
    """Whether one can fly `leg` from one of `deps` to one of `arrs` (each
    optional): any airport of its itinerary before any later one, so LH568
    FRA-LOS-SSG serves FRA->LOS, LOS->SSG and FRA->SSG."""
    route = itinerary(leg)
    starts = [i for i, a in enumerate(route[:-1]) if not deps or a in deps]
    ends = [j for j, a in enumerate(route) if j and (not arrs or a in arrs)]
    return bool(starts) and bool(ends) and min(starts) < max(ends)


def route_key(leg):
    return "%s-%s" % (leg.get("dep") or "?", leg.get("arr") or "?")


class Stats:
    """Everything the model reads, aggregated from settled legs."""

    def __init__(self, now, weeks):
        self.now = now
        self.weeks = weeks
        self.hold = defaultdict(lambda: [0, 0])      # (kind, key, band) -> [held, n]
        self.inside = defaultdict(lambda: [0, 0])    # (kind, key, band) -> [held one band
                                                     #   in, n] over the legs seen at band
        self.flight = defaultdict(Counter)           # flight_number -> truth tails
        self.route = defaultdict(Counter)            # "FRA-HND" -> truth tails
        self.type = defaultdict(Counter)             # fleet type -> truth tails
        self.fit = {}                                # fleet type -> fitted share knobs
        self.fleet = defaultdict(set)                # fleet type -> tails seen flying it
        self.tail_type = {}                          # tail -> its (modal) fleet type
        self.last_flown = {}                         # tail -> latest operated dep_utc
        self.n_active = Counter()                    # fleet type -> tails flown within ACTIVE_DAYS
        self.n_legs = 0
        self.plan = None                             # PlanIndex for clash checks (None: none)


def _settled(leg, lo, hi):
    dep = leg.get("dep_utc")
    return (bool(leg.get("truth_tail")) and dep is not None and lo <= dep < hi
            and not leg.get("cancelled"))


def build_stats(legs, now, weeks=HISTORY_WEEKS, fit=True, plan=None, holds=True):
    """Aggregate the settled legs that departed in [now - weeks, now), and (with
    `fit`) choose each fleet type's share shrinkage on a temporal holdout.

    `plan` is the PlanIndex clash checks read — pass one built over history
    *and* upcoming legs (the dashboard) or over everything (the benchmark, which
    shares one across builds); default: one over `legs`. `holds=False` skips the
    hold cells (the share fit needs none)."""
    legs = legs if isinstance(legs, list) else list(legs)
    st = Stats(now, weeks)
    if holds:
        st.plan = plan if plan is not None else PlanIndex(legs)
    cutoff = now - timedelta(weeks=weeks)
    tail_types = defaultdict(Counter)
    for leg in legs:
        if not _settled(leg, cutoff, now):
            continue
        truth, dep = leg["truth_tail"], leg["dep_utc"]
        ftype = leg.get("fleet_type") or "?"
        route = route_key(leg)
        st.flight[leg["flight_number"]][truth] += 1
        st.route[route][truth] += 1
        st.type[ftype][truth] += 1
        st.fleet[ftype].add(truth)
        tail_types[truth][ftype] += 1
        if dep > st.last_flown.get(truth, dep - timedelta(seconds=1)):
            st.last_flown[truth] = dep
        if holds:
            _count_holds(st, leg, truth, route, ftype)
        st.n_legs += 1
    if holds and HOLD_CHAIN:
        chain_holds(st)
    st.tail_type = {t: c.most_common(1)[0][0] for t, c in tail_types.items()}
    for ftype, tails in st.fleet.items():
        st.n_active[ftype] = sum(1 for t in tails if _is_active(st, t))
    if fit:
        st.fit = fit_knobs(legs, now, weeks)
    return st


def _count_holds(st, leg, truth, route, ftype):
    """One settled leg into the hold cells.

    ("all", type, band)      every publication — what /insights shows
    (route|type|overall, ., band)
                             publications consistent with the tail's plan —
                             what the model quotes for a consistent one
    ("far:<kind>", ., FAR_BAND_H), ("clash:<kind>", ., group)
                             pooled cells; a leg counts once in each — the mean
                             of its bands there — so a leg looked at five times
                             far out is not five legs' worth of evidence

    Alongside, st.inside counts the band one in, as its own cells would, over
    only the legs seen at the band (the far cells: the band just inside them):
    what chain_holds reads."""
    timeline, first = leg.get("timeline") or [], leg.get("first_lead_h")
    contexts = (("route", route), ("type", ftype), ("overall", ""))
    far, clash, seen = [], defaultdict(list), {}
    for b in BANDS_H:
        pub = tail_at(timeline, first, b)
        if pub is None:
            continue
        held = pub == truth
        cell = st.hold[("all", ftype, b)]
        cell[0] += held
        cell[1] += 1
        consistent = not (st.plan is not None and conflict_at_band(st.plan, leg, pub, b))
        seen[b] = (held, consistent)
        if not consistent:
            clash[clash_group(b)].append(held)
            continue
        for kind, key in contexts:
            cell = st.hold[(kind, key, b)]
            cell[0] += held
            cell[1] += 1
        if b >= FAR_BAND_H:
            far.append(held)
    for b, nearer in zip(BANDS_H, BANDS_H[1:]):
        if b in seen:  # then the nearer band too: a look counts at every band inside it
            held_n, ok_n = seen[nearer]
            for kind, key in [("all", ftype)] + (list(contexts) if ok_n else []):
                _add_inside(st, (kind, key, b), held_n)
    pooled = [("far", FAR_BAND_H, far)] if far else []
    pooled += [("clash", g, vals) for g, vals in clash.items()]
    inside = seen.get(_FAR_INSIDE)
    if FAR_BAND_H in seen and inside[1]:
        for kind, key in contexts:
            _add_inside(st, ("far:" + kind, key, FAR_BAND_H), inside[0])
    for cell_kind, g, vals in pooled:
        frac = sum(vals) / len(vals)
        for kind, key in contexts:
            if cell_kind == "clash" and kind == "route":
                continue  # far too thin per route
            cell = st.hold[(cell_kind + ":" + kind, key, g)]
            cell[0] += frac
            cell[1] += 1


def _add_inside(st, k, held):
    c = st.inside[k]
    c[0] += held
    c[1] += 1


_CONTEXTS = ("route", "type", "overall")


def chain_holds(st):
    """Correct the band cells for which legs were seen that far out
    (HOLD_CHAIN). From the nearest band out, a band's rate moves by the gap,
    one band in, between all the legs there (as corrected) and the legs seen
    at this band: rate - (inside - nearer). Where every leg one band in was
    seen at this band too (inside four days, nearly always) the gap is nil;
    where the collector has only lately looked that far out, the recent legs
    tell the band's step and all of them its level. The far cells chain onto
    the band just inside them; the clash cells, pooled over groups of bands
    already, stay as counted. Each cell keeps its n."""
    chained = {}
    for kind, key in {(k, key) for k, key, _b in list(st.hold) if k in ("all",) + _CONTEXTS}:
        p = None
        for b in reversed(BANDS_H):
            cell = st.hold.get((kind, key, b))
            if not cell or not cell[1]:
                p = None
                continue
            p = corrected_rate(cell, st.inside.get((kind, key, b)), p)
            cell[0] = p * cell[1]
            chained[(kind, key, b)] = p
    for (k, key, g), cell in list(st.hold.items()):
        if k.startswith("far:") and cell[1]:
            p = corrected_rate(cell, st.inside.get((k, key, g)), chained.get((k[4:], key, _FAR_INSIDE)))
            cell[0] = p * cell[1]


def corrected_rate(cell, inside, nearer):
    """A band's hold rate corrected for which legs were seen that far out.
    `cell` [held, n] over the legs seen at the band, `inside` [held, n] the
    band one in over those same legs, `nearer` that band's corrected rate over
    all of its legs. As counted when there is no nearer rate or no leg to
    compare."""
    p = cell[0] / cell[1]
    if nearer is None or not inside or not inside[1]:
        return p
    return _clip(p - (inside[0] / inside[1] - nearer))


def _clip(p):
    return min(max(p, 0.0), 1.0)


def _pick(grid, cell, n, order):
    """Best grid point, or the most conservative one within FIT_TOLERANCE of it."""
    floor = max(cell) - FIT_TOLERANCE * n
    ok = [i for i in range(len(grid)) if cell[i] >= floor]
    return grid[max(ok, key=lambda i: order(grid[i]) + (cell[i],))]


def fit_knobs(legs, now, weeks=HISTORY_WEEKS):
    """{fleet type: knobs} — the grid point that best predicts the last
    VALIDATION_DAYS from the weeks before them (log-likelihood of the tail that
    flew). Two passes: shrinkage (m, m_type) at the default idle weights, then
    the idle weights at the chosen shrinkage. Types with fewer than
    MIN_VALIDATION_LEGS holdout legs are left out (callers use DEFAULTS)."""
    split = now - timedelta(days=VALIDATION_DAYS)
    train = build_stats(legs, split, weeks, fit=False, holds=False)
    hold = defaultdict(list)
    for leg in legs:
        if _settled(leg, split, now):
            hold[leg.get("fleet_type") or "?"].append(leg)

    def loglik(ftype, knobs):
        return sum(math.log(max(share(train, leg["truth_tail"], leg["flight_number"],
                                      route_key(leg), ftype, knobs,
                                      (leg["dep_utc"] - split).total_seconds() / 86400.0)[0],
                                1e-6))
                   for leg in hold[ftype])

    fitted = {}
    for ftype, hlegs in hold.items():
        if len(hlegs) < MIN_VALIDATION_LEGS:
            continue
        grid = [dict(DEFAULTS, m=m, m_type=mt) for m in M_GRID for mt in M_TYPE_GRID]
        best = _pick(grid, [loglik(ftype, k) for k in grid], len(hlegs),
                     lambda k: (k["m"], k["m_type"]))
        grid = [dict(best, idle_near=a, idle_far=b) for a in IDLE_W_GRID for b in IDLE_W_GRID]
        fitted[ftype] = _pick(grid, [loglik(ftype, k) for k in grid], len(hlegs),
                              lambda k: (-abs(k["idle_near"] - DEFAULTS["idle_near"]),
                                         -abs(k["idle_far"] - DEFAULTS["idle_far"])))
    return fitted


def hold_rate(st, band, route, ftype, m=HOLD_M, conflict=False):
    """(p, n, kind): the route's hold rate at `band`, shrunk toward the type's,
    which is shrunk toward the overall rate. `kind`/`n` name the most specific
    context that has data. None when no leg at all covers this band.

    From FAR_BAND_H out the pooled far cells answer (type pulled only
    FAR_TYPE_M toward the cross-type rate). With `conflict` the clash cells of
    the band's group answer (type -> overall), until a clash has been seen."""
    if conflict:
        g = clash_group(band)
        held_o, n_o = st.hold.get(("clash:overall", "", g), (0, 0))
        if n_o:
            p, kind, n = held_o / n_o, "overall", n_o
            held, cnt = st.hold.get(("clash:type", ftype, g), (0, 0))
            p = (held + m * p) / (cnt + m)
            if cnt:
                kind, n = "type", cnt
            return p, int(round(n)), kind
    if band >= FAR_BAND_H and st.hold.get(("far:overall", "", FAR_BAND_H), (0, 0))[1]:
        prefix, cell_band, type_m = "far:", FAR_BAND_H, FAR_TYPE_M
    else:
        prefix, cell_band, type_m = "", band, m
    held_o, n_o = st.hold.get((prefix + "overall", "", cell_band), (0, 0))
    if not n_o:
        return None
    p = held_o / n_o
    kind, n = "overall", n_o
    for k, key, mk in (("type", ftype, type_m), ("route", route, m)):
        held, cnt = st.hold.get((prefix + k, key, cell_band), (0, 0))
        p = (held + mk * p) / (cnt + mk)
        if cnt:
            kind, n = k, cnt
    return p, int(round(n)), kind


# how specific a hold cell is, for naming what an interpolated rate rests on
_KIND_RANK = {"overall": 0, "type": 1, "route": 2}


def hold_at(st, lead_h, route, ftype, m=HOLD_M, conflict=False):
    """hold_rate at the lead a flight stands at: between two bands, the two
    cells' rates interpolated in the lead (HOLD_INTERPOLATE). `n` and `kind`
    are those of the weaker of the two cells. None when neither has data."""
    near, far, w = bands_around(lead_h)
    a = hold_rate(st, near, route, ftype, m, conflict)
    if w <= 0.0 or not HOLD_INTERPOLATE:
        return a
    b = hold_rate(st, far, route, ftype, m, conflict)
    if a is None or b is None:
        return b if a is None else a
    _, n, kind = min(a, b, key=lambda c: (_KIND_RANK[c[2]], c[1]))
    return (1.0 - w) * a[0] + w * b[0], n, kind


def _is_active(st, tail):
    last = st.last_flown.get(tail)
    return last is not None and (st.now - last) <= timedelta(days=ACTIVE_DAYS)


def knobs_for(st, ftype, knobs=None):
    return knobs or st.fit.get(ftype) or DEFAULTS


def type_share(st, tail, ftype, knobs=None, lead_days=0.0):
    """The tail's share of a fleet type's legs: its recent share, shrunk toward
    an even split over the fleet in which idle tails count an idle weight (near
    or far by the flight's lead). A tail never seen on the type keeps a small
    floor (cross-type substitutions happen)."""
    k = knobs_for(st, ftype, knobs)
    fleet = st.fleet.get(ftype) or ()
    n_fleet = len(fleet)
    if tail in fleet:
        n_act = st.n_active[ftype]
        iw = k["idle_near"] if lead_days < NEAR_DAYS else k["idle_far"]
        w = 1.0 if _is_active(st, tail) else iw
        even = w / (n_act + iw * (n_fleet - n_act))
    else:
        even = 0.1 / (n_fleet + 1)
    t = st.type.get(ftype) or Counter()
    return (t[tail] + k["m_type"] * even) / (sum(t.values()) + k["m_type"] + 1e-9)


def share(st, tail, flight_number, route, ftype, knobs=None, lead_days=0.0):
    """The tail's shrunk share of a flight: flight number -> route -> type, with
    the type's fitted knobs unless given. Returns (share, n_flight_legs,
    k_flown_by_tail)."""
    k = knobs_for(st, ftype, knobs)
    m = k["m"]
    s_type = type_share(st, tail, ftype, k, lead_days)
    r = st.route.get(route) or Counter()
    s_route = (r[tail] + m * s_type) / (sum(r.values()) + m)
    f = st.flight.get(flight_number) or Counter()
    nf = sum(f.values())
    return (f[tail] + m * s_route) / (nf + m), nf, f[tail]


def idle_days(st, tail):
    last = st.last_flown.get(tail)
    return None if last is None else (st.now - last).total_seconds() / 86400.0


def p_target(st, target, leg, published=None, now=None, knobs=None, hold_m=HOLD_M,
             swap_mix=SWAP_MIX):
    """P(`target` operates `leg`), with the evidence it rests on.

    `published` is the tail on the flight as of `now` (None beyond the feed's
    horizon or before our first look). Returns a dict:
        p, regime ('published' | 'swap' | 'history' | 'cancelled' | 'departed'),
        basis ('flight' | 'fleet': whether this fleet's flight history is used),
        lead_h, share / share_n / share_k (flight-number history),
        hold / hold_n / hold_kind / band (published regimes), idle_days, why.
    """
    now = now or st.now
    lead_h = (leg["dep_utc"] - now).total_seconds() / 3600.0
    route = route_key(leg)
    ftype = leg.get("fleet_type") or "?"
    lead_d = lead_h / 24.0
    s, nf, kf = share(st, target, leg["flight_number"], route, ftype, knobs, lead_d)
    out = {"p": None, "basis": None, "lead_h": round(lead_h, 1), "share": s,
           "share_n": nf, "share_k": kf,
           "hold": None, "hold_n": None, "hold_kind": None, "band": None,
           "conflict": None, "idle_days": idle_days(st, target)}
    flight = "LH%s" % leg["flight_number"]

    if leg.get("cancelled"):
        out.update(p=0.0, regime="cancelled", why="%s is cancelled." % flight)
        return out
    if lead_h <= 0:
        out.update(p=1.0 if published == target else 0.0, regime="departed",
                   why="%s has departed." % flight)
        return out

    # What the history part of the number rests on. On a fleet whose flights
    # carry no signal (fitted m -> large) the flight's own record is not used,
    # so say so rather than quote a count the number ignores.
    k = knobs_for(st, ftype, knobs)
    n_active = st.n_active[ftype]
    if k["m"] >= NO_SIGNAL_M:
        out["basis"] = "fleet"
        hist_why = "even share of the %d tails in service on this fleet" % n_active
    else:
        out["basis"] = "flight"
        hist_why = ("%s flew %d of the last %d %s" % (target, kf, nf, flight) if nf
                    else "no settled %s legs in the last %d weeks, so the route and "
                         "fleet decide" % (flight, st.weeks))
    idle = out["idle_days"]
    idle_why = (" %s has not flown for %d days (maintenance?)." % (target, idle)
                if idle is not None and idle > ACTIVE_DAYS else "")

    clash = plan_conflict(st.plan, leg, published, now) if published else None
    h = (hold_at(st, lead_h, route, ftype, hold_m, conflict=clash is not None)
         if published else None)
    if published and h is not None:
        hp, hn, kind = h
        out.update(hold=hp, hold_n=hn, hold_kind=kind, band=band_for(lead_h),
                   conflict=_clash_why(published, leg, clash))
        ctx = {"route": "this route", "type": "this type", "overall": "all flights"}[kind]
        if clash:
            ctx = "flights whose tail clashes like this on " + ctx
        if published == target:
            out.update(p=hp, regime="published",
                       why="%s is published%s; %dh out, the published tail holds %d%% of the "
                           "time on %s (n=%d)." % (target, out["conflict"] or "", round(lead_h),
                                                    round(hp * 100), ctx, hn))
        else:
            # who replaces it: the target's share among the tails that are not
            # the published one — of this flight, mixed with of the whole type
            s_pub = share(st, published, leg["flight_number"], route, ftype, knobs, lead_d)[0]
            q_flight = s / (1.0 - s_pub) if s_pub < 1.0 else 0.0
            t_pub = type_share(st, published, ftype, knobs, lead_d)
            q_type = (type_share(st, target, ftype, knobs, lead_d) / (1.0 - t_pub)
                      if t_pub < 1.0 else 0.0)
            q = min(swap_mix * q_flight + (1.0 - swap_mix) * q_type, 1.0)
            out.update(p=(1.0 - hp) * q, regime="swap",
                       why="%s is published%s; %dh out it changes %d%% of the time on %s "
                           "(n=%d); the replacement: %s.%s"
                           % (published, out["conflict"] or "", round(lead_h),
                              round((1 - hp) * 100), ctx, hn, hist_why, idle_why))
        return out

    out.update(p=s, regime="history", why="Not published yet; %s.%s" % (hist_why, idle_why))
    return out


def _clash_why(tail, leg, clash):
    """', but …' clause naming the clash, or None."""
    if not clash:
        return None
    kind, other = clash
    if kind == "overlap":
        return (", but the plan also has %s on LH%s %s-%s at the same time"
                % (tail, other["flight_number"], other.get("dep") or "?", other.get("arr") or "?"))
    return (", but %s's previous flight (LH%s) lands in %s, not %s"
            % (tail, other["flight_number"], other.get("arr") or "?", leg.get("dep") or "?"))


def _shift_segments(segs, shift):
    """A flight's legs moved by `shift` (their local times are ISO strings)."""
    if not segs:
        return segs
    out = []
    for s in segs:
        s = dict(s)
        for k in ("dep_local", "arr_local"):
            if s.get(k):
                s[k] = (datetime.fromisoformat(s[k]) + shift).isoformat()
        out.append(s)
    return out


# --- timetable beyond the published window ---------------------------------
# FIS only knows flights a few days out, so the planner projects the rest: a
# flight number that operated (on the fleet type) on the same weekday in at
# least PROJECT_MIN_WEEKS of the last PROJECT_WEEKS is assumed to operate again,
# at the time it last did. What the feed already lists ahead counts as runs
# too, and its latest word decides: a flight it lists next week is projected
# from that listing, one it was asked for and does not have is not projected.
# A timetable guess all the same — where it reaches across a change of
# timetable (season_start) with nothing from the new one to go on, the
# projection says so (`timetable_change`).
PROJECT_WEEKS = 4
PROJECT_MIN_WEEKS = 2


def _last_sunday(year, month):
    d = date(year, month, 31)               # March and October have 31 days
    return d - timedelta(days=(d.weekday() + 1) % 7)


def season_start(d):
    """First day of the timetable season `d` lies in. Airlines change
    timetables with the clocks: summer from the last Sunday of March, winter
    from the last Sunday of October — routes, days of operation and the fleet
    on a flight number all move then."""
    summer, winter = _last_sunday(d.year, 3), _last_sunday(d.year, 10)
    if d >= winter:
        return winter
    return summer if d >= summer else _last_sunday(d.year - 1, 10)


def project_schedule(legs, dates, now, ftype=None, deps=None, arrs=None, ahead=(), absent=()):
    """Projected legs on `dates` (list of date), optionally one fleet type and
    dep/arr airport sets. Each is a leg dict shaped like the input, dates
    shifted, nothing published, with projected=True — timed like the flight's
    latest same-weekday run, with the layout that flight number is usually sold
    with (a single run may have been published without First).

    `legs` is the history; `ahead` the flights the feed lists from today on,
    which count as runs like the flown ones (a date the feed lists is not
    projected: it is there). `absent` holds the (flight_date, flight_number)
    the feed was asked for and answered "no such flight": such a day is not
    projected, nor a later one while that answer is the feed's latest word on
    the flight's weekday. A projection whose latest run lies before the start
    of the season its date lies in carries that start as `timetable_change`
    (else None): it is the old timetable's flight."""
    since = now - timedelta(weeks=PROJECT_WEEKS)
    runs = defaultdict(dict)  # (flight_number, weekday) -> {flight_date: leg}
    layouts = defaultdict(Counter)  # flight_number -> seat_config counts
    for leg, listed in [(l, False) for l in legs] + [(l, True) for l in ahead]:
        dep = leg.get("dep_utc")
        if leg.get("cancelled") or dep is None or dep < since or not (listed or dep < now):
            continue
        if ftype and leg.get("fleet_type") != ftype:
            continue
        if (deps or arrs) and not serves(leg, deps, arrs):
            continue
        runs[(leg["flight_number"], leg["flight_date"].weekday())][leg["flight_date"]] = leg
        if leg.get("seat_config"):
            layouts[leg["flight_number"]][leg["seat_config"]] += 1
    absent = set(absent)
    said_no = {}  # (flight_number, weekday) -> latest date the feed had no such flight
    for d, fnum in absent:
        k = (fnum, d.weekday())
        said_no[k] = max(said_no.get(k, d), d)
    out = []
    for d in dates:
        for (fnum, weekday), by_date in runs.items():
            if weekday != d.weekday() or len(by_date) < PROJECT_MIN_WEEKS or d in by_date:
                continue
            latest = max(by_date)
            if (d, fnum) in absent or said_no.get((fnum, weekday), latest) > latest:
                continue
            ref = by_date[latest]
            shift = d - ref["flight_date"]
            change = season_start(d)
            out.append(dict(ref, flight_date=d,
                            dep_local=ref["dep_local"] + shift if ref.get("dep_local") else None,
                            arr_local=ref["arr_local"] + shift if ref.get("arr_local") else None,
                            segments=_shift_segments(ref.get("segments"), shift),
                            dep_utc=ref["dep_utc"] + shift, truth_tail="", latest_tail="",
                            latest_status="", timeline=[], first_lead_h=None, projected=True,
                            timetable_change=change if change > latest else None,
                            seat_config=(layouts[fnum].most_common(1)[0][0] if layouts[fnum]
                                         else ref.get("seat_config"))))
    return out
