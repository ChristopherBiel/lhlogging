"""
Descriptive statistics over the leg layer (fis_legs) — the shared arithmetic
behind /insights, the /airframe page and the /book network map.

Pure and stdlib-only (like booking_model.py), so it is unit-tested without a
database. A leg is the dict dashboard/app.py builds from a fis_legs row:
flight_date, flight_number, dep, arr, fleet_type, dep_utc, duration_min,
truth_tail ('' until operated), cancelled, timeline [[lead_h, tail], ...],
seat_config (the layout the flight is SOLD with — First can be sold as
Business on some routes, so it is per flight, not per airframe), segments
(the legs of a flight of several: LH568 FRA-LOS-SSG is one row, dep FRA, arr SSG).
"""
from __future__ import annotations

import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from booking_model import BANDS_H, HISTORY_WEEKS, TRUTH_LAG, arr_utc, itinerary, tail_at

BERLIN = ZoneInfo("Europe/Berlin")
# German hubs: a network line is drawn and labelled hub -> outstation.
DE_HUBS = {"FRA", "MUC", "DUS", "BER", "HAM", "STR", "CGN", "NUE", "LEJ", "TXL"}
# A cabin configuration seen for less than this is a feed glitch, not a refit.
GLITCH_SPAN = timedelta(hours=6)

# /insights reads the same weeks as the booking model, so its figures explain
# the numbers /book quotes. The fleet calendar looks further back: a heavy
# check outlasts eight weeks.
WINDOW_WEEKS = HISTORY_WEEKS
CALENDAR_DAYS = 17 * 7
# Fewer legs than this behind a rate: drawn hollow, and no claim rests on it.
MIN_N = 30
# This long without a recorded flight, a tail is out of the rotation (99% of
# hub turns are under 54 hours).
LONG_STOP = timedelta(days=3)
# A hub stop up to this long is a turnaround: the tail takes one of the next
# day's flights at the latest.
TURN_MAX = timedelta(hours=36)
# The lead times /insights compares routes at, in hours before departure.
ROUTE_LEADS = (24, 48, 72)
# What counts as news on /insights: the last fortnight, measured against the
# four weeks before it; a tail flying again is news after a stop this long.
RECENT = timedelta(days=14)
BEFORE = timedelta(days=28)
NEWS_STOP = timedelta(days=5)


def flown(legs):
    """Legs that operated (truth known, not cancelled), oldest first."""
    return sorted((l for l in legs if l.get("truth_tail") and not l.get("cancelled")
                   and l.get("dep_utc") is not None), key=lambda l: l["dep_utc"])


def pair_key(dep, arr):
    """Undirected route key, oriented hub -> outstation (else alphabetical)."""
    if dep in DE_HUBS and arr not in DE_HUBS:
        return dep, arr
    if arr in DE_HUBS and dep not in DE_HUBS:
        return arr, dep
    return tuple(sorted((dep, arr)))


def sells_first(seat_config):
    """True/False from a FIS layout string ('F8C80...' -> True), None if unknown."""
    return None if not seat_config else "F" in seat_config


def network(legs, tail_first=None):
    """Routes flown, both directions merged: [{a, b, n, types{type:n},
    tails{reg:n}, first{reg:n}}] where `first` counts the legs sold with First.
    A leg whose layout is unknown falls back to its tail's physical cabin
    (`tail_first`: reg -> bool)."""
    tail_first = tail_first or {}
    by = {}
    for l in flown(legs):
        route = itinerary(l)
        f = sells_first(l.get("seat_config"))
        sold_first = f if f is not None else tail_first.get(l["truth_tail"])
        # a flight of several legs draws each of them (FRA-LOS, LOS-SSG)
        for dep, arr in zip(route, route[1:]):
            if not dep or not arr or dep == arr:
                continue
            a, b = pair_key(dep, arr)
            e = by.setdefault((a, b), {"a": a, "b": b, "n": 0, "types": Counter(),
                                       "tails": Counter(), "first": Counter()})
            e["n"] += 1
            e["types"][l.get("fleet_type") or "?"] += 1
            e["tails"][l["truth_tail"]] += 1
            if sold_first:
                e["first"][l["truth_tail"]] += 1
    out = [dict(e, types=dict(e["types"]), tails=dict(e["tails"]), first=dict(e["first"]))
           for e in by.values()]
    out.sort(key=lambda e: -e["n"])
    return out


def first_not_sold(legs, ftype, tail_first):
    """Routes where tails of `ftype` that HAVE a First cabin (`tail_first`)
    were published without it: [{route, dep, arr, legs, without}] — e.g. a
    747-8 on LH754 to Bengaluru, a route without First, is published C88E32M244.
    Legs of unknown layout skip."""
    n, without = Counter(), Counter()
    for l in flown(legs):
        if l.get("fleet_type") != ftype or not tail_first.get(l["truth_tail"]):
            continue
        f = sells_first(l.get("seat_config"))
        if f is None:
            continue
        key = (l["dep"], l["arr"])
        n[key] += 1
        if not f:
            without[key] += 1
    return [{"route": "%s-%s" % k, "dep": k[0], "arr": k[1], "legs": n[k], "without": w}
            for k, w in sorted(without.items(), key=lambda kv: (-kv[1] / n[kv[0]], kv[0]))]


def route_shares(legs, reg):
    """For every route the tail flew: its legs k, all operated legs n on that
    route, and its share k/n. Sorted by k."""
    all_n, mine = Counter(), Counter()
    for l in flown(legs):
        key = (l["dep"], l["arr"])
        all_n[key] += 1
        if l["truth_tail"] == reg:
            mine[key] += 1
    return [{"route": "%s-%s" % key, "dep": key[0], "arr": key[1], "k": k, "n": all_n[key],
             "share": round(k / all_n[key], 3)} for key, k in mine.most_common()]


def cabin_spans(rows):
    """airframe_cabin rows (seat_config, allegris, sub_type, first_seen, last_seen)
    for one tail -> [{..., glitch, concurrent}] oldest first. A span under
    GLITCH_SPAN is a one-off feed glitch. A span that overlaps another real one
    is the same cabin sold differently on some flights (no First on a route without it),
    not a refit; only real spans that follow each other mean the cabin changed."""
    spans = sorted(rows, key=lambda r: r[3])
    real = [r for r in spans if (r[4] - r[3]) >= GLITCH_SPAN]
    out = []
    for span in spans:
        seat, alleg, sub, first, last = span
        glitch = (last - first) < GLITCH_SPAN
        concurrent = not glitch and any(o[:3] != span[:3] and o[3] < last and first < o[4]
                                        for o in real)
        out.append({"seat_config": seat, "allegris": bool(alleg), "sub_type": sub,
                    "first_seen": first.isoformat(), "last_seen": last.isoformat(),
                    "glitch": glitch, "concurrent": concurrent})
    return out


def cabin_variants(latest, ftype):
    """Group one fleet's tails by identical cabin (seat layout, Allegris, FIS
    sub-type). `latest`: reg -> (fleet_type, seat_config, allegris, sub_type).
    Returns [{label, seat_config, allegris, sub_type, tails[]}], biggest first,
    labelled A, B, C..."""
    groups = defaultdict(list)
    for reg, (t, seat, alleg, sub) in latest.items():
        if t == ftype:
            groups[(seat, bool(alleg), sub)].append(reg)
    ordered = sorted(groups.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    return [{"label": chr(ord("A") + i), "seat_config": k[0], "allegris": k[1],
             "sub_type": k[2], "tails": sorted(v)} for i, (k, v) in enumerate(ordered)]


# --- /insights: the fleet field guide --------------------------------------
# Six questions, each answered from the same legs: when a published tail can
# be trusted (hold_curve, settling), which flights are the safer bet
# (route_hold), what a plane does next (hub_turns), who flies where
# (tail_routes), whether a tail is flying at all (calendar) and what changed
# lately (events). insights() assembles them into the /api/insights payload.

def wilson(k, n, z=1.96):
    """95% Wilson score interval (lo, hi) for k of n."""
    if not n:
        return 0.0, 1.0
    p = k / n
    d = 1 + z * z / n
    mid = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, mid - half), min(1.0, mid + half)


def _rate(k, n):
    lo, hi = wilson(k, n)
    return {"k": k, "n": n, "p": round(k / n, 4) if n else None,
            "lo": round(lo, 4), "hi": round(hi, 4)}


def _published(leg):
    """{band: the tail published that far out} for the bands we looked at."""
    tl, first = leg.get("timeline") or [], leg.get("first_lead_h")
    out = {}
    for b in BANDS_H:
        pub = tail_at(tl, first, b)
        if pub is not None:
            out[b] = pub
    return out


def hold_curve(legs):
    """How often the tail published `band` hours out is the one that flew:
    [{band, k, n, p, lo, hi}], farthest band first. The booking model's
    ("all", type, band) cells, with their interval."""
    cells = {b: [0, 0] for b in BANDS_H}
    for l in flown(legs):
        for b, pub in _published(l).items():
            cells[b][0] += pub == l["truth_tail"]
            cells[b][1] += 1
    return [dict(_rate(k, n), band=b) for b, (k, n) in cells.items() if n]


def settling(legs, min_n=MIN_N):
    """What waiting buys: between neighbouring bands, on the legs published at
    both, the share whose tail became the one that flew, and that gain per
    hour waited. [{far, near, n, gain, per_hour}], farthest first."""
    pairs = list(zip(BANDS_H, BANDS_H[1:]))
    cells = {pr: [0, 0, 0] for pr in pairs}
    for l in flown(legs):
        pub = _published(l)
        for far, near in pairs:
            if far in pub and near in pub:
                c = cells[(far, near)]
                c[0] += pub[far] == l["truth_tail"]
                c[1] += pub[near] == l["truth_tail"]
                c[2] += 1
    return [{"far": far, "near": near, "n": n, "gain": round((k1 - k0) / n, 4),
             "per_hour": round((k1 - k0) / n / (far - near), 5)}
            for (far, near), (k0, k1, n) in cells.items() if n >= min_n]


def crossing(curve, target, min_n=MIN_N):
    """Hours before departure from which the hold rate stays at `target` or
    above: read off the curve from departure outward, interpolated on log lead
    between the two bands around it. None when even the nearest band is below
    the target, or no band is."""
    pts = sorted((c for c in curve if c["n"] >= min_n), key=lambda c: c["band"])
    if not pts or pts[0]["p"] < target:
        return None
    for near, far in zip(pts, pts[1:]):
        if far["p"] < target:
            f = (near["p"] - target) / (near["p"] - far["p"])
            lo, hi = math.log(near["band"]), math.log(far["band"])
            return round(math.exp(lo + f * (hi - lo)), 1)
    return None


def _by_tail(legs):
    per = defaultdict(list)
    for l in flown(legs):
        per[l["truth_tail"]].append(l)
    return per


def _stops(tail_legs, horizon, long_stop=LONG_STOP):
    """A tail's stretches of `long_stop` or more without a flight, from its
    operated legs (oldest first): [(from, to, airport it was last at)]. The
    last one is open (to = None) when the tail has not flown again by
    `horizon`."""
    out = []
    for a, c in zip(tail_legs, tail_legs[1:]):
        landed = arr_utc(a) or a["dep_utc"]
        if c["dep_utc"] - landed >= long_stop:
            out.append((landed, c["dep_utc"], a.get("arr")))
    if tail_legs:
        last = tail_legs[-1]
        landed = arr_utc(last) or last["dep_utc"]
        if horizon - landed >= long_stop:
            out.append((landed, None, last.get("arr")))
    return out


def calendar(legs, now, days=CALENDAR_DAYS):
    """The fleet day by day over the last `days` UTC days (fewer while the
    data is younger): per tail the block hours flown each day and its long
    stops, per day the tails that flew. Days from `complete` on are still
    waiting for their truth pass. A stop is a stretch without a recorded
    flight — the tail is usually in the hangar, but a leg FIS never listed
    looks the same.

    {start, days, complete, stop_days, flying[], tails[{reg, type, hours[],
    legs, per_day, first, stops[{from, to, days, at, open}]}]} — stop and
    first-seen positions are day offsets from `start` (fractions allowed)."""
    per = _by_tail(legs)
    since = min((ls[0]["dep_utc"] for ls in per.values()), default=now)
    end = now.astimezone(timezone.utc).date()
    start = max(end - timedelta(days=days - 1), since.astimezone(timezone.utc).date())
    days = (end - start).days + 1
    t0 = datetime.combine(start, time.min, tzinfo=timezone.utc)
    horizon = now - TRUTH_LAG
    complete = max(0, min(days, (horizon.astimezone(timezone.utc).date() - start).days + 1))
    off = lambda t: round((t - t0).total_seconds() / 86400.0, 2)
    flying = [0] * days
    tails = []
    for reg in sorted(per):
        ls = per[reg]
        hours = [0.0] * days
        n = 0
        for l in ls:
            cur, stop = l["dep_utc"].astimezone(timezone.utc), arr_utc(l)
            stop = stop.astimezone(timezone.utc) if stop else cur
            n += cur >= t0
            while cur < stop:
                midnight = datetime.combine(cur.date() + timedelta(days=1), time.min,
                                            tzinfo=timezone.utc)
                nxt = min(stop, midnight)
                i = (cur.date() - start).days
                if 0 <= i < days:
                    hours[i] += (nxt - cur).total_seconds() / 3600.0
                cur = nxt
        if not n and ls[-1]["dep_utc"] < t0 - timedelta(days=CALENDAR_DAYS):
            continue  # gone for longer than the calendar shows: no longer a row
        for i, h in enumerate(hours):
            flying[i] += h > 0
        first = ls[0]["dep_utc"]
        stops = [{"from": off(max(a, t0)), "to": off(b) if b else None,
                  "days": round(((b or horizon) - a).total_seconds() / 86400.0, 1),
                  "at": at, "open": b is None}
                 for a, b, at in _stops(ls, horizon) if (b or now) > t0]
        tails.append({
            "reg": reg, "type": Counter(l.get("fleet_type") for l in ls).most_common(1)[0][0],
            "hours": [round(h, 1) for h in hours], "legs": n,
            "per_day": round(sum(hours[:complete]) / complete, 2) if complete else None,
            # first seen inside the window, with data from before it to say so
            "first": off(first) if first > t0 and first - since >= LONG_STOP else None,
            "stops": stops,
        })
    return {"start": start.isoformat(), "days": days, "complete": complete,
            "stop_days": LONG_STOP.days, "flying": flying, "tails": tails}


def _clock(t):
    """Minute of the Berlin day, to five minutes (timetable times)."""
    t = t.astimezone(BERLIN)
    return (t.hour * 60 + t.minute) // 5 * 5


def _choices(pairs):
    """(before, after): how many equally likely next flights a hub turn is
    worth without and with knowing the inbound flight — the perplexity of the
    outbound, each turn scored on the others (leave-one-out, one pseudo-turn
    of the coarser guess), so a sparse pairing can't flatter it."""
    n_out = Counter(o for _, o in pairs)
    n_in = Counter(i for i, _ in pairs)
    n_pair = Counter(pairs)
    total, kinds = len(pairs), len(n_out)
    if total < 2:
        return None
    s0 = s1 = 0.0
    for i, o in pairs:
        p0 = (n_out[o] - 1 + 1.0 / kinds) / total
        p1 = (n_pair[(i, o)] - 1 + p0) / n_in[i]
        s0 -= math.log(p0)
        s1 -= math.log(p1)
    return round(math.exp(s0 / total), 1), round(math.exp(s1 / total), 1)


def hub_turns(legs, hubs=DE_HUBS, turn_max=TURN_MAX, min_node=5, min_turns=MIN_N):
    """What a tail flies next from a hub: every turnaround (arrival, next
    departure of the same tail, at most `turn_max` apart) as a link between
    the inbound and the outbound flight.

    [{hub, n, long, rare, choices[before, after], inbound[{flight, apt, t, n}],
    outbound[...], links[{i, o, d, n, turn}]}] per hub with enough turns,
    busiest first. t = usual Berlin clock time in minutes, d = days between
    arrival and departure date, turn = median ground minutes. Flights seen in
    fewer than `min_node` turns are left out and counted in `rare`. A hub whose
    departures all go to one airport (the 787's Munich end of the FRA-MUC
    shuttle) has no links: there is nothing to choose between."""
    turns = defaultdict(list)
    long_stops = Counter()
    for ls in _by_tail(legs).values():
        for a, c in zip(ls, ls[1:]):
            hub, landed = a.get("arr"), arr_utc(a)
            if hub not in hubs or c.get("dep") != hub or landed is None:
                continue
            ground = c["dep_utc"] - landed
            if ground < timedelta(0):
                continue
            if ground > turn_max:
                long_stops[hub] += 1
                continue
            day = (c["dep_utc"].astimezone(BERLIN).date() - landed.astimezone(BERLIN).date()).days
            turns[hub].append(((a["flight_number"], a.get("dep")), (c["flight_number"], c.get("arr")),
                               day, ground.total_seconds() / 60.0, _clock(landed),
                               _clock(c["dep_utc"])))
    out = []
    for hub, ts in turns.items():
        if len(ts) < min_turns:
            continue
        n_in, n_out = Counter(t[0] for t in ts), Counter(t[1] for t in ts)
        t_in, t_out = defaultdict(Counter), defaultdict(Counter)
        for i, o, _d, _g, ci, co in ts:
            t_in[i][ci] += 1
            t_out[o][co] += 1

        def nodes(count, clocks):
            keep = sorted((k for k, n in count.items() if n >= min_node),
                          key=lambda k: (clocks[k].most_common(1)[0][0], k))
            return keep, [{"flight": k[0], "apt": k[1], "n": count[k],
                           "t": clocks[k].most_common(1)[0][0]} for k in keep]

        ins, inbound = nodes(n_in, t_in)
        outs, outbound = nodes(n_out, t_out)
        if len({k[1] for k in n_out}) < 2:
            out.append({"hub": hub, "n": len(ts), "long": long_stops[hub], "rare": 0,
                        "choices": None, "only": next(iter(n_out))[1],
                        "inbound": [], "outbound": [], "links": []})
            continue
        ii, oo = {k: x for x, k in enumerate(ins)}, {k: x for x, k in enumerate(outs)}
        grounds = defaultdict(list)
        for i, o, d, g, _ci, _co in ts:
            if i in ii and o in oo:
                grounds[(ii[i], oo[o], d)].append(g)
        links = [{"i": i, "o": o, "d": d, "n": len(g), "turn": int(statistics.median(g))}
                 for (i, o, d), g in sorted(grounds.items())]
        shown = sum(l["n"] for l in links)
        out.append({"hub": hub, "n": len(ts), "long": long_stops[hub], "rare": len(ts) - shown,
                    "choices": _choices([(t[0], t[1]) for t in ts]),
                    "inbound": inbound, "outbound": outbound, "links": links})
    out.sort(key=lambda h: -h["n"])
    return out


def _direction(leg):
    """(route pair hub-first, 'out' | 'back') for a leg, None for a loop."""
    dep, arr = leg.get("dep"), leg.get("arr")
    if not dep or not arr or dep == arr:
        return None
    pair = pair_key(dep, arr)
    return pair, "out" if dep == pair[0] else "back"


def route_hold(legs, leads=ROUTE_LEADS):
    """Hold rate per route and direction at a few lead times, with the fleet's
    rate per direction to compare against.

    {leads, all{out{lead: rate}, back{...}}, routes[{a, b, via[], out{...},
    back{...}}]} — a = hub end, 'out' = flights leaving it; lead keys are
    strings (JSON)."""
    cells = defaultdict(lambda: [0, 0])
    via = {}
    for l in flown(legs):
        key = _direction(l)
        if key is None:
            continue
        pair, way = key
        stops = itinerary(l)[1:-1]
        via.setdefault(pair, stops if way == "out" else stops[::-1])
        pub = _published(l)
        for lead in leads:
            if lead in pub:
                held = pub[lead] == l["truth_tail"]
                for k in ((pair, way, lead), (None, way, lead)):
                    cells[k][0] += held
                    cells[k][1] += 1

    def side(pair, way):
        return {str(lead): _rate(*cells[(pair, way, lead)]) for lead in leads
                if cells.get((pair, way, lead), (0, 0))[1]}

    routes = [{"a": pair[0], "b": pair[1], "via": via[pair],
               "out": side(pair, "out"), "back": side(pair, "back")} for pair in sorted(via)]
    return {"leads": list(leads), "all": {"out": side(None, "out"), "back": side(None, "back")},
            "routes": [r for r in routes if r["out"] or r["back"]]}


def _seriate(rows, cols, cell, row_group, col_group, rounds=8):
    """Order both sides of a count matrix so tails that share routes sit
    together: each side in turn is sorted, inside its group, by the mean
    position of its partners (the barycentre heuristic). Starts from the given
    orders, so the result is deterministic."""
    by_row, by_col = defaultdict(dict), defaultdict(dict)
    for (r, c), n in cell.items():
        by_row[r][c] = n
        by_col[c][r] = n

    def centre(partners, pos):
        total = sum(partners.values())
        return sum(pos[k] * n for k, n in partners.items()) / total if total else 0.0

    for _ in range(rounds):
        pos = {r: i for i, r in enumerate(rows)}
        cols = sorted(cols, key=lambda c: (col_group[c], centre(by_col[c], pos), c))
        pos = {c: i for i, c in enumerate(cols)}
        rows = sorted(rows, key=lambda r: (row_group[r], centre(by_row[r], pos), r))
    return rows, cols


def _effective(counts):
    """How many equally used options a distribution is worth (exp of its entropy)."""
    total = sum(counts)
    return round(math.exp(-sum(n / total * math.log(n / total) for n in counts if n)), 1)


def tail_routes(legs, min_route=3):
    """Who flies where: operated legs per tail and route (both directions
    merged), tails grouped by cabin, both sides ordered so shared routes line
    up. A route flown fewer than `min_route` times (a diversion, a one-off
    substitution) is left out.

    {fleet, groups[{seat_config, allegris, tails}], tails[{reg, group, n,
    routes, eff}], routes[{a, b, via[], n, tails, eff, first, allegris}],
    cells[[tail index, route index, legs]]} — eff = effective number of routes
    (tails) the legs spread over; first / allegris = share of the route's legs
    sold with First / flown by an Allegris cabin."""
    cell = Counter()
    via, layouts, alleg = {}, defaultdict(Counter), defaultdict(Counter)
    sold = defaultdict(lambda: [0, 0, 0])            # pair -> [with First, layout known, Allegris]
    for l in flown(legs):
        key = _direction(l)
        if key is None:
            continue
        pair, way = key
        reg = l["truth_tail"]
        stops = itinerary(l)[1:-1]
        via.setdefault(pair, stops if way == "out" else stops[::-1])
        cell[(reg, pair)] += 1
        if l.get("seat_config"):
            layouts[reg][l["seat_config"]] += 1
            sold[pair][0] += "F" in l["seat_config"]
            sold[pair][1] += 1
        alleg[reg][bool(l.get("allegris"))] += 1
        sold[pair][2] += bool(l.get("allegris"))
    flights = Counter()
    for (_, p), n in cell.items():
        flights[p] += n
    cell = Counter({k: n for k, n in cell.items() if flights[k[1]] >= min_route})
    if not cell:
        return {"fleet": 0, "groups": [], "tails": [], "routes": [], "cells": []}
    regs = sorted({r for r, _ in cell})
    pairs = sorted({p for _, p in cell})
    # a tail's cabin: the layout it is most often published with
    cabin = {r: (layouts[r].most_common(1)[0][0] if layouts[r] else None,
                 alleg[r].most_common(1)[0][0]) for r in regs}
    sizes = Counter(cabin.values())
    kinds = sorted(sizes, key=lambda k: (-sizes[k], str(k)))
    hub_legs = Counter()
    for (_, p), n in cell.items():
        hub_legs[p[0]] += n
    hubs = sorted(hub_legs, key=lambda h: (-hub_legs[h], h))
    regs, pairs = _seriate(regs, pairs, cell, {r: kinds.index(cabin[r]) for r in regs},
                           {p: hubs.index(p[0]) for p in pairs})
    ri, pi = {r: i for i, r in enumerate(regs)}, {p: i for i, p in enumerate(pairs)}
    row, col = defaultdict(list), defaultdict(list)
    for (r, p), n in cell.items():
        row[r].append(n)
        col[p].append(n)
    return {
        "fleet": len(regs),
        "groups": [{"seat_config": k[0], "allegris": k[1], "tails": sizes[k]} for k in kinds],
        "tails": [{"reg": r, "group": kinds.index(cabin[r]), "n": sum(row[r]),
                   "routes": len(row[r]), "eff": _effective(row[r])} for r in regs],
        "routes": [{"a": p[0], "b": p[1], "via": via[p], "n": sum(col[p]), "tails": len(col[p]),
                    "eff": _effective(col[p]),
                    "first": round(sold[p][0] / sold[p][1], 3) if sold[p][1] else None,
                    "allegris": round(sold[p][2] / sum(col[p]), 3)} for p in pairs],
        "cells": sorted([ri[r], pi[p], n] for (r, p), n in cell.items()),
    }


def _day(t):
    return "%d %s" % (t.day, t.strftime("%b"))


def _flight(leg):
    return "LH%s %s" % (leg["flight_number"], "–".join(a or "?" for a in itinerary(leg)))


def events(legs, now, recent=RECENT, before=BEFORE):
    """What changed lately: {now[], recent[], days} of {kind, date, reg,
    text}. `now` lists the tails without a recorded flight for LONG_STOP or
    more, longest first; `recent` what the last fortnight brought, newest
    first — tails flying again after NEWS_STOP or seen for the first time,
    routes that appeared or went quiet, diversions and cancellations. `text`
    reads on after the tail (when there is one)."""
    horizon = now - TRUTH_LAG
    cut = now - recent
    per = _by_tail(legs)
    done = flown(legs)
    since = done[0]["dep_utc"] if done else now
    state, news = [], []

    def add(to, kind, when, reg, text):
        to.append({"kind": kind, "date": when.date().isoformat(), "reg": reg, "text": text})

    for reg, ls in per.items():
        for a, b, at in _stops(ls, horizon):
            days = ((b or horizon) - a).days
            where = ", last at %s" % at if at else ""
            if b is None:
                add(state, "grounded", a, reg, "no flight since %s (%d days%s)"
                    % (_day(a), days, where))
            elif b >= cut and b - a >= NEWS_STOP:
                nxt = next(l for l in ls if l["dep_utc"] == b)
                add(news, "returned", b, reg, "flying again after %d days without a flight%s: %s"
                    % (days, where, _flight(nxt)))
        first = ls[0]
        if first["dep_utc"] >= cut and first["dep_utc"] - since >= before:
            add(news, "new_tail", first["dep_utc"], reg, "seen for the first time, on %s"
                % _flight(first))

    old, new, last = Counter(), defaultdict(list), {}
    for l in done:
        key = _direction(l)
        if key is None:
            continue
        pair = key[0]
        if l["dep_utc"] >= cut:
            new[pair].append(l)
        else:
            if l["dep_utc"] >= cut - before:
                old[pair] += 1
            last[pair] = l
    if cut - since >= before:
        for pair, ls in new.items():
            if pair not in last:
                add(news, "new_route", ls[0]["dep_utc"], None,
                    "%s–%s is new for this fleet: %d flight%s since %s, first %s with %s"
                    % (pair[0], pair[1], len(ls), "" if len(ls) == 1 else "s",
                       _day(ls[0]["dep_utc"]), "LH%s" % ls[0]["flight_number"],
                       ls[0]["truth_tail"]))
        for pair, n in old.items():
            # was flown at least every other day, and the gap is no truth lag
            if n >= before.days / 2 and pair not in new and horizon - last[pair]["dep_utc"] >= LONG_STOP:
                add(news, "gone_route", last[pair]["dep_utc"], None,
                    "%s–%s: no flight since %s (%d in the four weeks before)"
                    % (pair[0], pair[1], _day(last[pair]["dep_utc"]), n))

    cancelled = []
    for l in legs:
        dep = l.get("dep_utc")
        if dep is None or not (cut <= dep < now):
            continue
        if l.get("cancelled"):
            cancelled.append(l)
        elif (l.get("latest_status") or "").upper() == "DIVERTED":
            add(news, "diverted", dep, l.get("truth_tail") or None,
                "%s%s on %s" % ("diverted on " if l.get("truth_tail") else "Diverted: ",
                                _flight(l), _day(dep)))
    if len(cancelled) > 3:
        newest = max(l["dep_utc"] for l in cancelled)
        add(news, "cancelled", newest, None, "%d flights cancelled since %s, the latest %s on %s"
            % (len(cancelled), _day(cut), _flight(max(cancelled, key=lambda l: l["dep_utc"])),
               _day(newest)))
    else:
        for l in cancelled:
            add(news, "cancelled", l["dep_utc"], None,
                "%s on %s was cancelled" % (_flight(l), _day(l["dep_utc"])))
    seen = set()
    news = [e for e in news
            if not (tuple(e.values()) in seen or seen.add(tuple(e.values())))]
    state.sort(key=lambda e: (e["date"], e["reg"]))
    news.sort(key=lambda e: (e["date"], e["reg"] or ""), reverse=True)
    return {"now": state, "recent": news, "days": recent.days}


def insights(legs, hist, families, family, now, weeks=WINDOW_WEEKS):
    """The /api/insights payload for one fleet family.

    `legs`: that family's legs, CALENDAR_DAYS back or more (the calendar and
    the news read all of them). `hist`: every fleet's legs over the last
    `weeks` — the trust figure compares the fleets. `families`: {name: member
    fleet types}, in display order."""
    cut = now - timedelta(weeks=weeks)
    inside = lambda l: l.get("dep_utc") is not None and cut <= l["dep_utc"] < now
    fleets = []
    for name, members in families.items():
        fl = [l for l in hist if l.get("fleet_type") in members and inside(l)]
        curve = hold_curve(fl)
        fleets.append({"id": name, "tails": len({l["truth_tail"] for l in flown(fl)}),
                       "curve": curve, "t50": crossing(curve, 0.5), "t80": crossing(curve, 0.8),
                       "settling": settling(fl) if name == family else None})
    win = [l for l in legs if inside(l)]
    done = flown(win)
    return {
        "family": family, "members": list(families[family]),
        "meta": {"weeks": weeks, "from": cut.date().isoformat(), "to": now.date().isoformat(),
                 "flights": len(done), "tails": len({l["truth_tail"] for l in done})},
        "trust": {"fleets": fleets, "min_n": MIN_N},
        "routes": route_hold(win),
        "turns": {"hubs": hub_turns(win), "max_h": int(TURN_MAX.total_seconds() // 3600)},
        "spread": tail_routes(win),
        "calendar": calendar(legs, now),
        "events": events(legs, now),
        "generated": now.isoformat(),
    }
