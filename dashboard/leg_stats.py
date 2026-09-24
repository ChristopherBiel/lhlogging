"""
Descriptive statistics over the leg layer (fis_legs) — the shared arithmetic
behind /insights, the /airframe page and the /book network map.

Pure and stdlib-only (like booking_model.py), so it is unit-tested without a
database. A leg is the dict dashboard/app.py builds from a fis_legs row:
flight_date, flight_number, dep, arr, fleet_type, dep_utc, duration_min,
truth_tail ('' until operated), cancelled, timeline [[lead_h, tail], ...],
seat_config (the layout the flight is SOLD with — First can be sold as
Business on some routes, so it is per flight, not per airframe).
"""
from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from datetime import timedelta
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")
# German hubs: a network line is drawn and labelled hub -> outstation.
DE_HUBS = {"FRA", "MUC", "DUS", "BER", "HAM", "STR", "CGN", "NUE", "LEJ", "TXL"}
# A cabin configuration seen for less than this is a feed glitch, not a refit.
GLITCH_SPAN = timedelta(hours=6)


def flown(legs):
    """Legs that operated (truth known, not cancelled), oldest first."""
    return sorted((l for l in legs if l.get("truth_tail") and not l.get("cancelled")
                   and l.get("dep_utc") is not None), key=lambda l: l["dep_utc"])


def route_counts(legs, top=25):
    """[{route, dep, arr, n, median_min}] by frequency."""
    by = defaultdict(list)
    for l in flown(legs):
        by[(l["dep"], l["arr"])].append(l.get("duration_min"))
    out = []
    for (d, a), durs in by.items():
        durs = [x for x in durs if x]
        out.append({"route": "%s-%s" % (d, a), "dep": d, "arr": a, "n": len(by[(d, a)]),
                    "median_min": int(statistics.median(durs)) if durs else None})
    out.sort(key=lambda r: (-r["n"], r["route"]))
    return out[:top]


def rotation(legs, top=60):
    """What a tail flies next after each route: [{from, to, n}]."""
    per_tail = defaultdict(list)
    for l in flown(legs):
        per_tail[l["truth_tail"]].append("%s-%s" % (l["dep"], l["arr"]))
    pairs = Counter()
    for seq in per_tail.values():
        for a, b in zip(seq, seq[1:]):
            pairs[(a, b)] += 1
    return [{"from": a, "to": b, "n": n} for (a, b), n in pairs.most_common(top)]


def airframe_profiles(legs):
    """Per tail: legs, block hours, first/last date, modal type, and the longest
    run of days without an operated leg (a maintenance visit shows up here)."""
    per = defaultdict(list)
    for l in flown(legs):
        per[l["truth_tail"]].append(l)
    out = []
    for reg, ls in per.items():
        dates = sorted({l["flight_date"] for l in ls})
        gaps = [(b - a).days for a, b in zip(dates, dates[1:])]
        out.append({
            "reg": reg, "legs": len(ls),
            "hours": round(sum(l.get("duration_min") or 0 for l in ls) / 60.0, 1),
            "first": dates[0].isoformat(), "last": dates[-1].isoformat(),
            "type": Counter(l.get("fleet_type") for l in ls).most_common(1)[0][0],
            "max_ground_days": max(gaps) if gaps else None,
        })
    out.sort(key=lambda a: (-a["legs"], a["reg"]))
    return out


def reschedulings(legs, max_lead_h):
    """Tail changes per day, dated by the look that revealed them (Berlin date),
    counting only changes seen within `max_lead_h` of departure — so widening
    the collection horizon doesn't inflate the series. [{date, n}] ascending."""
    per_day = Counter()
    days = set()
    for l in legs:
        dep = l.get("dep_utc")
        tl = l.get("timeline") or []
        if dep is None or not tl:
            continue
        for i, (lead, _tail) in enumerate(tl):
            seen = (dep - timedelta(hours=lead)).astimezone(BERLIN).date()
            if lead <= max_lead_h:
                days.add(seen)
                if i:
                    per_day[seen] += 1
    return [{"date": d.isoformat(), "n": per_day[d]} for d in sorted(days)]


def status_mix(legs):
    """Latest status per flight: [{status, n}] by frequency."""
    c = Counter((l.get("latest_status") or "UNKNOWN") for l in legs)
    return [{"status": s, "n": n} for s, n in c.most_common()]


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
        if not l.get("dep") or not l.get("arr") or l["dep"] == l["arr"]:
            continue
        a, b = pair_key(l["dep"], l["arr"])
        e = by.setdefault((a, b), {"a": a, "b": b, "n": 0, "types": Counter(),
                                   "tails": Counter(), "first": Counter()})
        e["n"] += 1
        e["types"][l.get("fleet_type") or "?"] += 1
        e["tails"][l["truth_tail"]] += 1
        f = sells_first(l.get("seat_config"))
        if f if f is not None else tail_first.get(l["truth_tail"]):
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
