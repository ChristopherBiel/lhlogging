"""
Offline shadow run of the reconciliation pass (docs/reconciliation.md, R0).

Read-only: segments every aircraft's pulled position track with hindsight
(lhlogging.reconciler), matches the resulting legs against the provisional
rows in the flights export, and reports the actions an apply-mode reconciler
WOULD take (the 6-case table from the design doc). Nothing touches the DB.

Inputs (fresh pulls + the audit presplit cache):
    tools/pull_data.sh && tools/pull_positions.sh
    PYTHONPATH=tools python3 tools/audit_edge_cases.py presplit

Usage:
    PYTHONPATH=tools python3 tools/reconcile_shadow.py            # full fleet
    PYTHONPATH=tools python3 tools/reconcile_shadow.py --aircraft D-ABYJ
    PYTHONPATH=tools python3 tools/reconcile_shadow.py --fis      # + FIS check

Scoring window: legs/rows whose whole [first_seen, last_seen] lies within
[positions_start + edge_margin, positions_end - lag]. The margin keeps
coverage-cut legs (dep honestly unknown because the track begins mid-flight)
from being scored as corrections; the lag keeps the live edge with the
online detector, as in production.

Emits tmp/reconcile_shadow_actions.csv and a console summary.
"""
import argparse
import collections
import csv
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
from lhlogging.reconciler import ReconcilerConfig, reconcile_track  # noqa: E402

import _lhdata as L  # noqa: E402
from _airports import Airports  # noqa: E402
from detector_replay import _parse_ts, _parse_bool, _parse_float  # noqa: E402

TMP = Path(__file__).resolve().parent.parent / "tmp"
CACHE = TMP / "audit_cache" / "positions_by_icao"


def load_track(icao24):
    path = CACHE / f"{icao24}.csv"
    if not path.exists():
        return []
    out = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            out.append({
                "icao24": icao24, "callsign": r["callsign"],
                "captured_at": _parse_ts(r["captured_at"]),
                "latitude": _parse_float(r["latitude"]),
                "longitude": _parse_float(r["longitude"]),
                "altitude_m": _parse_float(r["altitude_m"]),
                "velocity_ms": _parse_float(r["velocity_ms"]),
                "on_ground": _parse_bool(r["on_ground"]),
            })
    out.sort(key=lambda p: p["captured_at"])
    return out


def norm(ap):
    return L.norm_ap(ap)


def overlap_s(a0, a1, b0, b1):
    return max(0.0, (min(a1, b1) - max(a0, b0)).total_seconds())


def in_window(t0, t1, w0, w1):
    return t0 is not None and t1 is not None and t0 >= w0 and t1 <= w1


def classify_aircraft(rows, legs, track, w0, w1):
    """Match provisional rows vs reconciled legs by time overlap -> actions."""
    prov = []
    for r in rows:
        t0, t1 = _parse_ts(r["first_seen"]), _parse_ts(r["last_seen"])
        if in_window(t0, t1, w0, w1) and (r["arrival_airport_icao"] or "").strip():
            prov.append({"row": r, "t0": t0, "t1": t1})
    recs = [l for l in legs
            if in_window(l.first_seen, l.last_seen, w0, w1)
            and l.arrival_airport_icao is not None]

    times = [p["captured_at"] for p in track]

    def n_fixes_between(t0, t1):
        import bisect
        return bisect.bisect_right(times, t1) - bisect.bisect_left(times, t0)

    # overlap matrix
    p_match = collections.defaultdict(list)   # prov idx -> [(secs, rec idx)]
    r_match = collections.defaultdict(list)
    for pi, p in enumerate(prov):
        for ri, l in enumerate(recs):
            o = overlap_s(p["t0"], p["t1"], l.first_seen, l.last_seen)
            if o > 0:
                p_match[pi].append((o, ri))
                r_match[ri].append((o, pi))

    actions = []
    claimed_r = set()
    for pi, p in enumerate(prov):
        r = p["row"]
        cands = sorted(p_match.get(pi, []), reverse=True)
        if not cands:
            has_data = n_fixes_between(p["t0"], p["t1"]) >= 1
            actions.append(("DELETE_PHANTOM" if has_data else "SKIP_NO_DATA",
                            p, None, ""))
            continue
        best_o, best_ri = cands[0]
        # is this prov the best for that rec too?
        rc = sorted(r_match.get(best_ri, []), reverse=True)
        if rc and rc[0][1] != pi:
            actions.append(("MERGE_FRAGMENT", p, recs[best_ri],
                            "absorbed by the max-overlap row"))
            continue
        claimed_r.add(best_ri)
        l = recs[best_ri]
        pd, pa = norm(r["departure_airport_icao"]), norm(r["arrival_airport_icao"])
        rd = norm(l.departure_airport_icao)
        ra = norm(l.arrival_airport_icao)
        diffs = []
        if pd != rd:
            diffs.append(f"dep {pd or '?'}->{rd or '?'}")
        if pa != ra:
            diffs.append(f"arr {pa or '?'}->{ra or '?'}")
        if diffs:
            actions.append(("CORRECT", p, l, ", ".join(diffs)))
        else:
            actions.append(("CONFIRM", p, l, ""))
    for ri, l in enumerate(recs):
        if ri not in claimed_r and not r_match.get(ri):
            actions.append(("INSERT_MISSED", None, l, ""))
    return actions, prov, recs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lag-hours", type=float, default=6.0)
    ap.add_argument("--edge-margin-hours", type=float, default=12.0)
    ap.add_argument("--aircraft", help="registration or icao24: debug one tail, "
                                       "print legs side by side")
    ap.add_argument("--fis", action="store_true",
                    help="cross-check reconciled legs against the FIS export")
    args = ap.parse_args()

    airports = Airports.load()
    rows = L.load()
    by_icao = collections.defaultdict(list)
    reg_of = {}
    for r in rows:
        icao = r["icao24"].strip()
        by_icao[icao].append(r)
        reg_of[icao] = r["registration"].strip()

    cfg = ReconcilerConfig()
    want = None
    if args.aircraft:
        want = {i for i, reg in reg_of.items()
                if reg == args.aircraft or i == args.aircraft.lower()}
        if not want:
            raise SystemExit(f"unknown aircraft {args.aircraft!r}")

    # dataset clock: the newest position across the pull
    t_end = None
    t_start = None
    stats = collections.Counter()
    act_rows = []
    rec_metrics = collections.Counter()
    prov_metrics = collections.Counter()
    legs_by_reg = collections.defaultdict(list)

    icaos = sorted(want) if want else sorted(p.stem for p in CACHE.glob("*.csv"))
    for icao in icaos:
        track = load_track(icao)
        if len(track) < 2:
            continue
        if t_end is None or track[-1]["captured_at"] > t_end:
            t_end = track[-1]["captured_at"]
        if t_start is None or track[0]["captured_at"] < t_start:
            t_start = track[0]["captured_at"]

    if t_end is None:
        raise SystemExit(f"no cached tracks under {CACHE} — run "
                         "'PYTHONPATH=tools python3 tools/audit_edge_cases.py presplit'")
    w0 = t_start + timedelta(hours=args.edge_margin_hours)
    w1 = t_end - timedelta(hours=args.lag_hours)
    window_days = (w1 - w0).total_seconds() / 86400
    print(f"scoring window: {w0.isoformat()[:16]} .. {w1.isoformat()[:16]} "
          f"({window_days:.1f} d), {len(icaos)} aircraft")

    for icao in icaos:
        track = load_track(icao)
        if len(track) < 2:
            continue
        legs = reconcile_track(track, airports.nearest, cfg)
        for l in legs:
            legs_by_reg[reg_of.get(icao, icao)].append(l)
        actions, prov, recs = classify_aircraft(by_icao.get(icao, []), legs, track, w0, w1)
        for l in recs:
            rec_metrics["legs"] += 1
            rd, ra = norm(l.departure_airport_icao), norm(l.arrival_airport_icao)
            if rd and rd == ra:
                rec_metrics["self_loop"] += 1
            if l.arrival_airport_icao == "UNKN":
                rec_metrics["arr_unkn"] += 1
            if l.departure_airport_icao is None:
                rec_metrics["dep_unknown"] += 1
            if l.needs_review:
                rec_metrics["review"] += 1
            rec_metrics[f"dep_src_{l.dep_source}"] += 1
            rec_metrics[f"arr_src_{l.arr_source}"] += 1
        for p in prov:
            r = p["row"]
            prov_metrics["rows"] += 1
            pd, pa = norm(r["departure_airport_icao"]), norm(r["arrival_airport_icao"])
            if pd and pd == pa:
                prov_metrics["self_loop"] += 1
            if (r["arrival_airport_icao"] or "").strip() == "UNKN":
                prov_metrics["arr_unkn"] += 1
            if not pd:
                prov_metrics["dep_unknown"] += 1
            if r["needs_review"] == "t":
                prov_metrics["review"] += 1
        for kind, p, l, note in actions:
            stats[kind] += 1
            act_rows.append({
                "icao24": icao, "registration": reg_of.get(icao, ""),
                "action": kind, "note": note,
                "p_callsign": (p["row"]["callsign"] or "").strip() if p else "",
                "p_dep": (p["row"]["departure_airport_icao"] or "").strip() if p else "",
                "p_arr": (p["row"]["arrival_airport_icao"] or "").strip() if p else "",
                "p_first": p["row"]["first_seen"] if p else "",
                "p_last": p["row"]["last_seen"] if p else "",
                "p_review": p["row"]["needs_review"] if p else "",
                "r_callsign": l.callsign or "" if l else "",
                "r_dep": (l.departure_airport_icao or "") if l else "",
                "r_arr": (l.arrival_airport_icao or "") if l else "",
                "r_dep_src": (l.dep_source or "") if l else "",
                "r_arr_src": (l.arr_source or "") if l else "",
                "r_first": l.first_seen.isoformat() if l else "",
                "r_last": l.last_seen.isoformat() if l else "",
                "r_review": ("t" if l.needs_review else "f") if l else "",
            })

    out = TMP / "reconcile_shadow_actions.csv"
    if act_rows and not args.aircraft:      # single-tail debug must not clobber
        with open(out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(act_rows[0].keys()))
            w.writeheader()
            w.writerows(act_rows)

    print(f"\nreconciled-leg metrics (settled window, {window_days:.1f} d):")
    for k in ("legs", "self_loop", "arr_unkn", "dep_unknown", "review"):
        n = rec_metrics.get(k, 0)
        print(f"    {k:12s}: {n:5d}  ({n / window_days:.1f}/day)")
    print("    provenance:", {k.replace('_src_', ':'): v for k, v in
                              sorted(rec_metrics.items()) if "_src_" in k})
    print(f"\nprovisional rows, same window:")
    for k in ("rows", "self_loop", "arr_unkn", "dep_unknown", "review"):
        n = prov_metrics.get(k, 0)
        print(f"    {k:12s}: {n:5d}  ({n / window_days:.1f}/day)")
    print("\nwould-be actions:")
    for k, n in stats.most_common():
        print(f"    {k:15s}: {n:5d}  ({n / window_days:.1f}/day)")
    print(f"\n  -> {out} ({len(act_rows)} rows)")

    if args.aircraft:
        for icao in icaos:
            reg = reg_of.get(icao, icao)
            print(f"\n--- {reg} provisional vs reconciled ---")
            for r in by_icao.get(icao, []):
                t0 = _parse_ts(r["first_seen"])
                if t0 and t_start and t0 >= t_start:
                    print(f"  P {(r['callsign'] or ''):9s}"
                          f"{(r['departure_airport_icao'] or '?'):5s}->"
                          f"{(r['arrival_airport_icao'] or 'open'):5s} "
                          f"{r['first_seen'][:16]} .. {r['last_seen'][:16]} rev={r['needs_review']}")
            for l in legs_by_reg.get(reg, []):
                print(f"  R {(l.callsign or ''):9s}"
                      f"{(l.departure_airport_icao or '?'):5s}->"
                      f"{(l.arrival_airport_icao or 'open'):5s} "
                      f"{l.first_seen.isoformat()[:16]} .. {l.last_seen.isoformat()[:16]} "
                      f"[{l.dep_source or '-'}/{l.arr_source or '-'}] "
                      f"rev={'t' if l.needs_review else 'f'} {l.flags}")

    if args.fis:
        fis_crosscheck(legs_by_reg, w0, w1)
    return 0


def fis_crosscheck(legs_by_reg, w0, w1):
    """Score reconciled legs against the FIS oracle (same matching as
    audit_edge_cases.cmd_fis, applied to the would-be table)."""
    from audit_edge_cases import IATA_ICAO
    latest = {}
    with open(TMP / "fis_export.csv", newline="") as f:
        for r in csv.DictReader(f):
            if r["found"] != "t" or not r["registration"]:
                continue
            key = (r["flight_date"], r["flight_number"])
            if key not in latest or r["observed_date"] >= latest[key]["observed_date"]:
                latest[key] = r
    results = collections.Counter()
    misses = []
    for (fdate, fnum), r in sorted(latest.items()):
        if not (w0.strftime("%Y-%m-%d") <= fdate <= w1.strftime("%Y-%m-%d")):
            continue
        dep_i, arr_i = IATA_ICAO.get(r["dep_airport_iata"]), IATA_ICAO.get(r["arr_airport_iata"])
        if not dep_i or not arr_i:
            continue
        reg = r["registration"].strip()
        cands = [l for l in legs_by_reg.get(reg, [])
                 if abs((l.first_seen.date().toordinal()
                         - __import__("datetime").datetime.fromisoformat(fdate).toordinal())) <= 1]
        hit = None
        for l in cands:
            if (norm(l.departure_airport_icao) == dep_i
                    and norm(l.arrival_airport_icao) == arr_i):
                hit = "MATCH"
                break
        if not hit:
            hit = "PARTIAL_OR_MISSING" if cands else "NO_LEGS"
            if len(misses) < 25:
                got = "; ".join(f"{l.departure_airport_icao or '?'}->"
                                f"{l.arrival_airport_icao or 'open'}" for l in cands[:4])
                misses.append(f"    {fdate} LH{fnum:>4s} {reg:7s} want {dep_i}->{arr_i}  got {got or '-'}")
        results[hit] += 1
    total = sum(results.values())
    print(f"\nFIS cross-check of the reconciled table ({total} checkable):")
    for k, n in results.most_common():
        print(f"    {k:18s}: {n:4d}  ({n / total:.0%})")
    print("\n".join(misses))


if __name__ == "__main__":
    raise SystemExit(main())
