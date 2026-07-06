"""
Run the committed regression corpus (tools/corpus/*.json) through the detector
replay and score it. Self-contained: uses the embedded position slices and the
pruned tools/corpus/airports.csv — no DB and no 149MB pull required.

Reports, under a given Config:
  - per-fixture PASS/FAIL (detected legs == expected legs, by canonical dep->arr)
  - leg-level precision / recall over all fixtures

This is the regression gate: a detector change must keep every fixture PASSing.
Compare configs to measure a proposed change:

  python3 tools/run_corpus.py                          # current behaviour (baseline)
  python3 tools/run_corpus.py --onground-max-speed 80  # candidate fix

--reconciler drives the fixtures through the HINDSIGHT segmenter
(app/lhlogging/reconciler.py, docs/reconciliation.md R0) instead of the
windowed cron simulation. In that mode a fixture's `expected_legs_reconciler`
key (full physically-true leg set) overrides `expected_legs`, and such
fixtures gate even when marked xfail — xfail describes open bugs of the
ONLINE detector, which the reconciler is required to not have.
"""
import argparse
import glob
import json
from pathlib import Path

import _lhdata as L
import detector_replay as D
from _airports import Airports

CORPUS = Path(__file__).resolve().parent / "corpus"


def load_fixture_positions(fx):
    out = []
    for p in fx["positions"]:
        out.append({
            "icao24": p["icao24"], "callsign": p["callsign"],
            "captured_at": D._parse_ts(p["captured_at"]),
            "latitude": D._parse_float(p["latitude"]),
            "longitude": D._parse_float(p["longitude"]),
            "altitude_m": D._parse_float(p["altitude_m"]),
            "velocity_ms": D._parse_float(p["velocity_ms"]),
            "on_ground": D._parse_bool(p["on_ground"]),
        })
    out.sort(key=lambda p: p["captured_at"])
    return out


def canon_pair(dep, arr):
    return (L.norm_ap(dep), L.norm_ap(arr))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--onground-max-speed", type=float, default=0.0)
    ap.add_argument("--onground-max-altitude", type=float, default=0.0)
    ap.add_argument("--landing-min-consecutive", type=int, default=1)
    ap.add_argument("--missed-departure-snap", action="store_true")
    ap.add_argument("--scan-arrival-max-km", type=float, default=0.0)
    ap.add_argument("--min-turnaround", type=int, default=0)
    ap.add_argument("--reconciler", action="store_true",
                    help="segment with hindsight (lhlogging.reconciler) instead "
                         "of the windowed replay")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    cfg = D.Config(onground_max_speed_ms=args.onground_max_speed,
                   onground_max_altitude_m=args.onground_max_altitude,
                   landing_min_consecutive=args.landing_min_consecutive,
                   missed_departure_snap=args.missed_departure_snap,
                   scan_arrival_max_km=args.scan_arrival_max_km,
                   min_turnaround_min=args.min_turnaround)

    airports = Airports.load(CORPUS / "airports.csv")
    files = sorted(glob.glob(str(CORPUS / "*.json")))
    if not files:
        raise SystemExit("No fixtures — run tools/build_corpus.py first.")

    if args.reconciler:
        from lhlogging.reconciler import reconcile_track  # noqa: E402 (path set by detector_replay)
        print("Mode: RECONCILER (hindsight segmentation)\n")
    else:
        print(f"Config: onground_max_speed={args.onground_max_speed} "
              f"landing_min_consecutive={args.landing_min_consecutive}\n")
    passed = failed = xfailed = xpassed = 0
    tp = fp = fn = 0
    for path in files:
        fx = json.load(open(path))
        has_rec_exp = args.reconciler and "expected_legs_reconciler" in fx
        xfail = bool(fx.get("xfail")) and not has_rec_exp
        positions = load_fixture_positions(fx)
        if args.reconciler:
            legs = reconcile_track(positions, airports.nearest)
        else:
            legs = D.replay_aircraft(positions, airports, cfg)
        key = lambda p: (str(p[0]), str(p[1]))
        got = sorted({canon_pair(l.departure_airport_icao, l.arrival_airport_icao) for l in legs}, key=key)
        exp_legs = fx["expected_legs_reconciler"] if has_rec_exp else fx["expected_legs"]
        exp = sorted({canon_pair(d, a) for d, a in exp_legs}, key=key)
        ok = got == exp
        if xfail:
            # expected legs encode the DESIRED behaviour of a known-open bug;
            # only gate-relevant once it starts passing (then promote it).
            xpassed += ok
            xfailed += not ok
            mark = "XPASS" if ok else "XFAIL"
        else:
            passed += ok
            failed += not ok
            mark = "PASS" if ok else "FAIL"
            # leg-level P/R over the gate fixtures only
            exp_set, got_set = set(exp), set(got)
            tp += len(exp_set & got_set)
            fn += len(exp_set - got_set)
            fp += len(got_set - exp_set)
        print(f"  [{mark}] {fx['id']:30s} ({fx['category']})")
        if (not ok and not xfail) or args.verbose:
            print(f"         expected: {exp}")
            print(f"         got:      {got}")

    prec = tp / (tp + fp) if (tp + fp) else 1.0
    rec = tp / (tp + fn) if (tp + fn) else 1.0
    n_gate = passed + failed
    print(f"\n  fixtures: {passed}/{n_gate} passed"
          + (f"   (+{xfailed} known-open xfail, {xpassed} xpass)" if (xfailed or xpassed) else ""))
    if xpassed:
        print("  NOTE: xpass fixtures now meet their desired behaviour — "
              "remove their xfail flag to add them to the gate.")
    print(f"  legs: precision={prec:.2f} recall={rec:.2f}  (TP={tp} FP={fp} FN={fn})")
    return 0 if passed == n_gate else 1


if __name__ == "__main__":
    raise SystemExit(main())
