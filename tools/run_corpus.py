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

    print(f"Config: onground_max_speed={args.onground_max_speed} "
          f"landing_min_consecutive={args.landing_min_consecutive}\n")
    passed = 0
    tp = fp = fn = 0
    for path in files:
        fx = json.load(open(path))
        positions = load_fixture_positions(fx)
        legs = D.replay_aircraft(positions, airports, cfg)
        key = lambda p: (str(p[0]), str(p[1]))
        got = sorted({canon_pair(l.departure_airport_icao, l.arrival_airport_icao) for l in legs}, key=key)
        exp = sorted({canon_pair(d, a) for d, a in fx["expected_legs"]}, key=key)
        ok = got == exp
        passed += ok
        # leg-level P/R (multiset by canonical pair)
        exp_set, got_set = set(exp), set(got)
        tp += len(exp_set & got_set)
        fn += len(exp_set - got_set)
        fp += len(got_set - exp_set)
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {fx['id']:30s} ({fx['category']})")
        if not ok or args.verbose:
            print(f"         expected: {exp}")
            print(f"         got:      {got}")

    prec = tp / (tp + fp) if (tp + fp) else 1.0
    rec = tp / (tp + fn) if (tp + fn) else 1.0
    print(f"\n  fixtures: {passed}/{len(files)} passed")
    print(f"  legs: precision={prec:.2f} recall={rec:.2f}  (TP={tp} FP={fp} FN={fn})")
    return 0 if passed == len(files) else 1


if __name__ == "__main__":
    raise SystemExit(main())
