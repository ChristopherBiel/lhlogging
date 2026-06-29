"""
Eval harness: replay the detector over the labeled corpus and score how it
handles each case, under a given Config. Lets a proposed change be measured —
does it MERGE the false cruise-snap splits while KEEPING real stops split, and
without harming clean controls?

Ground truth comes from tools/classify_splits.py (physics at the split
boundary) + consensus routes (tools/_lhdata.py). For each labeled candidate we
ask, after replay: is the true flight still SPLIT (phantom endpoint present) or
correctly MERGED (single true_dep->true_arr leg)?

  CRUISE_SNAP  want MERGED   (the split is the bug)
  REAL_STOP    want SPLIT    (it really stopped — keep both legs)

Also reports global leg health on the replayed fleet subset: how many
gap-split pairs remain (by the same consensus heuristic), and clean-leg count
(regression signal for controls).

Usage:
  python3 tools/eval_detector.py                          # baseline (faithful)
  python3 tools/eval_detector.py --onground-max-speed 60  # guard A
  python3 tools/eval_detector.py --landing-min-consecutive 3   # guard B
  python3 tools/eval_detector.py --onground-max-speed 60 --landing-min-consecutive 2
"""
import argparse
import collections
import csv
from datetime import datetime
from pathlib import Path

import _lhdata as L
import detector_replay as D
from _airports import Airports

TMP = Path(__file__).resolve().parent.parent / "tmp"


def parse_ts(s):
    s = s.strip().replace(" ", "T", 1)
    if s.endswith("+00"):
        s = s[:-3] + "+00:00"
    return datetime.fromisoformat(s)


def canon(code):
    return L.norm_ap(code)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--onground-max-speed", type=float, default=0.0)
    ap.add_argument("--onground-max-altitude", type=float, default=0.0)
    ap.add_argument("--landing-min-consecutive", type=int, default=1)
    ap.add_argument("--missed-departure-snap", action="store_true")
    ap.add_argument("--scan-arrival-max-km", type=float, default=0.0)
    ap.add_argument("--min-turnaround", type=int, default=0)
    ap.add_argument("--control-sample", type=int, default=40,
                    help="extra clean aircraft to replay as a regression control")
    args = ap.parse_args()

    cfg = D.Config(
        onground_max_speed_ms=args.onground_max_speed,
        onground_max_altitude_m=args.onground_max_altitude,
        landing_min_consecutive=args.landing_min_consecutive,
        missed_departure_snap=args.missed_departure_snap,
        scan_arrival_max_km=args.scan_arrival_max_km,
        min_turnaround_min=args.min_turnaround,
    )
    tag = (f"onground_max_speed={args.onground_max_speed} consec={args.landing_min_consecutive} "
           f"missed_dep_snap={args.missed_departure_snap} scan_km={args.scan_arrival_max_km}")

    # labeled candidates with retained positions
    cands = [c for c in csv.DictReader(open(TMP / "gap_split_classified.csv"))
             if c["_cat"] not in ("NO_DATA",)]
    involved = sorted({c["icao24"] for c in cands})

    # a control set of clean aircraft (not in the candidate set), for regression
    all_icaos = sorted({r["icao24"].strip() for r in L.load()})
    controls = [i for i in all_icaos if i not in set(involved)][:args.control_sample]
    replay_set = set(involved) | set(controls)

    airports = Airports.load()
    print(f"Loading positions for {len(replay_set)} aircraft "
          f"({len(involved)} candidate-involved + {len(controls)} controls)...")
    pos = D.load_positions(TMP / "positions_export.csv", icao24s=replay_set)

    print(f"Replaying [{tag}] ...")
    legs_by = {icao: D.replay_aircraft(p, airports, cfg) for icao, p in pos.items()}

    # --- score labeled candidates ---
    def overlapping(icao, t0, t1):
        out = []
        for l in legs_by.get(icao, []):
            if l.first_seen <= t1 and l.last_seen >= t0:
                out.append(l)
        return out

    def boundary_physics(icao, t0):
        """Min velocity + #near-stationary samples within ±20min of t0 — tells a
        genuine parked stop (min vel ~0) from a corridor/cruise snap (vel high)."""
        seq = pos.get(icao, [])
        near = [p for p in seq if abs((p["captured_at"] - t0).total_seconds()) <= 1200]
        vels = [p["velocity_ms"] for p in near if p["velocity_ms"] is not None]
        parked = sum(1 for p in near
                     if (p["velocity_ms"] is not None and p["velocity_ms"] < 15)
                     or (p["velocity_ms"] is None and p["on_ground"] is True))
        return (min(vels) if vels else None), parked

    outcome = collections.defaultdict(lambda: collections.Counter())
    misses = []
    for c in cands:
        t0, t1 = parse_ts(c["a_first"]), parse_ts(c["b_last"])
        phantom = canon(c["phantom"])
        tdep, tarr = canon(c["true_dep"]), canon(c["true_arr"])
        legs = overlapping(c["icao24"], t0, t1)
        pairs = {(canon(l.departure_airport_icao), canon(l.arrival_airport_icao)) for l in legs}
        endpoints = {e for p in pairs for e in p}
        phantom_present = phantom in endpoints
        merged = (not phantom_present) and ((tdep, tarr) in pairs)
        if phantom_present:
            res = "SPLIT"
        elif merged:
            res = "MERGED"
        else:
            res = "OTHER"
        outcome[c["_cat"]][res] += 1
        if c["_cat"] == "REAL_STOP" and res != "SPLIT":
            mv, npark = boundary_physics(c["icao24"], parse_ts(c["a_last"]))
            verdict = "GENUINE STOP -> TRUE regression" if (mv is not None and mv < 15) else "snap mislabel (OK to merge)"
            misses.append(f"  real-stop merged: {c['registration']}/{c['callsign']} "
                          f"{tdep}->{phantom}->{tarr}  minVel={mv}  parked={npark}  [{verdict}]")

    print(f"\n=== Candidate outcomes [{tag}] ===")
    print(f"{'category':12s} {'n':>4s}  {'MERGED':>7s} {'SPLIT':>7s} {'OTHER':>7s}   want")
    want = {"CRUISE_SNAP": "MERGED", "REAL_STOP": "SPLIT", "AMBIGUOUS": "?", "GAP_DESCENT": "?"}
    for cat in ["CRUISE_SNAP", "REAL_STOP", "AMBIGUOUS", "GAP_DESCENT"]:
        o = outcome[cat]
        n = sum(o.values())
        if n:
            print(f"{cat:12s} {n:4d}  {o['MERGED']:7d} {o['SPLIT']:7d} {o['OTHER']:7d}   {want[cat]}")
    if misses:
        print("\n".join(misses[:12]))

    # --- global leg health on the replayed subset ---
    total = clean = unkn = self_loop = 0
    phantom_pairs = collections.Counter()
    cmap = L.build_callsign_routes(L.load())
    for icao, legs in legs_by.items():
        legs = sorted(legs, key=lambda l: l.first_seen)
        for l in legs:
            total += 1
            d, a = canon(l.departure_airport_icao), canon(l.arrival_airport_icao)
            if l.arrival_airport_icao == "UNKN" or a is None:
                unkn += 1
            elif d == a:
                self_loop += 1
            elif d and a:
                clean += 1
        # consecutive same-airport pairs not on consensus route = phantom-ish
        for x, y in zip(legs, legs[1:]):
            cs = (x.callsign or "").strip()
            ax, dy = canon(x.arrival_airport_icao), canon(y.departure_airport_icao)
            if cs and ax and dy and ax == dy and cs == (y.callsign or "").strip():
                cons = cmap.get(cs)
                if cons and ax not in cons:
                    phantom_pairs[cs] += 1
    print(f"\n=== Global leg health (replayed subset, {len(legs_by)} aircraft) ===")
    print(f"  total legs: {total}   clean: {clean}   dep==arr: {self_loop}   UNKN/open: {unkn}")
    print(f"  gap-split pairs remaining (consensus heuristic): {sum(phantom_pairs.values())}")


if __name__ == "__main__":
    main()
