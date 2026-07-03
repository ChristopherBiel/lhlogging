"""
Build self-contained regression fixtures for the flight detector from the
recorded positions export. Each fixture embeds the position slice for one
flight + the EXPECTED legs (hand-verified against raw physics), so the
regression suite (tools/run_corpus.py) runs without the 149MB pull or a DB.

The curated set spans the failure modes found in the analysis:
  - cruise-snap (the dominant bug): spurious on_ground=true at cruise
  - alt-glitch variant; transatlantic variant
  - a genuine intermediate stop (must STAY split)
  - clean controls (must stay single, correct)

Outputs tools/corpus/*.json + tools/corpus/airports.csv (pruned to airports
within 60km of any fixture position, a safe superset of any <=50km lookup).

Usage:  python3 tools/build_corpus.py
"""
import csv
import json
import math
from datetime import datetime, timedelta
from pathlib import Path

TMP = Path(__file__).resolve().parent.parent / "tmp"
CORPUS = Path(__file__).resolve().parent / "corpus"


def pt(s):
    s = s.strip().replace(" ", "T", 1)
    if s.endswith("+00"):
        s = s[:-3] + "+00:00"
    return datetime.fromisoformat(s)


# (id, icao24, t_start, t_end, category, expected_legs, description[, xfail])
# Times bracket the flight; ±90min buffer is added so the windowed replay sees
# the pre-departure ground samples. Expected legs verified from raw positions.
# xfail=True marks a fixture whose expected legs encode the DESIRED behaviour
# for a known-open bug (see tools/EDGE_CASES.md): run_corpus reports it apart
# and the gate does not fail on it — it starts passing when the fix lands.
CASES = [
    ("cruise_snap_egte", "3c65a8", "2026-06-29 00:23:00+00", "2026-06-29 07:07:00+00",
     "CRUISE_SNAP", [["KBOS", "EDDM"]],
     "D-AIMH DLH425: on_ground=true @256m/s over Exeter snaps to EGTE mid-cruise"),
    ("cruise_snap_eddk", "3c65a1", "2026-06-06 05:07:00+00", "2026-06-06 15:28:00+00",
     "CRUISE_SNAP", [["KSFO", "EDDM"]],
     "D-AIMA DLH459: on_ground=true @253m/s snaps to EDDK; real arrival EDDM at 15:28"),
    ("cruise_snap_etsl_altglitch", "3c666a", "2026-06-26 20:21:00+00", "2026-06-26 22:00:00+00",
     "CRUISE_SNAP", [["EDDM", "LFBO"]],
     "D-AISJ DLH09X: on_ground=true with alt=27036m glitch snaps to ETSL right after EDDM departure"),
    ("cruise_snap_atlantic_cyyy", "3c4a02", "2026-06-22 13:03:00+00", "2026-06-22 20:15:00+00",
     "CRUISE_SNAP", [["EDDF", "CYUL"]],
     "D-ABPB DLH478: 3 consecutive on_ground=true @239m/s over the Atlantic snap to CYYY"),
    ("real_stop_mumbai", "3c65a8", "2026-06-12 11:28:00+00", "2026-06-12 23:27:00+00",
     "REAL_STOP", [["EDDM", "VABB"], ["VABB", "VIDP"]],
     "D-AIMH DLH762: genuine Munich->Mumbai->Delhi; real landing at VABB (vel 4-6, parked 100min). MUST stay split."),
    ("control_eddm_klax", "3c65a1", "2026-06-07 10:39:00+00", "2026-06-07 22:11:00+00",
     "CONTROL", [["EDDM", "KLAX"]],
     "D-AIMA DLH452: clean long-haul, single leg. MUST stay single+correct."),
    ("control_klax_eddm", "3c65a1", "2026-06-08 00:58:00+00", "2026-06-08 11:27:00+00",
     "CONTROL", [["KLAX", "EDDM"]],
     "D-AIMA DLH453: clean long-haul, single leg. MUST stay single+correct."),
    # --- 2026-07 edge-case audit additions (tools/EDGE_CASES.md) ---
    ("diversion_saar_saez", "3c4b33", "2026-06-30 19:30:00+00", "2026-07-01 14:30:00+00",
     "REAL_STOP", [["EDDF", "SAAR"], ["SAAR", "SAEZ"]],
     "D-ABYS DLH510: genuine diversion EDDF->SAAR (Santa Rosa) + hop to SAEZ. MUST stay split."),
    ("diversion_kclt_krdu", "3c6573", "2026-07-01 07:30:00+00", "2026-07-01 21:30:00+00",
     "REAL_STOP", [["EDDF", "KCLT"], ["KCLT", "KRDU"]],
     "D-AIKS DLH408: go-around + diversion stop at KCLT (~1h), continuation to KRDU. MUST stay split."),
    ("c6_callsign_flip_lqsa", "3c65cd", "2026-06-30 07:45:00+00", "2026-06-30 13:30:00+00",
     "C6_FLIP", [["EDDF", "UNKN"], ["UNKN", "EDDF"]],
     "D-AINM DLH3MU->DLH6YX: arrival at LQSA lost, return climbs out already on next callsign; "
     "C6 must keep the honest EDDF->UNKN + ?->EDDF pair (enrichment later mangles it — not the detector's fault). "
     "Pins that the EDDF landing is never lost."),
    ("p3_leftover_klax", "3c4b2a", "2026-07-01 09:00:00+00", "2026-07-02 09:00:00+00",
     "P3_LEFTOVER", [["EDDF", "KLAX"], ["KLAX", "EDDF"]],
     "D-ABYJ DLH456/457: in prod, a frozen-feed tail fix left after the KLAX close re-opened "
     "a phantom KLAX leg via P3 (review=false) and C6 then closed it UNKN. The replay does NOT "
     "reproduce it (needs ingest-lag simulation — see EDGE_CASES.md); kept as a control."),
    ("p3_approach_snap_edma", "3c4a02", "2026-06-21 14:30:00+00", "2026-06-21 17:30:00+00",
     "P3_APPROACH", [["UNKN", "EDDM"]],
     "D-ABPB DLH6EE: first sighting at 2308m FLAT on approach; P3 snaps dep to overflown "
     "EDMA (Augsburg) with review=false. Desired: honest unknown departure.", True),
    ("singleton_altglitch_eddf", "3c65d6", "2026-06-30 05:30:00+00", "2026-06-30 09:30:00+00",
     "SINGLETON", [["EDDF", "ESGG"]],
     "D-AINV DLH3YY: one corrupt 11894m sample while parked 20min before the real EDDF->ESGG "
     "departure fabricates a 2-min EDDF==EDDF 'flight'. Desired: only the real leg.", True),
]


def haversine_km(la1, lo1, la2, lo2):
    R = 6371.0088
    p = math.radians
    a = (math.sin((p(la2) - p(la1)) / 2) ** 2
         + math.cos(p(la1)) * math.cos(p(la2)) * math.sin((p(lo2) - p(lo1)) / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def main():
    CORPUS.mkdir(exist_ok=True)
    want = {c[1] for c in CASES}
    windows = {}
    for cid, icao, ts, te, *_ in CASES:
        windows.setdefault(icao, []).append((pt(ts) - timedelta(minutes=90),
                                             pt(te) + timedelta(minutes=90)))

    # stream positions once, collect rows for involved aircraft in any window
    rows_by = {icao: [] for icao in want}
    with open(TMP / "positions_export.csv", newline="") as f:
        for r in csv.DictReader(f):
            icao = r["icao24"].strip()
            if icao not in want:
                continue
            t = pt(r["captured_at"])
            if any(lo <= t <= hi for lo, hi in windows[icao]):
                rows_by[icao].append(r)
    for icao in rows_by:
        rows_by[icao].sort(key=lambda r: r["captured_at"])

    all_coords = []
    manifest = []
    for case in CASES:
        cid, icao, ts, te, cat, expected, desc = case[:7]
        xfail = bool(case[7]) if len(case) > 7 else False
        lo, hi = pt(ts) - timedelta(minutes=90), pt(te) + timedelta(minutes=90)
        rows = [r for r in rows_by[icao] if lo <= pt(r["captured_at"]) <= hi]
        positions = [{
            "icao24": r["icao24"].strip(), "callsign": r["callsign"],
            "captured_at": r["captured_at"].strip(),
            "latitude": r["latitude"], "longitude": r["longitude"],
            "altitude_m": r["altitude_m"], "velocity_ms": r["velocity_ms"],
            "on_ground": r["on_ground"].strip(),
        } for r in rows]
        for r in rows:
            try:
                all_coords.append((float(r["latitude"]), float(r["longitude"])))
            except ValueError:
                pass
        fixture = {"id": cid, "icao24": icao, "category": cat,
                   "description": desc, "expected_legs": expected,
                   "xfail": xfail,
                   "n_positions": len(positions), "positions": positions}
        (CORPUS / f"{cid}.json").write_text(json.dumps(fixture, indent=1))
        manifest.append((cid, cat, len(positions), expected))
        mark = "  [xfail]" if xfail else ""
        print(f"  {cid:30s} {cat:11s} {len(positions):4d} pos  expect={expected}{mark}")

    # prune airports to those within 60km of any fixture position (safe superset
    # of any <=50km nearest lookup) -> self-contained + small.
    aps = []
    with open(TMP / "airports_export.csv", newline="") as f:
        for r in csv.DictReader(f):
            try:
                la, lo = float(r["latitude"]), float(r["longitude"])
            except (ValueError, KeyError):
                continue
            aps.append((r["icao_code"], r.get("type", ""), la, lo))
    kept = [a for a in aps if any(haversine_km(a[2], a[3], cla, clo) <= 60
                                  for cla, clo in all_coords)]
    with open(CORPUS / "airports.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["icao_code", "type", "latitude", "longitude"])
        for a in kept:
            w.writerow(a)
    print(f"\nWrote {len(CASES)} fixtures + airports.csv "
          f"({len(kept)}/{len(aps)} airports, pruned to ≤60km of corpus).")


if __name__ == "__main__":
    main()
