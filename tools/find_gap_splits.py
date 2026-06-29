"""
Enumerate gap-split flight pairs from a flights CSV export.

A "gap-split pair" is one real flight that the detector logged as two rows:
two consecutive legs of the same aircraft + same callsign where
leg1.arrival == leg2.departure, and that shared airport is NOT an endpoint
of the callsign's consensus route (i.e. it is a phantom waypoint at a
coverage-gap boundary).

Pure stdlib; reads tmp/flights_export.csv (see tools/_lhdata.py).

Outputs:
  - a summary to stdout (counts, phantom-airport distribution, gap stats)
  - tmp/gap_split_pairs.csv   one row per detected pair (for the corpus manifest)
  - tmp/gap_split_icao24s.txt  unique icao24s involved (for tools/pull_positions.sh)

Usage:
    python3 tools/find_gap_splits.py
"""
import collections
import csv
from datetime import datetime
from pathlib import Path

import _lhdata as L

# German fields the dashboard /api/insights treats as diversion/incident markers.
# Mirrors dashboard/app.py _DIVERSION_AIRPORTS — used here only to flag which
# phantom splits would draw a FALSE incident diamond.
DIVERSION_AIRPORTS = {
    "EDDF", "EDDM", "EDDL", "EDDK", "EDDH", "EDDB", "EDDS", "EDDV", "EDDN",
    "EDDP", "EDDW", "EDDR", "EDLW", "EDDG", "EDDT", "EDDC", "EDDE",
}

OUT_DIR = Path(__file__).resolve().parent.parent / "tmp"


def parse_ts(s):
    # Postgres COPY timestamps look like "2026-06-29 00:23:00+00".
    s = (s or "").strip()
    if not s:
        return None
    s = s.replace(" ", "T", 1)
    # normalise +00 → +00:00 for fromisoformat
    if s.endswith("+00"):
        s = s[:-3] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def main():
    rows = L.load()
    cmap = L.build_callsign_routes(rows, min_support=3)

    by_reg = collections.defaultdict(list)
    for r in rows:
        by_reg[r["registration"]].append(r)
    for reg in by_reg:
        by_reg[reg].sort(key=lambda r: r["first_seen"])

    pairs = []
    for reg, legs in by_reg.items():
        for a, b in zip(legs, legs[1:]):
            csa = (a["callsign"] or "").strip()
            csb = (b["callsign"] or "").strip()
            if not csa or csa != csb:
                continue
            arr_a = L.norm_ap(a["arrival_airport_icao"])
            dep_b = L.norm_ap(b["departure_airport_icao"])
            if not arr_a or not dep_b or arr_a != dep_b:
                continue
            shared = arr_a
            cons = cmap.get(csa)
            if not cons:
                continue  # no consensus route → cannot classify confidently
            if shared in cons:
                continue  # shared airport is a real endpoint, not a phantom
            ta1, ta2 = parse_ts(a["first_seen"]), parse_ts(a["last_seen"])
            tb1, tb2 = parse_ts(b["first_seen"]), parse_ts(b["last_seen"])
            gap_min = None
            if ta2 and tb1:
                gap_min = (tb1 - ta2).total_seconds() / 60.0
            pairs.append({
                "icao24": a["icao24"].strip(),
                "registration": reg,
                "aircraft_type": a["aircraft_type"],
                "callsign": csa,
                "phantom": shared,
                "true_dep": cons[0],
                "true_arr": cons[1],
                "leg1_dep": L.norm_ap(a["departure_airport_icao"]),
                "leg2_arr": L.norm_ap(b["arrival_airport_icao"]),
                "a_first": a["first_seen"], "a_last": a["last_seen"],
                "b_first": b["first_seen"], "b_last": b["last_seen"],
                "gap_min": gap_min,
                "a_review": a["needs_review"], "b_review": b["needs_review"],
                "flight_date": a["flight_date"],
            })

    # ---- summary ----
    print(f"Loaded {len(rows)} flights, {len(cmap)} callsign consensus routes")
    print(f"Gap-split pairs: {len(pairs)}  "
          f"({2*len(pairs)} fragment rows, {2*len(pairs)/len(rows)*100:.2f}% of flights)")
    print(f"Distinct aircraft involved: {len({p['icao24'] for p in pairs})}")
    print(f"Distinct callsigns involved: {len({p['callsign'] for p in pairs})}")

    both_clean = sum(1 for p in pairs if p["a_review"] == "f" and p["b_review"] == "f")
    print(f"Over-confident (both fragments needs_review=f): {both_clean}")

    phantom_in_div = sum(1 for p in pairs if p["phantom"] in DIVERSION_AIRPORTS)
    print(f"Phantom waypoint inside _DIVERSION_AIRPORTS (false-incident risk): {phantom_in_div}")

    print("\nTop phantom waypoints:")
    for ap, n in collections.Counter(p["phantom"] for p in pairs).most_common(15):
        flag = "  <-- diversion set" if ap in DIVERSION_AIRPORTS else ""
        print(f"  {ap}: {n}{flag}")

    print("\nTop aircraft types:")
    for t, n in collections.Counter(p["aircraft_type"] for p in pairs).most_common(10):
        print(f"  {t}: {n}")

    gaps = sorted(p["gap_min"] for p in pairs if p["gap_min"] is not None)
    if gaps:
        def pct(q):
            return gaps[min(len(gaps) - 1, int(q * len(gaps)))]
        print(f"\nGap (leg1.last → leg2.first) minutes: "
              f"min={gaps[0]:.0f} p25={pct(.25):.0f} median={pct(.5):.0f} "
              f"p75={pct(.75):.0f} p95={pct(.95):.0f} max={gaps[-1]:.0f}")
        print(f"  pairs with gap < 8 min (within one session!): "
              f"{sum(1 for g in gaps if g < 8)}")
        print(f"  pairs with gap < 60 min (same lookback window): "
              f"{sum(1 for g in gaps if g < 60)}")

    OUT_DIR.mkdir(exist_ok=True)
    cols = ["icao24", "registration", "aircraft_type", "callsign", "phantom",
            "true_dep", "true_arr", "leg1_dep", "leg2_arr",
            "a_first", "a_last", "b_first", "b_last", "gap_min",
            "a_review", "b_review", "flight_date"]
    with open(OUT_DIR / "gap_split_pairs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(pairs)
    icao24s = sorted({p["icao24"] for p in pairs})
    (OUT_DIR / "gap_split_icao24s.txt").write_text("\n".join(icao24s) + "\n")
    print(f"\nWrote tmp/gap_split_pairs.csv ({len(pairs)} rows) and "
          f"tmp/gap_split_icao24s.txt ({len(icao24s)} aircraft)")


if __name__ == "__main__":
    main()
