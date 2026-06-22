"""
Data-quality snapshot of a flights CSV export (tools/pull_data.sh): how much
dep/arr the detector left unresolved, EDFE leftovers, needs_review volume, and
how much the callsign reference can recover. A quick health check on detection
+ enrichment.

Usage:
    python3 tools/data_quality_report.py
    python3 tools/data_quality_report.py --type B748
"""
import argparse
import collections

import _lhdata as lh


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(lh.DEFAULT_CSV))
    ap.add_argument("--type", default=None, dest="ac_type", help="restrict to one aircraft type")
    args = ap.parse_args()

    rows = lh.load(args.csv)
    cmap = lh.build_callsign_routes(rows)
    if args.ac_type:
        rows = [r for r in rows if r["aircraft_type"] == args.ac_type]

    def raw(x):
        return (x or "").strip()

    def missing(x):
        return raw(x) in lh.BAD

    n = len(rows)
    scope = args.ac_type or "all types"
    print(f"== Data-quality report — {scope}: {n} flights ==")
    if not n:
        return

    dep_missing = [r for r in rows if missing(r["departure_airport_icao"])]
    arr_missing = [r for r in rows if missing(r["arrival_airport_icao"])]
    edfe = sum(1 for r in rows if "EDFE" in (raw(r["departure_airport_icao"]), raw(r["arrival_airport_icao"])))
    review = sum(1 for r in rows if r["needs_review"] == "t")

    def pct(k):
        return f"{k} ({k / n:.1%})"

    print(f"  missing/UNKN departure : {pct(len(dep_missing))}")
    print(f"  missing/UNKN arrival   : {pct(len(arr_missing))}")
    print(f"  EDFE (Egelsbach) snaps : {edfe}")
    print(f"  needs_review           : {pct(review)}")

    # How much the callsign reference can recover.
    dep_fix = sum(1 for r in dep_missing if (r["callsign"] or "").strip() in cmap)
    arr_fix = sum(1 for r in arr_missing if (r["callsign"] or "").strip() in cmap)
    print(f"\n  callsign reference entries: {len(cmap)}")
    print(f"  departures resolvable by callsign: {dep_fix}/{len(dep_missing)}")
    print(f"  arrivals  resolvable by callsign: {arr_fix}/{len(arr_missing)}")

    # Worst callsigns by unresolved arrivals (candidates for a curated override).
    unresolved = collections.Counter(
        (r["callsign"] or "").strip()
        for r in arr_missing
        if (r["callsign"] or "").strip() and (r["callsign"] or "").strip() not in cmap
    )
    if unresolved:
        print("\n  top unresolved-arrival callsigns (no reference entry):")
        for cs, c in unresolved.most_common(10):
            print(f"    {cs:8} {c}")


if __name__ == "__main__":
    main()
