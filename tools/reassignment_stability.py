"""
Reassignment-stability report from a flight-status (FIS) export
(tools/pull_fis.sh -> tmp/fis_export.csv).

Answers the question the /book confidence chip is built on:

    Standing some days before departure, you see a tail assigned to a flight.
    How often does that same tail still hold as departure approaches?

The collector snapshots each flight nightly for +1..+4 days, so for a given
flight we have the assigned tail at lead 4, 3, 2, 1 days out. We take the
*closest-to-departure* snapshot (smallest lead, usually the night before) as the
"final" published assignment, then measure, for each earlier lead L, how often
the tail shown at L equals that final tail. Sliced overall / per type / per
route / per tail, always with the sample count, so small data stays honest.

Plan-stability only: "final" = the last published snapshot, not the ADS-B actual
(a cheap, fuzzy join we can layer on later). It directly mirrors what a booker
experiences re-checking the published schedule.

Usage:
    ./tools/pull_fis.sh                  # refresh tmp/fis_export.csv first
    python3 tools/reassignment_stability.py
    python3 tools/reassignment_stability.py --lead 2 --top 20
"""
import argparse
import collections
import csv
from datetime import date
from pathlib import Path

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "tmp" / "fis_export.csv"


def _bool(v):
    return (v or "").strip().lower() in ("t", "true", "1")


def _d(v):
    v = (v or "").strip()
    return date.fromisoformat(v[:10]) if v else None


def load(path):
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"No export at {path} — run tools/pull_fis.sh first.")
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def build_groups(rows):
    """Group found+tailed snapshots by flight, as {lead: snapshot}. lead = days before departure."""
    groups = collections.defaultdict(dict)  # (fdate, airline, fnum) -> {lead: snap}
    for r in rows:
        if not _bool(r.get("found")):
            continue
        reg = (r.get("registration") or "").strip().upper()
        if not reg:
            continue
        fdate, obs = _d(r.get("flight_date")), _d(r.get("observed_date"))
        if not fdate or not obs:
            continue
        lead = (fdate - obs).days
        if lead < 0:
            continue  # observed after the flight (shouldn't happen) — ignore
        key = (fdate, (r.get("airline") or "LH").strip().upper(), (r.get("flight_number") or "").strip())
        snap = {
            "lead": lead, "reg": reg,
            "type": (r.get("aircraft_type") or r.get("seed_type") or "").strip() or "?",
            "route": f"{(r.get('dep_airport_iata') or '?').strip()}-{(r.get('arr_airport_iata') or '?').strip()}",
        }
        # One row per (observed_date, flight) by the table's unique key, so one tail per lead.
        groups[key][lead] = snap
    return groups


def score(groups):
    """For each flight with >=2 leads, compare each earlier lead's tail to the final tail."""
    overall = collections.defaultdict(list)                 # lead -> [held]
    by_type = collections.defaultdict(list)                 # (type, lead) -> [held]
    by_route = collections.defaultdict(list)                # (route, lead) -> [held]
    by_tail = collections.defaultdict(list)                 # (tail, lead) -> [held]
    ever_reassigned = []                                    # [bool] per flight
    scored_flights = 0

    for snaps in groups.values():
        if len(snaps) < 2:
            continue
        final_lead = min(snaps)
        final = snaps[final_lead]
        ever_reassigned.append(len({s["reg"] for s in snaps.values()}) > 1)
        scored_any = False
        for lead, snap in snaps.items():
            if lead <= final_lead:
                continue
            held = snap["reg"] == final["reg"]
            overall[lead].append(held)
            by_type[(final["type"], lead)].append(held)
            by_route[(final["route"], lead)].append(held)
            by_tail[(final["reg"], lead)].append(held)
            scored_any = True
        if scored_any:
            scored_flights += 1

    return {
        "overall": overall, "by_type": by_type, "by_route": by_route,
        "by_tail": by_tail, "ever_reassigned": ever_reassigned,
        "scored_flights": scored_flights,
    }


def _rate(held):
    return (sum(held) / len(held)) if held else None


def _fmt_rate(held):
    r = _rate(held)
    return f"{r * 100:5.1f}%  (n={len(held)})" if r is not None else "    —  (n=0)"


def print_report(rows, res, lead, top):
    fdates = [_d(r.get("flight_date")) for r in rows if _d(r.get("flight_date"))]
    found = [r for r in rows if _bool(r.get("found"))]
    span = f"{min(fdates)} … {max(fdates)}" if fdates else "—"
    leads = sorted(res["overall"])

    print("=" * 64)
    print("REASSIGNMENT-STABILITY REPORT  (plan-stability, FIS only)")
    print("=" * 64)
    print(f"observations:   {len(rows)} ({len(found)} found w/ data)")
    print(f"flight-date span: {span}")
    print(f"scored flights:  {res['scored_flights']} (>=2 nightly snapshots)")
    if res["ever_reassigned"]:
        er = _rate(res["ever_reassigned"])
        print(f"ever reassigned: {er * 100:.1f}% of scored flights "
              f"changed tail at least once (n={len(res['ever_reassigned'])})")
    if not leads:
        print("\nNot enough multi-snapshot history yet to measure stability.")
        return

    print("\n-- Plan stability by lead (tail at lead L still the final published tail) --")
    print("   'final' = closest-to-departure snapshot (usually the night before)")
    for L in leads:
        flag = "  <- headline" if L == lead else ""
        print(f"   lead {L}d: {_fmt_rate(res['overall'][L])}{flag}")

    print("\n-- By aircraft type --")
    types = sorted({t for (t, _l) in res["by_type"]})
    for t in types:
        cells = " ".join(
            f"L{L}={_rate(res['by_type'][(t, L)]) * 100:.0f}%(n{len(res['by_type'][(t, L)])})"
            for L in leads if res["by_type"].get((t, L))
        )
        print(f"   {t:6s} {cells}")

    _print_slice(f"By route (top {top} by scored n, lead {lead}d)", res["by_route"], lead, top)
    _print_slice(f"By tail (top {top} by scored n, lead {lead}d)", res["by_tail"], lead, top)


def _print_slice(title, bucket, lead, top):
    # rank keys by total scored samples across all leads
    totals = collections.Counter()
    for (k, _l), held in bucket.items():
        totals[k] += len(held)
    print(f"\n-- {title} --")
    if not totals:
        print("   (no data)")
        return
    for k, _tot in totals.most_common(top):
        held = bucket.get((k, lead), [])
        pooled = [h for (kk, L), hs in bucket.items() if kk == k and L >= lead for h in hs]
        print(f"   {k:10s} lead{lead}={_fmt_rate(held)}   pooled(>= {lead}d)={_fmt_rate(pooled)}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--lead", type=int, default=2, help="headline lead in days (default 2)")
    ap.add_argument("--top", type=int, default=15, help="rows per route/tail table")
    args = ap.parse_args()

    rows = load(args.csv)
    res = score(build_groups(rows))
    print_report(rows, res, args.lead, args.top)


if __name__ == "__main__":
    main()
