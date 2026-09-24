"""
Build the leg-outcome table — the training substrate for "will this leg's
published tail hold until departure?".

Input:  tmp/fis_history.csv   (tools/pull_fis_history.sh — per-pass FIS history)
Output: tmp/leg_outcomes.csv  one row per leg (flight_date, airline, flight_number)
        tmp/tail_changes.csv  one row per observed tail change, with the
                              bracket [prev look, revealing look] it landed in
        plus a summary report on stdout.

A "leg" is one scheduled flight-number/date. Its history is the sequence of FIS
snapshots we happened to take, so everything derived here is resolution-limited:
we never see the moment a tail changed, only the interval between the look that
still showed the old tail and the look that first showed the new one. The table
records that interval explicitly (bracket_h) instead of pretending to a
timestamp, so downstream analysis can stay honest about censoring.

Key columns
  truth_tail        registration in the terminal (ARRIVED/DIVERTED) snapshot
  tail_at_<H>h      tail published at lead H (last look with lead >= H)
  hold_<H>h         did tail_at_<H>h equal truth_tail  (the prediction label)
  stale_<H>h        how old that snapshot already was at lead H, in hours
  settle_lead_h     lead time of the first look after which the tail never
                    changed again  (= when the assignment became final, as
                    resolved by our sampling)
  settle_bracket_h  width of the uncertainty interval around that moment;
                    settle_censored=1 means the truth tail was already there at
                    our first look, so it settled at or before settle_lead_h

Usage:
    ./tools/pull_fis_history.sh
    python3 tools/build_leg_outcomes.py
    python3 tools/build_leg_outcomes.py --type B748 --since 2026-07-15
"""
import argparse
import collections
import csv
import json
import re
import statistics
import sys
from datetime import datetime, date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TMP = ROOT / "tmp"
DEFAULT_CSV = TMP / "fis_history.csv"

# The leg logic itself lives in flightstatus/legs.py, shared with the collector
# (which fills the `fis_legs` table from it), so offline analysis and production
# can't drift apart. Re-exported here for tools/reassignment_timing.py.
sys.path.insert(0, str(ROOT / "flightstatus"))
from legs import (  # noqa: E402
    BANDS, BERLIN, WIDEBODY, build_leg)


_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?([+-]\d{2}(?::?\d{2})?)?$")


def _ts(v):
    """Parse a psql timestamptz ('2026-07-24 03:35:59.41+00').

    Rebuilt field by field rather than passed straight to fromisoformat: on 3.9
    that rejects both psql's two-digit offset and its variable-length
    fractional seconds.
    """
    v = (v or "").strip()
    if not v:
        return None
    m = _TS_RE.match(v)
    if not m:
        raise ValueError("unparseable timestamp: %r" % v)
    day, clock, frac, off = m.groups()
    frac = (frac or "0")[:6].ljust(6, "0")
    off = (off or "+00:00").replace(":", "")
    off = "%s:%s" % (off[:3], (off[3:] or "00"))
    return datetime.fromisoformat("%sT%s.%s%s" % (day, clock, frac, off))


def _d(v):
    v = (v or "").strip()
    return date.fromisoformat(v[:10]) if v else None


def load(path):
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"No export at {path} — run tools/pull_fis_history.sh first.")
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def prepare(rows):
    """CSV strings -> the observation shape legs.build_leg expects."""
    for r in rows:
        r["observed_at"] = _ts(r["observed_at"])
        r["dep_scheduled"] = _ts(r["dep_scheduled"])
        r["arr_scheduled"] = _ts(r.get("arr_scheduled"))
        r["found"] = (r["found"] or "").strip().lower() in ("t", "true", "1")
        r["registration"] = (r["registration"] or "").strip().upper()
        a = (r.get("allegris") or "").strip().lower()
        r["allegris"] = None if not a else a in ("t", "true", "1")
    return rows


def pct(n, d):
    return "  n/a" if not d else "%5.1f%%" % (100.0 * n / d)


def report(legs, changes, args):
    print("\n=== coverage " + "=" * 60)
    print("legs                        : %d" % len(legs))
    truth = [l for l in legs if l["truth_tail"]]
    print("legs with ARRIVED truth     : %d" % len(truth))
    print("legs w/ pre-dep snapshots   : %d" % sum(1 for l in truth if l["n_obs_pre_dep"]))
    print("obs per leg (median/max)    : %s / %s"
          % (statistics.median([l["n_obs"] for l in legs]) if legs else 0,
             max([l["n_obs"] for l in legs]) if legs else 0))
    by_type = collections.Counter(l["fleet_type"] or "?" for l in truth)
    print("truth legs by fleet type    : "
          + ", ".join("%s %d" % kv for kv in by_type.most_common()))

    print("\n=== hold rate by lead time " + "=" * 45)
    print("the label a model has to beat: P(tail published at lead H is the one that flew)")
    print("  lead    n   holds   changes  base rate   median staleness of that snapshot")
    for b in BANDS:
        rows = [l for l in truth if l["hold_%dh" % b] != ""]
        if not rows:
            continue
        holds = sum(l["hold_%dh" % b] for l in rows)
        st = [l["stale_%dh" % b] for l in rows if l["stale_%dh" % b] != ""]
        print("  %4dh %4d   %4d   %5d    %s      %.1fh"
              % (b, len(rows), holds, len(rows) - holds, pct(len(rows) - holds, len(rows)),
                 statistics.median(st) if st else -1))

    print("\n=== how much churn per leg " + "=" * 45)
    dist = collections.Counter(min(l["n_changes"], 4) for l in truth)
    for k in sorted(dist):
        label = "%d" % k if k < 4 else "4+"
        print("  %2s change(s): %4d legs  %s" % (label, dist[k], pct(dist[k], len(truth))))
    ch_legs = [l for l in truth if l["n_changes"]]
    print("  legs with >=1 observed change: %d (%s)"
          % (len(ch_legs), pct(len(ch_legs), len(truth))))
    print("  changes only visible post-departure: %d legs (%s)"
          % (sum(l["reassigned"] for l in truth),
             pct(sum(l["reassigned"] for l in truth), len(truth))))
    print("  cross-type substitutions: %d legs (%s)"
          % (sum(l["type_changed"] for l in truth),
             pct(sum(l["type_changed"] for l in truth), len(truth))))

    print("\n=== sampling density (pre-departure looks) " + "=" * 29)
    gaps = [l["median_gap_h"] for l in truth if l["median_gap_h"] != ""]
    mx = [l["max_gap_h"] for l in truth if l["max_gap_h"] != ""]
    if gaps:
        gaps.sort(); mx.sort()
        print("  median gap between looks : median %.1fh  p90 %.1fh"
              % (statistics.median(gaps), gaps[int(0.9 * (len(gaps) - 1))]))
        print("  worst gap per leg        : median %.1fh  p90 %.1fh  max %.1fh"
              % (statistics.median(mx), mx[int(0.9 * (len(mx) - 1))], mx[-1]))
    print("  change brackets           : %d changes, median width %.1fh, p90 %.1fh"
          % ((len(changes),) + tuple(
              (lambda v: (statistics.median(v), sorted(v)[int(0.9 * (len(v) - 1))]))(
                  [c["bracket_h"] for c in changes])) if changes else (0, 0, 0)))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--type", action="append", default=[],
                    help="restrict to fleet type(s), e.g. --type B748 (default: all widebody)")
    ap.add_argument("--since", help="only legs with flight_date >= this date")
    ap.add_argument("--all-types", action="store_true",
                    help="keep narrowbody/unknown legs too (default: widebody only)")
    args = ap.parse_args()

    rows = prepare(load(args.csv))

    groups = collections.defaultdict(list)
    for r in rows:
        groups[(r["flight_date"], r["airline"], r["flight_number"])].append(r)

    legs, changes = [], []
    for key in sorted(groups):
        leg, ch = build_leg(key, groups[key])
        if leg is None:
            continue
        legs.append(leg)
        changes.extend(ch)

    since = _d(args.since) if args.since else None
    want = [t.upper() for t in args.type]

    def keep(leg_type, fdate):
        if since and _d(fdate) < since:
            return False
        if want:
            return leg_type in want
        return args.all_types or leg_type in WIDEBODY

    legs = [l for l in legs if keep(l["fleet_type"], l["flight_date"])]
    kept = {(l["flight_date"], l["airline"], l["flight_number"]) for l in legs}
    changes = [c for c in changes
               if (c["flight_date"], c["airline"], c["flight_number"]) in kept]

    TMP.mkdir(exist_ok=True)
    out_legs, out_ch = TMP / "leg_outcomes.csv", TMP / "tail_changes.csv"
    if legs:
        with open(out_legs, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(legs[0].keys()))
            w.writeheader()
            w.writerows(dict(l, timeline=json.dumps(l["timeline"])) for l in legs)
    if changes:
        with open(out_ch, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(changes[0].keys()))
            w.writeheader()
            w.writerows(changes)
    print("wrote %s (%d legs) and %s (%d changes)"
          % (out_legs, len(legs), out_ch, len(changes)))
    report(legs, changes, args)


if __name__ == "__main__":
    main()
