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
import re
import statistics
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

TMP = Path(__file__).resolve().parent.parent / "tmp"
DEFAULT_CSV = TMP / "fis_history.csv"
BERLIN = ZoneInfo("Europe/Berlin")

# Lead times (hours before scheduled departure) the table snapshots the
# published tail at. Dense near departure, where the sweeps are dense too.
BANDS = [120, 96, 72, 48, 36, 24, 18, 12, 6, 3]

# Fleet types this study is about (the FIS catalog also drags in narrowbody
# substitutions and chained short-haul legs, which are not the subject).
WIDEBODY = ("B748", "A388", "B789", "B788", "B78X", "A359", "A35K")

# FIS free-text aircraftType -> fleet code, for the few tails the local
# `aircraft` table has never seen (new deliveries, wet-lease).
FIS_TYPE_MAP = {
    "boeing 747-8": "B748", "airbus a380-800": "A388", "boeing 787-9": "B789",
    "boeing 787-8": "B788", "boeing 787-10": "B78X", "boeing 787": "B789",
    "airbus a350-900": "A359", "airbus a350-1000": "A35K", "airbus a350": "A359",
}

TERMINAL = ("ARRIVED", "DIVERTED")  # the tail in these rows actually flew it


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


def fleet_type(row):
    t = (row.get("fleet_type") or "").strip().upper()
    if t:
        return t
    return FIS_TYPE_MAP.get((row.get("fis_type") or "").strip().lower(), "")


def modal(values):
    """Most common value, ties broken by last occurrence (latest plan wins)."""
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    counts = collections.Counter(vals)
    best = max(counts.values())
    for v in reversed(vals):
        if counts[v] == best:
            return v


def build_leg(key, obs):
    """Collapse one leg's snapshot sequence into an outcome row + change rows."""
    flight_date, airline, flight_number = key
    obs = sorted(obs, key=lambda r: r["observed_at"])
    found = [r for r in obs if r["found"]]
    if not found:
        return None, []

    # Lead-time reference: the modal scheduled departure across all looks. A
    # single re-timed snapshot (or a post-hoc row) can't shift the whole leg.
    dep_ref = modal([r["dep_scheduled"] for r in found])
    if dep_ref is None:
        return None, []

    for r in found:
        r["lead_h"] = (dep_ref - r["observed_at"]).total_seconds() / 3600.0

    # Truth = the tail in a terminal snapshot. Only legs that have one are
    # labelable; the rest are still-in-the-future legs (useful as features).
    terminal = [r for r in found if (r["overall_status"] or "").upper() in TERMINAL
                and r["registration"]]
    truth = terminal[-1] if terminal else None
    cancelled = any((r["overall_status"] or "").upper() == "CANCELLED" for r in found)

    pre = [r for r in found if r["lead_h"] > 0 and r["registration"]]
    post_dep_only = bool(truth) and not pre

    row = {
        "flight_date": flight_date, "airline": airline, "flight_number": flight_number,
        "dep_airport": modal([r["dep_airport_iata"] for r in found]),
        "arr_airport": modal([r["arr_airport_iata"] for r in found]),
        "dep_scheduled_utc": dep_ref.isoformat(),
        "dep_hour_berlin": dep_ref.astimezone(BERLIN).hour,
        "dep_dow": dep_ref.astimezone(BERLIN).isoweekday(),
        "seed_type": modal([r["seed_type"] for r in found]),
        "truth_tail": truth["registration"] if truth else "",
        "truth_status": (truth["overall_status"] or "").upper() if truth else "",
        "cancelled": int(cancelled),
        "n_obs": len(found),
        "n_obs_pre_dep": len(pre),
        "post_dep_only": int(post_dep_only),
    }

    # Type of the leg: what actually flew it, else what is currently published.
    types = [fleet_type(r) for r in pre if fleet_type(r)]
    truth_type = fleet_type(truth) if truth else ""
    row["fleet_type"] = truth_type or (types[-1] if types else "")
    row["type_changed"] = int(bool(truth_type) and bool(types)
                              and any(t != truth_type for t in types))

    # --- lead-time bands: the tail as it was published at each horizon -------
    for b in BANDS:
        at = [r for r in pre if r["lead_h"] >= b]
        snap = at[-1] if at else None
        row["tail_at_%dh" % b] = snap["registration"] if snap else ""
        row["stale_%dh" % b] = round(snap["lead_h"] - b, 1) if snap else ""
        if snap and truth:
            row["hold_%dh" % b] = int(snap["registration"] == truth["registration"])
        else:
            row["hold_%dh" % b] = ""

    # --- churn --------------------------------------------------------------
    seq = [r["registration"] for r in pre]
    changes = []
    for i in range(1, len(pre)):
        if pre[i]["registration"] == pre[i - 1]["registration"]:
            continue
        a, b = pre[i - 1], pre[i]
        changes.append({
            "flight_date": flight_date, "airline": airline, "flight_number": flight_number,
            "dep_airport": row["dep_airport"], "arr_airport": row["arr_airport"],
            "fleet_type": row["fleet_type"],
            "dep_scheduled_utc": dep_ref.isoformat(),
            "from_tail": a["registration"], "to_tail": b["registration"],
            "from_type": fleet_type(a), "to_type": fleet_type(b),
            # the change happened somewhere in this interval — we cannot know where
            "seen_after_utc": a["observed_at"].isoformat(),
            "seen_before_utc": b["observed_at"].isoformat(),
            "lead_h_hi": round(a["lead_h"], 2),   # lead at the last look showing the old tail
            "lead_h_lo": round(b["lead_h"], 2),   # lead at the look that revealed the new one
            "bracket_h": round(a["lead_h"] - b["lead_h"], 2),
            "is_final": int(bool(truth) and b["registration"] == truth["registration"]),
        })
    row["n_changes"] = len(changes)
    row["n_distinct_tails"] = len(set(seq))
    row["first_tail"] = seq[0] if seq else ""
    row["first_lead_h"] = round(pre[0]["lead_h"], 1) if pre else ""
    row["last_tail"] = seq[-1] if seq else ""
    row["last_lead_h"] = round(pre[-1]["lead_h"], 1) if pre else ""
    row["reassigned"] = int(bool(truth) and bool(seq) and seq[-1] != truth["registration"])
    row["missed_pre_dep"] = row["reassigned"]  # change we only learned post-departure

    gaps = [(pre[i]["observed_at"] - pre[i - 1]["observed_at"]).total_seconds() / 3600.0
            for i in range(1, len(pre))]
    row["median_gap_h"] = round(statistics.median(gaps), 1) if gaps else ""
    row["max_gap_h"] = round(max(gaps), 1) if gaps else ""

    # --- settle point: when did the truth tail arrive and stay? -------------
    row["settle_lead_h"] = row["settle_bracket_h"] = row["settle_censored"] = ""
    if truth and pre:
        tail = truth["registration"]
        idx = len(pre)
        while idx > 0 and pre[idx - 1]["registration"] == tail:
            idx -= 1
        if idx < len(pre):  # the truth tail is published in some pre-dep look
            row["settle_lead_h"] = round(pre[idx]["lead_h"], 1)
            if idx == 0:
                row["settle_censored"] = 1  # already right at our first look
                row["settle_bracket_h"] = ""
            else:
                row["settle_censored"] = 0
                row["settle_bracket_h"] = round(pre[idx - 1]["lead_h"] - pre[idx]["lead_h"], 2)
        else:
            row["settle_lead_h"] = 0  # never published before departure
            row["settle_censored"] = 0
    return row, changes


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

    rows = load(args.csv)
    for r in rows:
        r["observed_at"] = _ts(r["observed_at"])
        r["dep_scheduled"] = _ts(r["dep_scheduled"])
        r["found"] = (r["found"] or "").strip().lower() in ("t", "true", "1")
        r["registration"] = (r["registration"] or "").strip().upper()

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
            w.writerows(legs)
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
