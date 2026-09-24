"""
Walk-forward benchmark for the booking model (dashboard/booking_model.py) —
"how good is the P(target tail flies this flight) the planner shows?".

Replays every settled widebody leg as if standing some hours before its
departure, using only legs that had settled by then (truth-pass lag included),
and asks the model for P(X) for every tail X of the leg's fleet. Scores the
tail that actually flew.

Scenarios (one row each in the report):
    pre         nothing published — the history regime (what the planner shows
                beyond the feed's horizon, ~1-2 weeks out)
    <H>h        the tail published at lead H — the published/swap regimes

Baselines per scenario:
    uniform     1/N over the fleet of that type
    published   hold-overall for the published tail, the rest spread evenly
                (what today's /book chip amounts to)

Metrics: log-loss of the true tail's probability (lower is better), multiclass
Brier, top-1 hit rate, and a reliability table over every (leg, tail) pair —
the calibration of the number a visitor reads for *their* tail.

Input: tmp/leg_outcomes.csv from tools/build_leg_outcomes.py.

Usage:
    python3 tools/build_leg_outcomes.py --since 2026-07-21
    python3 tools/benchmark_booking.py
    python3 tools/benchmark_booking.py --type B748 --prior-m 5 --weeks 6
"""
import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard"))
import booking_model as bm  # noqa: E402

DEFAULT_CSV = ROOT / "tmp" / "leg_outcomes.csv"
WIDEBODY = ("B748", "A388", "B789", "B788", "B78X", "A359", "A35K")
# Far leads (216..120h) only fill once the D+5..D+9 horizon probe has run for a
# while (flightstatus/crontab, 22:00 pulse): they answer whether a tail FIS
# publishes that far out beats the history regime ("pre"), i.e. whether
# BOOK_HORIZON_DAYS can be raised.
SCENARIOS = ("pre", 216, 192, 168, 144, 120, 96, 72, 48, 24, 12, 6)
PRE_LEAD_H = 24 * 10      # "pre": standing 10 days out, nothing published
TRUTH_LAG = timedelta(days=2)   # a leg's truth lands with the D-1/D-2 truth pass
EPS = 1e-4
REL_BINS = (0.0, 0.01, 0.03, 0.06, 0.1, 0.2, 0.35, 0.5, 0.7, 0.85, 1.0001)


def load(path):
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"No leg export at {path} — run tools/build_leg_outcomes.py first.")
    legs = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            legs.append({
                "flight_date": r["flight_date"], "flight_number": r["flight_number"],
                "dep": r["dep_airport"], "arr": r["arr_airport"],
                "fleet_type": r["fleet_type"],
                "dep_utc": datetime.fromisoformat(r["dep_scheduled_utc"]),
                "truth_tail": r["truth_tail"],
                "timeline": json.loads(r["timeline"] or "[]"),
                "first_lead_h": float(r["first_lead_h"]) if r["first_lead_h"] else None,
                "cancelled": r["cancelled"] == "1",
            })
    return legs


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--type", action="append", default=[], help="restrict eval legs to a fleet type")
    ap.add_argument("--warmup-days", type=int, default=21,
                    help="first N days of data are history only, never scored (default 21)")
    ap.add_argument("--weeks", type=int, default=bm.HISTORY_WEEKS)
    ap.add_argument("--no-fit", action="store_true",
                    help="use booking_model.DEFAULTS for every type instead of fitting per type")
    ap.add_argument("--hold-m", type=float, default=bm.HOLD_M)
    ap.add_argument("--swap-mix", type=float, default=bm.SWAP_MIX)
    ap.add_argument("--quiet", action="store_true", help="scores only, no reliability tables")
    args = ap.parse_args()

    legs = load(args.csv)
    legs.sort(key=lambda l: l["dep_utc"])
    start = legs[0]["dep_utc"] + timedelta(days=args.warmup_days)
    want = [t.upper() for t in args.type] or list(WIDEBODY)
    evals = [l for l in legs if l["truth_tail"] and not l["cancelled"]
             and l["fleet_type"] in want and l["dep_utc"] >= start]
    print("legs: %d total, %d scored (%s, departing %s .. %s)"
          % (len(legs), len(evals), ",".join(want), start.date(),
             evals[-1]["dep_utc"].date() if evals else "-"))
    print("params: history %dw, share knobs %s, hold m=%g, swap mix=%g"
          % (args.weeks, "DEFAULTS %s" % bm.DEFAULTS if args.no_fit else "fitted per type",
             args.hold_m, args.swap_mix))

    stats_cache = {}

    def stats_for(now):
        key = now.date()
        if key not in stats_cache:
            # settled by then = departed before now - TRUTH_LAG
            stats_cache[key] = bm.build_stats(
                legs, datetime.combine(key, datetime.min.time(), tzinfo=now.tzinfo) - TRUTH_LAG,
                args.weeks, fit=not args.no_fit)
        return stats_cache[key]

    agg = defaultdict(lambda: defaultdict(float))
    rel = defaultdict(lambda: [[0, 0.0, 0] for _ in REL_BINS[:-1]])  # regime -> bins [n, sum_p, hits]
    for leg in evals:
        for sc in SCENARIOS:
            lead = PRE_LEAD_H if sc == "pre" else sc
            now = leg["dep_utc"] - timedelta(hours=lead)
            pub = None if sc == "pre" else bm.tail_at(leg["timeline"], leg["first_lead_h"], lead)
            if sc != "pre" and pub is None:
                continue  # we had no look that early for this leg
            st = stats_for(now)
            fleet = set(st.fleet.get(leg["fleet_type"]) or ())
            cands = fleet | {leg["truth_tail"]} | ({pub} if pub else set())
            truth = leg["truth_tail"]

            res = {x: bm.p_target(st, x, leg, published=pub, now=now,
                                  hold_m=args.hold_m, swap_mix=args.swap_mix) for x in cands}
            ps = {x: r["p"] for x, r in res.items()}
            n = max(len(fleet), 1)
            uni = {x: (1.0 / n if x in fleet else 0.0) for x in cands}
            if pub:
                cell = st.hold.get(("overall", "", bm.band_for(lead)))
                hp = cell[0] / cell[1] if cell and cell[1] else 0.5
                others = [x for x in fleet if x != pub]
                base = {x: (hp if x == pub else ((1 - hp) / len(others) if x in others else 0.0))
                        for x in cands}
            else:
                base = uni

            a = agg[sc]
            a["n"] += 1
            for name, dist in (("model", ps), ("uniform", uni), ("published", base)):
                pt = max(dist.get(truth, 0.0), EPS)
                a[name + "_ll"] += -math.log(pt)
                a[name + "_brier"] += sum((dist.get(x, 0.0) - (x == truth)) ** 2 for x in cands)
                a[name + "_top1"] += max(cands, key=lambda x: dist.get(x, 0.0)) == truth
                a[name + "_mass"] += sum(dist.values())

            for x, r in res.items():
                regime = "history" if r["regime"] == "history" else r["regime"]
                for i in range(len(REL_BINS) - 1):
                    if REL_BINS[i] <= r["p"] < REL_BINS[i + 1]:
                        cell = rel[regime][i]
                        cell[0] += 1
                        cell[1] += r["p"]
                        cell[2] += x == truth
                        break

    print("\n=== scores by scenario (log-loss / Brier: lower is better) " + "=" * 18)
    print("%-6s %6s | %-24s | %-24s | %-24s" % ("scen", "legs", "model  ll / brier / top1",
                                                 "uniform", "published-only"))
    for sc in SCENARIOS:
        a = agg.get(sc)
        if not a:
            continue
        n = a["n"]
        cols = []
        for name in ("model", "uniform", "published"):
            cols.append("%5.2f / %4.2f / %4.0f%%" % (a[name + "_ll"] / n, a[name + "_brier"] / n,
                                                     100 * a[name + "_top1"] / n))
        label = "pre" if sc == "pre" else "%dh" % sc
        print("%-6s %6d | %-24s | %-24s | %-24s" % ((label, int(n)) + tuple(cols)))
    mass = [agg[sc]["model_mass"] / agg[sc]["n"] for sc in SCENARIOS if agg.get(sc)]
    print("model probability mass per leg (should be ~1): %.3f .. %.3f" % (min(mass), max(mass)))

    if args.quiet:
        return
    print("\n=== reliability: predicted P(target) vs how often it flew " + "=" * 18)
    for regime in ("published", "swap", "history"):
        bins = rel.get(regime)
        if not bins:
            continue
        print("  %s" % regime)
        print("    %-12s %8s %9s %9s" % ("p bin", "pairs", "mean p", "observed"))
        for i, (n, sp, hits) in enumerate(bins):
            if n:
                print("    %4.0f-%3.0f%%    %8d %8.1f%% %8.1f%%"
                      % (100 * REL_BINS[i], min(100, 100 * REL_BINS[i + 1]), n,
                         100 * sp / n, 100 * hits / n))


if __name__ == "__main__":
    main()
