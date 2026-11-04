"""
The planner as one tail sees it — "of the flights in my dates, are the ones
ranked first the ones my tail flies?".

tools/benchmark_booking.py scores every flight's distribution over the tails
(log-loss: is the tail that flew given a fair probability?). A visitor asks the
other way round: one tail, every flight of a few days, ranked. A publication
that holds 13% of the time is a wash on the first score and still puts the
right flight on top of the list twice as often as an even guess. This is the
score that decides how far ahead the pages show a published tail
(BOOK_HORIZON_BY_TYPE in dashboard/app.py).

Every evening (23:00 Berlin, after the horizon probe) and for every tail of a
fleet, the fleet's flights of two windows are ranked by booking_model.p_target:

    D+1..D+4    what every tier is read to
    D+5..D+9    the horizon probe (flightstatus/crontab, 22:00)

twice — with the tail published at that moment ("open"), and with it withheld
("capped": the history regime, what a page shows beyond its horizon). Scored on
the flights that tail then flew:

    top-1       the first-ranked flight was flown by the tail
    top-3       share of the first three that were (ties share their places)
    AUC         a flight it flew ranks above one it did not (0.5 = no order)
    base        the share of the window's flights the tail flew: what any
                order scores at top-1 by luck

Shares and near hold rates are walk-forward (only legs settled by then). The
far hold rate is too young for that — the probe started 2026-09-24, a leg's
far looks settle nine days later — so by default it is taken from the other
evenings' legs (every settled leg outside the ranked window); --strict keeps
it walk-forward and shows how cold that still is.

Input: tmp/leg_outcomes.csv from tools/build_leg_outcomes.py.

Usage:
    ./tools/pull_fis_history.sh
    python3 tools/build_leg_outcomes.py --since 2026-07-21
    python3 tools/benchmark_ranking.py
    python3 tools/benchmark_ranking.py --type B748 --since 2026-09-24
"""
import argparse
import sys
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard"))
sys.path.insert(0, str(ROOT / "tools"))
import booking_model as bm  # noqa: E402
from benchmark_booking import DEFAULT_CSV, TRUTH_LAG, load  # noqa: E402

BERLIN = ZoneInfo("Europe/Berlin")
SNAP = time(23, 0)                       # Berlin: the 22:00 probe has run
WINDOWS = (("D+1..D+4", 1, 4), ("D+5..D+9", 5, 9))
TYPES = ("B748", "A388", "B789", "A359")
REL_BINS = (0.0, 0.03, 0.06, 0.1, 0.2, 0.35, 0.5, 0.7, 1.0001)
ALL_WEEKS = 520                          # "every leg" for the hold cells of the other evenings


def auc(scores, flown):
    """P(a flown flight ranks above one not flown), ties counting half."""
    pos = [s for s, y in zip(scores, flown) if y]
    neg = [s for s, y in zip(scores, flown) if not y]
    if not pos or not neg:
        return None
    wins = sum((p > n) + 0.5 * (p == n) for p in pos for n in neg)
    return wins / (len(pos) * len(neg))


def top_k(scores, flown, k):
    """Share of the k first-ranked flights that were flown; flights with the
    same score share the places they tie for."""
    levels = defaultdict(lambda: [0, 0])                 # score -> [flights, flown]
    for s, y in zip(scores, flown):
        levels[s][0] += 1
        levels[s][1] += bool(y)
    left, hits = float(min(k, len(scores))), 0.0
    for s in sorted(levels, reverse=True):
        n, y = levels[s]
        take = min(left, n)
        hits += take * y / n
        left -= take
        if left <= 0:
            break
    return hits / min(k, len(scores))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--type", action="append", default=[], help="fleet type(s) to score")
    ap.add_argument("--since", default="2026-09-24",
                    help="first evening ranked (default: the first probe night)")
    ap.add_argument("--warmup-days", type=int, default=21)
    ap.add_argument("--strict", action="store_true",
                    help="far hold rate walk-forward too (cold until the probe has weeks behind it)")
    args = ap.parse_args()

    legs = load(args.csv)
    legs.sort(key=lambda l: l["dep_utc"])
    for l in legs:
        l["flight_date"] = date.fromisoformat(l["flight_date"])
    plan = bm.PlanIndex(legs)
    first = max(date.fromisoformat(args.since),
                (legs[0]["dep_utc"] + timedelta(days=args.warmup_days)).date())
    last = legs[-1]["dep_utc"].date()
    by_date = defaultdict(list)
    for l in legs:
        if not l["cancelled"]:
            by_date[l["flight_date"]].append(l)

    # every settled leg's hold cells, to take the far rate from the other evenings
    end = legs[-1]["dep_utc"] + timedelta(days=1)
    everything = bm.build_stats(legs, end, weeks=ALL_WEEKS, fit=False, plan=plan)
    stats_cache = {}

    def stats_for(snap):
        key = snap.date()
        if key not in stats_cache:
            cut = datetime.combine(key, time.min, tzinfo=snap.tzinfo) - TRUTH_LAG
            stats_cache[key] = bm.build_stats(legs, cut, plan=plan)
        return stats_cache[key]

    def with_other_evenings(st, window):
        """`st`, its far hold cells replaced by those of every settled leg
        outside `window` (the legs being ranked must not vouch for themselves)."""
        own = bm.Stats(end, ALL_WEEKS)
        own.plan = plan
        for l in window:
            if l["truth_tail"]:
                bm._count_holds(own, l, l["truth_tail"], bm.route_key(l), l["fleet_type"] or "?")
        out = bm.Stats(st.now, st.weeks)
        out.__dict__.update(st.__dict__)
        out.hold = dict(st.hold)
        for k, (held, n) in everything.hold.items():
            if k[0].startswith("far:"):
                o = own.hold.get(k, (0, 0))
                out.hold[k] = [held - o[0], n - o[1]]
        return out

    want = [t.upper() for t in args.type] or list(TYPES)
    agg = defaultdict(lambda: defaultdict(float))            # (type, window, variant) -> sums
    rel = defaultdict(lambda: [[0, 0.0, 0] for _ in REL_BINS[:-1]])
    pub_days = defaultdict(lambda: [0, 0])                   # (type, lead day) -> [held, published]
    day = first
    while day <= last:
        snap = datetime.combine(day, SNAP, tzinfo=BERLIN)
        st = stats_for(snap)
        for name, lo, hi in WINDOWS:
            window = [l for d in range(lo, hi + 1) for l in by_date.get(day + timedelta(days=d), ())
                      if l["truth_tail"] and l["dep_utc"] > snap]
            st_far = st if (args.strict or lo < 5) else with_other_evenings(st, window)
            for ftype in want:
                cands = [l for l in window if l["fleet_type"] == ftype]
                fleet = sorted(st.fleet.get(ftype) or ())
                if len(cands) < 5 or not fleet:
                    continue
                pubs = [bm.tail_as_of(l, snap) for l in cands]
                if not any(pubs):
                    continue                                  # nothing read that far that evening
                for l, pub in zip(cands, pubs):
                    if pub:
                        cell = pub_days[(ftype, (l["flight_date"] - day).days)]
                        cell[0] += pub == l["truth_tail"]
                        cell[1] += 1
                scores = {"open": {}, "capped": {}}
                for x in fleet:
                    scores["open"][x] = [bm.p_target(st_far, x, l, published=pub, now=snap)["p"]
                                         for l, pub in zip(cands, pubs)]
                    scores["capped"][x] = [bm.p_target(st, x, l, published=None, now=snap)["p"]
                                           for l in cands]
                for x in fleet:
                    flown = [l["truth_tail"] == x for l in cands]
                    if not any(flown) or all(flown):
                        continue
                    for variant in ("open", "capped"):
                        s = scores[variant][x]
                        a = agg[(ftype, name, variant)]
                        a["n"] += 1
                        a["base"] += sum(flown) / len(flown)
                        a["flights"] += len(flown)
                        a["top1"] += top_k(s, flown, 1)
                        a["top3"] += top_k(s, flown, 3)
                        a["auc"] += auc(s, flown)
                    for p, y in zip(scores["open"][x], flown):
                        for b in range(len(REL_BINS) - 1):
                            if REL_BINS[b] <= p < REL_BINS[b + 1]:
                                cell = rel[(ftype, name)][b]
                                cell[0] += 1
                                cell[1] += p
                                cell[2] += y
                                break
        day += timedelta(days=1)

    print("evenings %s .. %s (23:00 Berlin); far hold rate %s"
          % (first, last, "walk-forward" if args.strict else "from the other evenings' legs"))
    print("\n=== one tail's flights, ranked " + "=" * 46)
    print("%-5s %-9s %7s %8s %6s | %-22s | %-22s" % (
        "type", "window", "rankings", "flights", "base", "capped  top1 top3  AUC", "open    top1 top3  AUC"))
    for ftype in want:
        for name, _lo, _hi in WINDOWS:
            a = agg.get((ftype, name, "open"))
            if not a:
                continue
            n = a["n"]
            cols = []
            for variant in ("capped", "open"):
                v = agg[(ftype, name, variant)]
                cols.append("       %3.0f%% %3.0f%% %5.3f" % (100 * v["top1"] / n, 100 * v["top3"] / n,
                                                              v["auc"] / n))
            print("%-5s %-9s %7d %8.0f %5.1f%% | %-22s | %-22s"
                  % (ftype, name, n, a["flights"] / n, 100 * a["base"] / n, cols[0], cols[1]))

    print("\n=== a tail published that evening: how often it flew the flight " + "=" * 13)
    for ftype in want:
        cells = [(d, pub_days[(ftype, d)]) for d in range(1, 10) if pub_days.get((ftype, d))]
        if cells:
            print("  %-5s " % ftype + "  ".join("D+%d %3.0f%% (%d)" % (d, 100.0 * c[0] / c[1], c[1])
                                                 for d, c in cells))

    print("\n=== reliability of the open number (every tail x flight) " + "=" * 20)
    for ftype in want:
        for name, _lo, _hi in WINDOWS:
            bins = rel.get((ftype, name))
            if not bins:
                continue
            print("  %s, %s" % (ftype, name))
            print("    %-12s %8s %9s %9s" % ("p bin", "pairs", "mean p", "observed"))
            for b, (n, sp, hits) in enumerate(bins):
                if n:
                    print("    %4.0f-%3.0f%%    %8d %8.1f%% %8.1f%%"
                          % (100 * REL_BINS[b], min(100, 100 * REL_BINS[b + 1]), n,
                             100 * sp / n, 100 * hits / n))


if __name__ == "__main__":
    main()
