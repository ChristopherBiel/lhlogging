"""
The fleet-physics facts the fleet simulation (dashboard/fleet_sim.py) rests on,
measured on the operated legs — re-run as data grows to see they still hold.

  chains       a tail's consecutive operated legs: does the next one depart
               where the last one landed? (the rule the simulation enforces)
  turns        ground time between them, at hubs and outstations
  trips        after a hub departure, is the tail's next leg the way home?
  on ground    at each hub departure: was the tail that flew on the ground
               there, and how many tails of the type were?
  lines        does the inbound flight predict the next outbound (60/40 split
               by date, modal pairing)?

Clash hold rates (a publication inconsistent with its tail's plan) are in
tools/benchmark_booking.py's reliability tables ("published (clash)").

Usage:
    python3 tools/build_leg_outcomes.py --since 2026-07-21
    python3 tools/fleet_state_report.py [--type B748 ...]
"""
import argparse
import bisect
import statistics
import sys
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard"))
sys.path.insert(0, str(ROOT / "tools"))
import booking_model as bm  # noqa: E402
from benchmark_booking import DEFAULT_CSV, load  # noqa: E402

HUBS = {"FRA", "MUC"}
TYPES = ("B748", "A388", "A359", "B789")


def q(values, p):
    v = sorted(values)
    return v[int(p * (len(v) - 1))] if v else float("nan")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--type", action="append", default=[])
    args = ap.parse_args()
    types = [t.upper() for t in args.type] or list(TYPES)

    legs = [l for l in load(args.csv) if l["truth_tail"] and not l["cancelled"]
            and l["duration_min"]]
    by_tail = defaultdict(list)
    for l in legs:
        l["arr_utc"] = bm.arr_utc(l)
        by_tail[l["truth_tail"]].append(l)
    for chain in by_tail.values():
        chain.sort(key=lambda l: l["dep_utc"])
    tail_type = {t: Counter(l["fleet_type"] for l in c).most_common(1)[0][0]
                 for t, c in by_tail.items()}
    start = min(l["dep_utc"] for l in legs) + timedelta(days=3)
    end = max(l["dep_utc"] for l in legs) - timedelta(days=2)
    print("%d operated legs, %s .. %s" % (len(legs), min(l["flight_date"] for l in legs),
                                         max(l["flight_date"] for l in legs)))

    for ftype in types:
        tails = [t for t, ty in tail_type.items() if ty == ftype]
        ok = br = 0
        breaks, ground = Counter(), {"hub": [], "outstation": []}
        trips = Counter()
        pairs = []
        for t in tails:
            chain = by_tail[t]
            for a, b in zip(chain, chain[1:]):
                if a["arr"] != b["dep"]:
                    br += 1
                    breaks[(a["arr"], b["dep"])] += 1
                    continue
                ok += 1
                g = (b["dep_utc"] - a["arr_utc"]).total_seconds() / 3600
                ground["hub" if b["dep"] in HUBS else "outstation"].append(g)
                if a["arr"] in HUBS:
                    pairs.append((a, b))
            for i, a in enumerate(chain[:-1]):
                if a["dep"] in HUBS:
                    b = chain[i + 1]
                    trips["break" if b["dep"] != a["arr"] else
                          "home" if b["arr"] in HUBS else "onward"] += 1
        n_trips = sum(trips.values()) or 1
        print("\n== %s: %d tails" % (ftype, len(tails)))
        print("  chains      %.1f%% continuous (%d breaks; most: %s)"
              % (100 * ok / max(ok + br, 1), br,
                 ", ".join("%s->%s %d" % (a, b, n) for (a, b), n in breaks.most_common(3))))
        for k, v in ground.items():
            print("  turns       %-10s n=%d  p5 %.1fh  median %.1fh  p95 %.1fh"
                  % (k, len(v), q(v, .05), q(v, .5), q(v, .95)))
        print("  trips       after a hub departure: home next %.0f%%, onward %.0f%%, break %.0f%%"
              % tuple(100 * trips[k] / n_trips for k in ("home", "onward", "break")))

        # on the ground at the hub at each hub departure (truth chains)
        deps = {t: [l["dep_utc"] for l in by_tail[t]] for t in tails}
        sizes, hit, n = [], 0, 0
        for l in legs:
            if l["fleet_type"] != ftype or l["dep"] not in HUBS or not start <= l["dep_utc"] <= end:
                continue
            n += 1
            here = set()
            for t in tails:
                i = bisect.bisect_left(deps[t], l["dep_utc"] - timedelta(minutes=1))
                if i and by_tail[t][i - 1]["arr_utc"] <= l["dep_utc"] \
                        and by_tail[t][i - 1]["arr"] == l["dep"]:
                    here.add(t)
            hit += l["truth_tail"] in here
            sizes.append(len(here))
        if n:
            print("  on ground   the tail that flew was on the ground there %.1f%%; tails of the "
                  "type on the ground: median %d (p25 %d, p75 %d) of %d"
                  % (100 * hit / n, statistics.median(sizes), q(sizes, .25), q(sizes, .75),
                     len(tails)))

        pairs.sort(key=lambda p: p[0]["dep_utc"])
        cut = int(.6 * len(pairs))
        modal = defaultdict(Counter)
        for a, b in pairs[:cut]:
            modal[a["flight_number"]][b["flight_number"]] += 1
        test = [(a, b) for a, b in pairs[cut:] if modal.get(a["flight_number"])]
        if test:
            top1 = sum(modal[a["flight_number"]].most_common(1)[0][0] == b["flight_number"]
                       for a, b in test) / len(test)
            print("  lines       the inbound's usual next outbound is the one flown: %.0f%% "
                  "(%d later turns)" % (100 * top1, len(test)))


if __name__ == "__main__":
    main()
