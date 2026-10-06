"""
Walk-forward benchmark of the fleet simulation (dashboard/fleet_sim.py) against
the per-flight model (booking_model.p_target), on identical (leg, now) pairs.

The simulation runs once per snapshot (every --snap-h hours, aligned to 00 UTC)
and fleet type, from the plan as it stood then (booking_model.tail_as_of: no
look after the snapshot, truths only once the truth pass has them). Each
settled leg is scored at every scenario lead L (as tools/benchmark_booking.py:
"pre" = 10 days out, then 216h..6h) from the latest snapshot at least L before
its departure, so both models see exactly the same information.

Columns: the per-flight model, then the simulation mixed with it at several
weights (mix=0: simulation alone). Metrics as in benchmark_booking: log-loss of
the tail that flew (lower is better), Brier, top-1. Coherence: of the pairs of
overlapping flights of one type in the next 4 days, how often some tail gets
more than 100% between the two (impossible: it can only fly one).

Not quite a forecast in one respect: the schedule the simulation flies is the
legs that actually operated (cancellations dropped), not a timetable guess.

Usage:
    python3 tools/benchmark_fleet.py --type A388
    python3 tools/benchmark_fleet.py --type B748 --conn 3 --samples 400
"""
import argparse
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard"))
sys.path.insert(0, str(ROOT / "tools"))
import booking_model as bm  # noqa: E402
import fleet_sim as fs  # noqa: E402
from benchmark_booking import (DEFAULT_CSV, EPS, PRE_LEAD_H, REL_BINS,  # noqa: E402
                               SCENARIOS, TRUTH_LAG, load)

# (label, mix, floor): label "model" is p_target alone; "sim" the simulation
# alone; "raw" mixes p_target in as is (it can put a tail on two flights at
# once); "hard" via fleet_sim.blend_hard (p_target's weight cut for tails that
# cannot reach the airport in time: to 0, or to a tenth with "hard.1")
MIXES = (("sim", 0.0, None), ("raw .25", 0.25, "raw"), ("raw .5", 0.5, "raw"),
         ("hard .25", 0.25, "hard"), ("hard .5", 0.5, "hard"), ("hard .5 f.1", 0.5, "hard.1"))
SERVED = "hard .5 f.1"
COHERENCE_DAYS = 4


def floor_snap(t, snap_h):
    day = t.replace(hour=0, minute=0, second=0, microsecond=0)
    return day + timedelta(hours=snap_h * ((t - day).total_seconds() // (3600 * snap_h)))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--type", required=True, help="fleet type to score (one per run)")
    ap.add_argument("--warmup-days", type=int, default=21)
    ap.add_argument("--snap-h", type=int, default=6)
    ap.add_argument("--samples", type=int, default=fs.N_SAMPLES)
    ap.add_argument("--conn", type=float, default=fs.DEFAULTS["conn"])
    ap.add_argument("--horizon-h", type=float, default=fs.HORIZON.total_seconds() / 3600,
                    help="simulate this far ahead; beyond it the served number is the model's")
    ap.add_argument("--all-legs", action="store_true",
                    help="score every leg at every lead, those with nothing published yet "
                         "too (compares collection schedules on the same legs)")
    ap.add_argument("--quiet", action="store_true", help="scores only, no reliability tables")
    args = ap.parse_args()
    ftype = args.type.upper()

    legs = load(args.csv)
    legs.sort(key=lambda l: l["dep_utc"])
    plan = bm.PlanIndex(legs)
    start = legs[0]["dep_utc"] + timedelta(days=args.warmup_days)
    evals = [l for l in legs if l["truth_tail"] and not l["cancelled"]
             and l["fleet_type"] == ftype and l["dep_utc"] >= start]
    print("%s: %d legs scored, departing %s .. %s; snapshots every %dh, %d samples, conn=%g, "
          "horizon %gh" % (ftype, len(evals), start.date(),
                           evals[-1]["dep_utc"].date() if evals else "-",
                           args.snap_h, args.samples, args.conn, args.horizon_h))

    stats_cache, conn_cache, sims = {}, {}, {}

    def day_cut(now):
        return datetime.combine(now.date(), datetime.min.time(), tzinfo=now.tzinfo) - TRUTH_LAG

    def stats_for(now):
        key = now.date()
        if key not in stats_cache:
            stats_cache[key] = bm.build_stats(legs, day_cut(now), plan=plan)
            conn_cache[key] = fs.connections(legs, day_cut(now))
        return stats_cache[key], conn_cache[key]

    def sim_for(snap):
        if snap not in sims:
            st, conn = stats_for(snap)
            sims[snap] = fs.simulate(
                st, legs, snap, ftype, timedelta(hours=args.horizon_h),
                n=args.samples, knobs={"conn": args.conn}, conn=conn,
                seed=int(snap.timestamp()) + sum(map(ord, ftype)))
        return sims[snap]

    names = ["model"] + [label for label, _, _ in MIXES]

    def served(res, leg, model, label):
        _, mix, floor = next(m for m in MIXES if m[0] == label)
        if floor == "raw":
            sim = res.dist(leg) or {}
            return {x: (1 - mix) * sim.get(x, 0.0) + mix * p for x, p in model.items()}
        return fs.blend_hard(res, leg, model, mix, 0.0 if floor == "hard" else 0.1)
    agg = defaultdict(lambda: defaultdict(float))
    rel = defaultdict(lambda: [[0, 0.0, 0] for _ in REL_BINS[:-1]])
    for i, leg in enumerate(evals):
        if i and i % 200 == 0:
            print("  .. %d/%d legs, %d simulations" % (i, len(evals), len(sims)), file=sys.stderr)
        for sc in SCENARIOS:
            lead = PRE_LEAD_H if sc == "pre" else sc
            snap = floor_snap(leg["dep_utc"] - timedelta(hours=lead), args.snap_h)
            pub = bm.tail_as_of(leg, snap)
            if sc != "pre" and pub is None and not args.all_legs:
                continue
            if sc == "pre" and pub is not None:
                continue  # "pre" means nothing published yet
            st, _ = stats_for(snap)
            res = sim_for(snap)
            fleet = set(st.fleet.get(ftype) or ())
            truth = leg["truth_tail"]
            cands = fleet | {truth} | ({pub} if pub else set())
            model = {x: bm.p_target(st, x, leg, published=pub, now=snap)["p"] for x in cands}
            covered = leg in res
            dists = {"model": model}
            for label, _, _ in MIXES:
                dists[label] = served(res, leg, model, label) if covered else model
            a = agg[sc]
            a["n"] += 1
            a["covered"] += covered
            a["truth_unreachable"] += covered and not res.reachable(leg, truth)
            for name, dist in dists.items():
                pt = max(dist.get(truth, 0.0), EPS)
                a[name + "_ll"] += -math.log(pt)
                a[name + "_brier"] += sum((dist.get(x, 0.0) - (x == truth)) ** 2 for x in cands)
                a[name + "_top1"] += max(cands, key=lambda x: dist.get(x, 0.0)) == truth
            for name in ("model", SERVED):
                for x, p in dists[name].items():
                    for b in range(len(REL_BINS) - 1):
                        if REL_BINS[b] <= p < REL_BINS[b + 1]:
                            cell = rel[(name, "pub" if pub else "pre")][b]
                            cell[0] += 1
                            cell[1] += p
                            cell[2] += x == truth
                            break

    print("\n=== scores by scenario: log-loss / top-1 (lower log-loss is better) " + "=" * 10)
    print("%-5s %5s %4s | " % ("scen", "legs", "sim") + " | ".join("%-14s" % n for n in names))
    for sc in SCENARIOS:
        a = agg.get(sc)
        if not a:
            continue
        n = a["n"]
        label = "pre" if sc == "pre" else "%dh" % sc
        print("%-5s %5d %3.0f%% | " % (label, n, 100 * a["covered"] / n) + " | ".join(
            "%5.3f / %3.0f%%  " % (a[nm + "_ll"] / n, 100 * a[nm + "_top1"] / n) for nm in names))
    tot = defaultdict(float)
    for sc in SCENARIOS:
        if sc in (96, 72, 48, 24):
            for k, v in agg.get(sc, {}).items():
                tot[k] += v
    if tot["n"]:
        print("24-96h pooled (%d) | " % tot["n"] + " | ".join(
            "%5.3f" % (tot[nm + "_ll"] / tot["n"]) for nm in names))
    diag = defaultdict(int)
    for r in sims.values():
        for k, v in r.diag.items():
            diag[k] += v
    print("simulation fallbacks (samples): %s" % (dict(diag) or "none"))
    gaps = defaultdict(int)
    for r in sims.values():
        for k, v in r.gaps.items():
            gaps[k] += v
    if gaps:
        print("  most frequent (fallback, airport, flight): " + ", ".join(
            "%s %s LH%s %d" % (k + (v,)) for k, v in sorted(gaps.items(), key=lambda kv: -kv[1])[:5]))
    print("tail that flew was unreachable from where the plan had it: " + ", ".join(
        "%s %.1f%%" % ("pre" if sc == "pre" else "%dh" % sc,
                       100 * agg[sc]["truth_unreachable"] / max(agg[sc]["covered"], 1))
        for sc in SCENARIOS if agg.get(sc) and agg[sc]["covered"]))

    # coherence: overlapping pairs in the next COHERENCE_DAYS of a sample of snapshots
    viol = defaultdict(int)
    pairs = 0
    snaps = sorted(sims)[::8]
    for snap in snaps:
        st, _ = stats_for(snap)
        res = sims[snap]
        fleet = sorted(st.fleet.get(ftype) or ())
        near = [l for l in legs if l["fleet_type"] == ftype and not l["cancelled"]
                and snap < l["dep_utc"] <= snap + timedelta(days=COHERENCE_DAYS) and l in res]
        model_p, mixed_p = {}, {}
        for l in near:
            pub = bm.tail_as_of(l, snap)
            k = bm.leg_key(l)
            model_p[k] = {x: bm.p_target(st, x, l, published=pub, now=snap)["p"] for x in fleet}
            mixed_p[k] = served(res, l, model_p[k], SERVED)
        for i, l1 in enumerate(near):
            e1 = bm.arr_utc(l1) or l1["dep_utc"]
            for l2 in near[i + 1:]:
                if l2["dep_utc"] >= e1 + bm.TURN:
                    break
                pairs += 1
                k1, k2 = bm.leg_key(l1), bm.leg_key(l2)
                for name, d in (("model", model_p), ("served", mixed_p)):
                    worst = max(d[k1][x] + d[k2][x] for x in fleet)
                    viol[name] += worst > 1.0 + 1e-9
                    viol[name + " >1.05"] += worst > 1.05
                if any((res.p(l1, x) or 0) + (res.p(l2, x) or 0) > 1.0 + 1e-9 for x in fleet):
                    viol["fleet"] += 1
    if pairs:
        print("coherence: %d overlapping flight pairs (%d snapshots) — a tail above 100%% "
              "(above 105%%) between the two: model %d (%d), simulation %d, %s %d (%d)"
              % (pairs, len(snaps), viol["model"], viol["model >1.05"], viol["fleet"], SERVED,
                 viol["served"], viol["served >1.05"]))

    if args.quiet:
        return
    print("\n=== reliability: predicted P(target) vs how often it flew " + "=" * 18)
    for (name, part), bins in sorted(rel.items()):
        print("  %s, %s" % (name, "published" if part == "pub" else "nothing published"))
        print("    %-12s %8s %9s %9s" % ("p bin", "pairs", "mean p", "observed"))
        for b, (n, sp, hits) in enumerate(bins):
            if n:
                print("    %4.0f-%3.0f%%    %8d %8.1f%% %8.1f%%"
                      % (100 * REL_BINS[b], min(100, 100 * REL_BINS[b + 1]), n,
                         100 * sp / n, 100 * hits / n))


if __name__ == "__main__":
    main()
