"""
Rotation model + walk-forward backtest for one aircraft, run offline against a
flights CSV export (tools/pull_data.sh). Mirrors the dashboard's /api/b748-analysis
model so we can tune and validate it without the DB.

Usage:
    python3 tools/analyze_rotation.py                       # D-ABYN, B748, default targets
    python3 tools/analyze_rotation.py --reg D-ABYO
    python3 tools/analyze_rotation.py --targets RJTT,SAEZ,FAOR --horizon 30
"""
import argparse
import collections
import random
from datetime import date, timedelta

import _lhdata as lh


def _trans_counts(sequences):
    m = collections.defaultdict(collections.Counter)
    for seq in sequences:
        types = [t for _, t in seq]
        for a, b in zip(types, types[1:]):
            m[a][b] += 1
    return m


def predict(tail_seq, fleet_counts, states, today, targets,
            horizon=30, n_sims=4000, alpha=5.0, seed=1234):
    if len(tail_seq) < 3:
        return None
    deps = [d for d, _ in tail_seq]
    gaps = [(deps[i + 1] - deps[i]).days for i in range(len(deps) - 1)
            if (deps[i + 1] - deps[i]).days >= 1]
    if not gaps:
        return None
    tail_counts = _trans_counts([tail_seq])
    state_list = list(states)

    def fleet_p(cur):
        c = fleet_counts.get(cur, {})
        tot = sum(c.values())
        if tot == 0:
            return {s: 1.0 / len(state_list) for s in state_list}
        return {s: c.get(s, 0) / tot for s in state_list}

    def trans_p(cur):
        fp = fleet_p(cur)
        c = tail_counts.get(cur, {})
        tot = sum(c.values())
        return {s: (c.get(s, 0) + alpha * fp[s]) / (tot + alpha) for s in state_list}

    weights = {s: [trans_p(s)[t] for t in state_list] for s in state_list}
    rng = random.Random(seed)
    start_date, start_state = tail_seq[-1]
    occ = {t: [] for t in targets}
    next_dep = collections.Counter()

    for _ in range(n_sims):
        cur, d, first, first_event = start_state, start_date, {}, None
        for _step in range(60):
            d += timedelta(days=rng.choice(gaps))
            cur = rng.choices(state_list, weights=weights[cur])[0]
            if d >= today:
                if first_event is None:
                    first_event = cur
                if cur in targets and cur not in first:
                    first[cur] = d
            if (d - today).days > horizon:
                break
        if first_event:
            next_dep[first_event] += 1
        for t in targets:
            occ[t].append(first.get(t))

    per_target = {}
    for t in targets:
        hits = sorted(x for x in occ[t] if x is not None)
        if hits:
            per_target[t] = (len(hits) / n_sims, hits[len(hits) // 4],
                             hits[len(hits) // 2], hits[min(len(hits) - 1, 3 * len(hits) // 4)])
        else:
            per_target[t] = (0.0, None, None, None)
    total = sum(next_dep.values()) or 1
    return {"per_target": per_target,
            "next_dep": [(k, v / total) for k, v in next_dep.most_common()],
            "gap_median": sorted(gaps)[len(gaps) // 2], "horizon": horizon}


def backtest(tail_types, fleet_counts, states, alpha=5.0, k=20):
    if len(tail_types) < 5:
        return None
    state_list = list(states)
    base_top = collections.Counter(tail_types).most_common(1)[0][0]

    def fleet_p(cur):
        c = fleet_counts.get(cur, {})
        tot = sum(c.values())
        if tot == 0:
            return {s: 1.0 / len(state_list) for s in state_list}
        return {s: c.get(s, 0) / tot for s in state_list}

    hits = top2 = base = n = 0
    for i in range(max(2, len(tail_types) - k), len(tail_types)):
        prefix, cur = tail_types[:i], tail_types[i - 1]
        tc = collections.Counter(b for a, b in zip(prefix, prefix[1:]) if a == cur)
        fp = fleet_p(cur)
        tot = sum(tc.values())
        p = {s: (tc.get(s, 0) + alpha * fp[s]) / (tot + alpha) for s in state_list}
        ranked = sorted(p, key=lambda s: -p[s])
        actual = tail_types[i]
        n += 1
        hits += ranked[0] == actual
        top2 += actual in ranked[:2]
        base += actual == base_top
    return {"n": n, "top1": hits / n, "top2": top2 / n, "base": base / n}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(lh.DEFAULT_CSV))
    ap.add_argument("--reg", default="D-ABYN")
    ap.add_argument("--type", default="B748", dest="ac_type")
    ap.add_argument("--targets", default="RJTT,SAEZ,FAOR", help="target arrival ICAOs (comma-sep)")
    ap.add_argument("--horizon", type=int, default=30)
    ap.add_argument("--sims", type=int, default=4000)
    args = ap.parse_args()

    rows = lh.load(args.csv)
    cmap = lh.build_callsign_routes(rows)
    by_reg = lh.by_registration(rows, args.ac_type)
    if args.reg not in by_reg:
        raise SystemExit(f"{args.reg} not found among {args.ac_type} ({len(by_reg)} tails)")

    fleet_seqs = [lh.outbound_turns(fl, cmap) for fl in by_reg.values()]
    fleet_counts = _trans_counts(fleet_seqs)
    targets = [lh.turn_type(a.strip()) for a in args.targets.split(",")]
    states = sorted({t for s in fleet_seqs for _, t in s} | set(targets) | {"OTHER"})

    tail_seq = lh.outbound_turns(by_reg[args.reg], cmap)
    today = max((d for d, _ in tail_seq), default=date.today()) + timedelta(days=1)

    print(f"== {args.reg} ({args.ac_type}) rotation — {len(tail_seq)} outbound turns, "
          f"as of {today} ==")
    print("  turn mix:", dict(collections.Counter(t for _, t in tail_seq).most_common()))

    bt = backtest([t for _, t in tail_seq], fleet_counts, states)
    if bt:
        print(f"  backtest: top-1 {bt['top1']:.0%} / top-2 {bt['top2']:.0%} "
              f"(base {bt['base']:.0%}) over last {bt['n']}")

    pred = predict(tail_seq, fleet_counts, states, today, targets,
                   horizon=args.horizon, n_sims=args.sims)
    if not pred:
        raise SystemExit("Not enough rotation history to model.")
    print(f"\n  next departure (median gap {pred['gap_median']}d):")
    for turn, p in pred["next_dep"]:
        print(f"    {turn:6} {p:5.0%}")
    print(f"\n  target routes within {pred['horizon']}d:")
    for t in targets:
        prob, q1, med, q3 = pred["per_target"][t]
        if med:
            print(f"    {t:4} P={prob:4.0%}  median {med}  IQR[{q1} .. {q3}]")
        else:
            print(f"    {t:4} P={prob:4.0%}  (not within horizon)")


if __name__ == "__main__":
    main()
