"""
When do Lufthansa widebody tail reassignments actually land?

Reads the per-pass FIS history (tools/pull_fis_history.sh), reuses the
leg-outcome builder, and answers the three questions the collection design
turns on:

  1. OBSERVABILITY — at each lead time, what fraction of legs do we have a look
     at? This is the ceiling on everything else: a change is only datable to the
     width of the gap between the two looks that bracket it.
  2. TIMING — at what lead time / hour of day do changes land? Every change sits
     inside a bracket [last look with the old tail, first look with the new
     one], so this is an interval-censored hazard problem, estimated as
     events per hour of exposure.
  3. SETTLING — how far out does the tail that actually flew become final?

Why the null calibration matters. Spreading each change uniformly over its
bracket is the obvious estimator, but with ~23h-wide brackets it flattens any
diurnal pattern by construction, so "looks flat" would prove nothing. Iterating
that spreading to the nonparametric MLE goes the other way and is worse: with
overlapping intervals on a repeating sampling grid it degenerates, parking
spikes on the pass boundaries themselves. So instead of trusting either shape,
the tool simulates the null — changes drawn at a constant rate through the very
same observation windows — pushes it through the same estimator, and reports
whether the real profile is distinguishable from it. A flat *or* spiky profile
inside the null band means the cadence cannot resolve the question, which is a
collection-design answer rather than a measurement.

The regime matters: the collector's cadence changed on 2026-07-15 (per-pass
history) and again on 2026-07-20 (far pass split out, catalog crash fixed), so
compare like with like via --since.

Usage:
    ./tools/pull_fis_history.sh
    python3 tools/reassignment_timing.py --since 2026-07-21
    python3 tools/reassignment_timing.py --since 2026-07-21 --type B748
    python3 tools/reassignment_timing.py --since 2026-07-21 --sims 0   # skip nulls
"""
import argparse
import collections
import math
import random
import statistics
from datetime import timedelta

from build_leg_outcomes import (BERLIN, DEFAULT_CSV, WIDEBODY, _d, _ts, build_leg, load)

# Lead-time bins (hours before scheduled departure), near-first.
LEAD_BINS = [(0, 3), (3, 6), (6, 12), (12, 24), (24, 36), (36, 48),
             (48, 72), (72, 96), (96, 120), (120, 1e9)]

# The collector's slots, by Berlin start hour (flightstatus/crontab), so a
# revealing observation can be attributed to the pass that made it. Inferred
# from the clock because batch_runs has no pass-kind column — keep in sync with
# the crontab, and note that a long sweep's later lookups spill into the
# following hours (a run is ~90 min of point queries, not an instant).
PASSES = [(0, 10, "00:10 pulse"), (2, 0, "02:00 pulse"), (2, 45, "02:45 watch"),
          (3, 30, "03:30 far"), (5, 45, "05:45 sweep-lite"), (9, 0, "09:00 pulse"),
          (10, 30, "10:30 watch"), (11, 35, "11:35 pulse"), (13, 0, "13:00 watch"),
          (14, 10, "14:10 pulse"), (15, 30, "15:30 watch"), (16, 45, "16:45 pulse"),
          (18, 15, "18:15 sweep-full"), (21, 15, "21:15 watch"), (22, 0, "22:00 pulse")]


def bin_label(lo, hi):
    return "%d-%dh" % (lo, hi) if hi < 1e9 else "%d+h" % lo


def which_pass(dt):
    """The scheduled slot an observation belongs to: the latest one that had
    started by then. A run is up to ~90 min of point queries and starts with up
    to 30 min of jitter, so its later lookups land well past its nominal time —
    which this handles correctly, since the next slot is always further out than
    the previous one runs."""
    local = dt.astimezone(BERLIN)
    best = None
    for hour, minute, name in PASSES:
        if (hour, minute) <= (local.hour, local.minute):
            best = name
    return best or PASSES[-1][2]  # before the first slot = last night's


def build(rows, since, want_types, all_types):
    groups = collections.defaultdict(list)
    for r in rows:
        groups[(r["flight_date"], r["airline"], r["flight_number"])].append(r)
    legs, changes, seqs = [], [], {}
    for key in sorted(groups):
        leg, ch = build_leg(key, groups[key])
        if leg is None:
            continue
        if since and _d(leg["flight_date"]) < since:
            continue
        t = leg["fleet_type"]
        if want_types:
            if t not in want_types:
                continue
        elif not all_types and t not in WIDEBODY:
            continue
        legs.append(leg)
        changes.extend(ch)
        seqs[key] = sorted([r for r in groups[key]
                            if r["found"] and r.get("lead_h", 0) > 0 and r["registration"]],
                           key=lambda r: r["observed_at"])
    return legs, changes, seqs


# --- interval-censored hazard ------------------------------------------------
def windows(legs, seqs, resolution_h=1.0):
    """Slice every consecutive pair of looks into fixed-width sub-intervals.

    Each pair is an observation window: its duration is *exposure* in the
    lead-time and hour-of-day bins it spans, and if the tail differs across it,
    one *event* landed somewhere inside — we cannot know where. Returns a list
    of (is_change, [(lead_bin, hour_bin, dt), ...], span_hours).

    Undercounts by construction: two changes inside one window look like one.
    """
    out = []
    for leg in legs:
        key = (leg["flight_date"], leg["airline"], leg["flight_number"])
        pre = seqs.get(key, [])
        dep = _ts(leg["dep_scheduled_utc"])
        for i in range(1, len(pre)):
            a, b = pre[i - 1], pre[i]
            span = (b["observed_at"] - a["observed_at"]).total_seconds() / 3600.0
            if span <= 0:
                continue
            steps = max(int(round(span / resolution_h)), 1)
            dt = span / steps
            slices = []
            for s in range(steps):
                t = a["observed_at"] + timedelta(hours=span * (s + 0.5) / steps)
                lead = (dep - t).total_seconds() / 3600.0
                lb = next((j for j, (lo, hi) in enumerate(LEAD_BINS) if lo <= lead < hi), None)
                if lb is None:
                    continue
                slices.append((lb, t.astimezone(BERLIN).hour, dt))
            if slices:
                out.append((a["registration"] != b["registration"], slices, span))
    return out


def exposure(wins):
    lead_exp, hour_exp = [0.0] * len(LEAD_BINS), [0.0] * 24
    for _, slices, _ in wins:
        for lb, hb, dt in slices:
            lead_exp[lb] += dt
            hour_exp[hb] += dt
    return lead_exp, hour_exp


def fit(wins, flags, lead_exp, hour_exp, em_iters=1):
    """Events per bin, apportioning each change over its bracket.

    em_iters=1 spreads uniformly (interpretable, flattens real structure);
    higher values iterate toward the MLE (sharper, and prone to spiking on the
    sampling grid). Both are reported and both get a null calibration.
    """
    nlead, nhour = len(LEAD_BINS), 24
    lead_h, hour_h = [1.0] * nlead, [1.0] * nhour
    lead_evt = hour_evt = None
    for _ in range(max(em_iters, 1)):
        lead_evt, hour_evt = [0.0] * nlead, [0.0] * nhour
        for (is_change, slices, _), flag in zip(wins, flags):
            if not flag:
                continue
            w = [lead_h[lb] * hour_h[hb] * dt for lb, hb, dt in slices]
            tot = sum(w)
            if tot <= 0:
                w = [dt for _, _, dt in slices]
                tot = sum(w) or 1.0
            for (lb, hb, _), wi in zip(slices, w):
                lead_evt[lb] += wi / tot
                hour_evt[hb] += wi / tot
        lead_h = [(lead_evt[i] / lead_exp[i] if lead_exp[i] else 0.0) for i in range(nlead)]
        hour_h = [(hour_evt[i] / hour_exp[i] if hour_exp[i] else 0.0) for i in range(nhour)]
    return lead_evt, hour_evt


def dispersion(evt, exp):
    """Exposure-weighted spread of the per-bin rate around the pooled rate.

    Zero when the hazard is constant; grows with real structure *and* with
    estimator artefacts — which is why it is only ever read against the null.
    """
    tot_e, tot_x = sum(evt), sum(exp)
    if not tot_e or not tot_x:
        return 0.0
    pooled = tot_e / tot_x
    num = sum(x * ((e / x - pooled) ** 2) for e, x in zip(evt, exp) if x > 0)
    return math.sqrt(num / tot_x) / pooled


def null_band(wins, lead_exp, hour_exp, em_iters, sims, seed=20260726):
    """Dispersion under a constant-rate null through the same windows."""
    n_evt = sum(1 for w in wins if w[0])
    tot_exp = sum(w[2] for w in wins)
    lam = n_evt / tot_exp if tot_exp else 0.0
    rng = random.Random(seed)
    lead_stats, hour_stats = [], []
    for _ in range(sims):
        flags = [rng.random() < 1.0 - math.exp(-lam * w[2]) for w in wins]
        le, he = fit(wins, flags, lead_exp, hour_exp, em_iters)
        lead_stats.append(dispersion(le, lead_exp))
        hour_stats.append(dispersion(he, hour_exp))
    return sorted(lead_stats), sorted(hour_stats)


def verdict(observed, null, label):
    if not null:
        return
    over = sum(1 for v in null if v >= observed)
    p = (over + 1.0) / (len(null) + 1.0)
    p95 = null[int(0.95 * (len(null) - 1))]
    mark = "RESOLVED" if p < 0.05 else "NOT RESOLVED by this cadence"
    print("   %s: dispersion %.3f vs constant-rate null median %.3f (p95 %.3f) "
          "-> p=%.3f  %s" % (label, observed, statistics.median(null), p95, p, mark))


# --- report sections --------------------------------------------------------
def observability(legs, seqs):
    print("\n=== 1. observability: do we have a look at this lead time? ===")
    print("   share of legs with >=1 look in the bin, and mean looks per leg in it")
    print("   %-9s %7s %7s   %s" % ("lead bin", "covered", "looks", ""))
    n = len(legs)
    for lo, hi in LEAD_BINS:
        have = total = 0
        for leg in legs:
            key = (leg["flight_date"], leg["airline"], leg["flight_number"])
            k = sum(1 for r in seqs.get(key, []) if lo <= r["lead_h"] < hi)
            total += k
            have += 1 if k else 0
        bar = "#" * int(round(30.0 * have / n)) if n else ""
        print("   %-9s %6.1f%% %7.2f   %s"
              % (bin_label(lo, hi), 100.0 * have / n if n else 0,
                 total / float(n) if n else 0, bar))
    gaps = []
    for leg in legs:
        key = (leg["flight_date"], leg["airline"], leg["flight_number"])
        pre = seqs.get(key, [])
        for i in range(1, len(pre)):
            gaps.append((pre[i]["observed_at"] - pre[i - 1]["observed_at"]).total_seconds() / 3600.0)
    if gaps:
        gaps.sort()
        print("   gap between consecutive looks: median %.1fh  p75 %.1fh  p90 %.1fh  max %.1fh"
              % (statistics.median(gaps), gaps[int(0.75 * (len(gaps) - 1))],
                 gaps[int(0.90 * (len(gaps) - 1))], gaps[-1]))


def hazard_table(labels, exp, evt, evt_em, title):
    print("\n   %s" % title)
    print("   %-9s %9s %8s %9s %9s   %s"
          % ("bin", "exposure", "events", "per 100h", "per 100h", ""))
    print("   %-9s %9s %8s %9s %9s   %s" % ("", "", "(uniform)", "uniform", "MLE", ""))
    rates = [(evt[i] / exp[i] if exp[i] else 0.0) for i in range(len(labels))]
    peak = max(rates) or 1.0
    for i, lab in enumerate(labels):
        r_em = 100.0 * evt_em[i] / exp[i] if exp[i] else 0.0
        bar = "#" * int(round(36.0 * rates[i] / peak))
        print("   %-9s %8.0fh %8.1f %8.2f %9.2f   %s"
              % (lab, exp[i], evt[i], 100.0 * rates[i], r_em, bar))


def timing(legs, seqs, changes, em_iters, sims):
    print("\n=== 2. timing: when do changes land? ===")
    if not changes:
        print("   (no changes in scope)")
        return
    brackets = sorted(float(c["bracket_h"] or 0) for c in changes)
    print("   %d changes; bracket width median %.1fh  p90 %.1fh"
          % (len(changes), statistics.median(brackets), brackets[int(0.9 * (len(brackets) - 1))]))
    for lim in (3, 6, 12, 24):
        k = sum(1 for b in brackets if b <= lim)
        print("     dated to within %2dh: %4d (%4.1f%%)" % (lim, k, 100.0 * k / len(changes)))

    wins = windows(legs, seqs)
    lead_exp, hour_exp = exposure(wins)
    flags = [w[0] for w in wins]
    lead_u, hour_u = fit(wins, flags, lead_exp, hour_exp, em_iters=1)
    lead_m, hour_m = fit(wins, flags, lead_exp, hour_exp, em_iters=em_iters)
    print("   %d observation windows, %.0f leg-hours of exposure"
          % (len(wins), sum(lead_exp)))

    hazard_table([bin_label(*b) for b in LEAD_BINS], lead_exp, lead_u, lead_m,
                 "hazard by lead time — changes per 100 leg-hours of exposure")
    hazard_table(["%02d" % h for h in range(24)], hour_exp, hour_u, hour_m,
                 "hazard by hour of day (Berlin)")

    if sims:
        print("\n   is the shape real? %d constant-rate simulations through the same windows"
              % sims)
        ln_u, hn_u = null_band(wins, lead_exp, hour_exp, 1, sims)
        verdict(dispersion(lead_u, lead_exp), ln_u, "lead time (uniform)")
        verdict(dispersion(hour_u, hour_exp), hn_u, "hour of day (uniform)")
        ln_m, hn_m = null_band(wins, lead_exp, hour_exp, em_iters, sims)
        verdict(dispersion(lead_m, lead_exp), ln_m, "lead time (MLE)  ")
        verdict(dispersion(hour_m, hour_exp), hn_m, "hour of day (MLE)")

    print("\n   which pass first revealed a change (attribution, not landing time)")
    rev = collections.Counter(which_pass(_ts(c["seen_before_utc"])) for c in changes)
    for k, v in rev.most_common():
        print("     %-18s %4d  %5.1f%%" % (k, v, 100.0 * v / len(changes)))

    fin = sum(int(c["is_final"] or 0) for c in changes)
    print("\n   was the new tail the one that flew?")
    print("     final assignment : %4d  %5.1f%%" % (fin, 100.0 * fin / len(changes)))
    print("     superseded again : %4d  %5.1f%%"
          % (len(changes) - fin, 100.0 * (len(changes) - fin) / len(changes)))


def settling(legs):
    print("\n=== 3. settling: how far out does the tail that flew become final? ===")
    have = [l for l in legs if l["settle_lead_h"] != "" and l["truth_tail"]]
    if not have:
        print("   (no labelled legs in scope)")
        return
    cens = [l for l in have if str(l["settle_censored"]) == "1"]
    obs = [l for l in have if str(l["settle_censored"]) != "1"]
    print("   %d labelled legs: %d already correct at our first look (censored, %.1f%%),"
          % (len(have), len(cens), 100.0 * len(cens) / len(have)))
    print("   %d with the settling bracketed" % len(obs))
    dist = collections.Counter()
    for l in obs:
        v = float(l["settle_lead_h"])
        for lo, hi in LEAD_BINS:
            if lo <= v < hi:
                dist[bin_label(lo, hi)] += 1
                break
    print("   %-9s %6s %6s   %s" % ("lead bin", "n", "share", "(uncensored only)"))
    for lo, hi in LEAD_BINS:
        k = bin_label(lo, hi)
        v = dist.get(k, 0)
        bar = "#" * int(round(40.0 * v / len(obs))) if obs else ""
        print("   %-9s %6d %5.1f%%   %s" % (k, v, 100.0 * v / len(obs) if obs else 0, bar))
    if obs:
        print("   median settle lead (uncensored): %.1fh"
              % statistics.median([float(l["settle_lead_h"]) for l in obs]))
        print("   censored legs settled EARLIER than our first look, so the true "
              "distribution\n      sits further out than this table shows.")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--since", help="only legs with flight_date >= this date "
                                    "(use a regime boundary, e.g. 2026-07-21)")
    ap.add_argument("--type", action="append", default=[])
    ap.add_argument("--all-types", action="store_true")
    ap.add_argument("--em", type=int, default=40,
                    help="EM iterations for the MLE column (default 40)")
    ap.add_argument("--sims", type=int, default=100,
                    help="constant-rate null simulations (0 to skip)")
    args = ap.parse_args()

    rows = load(args.csv)
    for r in rows:
        r["observed_at"] = _ts(r["observed_at"])
        r["dep_scheduled"] = _ts(r["dep_scheduled"])
        r["found"] = (r["found"] or "").strip().lower() in ("t", "true", "1")
        r["registration"] = (r["registration"] or "").strip().upper()

    legs, changes, seqs = build(rows, _d(args.since) if args.since else None,
                                [t.upper() for t in args.type], args.all_types)
    scope = ",".join(t.upper() for t in args.type) or ("all" if args.all_types else "widebody")
    print("scope: %s legs, flight_date >= %s — %d legs, %d changes"
          % (scope, args.since or "(all)", len(legs), len(changes)))
    if not legs:
        return
    observability(legs, seqs)
    timing(legs, seqs, changes, args.em, args.sims)
    settling(legs)


if __name__ == "__main__":
    main()
