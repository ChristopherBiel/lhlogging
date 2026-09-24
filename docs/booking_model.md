# Booking model — P(target tail operates a flight)

What the `/book` planner's percentages are, what they rest on, and how well
they score. Code: `dashboard/booking_model.py` (shared with the benchmark),
`flightstatus/legs.py` (the leg layer it reads), `tools/benchmark_booking.py`
(walk-forward scoring). Measured 2026-09-24 over 4,579 settled widebody legs
departing 2026-08-11..09-24, each scored using only what was known before it.

## The question

An award is booked for one exact airframe, typically 1-2 weeks out, but
Lufthansa's feed publishes the tail only a few days ahead (the collector reads
it to D+4; the feed has it to D+9, see *Horizon probe*). So every flight in a
booking window is in one of two states, and the model has one regime for each:

| regime | when | P(target) |
|---|---|---|
| **published** / **swap** | a tail is on the flight now | target published: the hold rate at this lead; other tail published: (1 − hold) × the target's share among the tails that could replace it |
| **history** | nothing published yet | the target's share of that flight's operated legs, shrunk toward the route's and the fleet's |

All shares and hold rates come from the last 8 weeks of settled legs (the
feed's own record of which tail flew, 99.4% complete since 2026-07-21).

## What the data said

**Before publication, a flight's history helps on some fleets and not on
others.** A350 and 787 sub-fleets stick to routes; 747-8 and A380 tails rotate
through the daily slots. Fitting one shrinkage for everyone made the 747-8 and
A380 *worse* than guessing evenly (747-8 log-loss 3.14 vs 2.97 uniform). So the
knobs are fitted per fleet on every build, on a holdout of the last 14 days,
with ties broken toward the more conservative setting. On today's data the fit
chooses: 747-8 → no flight or utilisation signal at all (an even share over the
tails in service); A380 → nearly the same; A350 and 787 → flight history
matters (m = 20 / 80).

**The fleet share is "is it flying", not "how much it flew".** Every active
widebody does ~9-11 legs a week; recent utilisation mostly reflects time spent
in maintenance. A tail idle for 0-3 days is ~100% active a week later; a 747-8
idle 4-7 days ~64%, 8+ days ~0%. Idle tails are down-weighted, with separate
fitted weights for flights under and over 5 days out (an idle tail is often
back from maintenance ten days on — a single harsh weight under-predicted those,
0.4% predicted vs 13% observed). Today the A350/A380/787 fits use 0.2 near /
0.5 far; the 747-8 fit uses 0.2 for both.

**Recency weighting bought nothing** (±0.01 log-loss) and was dropped.

## Scores (lower log-loss is better; top-1 = the model's favourite tail flew)

| scenario | legs | model | uniform over fleet | published-only (today's chip) |
|---|---|---|---|---|
| nothing published, 10 days out | 4579 | **2.67** / 12% | 3.06 / 6% | 3.06 / 6% |
| published, 96h out | 1090 | **2.36** / 35% | 2.67 | 2.38 / 35% |
| published, 72h | 3547 | **2.15** / 46% | 3.01 | 2.30 / 46% |
| published, 48h | 4030 | **1.81** / 58% | 3.00 | 1.94 / 58% |
| published, 24h | 4555 | **1.11** / 77% | 3.02 | 1.21 / 77% |
| published, 6h | 4573 | **0.46** / 92% | 3.01 | 0.50 / 92% |

Before publication, per fleet: 747-8 2.91 vs 2.97 uniform, A380 2.09 vs 2.15,
A350 2.73 vs 3.43, 787 2.65 vs 2.91 — better than an even guess on every fleet,
but for the 747-8 and A380 only marginally. **For those fleets the honest
answer 1-2 weeks out is "about 1 in 16 (747-8) / 1 in 8 (A380) for any flight
while the tail is in service"**; the planner says so rather than rank flights
on noise. The odds move once a tail is published.

Calibration (predicted vs observed, pooled over every leg × tail): history
regime 0.5→0.6%, 1.8→2.1%, 4.6→4.9%, 7.5→7.7%, 13.3→12.8%; swap regime
0.4→0.3%, 1.8→2.2%, 4.1→4.4%, 7.3→7.7%. The published regime under-predicts at
its low end (17.5% predicted → 28% observed, n=219) and is close elsewhere.

## Horizon probe

The feed returns a tail up to D+9 (D+10: no flight). Since 2026-09-24 the 22:00
pulse also looks at D+5..D+9 for the 747-8/A380 tier (`flightstatus/crontab`).
The pages stay capped at D+4 (`BOOK_HORIZON_DAYS`) until that data is scored:
after ~3 weeks, rebuild the leg export and read the benchmark's 120h..216h rows.
If a tail published 5-9 days out beats the history regime, raise the cap — the
planner then shows real published tails across most of a booking window.

```bash
./tools/pull_fis_history.sh
python3 tools/build_leg_outcomes.py --since 2026-07-21
python3 tools/benchmark_booking.py            # --type B748, --no-fit, --hold-m, --swap-mix
```
