# Booking model — P(target tail operates a flight)

What the `/book` planner's percentages are, what they rest on, and how well
they score. Code: `dashboard/booking_model.py` (shared with the benchmark),
`dashboard/fleet_sim.py` (the fleet simulation on top of it),
`flightstatus/legs.py` (the leg layer it reads), `tools/benchmark_booking.py`
and `tools/benchmark_fleet.py` (walk-forward scoring). Measured 2026-09-24 over 4,579 settled widebody legs
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

## Plan clashes and far-out holds (2026-10-06)

**A published tail that clashes with its own plan rarely flies.** The model
reads every tail's plan as of the moment it predicts (`PlanIndex`,
`tail_as_of`: published tails, and the truth once the truth pass has it). A
publication *clashes* when the plan also has the tail on a flight that overlaps
this one (45 min turn; 0.13% of real consecutive legs turn faster), or its
previous flight lands at another airport. That is 1–13% of publications, and
those hold 25–50% of the time against ~80% for consistent ones 24h out. They
get their own hold cells (three lead groups: ≤24h, 36–72h, 96h+; type →
overall), and the card says which flight it clashes with.

**Far out, a publication holds per type, not per band.** From 120h out (the
D+5..D+9 probe) the 747-8's published tail holds ~13% and the A380's ~48%,
roughly flat across 120–216h. Per-band cells were thin and pulled both toward
the cross-type rate (747-8 16%, A380 39%). The far bands now share one cell
per type — each leg counted once, its far bands averaged — pulled only 5
pseudo-legs toward the other types.

Walk-forward vs the previous model (departures 08-10..10-05): A380 168–192h
1.74/1.66 vs 2.05 log-loss (top-1 47–50% vs 8–18%); 747-8 144h 2.85 vs 3.02;
12h better on every fleet (−0.01 to −0.07); everything else within ±0.01
except the 747-8 at 192–216h (+0.05, n=62, a cold-start artefact: until the
747-8's own far cell fills it borrows the cross-type rate). Clash
publications: predicted 31–45%, observed 25–42%.

## Fleet simulation (2026-10-06)

`p_target` judges each flight on its own, so it could give a tail swap odds on
a flight while that tail was flying to Tokyo, or high odds on two flights that
leave at once. The fleet's physics are close to absolute (`tools/fleet_state_report.py`):
a tail's next leg leaves from where its last one landed (99.5–99.9% for the
747-8, A380 and A350), 99% of hub departures are out-and-back trips, and the
tail that flew was on the ground at the hub at departure 99.2–99.6% of the time,
when only ~7 of 18 747-8s (4 of 8 A380s, 7 of 31 A350s) were.

`dashboard/fleet_sim.py` plays each fleet's next 6 days forward, 400 times,
flight by flight: a flight goes to a tail on the ground at its airport in time;
the tail is then away until it lands at the other end (so a return goes to
whoever flew out). The published tail keeps the flight with `p_target`'s hold
rate, corrected for how often the simulation still has it there; otherwise a
tail on the ground is drawn by its history share, with a bonus when it came in
on the flight's usual inbound connection (the inbound predicts the next
outbound 24% / 54% / 38% for 747-8 / A380 / A350). It runs with every model
build (~1 s for all fleets) in the warmer.

**The served number** is half the simulation, half `p_target` — with the
`p_target` odds of tails that cannot reach the airport in time by *any* path
through the schedule cut to a tenth (`blend_hard`). The simulation alone is too
sure of itself: the tail that flew was often one it had elsewhere (a stale
look, a chain of swaps), so `p_target` stays as a safety net. Cutting that net
by the simulation's sampled positions scored worse; cutting it by the hard
rule scored best (the tail that flew was "unreachable" for 0–1.2% of legs,
nearly all inside 12h: swaps after our last look).

Walk-forward (`tools/benchmark_fleet.py`, departures 08-10..10-05, snapshots
every 6h, both models on identical information), log-loss 24–96h:

| fleet | p_target | served | best band gains |
|---|---|---|---|
| 747-8 | 2.001 | **1.934** | 48h 1.965→1.889, 24h 1.192→1.096 |
| A380 | 1.367 | **1.327** | 12h 0.691→0.578 |
| A350 | 1.773 | **1.584** | 48h 1.980→1.741 (top-1 53→55%) |
| 787 | 1.470 | 1.415 | — not enabled: the simulation alone 1.853 |

Better at every band with ≥100 legs on the three enabled fleets; beyond 6 days
(and before publication) the served number is `p_target`'s. Calibration
matches `p_target`'s (747-8 within ~3 points; the A380/A350 keep the same
over-confidence at the top that the hold rates already had, e.g. A350 94%
predicted → 90% observed). Two flights a tail cannot both fly: the share of
overlapping pairs where some tail gets >105% between them fell (747-8 15→11,
A380 14→8, A350 16→7); the share just above 100% did not (the safety net
renormalised onto the reachable tails) — the simulation's own share is coherent
(6–21 of 7k–117k pairs, from data gaps).

Not enabled for the 787: multi-stop flights are stored by their first leg only
(LH568/569 FRA–LOS–SSG: 110 of its 116 chain breaks) and the broad tier is
looked up sparsely, so the tail that flew was unreachable 5–8% of the time.

`truth_observed_at` (legs.py, first terminal look) lets the replays use a truth
only once it was seen — 18–20h after departure (median), not the 2-day lag the
model assumed before; the dashboard has only truths it has seen.

```bash
python3 tools/fleet_state_report.py            # the physics above
python3 tools/benchmark_fleet.py --type B748   # --conn, --samples, --horizon-h
```

## Horizon probe

The feed returns a tail up to D+9 (D+10: no flight). Since 2026-09-24 the 22:00
pulse also looks at D+5..D+9 for the 747-8/A380 tier (`flightstatus/crontab`).
The pages stay capped at D+4 (`BOOK_HORIZON_DAYS`) until that data is scored:
after ~3 weeks, rebuild the leg export and read the benchmark's 120h..216h rows.
If a tail published 5-9 days out beats the history regime, raise the cap — the
planner then shows real published tails across most of a booking window.

Early look (2026-10-06, 11 probe nights, leave-one-departure-date-out because
the walk-forward is still cold at 168h+): the A380's far tail is real signal
(holds ~48% vs 1 in 8; −0.30 nats/leg against withholding it); the 747-8's is
a ~2× lift for the published tail (10–16% vs 1 in 17) but a wash overall
(+0.01 nats/leg). The benchmark's "unpublished (same now)" column scores each
leg again with the publication withheld — the paired answer to whether a far
publication beats not looking.

```bash
./tools/pull_fis_history.sh
python3 tools/build_leg_outcomes.py --since 2026-07-21
python3 tools/benchmark_booking.py            # --type B748, --no-fit, --no-plan, --hold-m, --swap-mix
```
