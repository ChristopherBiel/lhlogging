# Booking model — P(target tail operates a flight)

What the `/book` planner's percentages are, what they rest on, and how well
they score. Code: `dashboard/booking_model.py` (shared with the benchmark),
`dashboard/fleet_sim.py` (the fleet simulation on top of it),
`flightstatus/legs.py` (the leg layer it reads), `tools/benchmark_booking.py`,
`tools/benchmark_fleet.py` (walk-forward scoring) and `tools/benchmark_ranking.py`
(one tail's flights, ranked). Measured 2026-09-24 over 4,579 settled widebody legs
departing 2026-08-11..09-24, each scored using only what was known before it;
the sections dated later say what they were measured on.

## The question

An award is booked for one exact airframe, typically 1-2 weeks out, but
Lufthansa's feed publishes the tail only a few days ahead (every fleet is read
to D+4, the 747-8 and A380 to D+9, see *Horizon*). So every flight in a
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

## Between the bands (2026-10-13)

A hold cell says how often the tail published at exactly that band flew. The
model read a flight's rate from the largest band not beyond its lead — the
nearer one, where a tail holds more often. A flight 71h out was quoted the 48h
cell. On the same legs (departures 07-21..10-11), the tail as it stood an hour
before the next band against the cell it was quoted from:

| standing at | cell quoted | 747-8 held / quoted | A380 | 787 | A350 |
|---|---|---|---|---|---|
| 95h | 72h | 28% / 39% | 53 / 57 | 54 / 54 | 50 / 53 |
| 71h | 48h | 40% / 56% | 56 / 65 | 56 / 62 | 51 / 57 |
| 47h | 36h | 56% / 66% | 66 / 73 | 62 / 71 | 58 / 68 |
| 35h | 24h | 67% / 78% | 74 / 80 | 71 / 79 | 69 / 77 |
| 23h | 18h | 79% / 82% | 80 / 84 | 80 / 83 | 77 / 83 |

That is the far end of each band's stretch; over a whole stretch the gap is
about half. No benchmark saw it: `benchmark_booking.py` scored at the bands
only, `benchmark_fleet.py` at most 6h off them.

The rate is now interpolated between the two cells a lead lies between
(`booking_model.hold_at`; `n` and the context a card names are the weaker
cell's), and the benchmark also scores three quarters of the way from each
band to the next. Walk-forward over 6,867 legs (departures 08-10..10-12), the
published tail's number there:

| predicted | 50–70% | 70–85% | 85–100% |
|---|---|---|---|
| nearer band's rate: mean → observed | 61.9 → 55.2% | 78.4 → 72.8% | 92.7 → 88.0% |
| interpolated | 61.1 → 60.5% | 78.1 → 77.7% | 91.1 → 90.7% |

Log-loss between the bands moves a little (66h 2.05 → 2.03, 33h 1.40 → 1.38,
10.5h 0.64 → 0.62); on the bands nothing changes, by construction.
`--step-hold` scores the old reading.

## Which legs a band speaks for (2026-10-16)

A band counts only the legs looked at that far out, and the collector has
looked further ahead over time: D+5..D+9 for the 747-8 and A380 since 09-24,
D+4 for the 787/A350 since 10-06. Counted as is, the far bands held recent
legs and the near ones all eight weeks, and /insights drew an A380 tail
published five days out as surer than one four days out (66% against 57%;
the same 131 flights held 66% and then 72%).

`booking_model.corrected_rate`: from the nearest band out, a band's rate moves
by the gap, one band in, between all the legs there and the legs seen at this
band — the recent legs give the step, all of them the level. Where every leg
one band in was also seen at this band (inside four days, nearly always) the
gap is nil. The corrected curve steps by exactly the paired gains of Panel B.
Live, on the export of 10-16: A380 120h 73 → 54%, 144h 67 → 45%, 192h 47 →
24%; 747-8 within two points; A350 96h 35 → 48% (its recent flights churned
more).

/insights draws the corrected curve. The model does not (`HOLD_CHAIN = False`):
walk-forward over 7,251 legs (departures 08-10..10-15) it moved no lead by
more than 0.02 log-loss (A380 144h 1.37 → 1.35, 747-8 168h 2.81 → 2.83).
A first version that chained the plan-consistent cells through legs
consistent at *both* bands shifted the near bands too (A380 96h 59 → 71%) —
that filter differs per band, so pairs on it change what a band measures.

What the far A380 quote misses is open item (3) below, not the coverage: the
tail published 5–7 days out was quoted about 41% walk-forward and held
56–73%. Four weeks of history instead of eight did not close it (far leads
up to 0.29 worse, the A380 at 96h and in at most 0.02 better).

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
| 787 | 1.465 | **1.376** | 48h 1.650→1.537, 24h 1.044→0.956 (after multi-leg flights, below) |

Better at every band with ≥100 legs on all four fleets; beyond 6 days
(and before publication) the served number is `p_target`'s. Calibration
matches `p_target`'s (747-8 within ~3 points; the A380/A350 keep the same
over-confidence at the top that the hold rates already had, e.g. A350 94%
predicted → 90% observed). Two flights a tail cannot both fly: the share of
overlapping pairs where some tail gets >105% between them fell (747-8 15→11,
A380 14→8, A350 16→7); the share just above 100% did not (the safety net
renormalised onto the reachable tails) — the simulation's own share is coherent
(6–21 of 7k–117k pairs, from data gaps).

**Multi-leg flights (migration 013).** The 787 was first left out: the leg
layer kept only a flight's first leg, so LH568/569 FRA–LOS–SSG read as FRA→LOS
and SSG→LOS (110 of its 116 chain breaks; the tail that flew was "unreachable"
5–8% of the time, and the simulation alone scored 1.853). A leg row is now the
whole itinerary (`legs.py`: ends, gate-to-gate time including stops, each leg
in `segments`; a diversion ends where the operated look says). 787 chains went
93.5% → 99.6% continuous, unreachable 1.2–2.2%, the simulation alone 1.491, the
served number 1.376 — enabled. Its served number is slightly less coherent
than p_target's (18 vs 8 of 23.8k overlapping pairs above 105%). What is left
of its fallbacks is coverage: the broad tier is not looked up on every flight
(LH760/761 DEL, LH756/757 BOM often have one direction missing).

`truth_observed_at` (legs.py, first terminal look) lets the replays use a truth
only once it was seen — 18–20h after departure (median), not the 2-day lag the
model assumed before; the dashboard has only truths it has seen.

```bash
python3 tools/fleet_state_report.py            # the physics above
python3 tools/benchmark_fleet.py --type B748   # --conn, --samples, --horizon-h
```

## Horizon: how far ahead a tail is shown (2026-10-13)

The feed returns a tail up to D+9 (D+10: no flight). Since 2026-09-24 the 22:00
pulse also looks at D+5..D+9 for the 747-8/A380 tier (`flightstatus/crontab`);
the pages stayed on D+4 until that was scored.

**The score that decides it is a ranking, not a log-loss.** `benchmark_booking.py`
asks of each flight whether the tail that flew got a fair probability; a
visitor asks of one tail which of many flights to book. A publication that
holds one time in five barely moves the first score and still puts the right
flight on top of the list. `tools/benchmark_ranking.py` ranks, every evening
after the probe and for every tail of a fleet, the fleet's flights of D+5..D+9
— with the tail published then ("open") and with it withheld ("capped", what a
page shows beyond its horizon) — and scores them on what that tail then flew.
Thirteen probe evenings (09-24..10-06, those whose flights had flown by
10-11), shares walk-forward, the far hold rate from the other evenings' legs
(walk-forward it is still cold):

| D+5..D+9 | flights ranked | even guess | capped: top-1 / top-3 / AUC | open: top-1 / top-3 / AUC |
|---|---|---|---|---|
| 747-8 | 96 | 6% | 9% / 7% / 0.53 | **25% / 21% / 0.58** |
| A380 | 48 | 15% | 27% / 26% / 0.63 | **65% / 60% / 0.78** |

(D+1..D+4 for scale: 747-8 79% / 65% / 0.83, A380 90% / 84% / 0.93.) A tail
published that evening flew the flight, by day: 747-8 D+5 26%, D+6 23%, D+7
19%, D+8 19%, D+9 15%; A380 72%, 65%, 52%, 44%, 43%.

The per-flight score agrees, now that there is more of it. Strictly
walk-forward, log-loss with the publication against the same moment without:
747-8 2.71 vs 2.87 at 120h, 2.77 vs 2.89 at 144h, 2.79 vs 2.89 at 168h, 2.85
vs 2.90 at 192h (216h: 3.01 vs 3.00, n=39); A380 1.41 vs 1.98, 1.55 vs 1.97,
1.73 vs 1.97, 1.79 vs 1.99.

**So both fleets are shown as far as they are read.** `BOOK_HORIZON_BY_TYPE`
(dashboard, default `B748=9,A388=9`) names the fleets whose horizon differs
from `BOOK_HORIZON_DAYS` (4, the day every tier is read to). A tail beyond D+4
is labelled **pencilled** on `/book`: an early plan, with its own hold rate on
the card. The 787/A350 stay on D+4 — nothing reads their tails further out —
and the Schedule's gantt keeps D+4 for every fleet (nine days on its axis
would halve its scale; that is a choice still open).

Two things the same change settles:

- **The fleet simulation obeys the horizon.** It read every tail the plan
  had, the probe's included, so since 2026-10-06 a flight five or six days out
  could carry "Not published yet … Playing the fleet forward: 51%" — a number
  moved by a publication its card did not show. A tail beyond its fleet's
  horizon is now withheld from the simulation too (`booking_model.unpublished`).
- **A look older than 36 hours is no publication.** Every day a page shows is
  read at least once a day, so an older look means its reader failed, and a
  stale tail is a much worse bet than its lead says: an A380 published 120h
  out held 83% when the look was under 6 hours old, 69% at 6–18h, 47% beyond
  (144h: 76 / 58 / 34%; departures from 09-29). The monitor counts a day ahead
  as known by the same rule.

**Open.** (1) The far cell is one rate per type over 120–216h; the rate does
fall with the lead (above). Bending the type's flat rate by each band's own
cell scored within ±0.01 of it walk-forward, so it was left out until the far
cells have weeks behind them. (2) How old the look is matters most far out
and is not in the hold rate, only in the 36-hour cut-off. (3) The A380's plan
has firmed up since the end of September — a tail published 96h out held 48%
until 09-28 and 72% from 09-29 (72h: 53 → 72%, 48h: 64 → 74%; the 747-8 did
not move) — and the eight-week window follows that slowly: its A380 numbers
are low for now.

```bash
./tools/pull_fis_history.sh
python3 tools/build_leg_outcomes.py --since 2026-07-21
python3 tools/benchmark_booking.py            # --type B748, --no-fit, --no-plan, --hold-m, --swap-mix, --step-hold, --chain
python3 tools/benchmark_ranking.py            # --type A388, --since, --strict
```

## The timetable beyond what the feed lists (2026-10-13)

Past the days it has read, the planner's candidates are a projection: a flight
number that ran on a weekday in two of the last four weeks is assumed to run
again (`booking_model.project_schedule`). A change of timetable — the last
Sunday of October and of March — makes that wrong for weeks: a flight that
ended stays a candidate until four weeks have forgotten it, a new one appears
after its second run. Three things now bound the damage:

- **What the feed lists ahead counts as a run, and times the projection.** A
  flight listed for next week is projected from that listing on, so the new
  timetable is followed as far as the feed shows it (D+9 for the 747-8/A380,
  D+8 for the 787/A350 once the collector's timetable look runs).
- **The feed's "no such flight" counts too.** A flight it was asked for and
  does not have is not projected onto that day, nor onto later days of that
  weekday while that answer is its latest word. This needs the collector to
  tell an empty answer from a refused lookup, which it does from 2026-10-13
  on (`raw` = `{}`; older rows cannot be told apart and are ignored). Only an
  answer given within eight days counts: at 22:00 the feed does not have day
  9's late departures yet — 86% of day-9 lookups found their flight against
  97–99.6% for days 5–8, and 32 of 34 that did not were there on a later look.
- **A projection across the change says so.** Where a flight's latest run
  lies before the start of the season its date lies in, the card reads "from
  the timetable before Sun 25 Oct".

The projection now also covers D+4 itself: the 787/A350 get that day from the
far pass at noon, and until then it was missing from their plans every morning.

Not done: a projection's UTC time is its last run's shifted by whole days, so
across the change of clocks it is an hour off (the local time is right).
