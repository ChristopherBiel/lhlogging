# Flight-status collection schedule

How the FIS reader decides what to ask Lufthansa's feed for, and the numbers
behind it. Companion to `flightstatus/crontab` (the schedule),
`flightstatus/plan.py` (the read plan) and `flightstatus/fetch_flightstatus.py`
(the run modes). Since 2026-11-01 the reader follows the read plan below; the
fifteen passes a day after it were the schedule until then, and their
measurements are what the plan was built from.

## The read plan (2026-11-01)

From 27 October the feed was shut most of the day (see "When the feed is shut"
below): a page load without the bot check, every lookup refused. The fifteen
passes could not finish, and the pages ran days behind. Their ~2,270 lookups a
day were also more than the data needs: only 2.5–10% of the reads brought a
change. The read plan asks for about a third of that, and for what matters
most first, so that what the feed lets through is spent well.

**One kind of run, every hour** (`--due`, at seven past, with up to ten minutes
of random wait). Each run asks for what has fallen due since the last one, and
reads while the feed answers, until shortly before the next run is due (that
slot less `FIS_DOOR_MARGIN_MIN`); what is left, that run plans again. A run
with nothing due opens no browser. When the feed is shut it waits for it as
before (`door.py`), and the next run, finding that the run before stopped for
that, waits at its first shut session instead of its third. Until 7 Nov a run
stopped at 150 lookups (`FIS_DUE_MAX`, now 0: no cap) whether the feed was open
or not: on 5–6 Nov five runs ended so with up to 373 due, and on 5 Nov most of
two hours of open feed went unused between them.

**Pools.** Tails swap almost only within their pool: of the changes on flights
departing 12–26 Oct, 93–100% brought a tail of the same pool. A pool is
therefore read completely or not at all — what the fleet simulation and the
hold rates need:

| pool | tails | legs a day | numbers | swaps within | level | read to |
|---|---|---|---|---|---|---|
| 747-8 | 17 | 21 | 29 | 100% | close | D+9 |
| A380 | 8 | 10 | 14 | 100% | close | D+9 |
| A350 Allegris (Munich) | 10 | 14 | 29 | 93% | off (near until 7 Nov) | – (D+9) |
| 787 (Frankfurt) | 20 | 31 | 54 | 100% | off (near until 7 Nov) | – (D+4) |
| A350 Munich, without Allegris | 16 | 24 | 46 | 99% | off (near until 7 Nov) | – (D+4) |
| A350 Frankfurt | 5 | 6 | 13 | 98% | off | – |

**Only the 747-8 and the A380, since 2026-11-07.** The feed let 56–1,365
lookups a day through from 30 Oct to 5 Nov, in spells of minutes to a few hours,
and a 24 h pause (5–6 Nov 16:00) did not open it for longer: the evening after
looked like the evenings before. That is too little for every pool, so the
A350 and 787 pools are off: no read at all, not near their departure, not of a
watched tail (D-AIXL, D-ABPU), not after a swap. The pages show the 747-8 and
the A380 only (`dashboard/app.py`, `_SCHEDULE_TYPES` and `_INSIGHT_FAMILIES`).
A 747-8 or A380 on a number of theirs is still read, as a 747-8 or A380.
`FIS_POOLS=a350-allegris=near:9,b787=near:4,a350-muc=near:4` reads them again
as until then; the pages need their types back too.

A tail's pool comes from its type, its cabin (`airframe_cabin`: Allegris
published in the last 45 days) and the hub ADS-B saw it at most in two weeks
(`plan.pool_of`). A flight number belongs to every pool that flew a third of
its legs or more in two weeks (the feed's looks and ADS-B's callsigns
together, three legs at least: a charter flown once is no flight of a pool),
and a flight day is read at the highest level of its number's pools and its
tail's pool: a 747-8 swapped onto an A350 number is read as a 747-8. The
A350 Frankfurt is left out because nothing there is wanted; ADS-B still logs
its flights. `FIS_POOLS` changes a pool's level and days
(`a350-fra=near:4` reads it again).

**Levels.** Every flight day of a pool that reads is read once a day, out to
the pool's last day ("day"): the data stays whole, and the pages can trust a
look for 36 hours. On top:

- *near*: a read within 12 h and one within 3 h of a hub departure. Replayed
  on the October looks (legs departing 1–26 Oct), the tail shown at departure
  is the one that flies 99.3–100% of the time instead of 95–98.5%.
- *close*: near, and a read every 6 h one to three days out. A change then
  shows within hours: for the 747-8 it is dated to a 5.4 h window at 1–2 days
  instead of 24 h, and 90% are seen within 3.3 h instead of 18.3 h (A380: 7.8 h
  instead of 26 h). This is the sampling grid the timing analyses learn from,
  and it covers the whole of both fleets, so it is not biased toward any tail.

The far days hold well enough to show: a published A380 tail flies it 78% of
the time five days out and 58% at eight; the 747-8 26% and 16% — three times a
random pick — and an Allegris cabin, once published, is Allegris on the day
99–100% of the time (its tail 54% four days out).

**Return legs.** A flight's `previousFlight` is the airframe's flight before.
At an outstation that is the outbound that brought it there, and the two carry
the same tail: 99.8% of the return legs' looks agreed with their outbound's
own. So an outbound's daily and 6-hourly reads are its return leg's (the work
item `covers` it); the outbound is asked for itself only for its first read,
its near reads and its flown tail. A run checks that the return leg still
names it, and reads the outbound itself if not. The leg layer takes such a
look as the outbound's where its own newest look is more than 6 h older
(`legs.stand_ins`; schedule, status and cabin stay its own). On the looks of
29 Oct–1 Nov this filled 252 of 747 widebody legs and changed no published
tail; it caught LH772's change to D-AIMM 10.7 h before departure, where the
outbound's own looks only showed it after it had left.

**Watched tails** (`FIS_WATCH_TAILS`, default D-AIMH, D-ABYN, D-AIXL, D-ABPU,
and with `FIS_WATCH_LIVERIES=1` every widebody the feed has shown in a special
livery in 30 days: D-ABPU "LH 100 years", D-ABYP "1500th 747", D-ABYT "Retro")
are read every 6 h from today to D+3 in any pool that is read (not D-AIXL and
D-ABPU while the A350 and 787 are off), and near their hub departures. When a read shows a watched tail leave a flight or take one, the
run reads the flights the other tail is shown on: in a swap, that is where the
watched tail went (a single such read found it 47% of the time).

**Idle tails.** A tail ADS-B has not seen fly for 36 h is likely to lose its
flights: 12–72 h out, a 747-8 or A380 tail idle that long changed within 24 h
36% of the time, an active one 18% (15% of all changes came after such an
idle spell). Its flights from today to D+3 get the 6-hourly reads. When more
than 40% of the fleet looks idle, the ADS-B log is behind, not the fleet, and
nobody counts as idle.

**Flown tails from ADS-B.** The truth pass read every flight after it flew.
Now the tail that flew it comes from ADS-B: an aircraft logged under the
callsign `DLH<number>` leaving within 3 h of the scheduled departure, if it is
the only one (`legs.flown_tails`). On the legs of 1–26 Oct that found the tail
of 91% of the 747-8/A380 legs and 83% of the others, and never a wrong one;
the rest fly under tactical callsigns or out of the receivers' reach. A flight
ADS-B did not see is asked for 3 h after it landed, again 12 h later, up to
three days after departure ("flown"). The leg layer stores such a truth with
`truth_status` `ADSB` (the feed's own word, `ARRIVED`/`DIVERTED`, wins where
there is one). In the week to 1 Nov, while the feed was shut, 183 widebody legs
had no flown tail; ADS-B gives one for 165 of them.

**The order** when the feed lets a run read only part of what is due:

| class | what |
|---|---|
| 0 | near reads of every pool and of the watched tails, and where a watched tail went after a swap |
| 1 | the 747-8 and A380 daily reads (D0..D+9), and their flights not read yet |
| 2 | their 6-hourly reads, the watched and the idle tails' |
| 3 | A350 Allegris to D+4 (empty since 7 Nov: the pool is off) |
| 4 | 787 and A350 Munich to D+2 (empty since 7 Nov) |
| 5 | flown tails ADS-B did not see (the feed keeps a few days of the past) |
| 6 | the far days: Allegris D+5..D+9, 787 and A350 Munich D+3..D+4 (empty since 7 Nov) |
| 7 | numbers no pool has flown lately (one read a day each, so a new flight finds its pool) |

Inside a class the nearest day first, then the earliest flight. About 350
lookups a day keep the 747-8 and the A380 complete. A flight a read names that
the feed was never asked about (a chain) takes the class of the read that
named it: it is nearly always a day the plan reads anyway, often a far one.

Until 5 Nov the near reads of the other pools came in their pool's class (3
and 4), and a swap's follow-ups in class 2. In the hourly plan's first 18
hours the feed let 160 lookups through, the 747-8/A380 days to D+9 took 158 of
them, and no 787 or A350 was read, not even 3 h before it left; 16 follow-ups
after a watched tail's swap queued behind them and were dropped when the feed
shut. The near reads are few at any time: 15 due in a run on average, 33 at
most, in those 18 hours.

**Day nine.** The feed lists a flight from about nine days before it leaves;
asked earlier, it says "no such flight" for a flight it lists later that day
(14% of the day-9 lookups at 22:00). A day nine out is asked for once its
number's usual departure is that close.

**Budget.** `tools/plan_preview.py --days 4` replays the plan hour by hour on
the production rows, as if the feed answered every read. On the rows of
1 Nov 2026 (17:25 UTC): about 810 lookups a day once the backlog is read —
747-8 ~280, 787 ~165, A380 ~160, A350 Allegris ~155, A350 Munich ~50 — where
the fifteen passes made ~2,270. The 6-hourly reads of the 747-8/A380 are the
largest part (~255); the replay of 1 Nov had put the plan at ~700, but it
could count D+3 only as often as the passes had read it (about twice a day).
The first runs after a shut spell read the backlog while the feed answers; at
night, when the feed is often shut, what falls due waits for the morning, so
the real count is somewhat lower.

With the 747-8 and the A380 only (the rows of 7 Nov 2026, 01:01 UTC): about
390 lookups a day — 747-8 ~272, A380 ~115, numbers no pool has flown 3 — and
the census's ~47. At a lookup every ~11 s that is about 70 minutes of open
feed a day; it let more through on five of the seven days to 5 Nov.

**What a run records.** `batch_runs.coverage` of a run of the plan holds
`plan: "due"` (written as it starts: the monitor tells the hourly runs from the
passes by it), what fell due per pool and reason, what was read, what was left
for the next run, how many outbound days a return leg stood in for, the chains
followed, the watched tails' follow-ups, and the watched and idle tails. A
flight day not read because the feed was shut is due again in the next run,
and one read counts for it however late: a day the feed was shut shows as
reads that did not happen, never as days without a change.

**Upkeep.** The catalog's discovery, pairing and lifecycle (the sweeps did
them twice a day) run once a day, in the first run 20 h after the last;
a number accrues a miss only on a day it was asked about, so the numbers of
a pool that is not read keep their place.

**Dropped:** the timetable look (`FIS_TIMETABLE_OFFSETS`): the plan reads the
days the pages show; the booking planner's projection covers the 787/A350
beyond D+4 again, as before 2026-10-30. The modes of the passes stay for runs
by hand.

```
python3.12 tools/plan_preview.py               # what the next run would read (read-only, on production rows)
python3.12 tools/plan_preview.py --days 3      # and three days of hourly runs: lookups a day by pool
docker compose exec flightstatus python fetch_flightstatus.py --due --dry-run    # the same, in the container
```

---

The rest of this document is the schedule until 2026-10-31 and what was
measured on it.

## The question the schedule is designed around

We want to predict, ahead of departure, which scheduled widebody legs will have
their assigned tail reassigned — and, underneath that, to know *when*
reassignments actually happen. FIS gives us the published assignment at whatever
moments we choose to look, so the collection design is a sampling problem: the
data can only ever date a change to the gap between two consecutive looks.

## What the data said (measured 2026-07-26)

Built from `tools/pull_fis_history.sh` → `tools/build_leg_outcomes.py` →
`tools/reassignment_timing.py`, over the 410 labelled legs departing
2026-07-21..24 (the first four days under a stable cadence — see *Regime
boundaries* below).

**The published tail is wrong far more often than it feels.**

| lead | 72h | 48h | 24h | 12h | 6h | 3h | last look before departure |
|---|---|---|---|---|---|---|---|
| wrong | 45% | 41% | 24% | 14% | 8% | 4% | 0.2% |

**The hazard is almost flat in lead time.** Estimated as changes per 100
leg-hours of exposure, so the sampling pattern is normalised out:

| band | D0 0–24h | D+1 24–48h | D+2 48–72h | D+3 72–96h | D+4 96–120h | D+5+ |
|---|---|---|---|---|---|---|
| changes /100 leg-h | 1.05 | 1.37 | 1.68 | 1.18 | 1.53 | 1.64 |

There is no decision moment to catch. Reassignment behaves like a roughly
constant-rate process out to five days, tapering only inside the last ~6h. The
consequence for modelling is that the target is a calibrated probability, not a
snapshot to chase; the consequence for collection is that **more lookahead adds
nothing that more frequent looks don't add better**.

**But a far-out look is worth little.** P(a change first seen at lead L is the
tail that actually flew):

| lead | <6h | 6–12h | 12–24h | 24–48h | 48–96h | 96h+ |
|---|---|---|---|---|---|---|
| is final | 89% | 82% | 53% | 31% | 5% | 0% |

D+4 and D+5 lookups were therefore recording churn that gets superseded before
it means anything.

**Time of day is not recoverable at two looks a day.** Median bracket was 23.4h,
and *zero* of 189 B748 changes were dated to within 6h. Both estimators fail in
opposite directions: spreading each change uniformly over its bracket flattens
any real pattern by construction, and iterating that to the nonparametric MLE
degenerates, parking spikes on the pass boundaries themselves. Simulating a
constant-rate null through the same observation windows confirms neither shape
is distinguishable from noise (p=0.079 uniform, p=0.42 MLE). Lead-time structure
*is* resolvable at that cadence (p=0.02); hour-of-day is not.

**Reassignments are swaps, not independent per-leg events.** 98.6% of the time
the incoming tail is simultaneously released from another leg, 84% of the time
on the same date. Predicting *which* tail a leg becomes is a fleet-wide
assignment problem, not a per-leg property.

## The design that follows

Split the catalog by fleet type and spend the far-lead budget on cadence:

| tier | types | numbers | coverage |
|---|---|---|---|
| **deep** | B748, A388 | ~33 | D-2…D+2 sweeps (+1 day), D+1…D+4 far, **7 pulse passes/day at D+1/D+2**, D+5…D+9 probe |
| **wide** (since 2026-10-06) | B789, A359 (787/A350) | ~90 | D-2…D+2 sweeps (+1 day), D+3/D+4 far; one look at each flight day of D+5…D+8, for the timetable (2026-10-13) |
| **broad** | everything else (chained short-haul legs, strays) | — | D-2…D+2 sweeps, single far look at D+3 |

Tiering is resolved from the *observed* tail — the modal fleet type of each
flight number over the last `FIS_DEEP_TIER_DAYS` (6) days, joined through
`aircraft`. Deliberately not `fis_flight_catalog.seed_type`: that column records
why a number was first added and has drifted so far it currently labels no
number A388 at all. Modal rather than "ever seen" keeps a one-off A350
substitution on LH424 from dropping it out of the tier mid-week.

Three smaller changes close the gaps this opened up:

- **sweep-lite also takes deep D+2** and **sweep-full also takes deep D+3**. The
  latter closes a 39h hole between the far pass (~03:35 on F-3) and sweep-full
  (~18:30 on F-2) — the seam that used to leave a D+2/D+3 reassignment stale for
  most of two days. The former keeps D+1 and D+2 on an *identical* sampling
  pattern, so comparing their hazards isn't confounded by the sampling grid.
- **the far pass carries the deep tier's D+1/D+2** as well, and asks them first
  (nearest dates first), so it stands in for a pulse: until 2026-10-11 for the
  one missing between 03:00 and 05:00, since 2026-10-12 for the midday one
  (see "The far pass by day" below).

Result for a deep-tier leg — 10 looks/day at D+1/D+2, max gap ~2.8h:

```
02:00p → 03:30p → 06:30lite → 09:00p → 11:20far → 14:10p → 16:45p → 19:20full → 22:00p → 00:10p → 02:00p
   1.5h     3.0h       2.5h      2.3h      2.8h      2.6h      2.6h       2.7h      2.2h      1.8h
```

(Before 2026-10-12: `… 02:00p → 03:40far → 06:30lite → 09:00p → 11:35p → 14:10p …`.)

Expected yield: ~23 reassignments a day dated to under 3h, against zero dated to
within 6h before.

## Request budget

Distil block risk scales with aggregate request rate and burst shape, so every
cadence change is costed. Measured per-slice: a deep-tier date slice is 33
lookups, a broad slice ~88.

| | before | after |
|---|---|---|
| slots/day | 8 | 15 |
| lookups/day | ~1332 | ~1700 (+27%) |
| browser-active | ~4.2h | ~5.4h |
| largest single pass | 444 | 467 |
| pulse pass size | — | 66 (one Distil session) |

Pacing (5–10s), session recycling (`FIS_SESSION_LOOKUPS`=80), and the one-run-at-
a-time flock are all unchanged. A pulse is smaller than several existing watch
passes, so the burst shape the block risk actually depends on is no different —
there are simply more of them.

## Wide tier: the deep tier's cheap extras for the 787/A350 (2026-10-06)

Upgrading the 787/A350 to the deep schedule would roughly double the request
volume (+~1,660 lookups/day: each deep number costs ~28 lookups a day, a broad
one ~6.7). To see which parts of the deep schedule earn their cost, the deep
tier's own history was thinned to the broad schedule — pulses, the far pass's
D+1/D+2/D+4, the sweeps' extra day and the probe removed — and the booking
model (`tools/benchmark_fleet.py --all-legs`, identical legs) scored on each
variant. Served log-loss, mean over 24/48/72/96h (lower is better):

| variant | B748 | A388 | lookups/number/day over broad |
|---|---|---|---|
| full deep schedule | 2.009 | 1.405 | ~21 |
| extras, no pulses (far D+1…D+4, sweeps +1 day, probe) | 2.021 | 1.427 | ~10 |
| **lean: sweeps +1 day, far D+4** | 2.029 | 1.431 | **~3** |
| pulses only | 2.064 | 1.462 | ~14 |
| broad schedule | 2.130 | 1.523 | 0 |

The lean extras recover ~80% of the deep schedule's 24–96h advantage (96h top-1
B748 27% vs 14%, A388 44% vs 32%); the pulses buy the last ~20%, mostly inside
24h; the far pass's D+1/D+2 add almost nothing on top of the morning sweep
2.5h later. The 787/A350 should gain at least as much: at equal lead a fresh
snapshot holds markedly more often (A359 48h: 66% if ≤3h old vs 52% if 12h+;
its median snapshot age at 48h was ~25h), and in July, when the broad tier
still looked at D+4/D+5, the published tail held 54% (A359) / 50% (B789) 120h
out — A380-like.

So the wide tier gets the lean extras: **+~234 lookups/day (~+13%)**, the far
pass ~303, sweep-lite ~480, sweep-full ~620 (`FIS_MAX_LOOKUPS` raised to 800 to
keep the headroom). Not taken: the wide-tier pulses (+~1,090/day for the last
~20%) and a D+5..D+9 probe for it (+~390/day) — the latter only pays off once
the booking pages show tails past D+4 (`BOOK_HORIZON_DAYS`). Blocking comes in
waves (4–8% of lookups late Aug/early Sep and 09-28..10-02) that did not track
volume at ~1.6–2k/day; what a doubled volume would do is unmeasured.

**Fleet types.** `fix_fleet_types` (after every run and before
`--rebuild-legs`) re-types a tail FIS keeps publishing on widebody flights when
`aircraft` calls it something that isn't a widebody: the fleet jobs enrich new
tails from the OpenSky registry, which still lists D-ABQA..D-ABQD as the Dash 8
Q400s that carried those registrations before Lufthansa's 787-9s, so their
flights had fallen out of every 787 set (and of the wide tier).

## Regime boundaries

Any analysis of this data must filter on `flight_date`, because the cadence has
changed several times and staleness statistics are meaningless across the seams:

| from | regime |
|---|---|
| 2026-07-15 | migration 009: per-pass history (before this, one row per day per flight — later runs overwrote earlier ones) |
| 2026-07-17…19 | **sweeps crashing** (zero-padded chained flight numbers, fixed in e9ec83e) — watch passes only |
| 2026-07-21 | far pass split out (50df930); first stable 8-slot regime |
| 2026-07-26 | this schedule: tiering + pulses |
| 2026-09-24 | 22:00 pulse also probes the deep tier at D+5..D+9 |
| 2026-10-06 | wide tier (787/A350): sweeps +1 day, far D+4 |
| 2026-10-12 | far pass moved from 03:30 to 11:20; 03:30 is a pulse, the 11:35 pulse is gone (the far pass asks its slices first) |
| 2026-10-30 (written 10-13) | the 22:00 pulse also asks the wide tier's unread flight days of D+5..D+8; census at 08:15; an empty answer is stored as one (`raw` = `{}`) |
| 2026-10-30 | a run waits for the feed when it is shut (`door.py`) and gives way to the next slot; sweeps and the far pass read the 747-8/A380 first; a flight day answered "no such flight" is not asked again |
| first hourly run (`batch_runs.coverage->>'plan' = 'due'`) | the read plan: pools, a run every hour, outbounds through their return legs, flown tails from ADS-B, the A350 Frankfurt not read; 747-8/A380 every 6 h at D+1..D+3 and daily to D+9, A350 Allegris daily to D+9, 787/A350 Munich daily to D+4 |
| 2026-11-07 (the deploy) | the 747-8 and A380 only: no 787 or A350 read (their pools off, their watched tails too), and none shown; a run reads until the next is due, no 150 cap |

`tools/reassignment_timing.py --since 2026-07-21` is the honest floor for
anything measuring coverage or staleness.

## Verifying a change to the schedule

`--dry-run` works for every mode and touches neither the browser nor the DB, and
prints the planned lookups broken down by lead offset:

```
docker compose exec flightstatus python fetch_flightstatus.py --pulse --dry-run
docker compose exec flightstatus python fetch_flightstatus.py --far --dry-run
docker compose exec -e FIS_LOOKAHEAD_DAYS=1 flightstatus python fetch_flightstatus.py --dry-run
```

To validate *unreleased* fetcher code against production data without deploying
it, copy it into the container at a scratch path — `/app/fetch_flightstatus.py`
stays untouched, so cron keeps running the released version:

```
scp flightstatus/fetch_flightstatus.py $HOST:/tmp/fetch_new.py
ssh $HOST "cd $DIR && docker compose cp /tmp/fetch_new.py flightstatus:/tmp/fetch_new.py \
  && docker compose exec -T flightstatus python /tmp/fetch_new.py --pulse --dry-run"
```

(The script imports `legs` and `census` from its own directory: copy those next
to it under their own names when they changed too, or it will not start.)

After a week on a new cadence, re-run the measurement and check that the bracket
median moved and whether hour-of-day now clears the null:

```
./tools/pull_fis_history.sh
python3 tools/build_leg_outcomes.py --since <deploy-date>
python3 tools/reassignment_timing.py --since <deploy-date> --type B748 --type A388
```

## The far pass by day (2026-10-12)

The collector's log (`/var/log/lhlogging/flightstatus.log`, 20 Sep to 11 Oct)
shows that a run is decided when it opens its browser session, by how the
timetable page load is answered:

| page load answered | sessions | lookups that worked | refused |
|---|---|---|---|
| 403 — Cloudflare's check page (answered until the check completes, ~2 min) | 2,203 | 35,259 | 1,827 |
| 200 — the page without a check | 1,652 | 2 | 2,361 |

So a "blocked" run is neither a rate limit nor an outage: its session was never
checked, and the retry (wait 45 s, open a fresh session) loads the same page.
After an unchecked session the next one was checked 13 times out of 13 in July,
about one time in ten since August.

Unchecked sessions cluster between 02:00 and 06:30 Berlin: over the whole log
the first session of a run was unchecked in 29% of the runs starting in the
02:00 hour, 45% in the 03:00 hour, about 25% in the 05:00 and 06:00 hours, and
2–5% in every other hour. From 8 to 11 October the state held all night, and
the far pass — then at 03:30, and the only read of D+4 for the 787/A350 —
failed four nights running.

Lookups refused per Berlin hour, 20 Sep to 11 Oct (days with a fifth or more of
the hour's lookups refused, of the days with lookups in that hour):

| 00h | 02h | 03h | 04h | 05h | 06h | 07h | 09h | 10h | 11h | 13h | 14h | 15h | 16h | 18h | 19h | 20h | 21h | 22h |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1/21 | 8/22 | 10/22 | 6/19 | 2/15 | 7/20 | 3/15 | 0/22 | 1/22 | 0/22 | 0/22 | 1/22 | 3/22 | 3/21 | 1/22 | 1/21 | 0/12 | 2/21 | 4/21 |

The far pass therefore took the 11:35 pulse's place, and 03:30 got a plain pulse
in exchange (the far pass asks the deep tier's D+1/D+2 first, so it is that
pulse with ~245 more lookups behind it). 09:00 is as clean but leaves 90 min to
the 10:30 watch; the far pass's ~311 lookups take 55–65 min by day. Starting at
11:20 with the pulses' 10 min of jitter it ends by ~12:35, 25 min before the
13:00 watch. Lookups per day are unchanged; the deep tier's afternoon bracket
widens from 2.6h to 2.8h.

Why the page comes without a check at night is not known: served differently
off-peak (a cache, a reduced mode), or the bot protection treating the address
differently then. `FIS_DIAG_UNTIL` (crontab; see "diagnostics" in
`fetch_flightstatus.py`) makes every session write what its page load came back
with and what its first refusal said to `/var/log/lhlogging/fis-diag/`, to tell
the two apart.

## The feed as the timetable (2026-10-13)

Everything above is about tails. Two smaller additions are about which flights
exist at all, because the booking planner lists flights well past the days
their tails are read, and until now took them from the last four weeks
(`docs/booking_model.md`, "The timetable beyond what the feed lists"). At a
change of timetable — 25 October 2026 is the next — that is wrong for weeks,
and a new flight number only entered the catalog once it had flown: from the
ADS-B log, from its even/odd sibling, or backwards along a known flight's
`previousFlight`.

**An empty answer is an answer.** A lookup the feed answers with "no such
flight" (an empty list, or its JSON 404) was stored exactly like one that was
refused: not found, `raw` NULL. It now keeps `raw` = `{}`. That is what lets a
reader say "this flight does not run that day" — the planner drops such a
projection — and what lets the timetable look below ask only once. Rows from
before the change cannot be told apart and count as unknown.

**Timetable look** (`FIS_TIMETABLE_OFFSETS=5,6,7,8` on the 22:00 pulse). After
the deep tier's work the pulse asks, for each wide-tier number, for the flight
days of D+5..D+8 that have no answer yet. Once per flight and day: in the
steady state that is the day new at D+8, about 90 lookups (one per wide-tier
number); the first nights, and the night after a refused run,
catch up nearest day first, capped at 160 (`FIS_TIMETABLE_MAX`). With the
probe the pass comes to ~315 lookups, ~55 min typical, and still ends before
the 00:10 pulse at the slowest pace in the log (a catch-up night at that pace
would cost that one pulse). It rides on the pulse and cannot fail it: if its
candidate query errors, the pulse runs as before.

Day 9 is left out on purpose. At 22:00 the feed has not reached that day's
late departures: of the deep tier's day-9 lookups since 09-24, 86% found their
flight against 97–99.6% for days 5–8, and 32 of the 34 that came back empty
were there on a later look. An empty answer nine days out says nothing.

The tail such a look brings back is a by-product, seen once and days old by
the time the flight nears. The pages keep the 787/A350 on D+4 and show no tail
whose look is older than 36 hours, so it never stands in for the far pass; the
monitor counts a day ahead as known by the same rule.

**Census** (`--census`, 08:15; `flightstatus/census.py`). Lufthansa's long-haul
numbers sit in one block: of the 138 widebody flight numbers flown from 21 July
to 5 October, 128 are LH400–799, with 97% of the legs (the rest are the
Frankfurt–Munich shuttles LH96–115). The census asks for the numbers of that
block the catalog does not sweep — 280 on 2026-10-12 — eight days ahead, a
sixth of them a day (`FIS_CENSUS_CYCLE_DAYS`): ~47 lookups, nearly all answered
at once with "no such flight". Six days a round, not seven, so the weekday a
number is asked about moves on by one each time and a flight that runs once a
week is met within six rounds; a daily one in the first. A widebody found on a
number is an ordinary observation, and the next sweep's catalog refresh takes
the number in from there (source A) — a week before the flight operates. From
then on the nightly far looks keep finding it, which keeps the catalog's
lifecycle from retiring it before its first departure.

The census logs its own kind of run (`fis_census` in `batch_runs`): the monitor
judges a `flightstatus` run by how much it found, and a census that finds
nothing has had a normal day. It starts after the morning sweep and is done by
about 08:35; if the sweep is still running the slot is skipped, like any other.
`--census-all` asks for the whole block in one run (by hand; ~280 lookups).

Budget: +~90 a night and +~47 a morning, about 2,240 lookups a day (+6%).

```
docker compose exec flightstatus python fetch_flightstatus.py --census --dry-run
docker compose exec -e FIS_PULSE_OFFSETS=1,2,5,6,7,8,9 -e FIS_TIMETABLE_OFFSETS=5,6,7,8 \
    flightstatus python fetch_flightstatus.py --pulse --dry-run
```

## When the feed is shut (2026-10-30)

From 27 October the page without the check stopped keeping to the night. Share
of the day's page loads that came without it:

| 15–26 Oct | 27 Oct | 28 Oct | 29 Oct | 30 Oct, to 10:30 |
|---|---|---|---|---|
| 1–6% | 34% | 47% | 79% | 94% |

From 29 Oct 14:13 every run was refused from its first lookup, except the
10:30 and 13:00 watches and part of the 02:00 pulse; each spent ~19 min on 21
sessions (10 lookups × 3 attempts) before the circuit breaker stopped it, and
found nothing. Nothing had changed on our side (collector code from 12 Oct).
Why the check page stays away is still not known.

**The door** (`flightstatus/door.py`). A session that opened without the check
and had its first lookup refused counts as shut; a session that opened that way
but is answered counts as open, so the rule follows if Cloudflare changes. After
three shut sessions in a row (`FIS_DOOR_TRIES`), a scheduled run waits 15 min
(`FIS_DOOR_WAIT_MIN`, set in the crontab; a run by hand stops at once), holding
no session, and tries one more — until 3 min before the next line of the
crontab is due (`FIS_DOOR_MARGIN_MIN`). Then it stops, `blocked`, with a detail
such as "feed shut since 14:13: 7 sessions opened without the bot check and
were refused, waited 14:16-15:01 (0 of 62 done)". A run that waited and got in
also stops reading at that point ("gave way to the next run …"), so the next
slot runs with its own fresh work instead of being skipped by the lock. A run
that never had to wait keeps the old behaviour. A shut spell now costs a run
3 page loads and one more per 15 min instead of 21, and the reader keeps
trying through the gaps between slots. Lookups the door stopped are not done
and leave no row; they are not "blocked" lookups. While a run waits, its
`batch_runs.error_detail` says "waiting for the feed, next try HH:MM", and the
monitor shows it. Lookups refused in sessions that did open are handled as
before (three attempts in fresh sessions, then the breaker).

What a session back from the check page brings: after an unchecked session the
next was checked within 3–8 min 13% of the time, within 25–60 min ~21%, within
90–150 min 49% (20 Sep–11 Oct). A 15 min step is a page load every quarter
hour, where the old retries made 21 in 19 min.

**Most valuable first.** The sweeps and the far pass sort their work by tier
(747-8/A380, then 787/A350, then the rest), then nearest day, a day ahead
before the day as far back (`prioritised`). Until now it was nearest day first
across all tiers: the 18:15 sweep the feed cut at 237 of 670 on 27 Oct had read
D0, part of D+1 and 38 lookups of D-1 for every tier, and nothing of the 747-8's
and A380's D+2 and D+3.

**No such flight, asked once.** From 15 to 29 Oct the feed answered "no such
flight" 3,620 times for 523 flight days — about seven asks each, mostly
flights that do not run every day (LH510/511 alone: 464), ~500 a day by 26 Oct
after the timetable change, a fifth of all lookups. None of those answers given
up to eight days ahead was contradicted by a later look (nine days ahead it
can be, see "Day 9" above). So a flight day whose latest answer was "no such
flight", given at most eight days ahead, is no longer asked by the sweeps, the
far pass or the pulses (`answered_absent`, the same reading as the dashboard's
`_ABSENT_SQL`); a flight the feed names as a `previousFlight` is still followed.
This needs the `raw` = `{}` marker for empty answers (2026-10-13 above): rows
before it count as unknown, so the saving builds up over the first day of runs
— about 200 lookups a day at the 15–29 Oct rate.

```
docker compose exec flightstatus grep -E "door:|not asked again" /var/log/lhlogging/flightstatus.log | tail
```

## Known gaps

- **`batch_runs` has no pass-kind column.** Analyses infer which pass made an
  observation from the Berlin clock hour (`PASSES` in
  `tools/reassignment_timing.py`), which is fragile and got more so at 15 slots.
- **Truncation is invisible.** If `FIS_MAX_LOOKUPS` bites, the dropped work
  leaves no trace, so a missing row is ambiguous between "not queried" and
  "queried, not found". This is how the 07-16…07-19 holes formed.
- **B744 is still flying** (2 tails) but is not in `FIS_SEED_TYPES`, so B748→B744
  substitutions are only caught when they land on an already-catalogued number.
