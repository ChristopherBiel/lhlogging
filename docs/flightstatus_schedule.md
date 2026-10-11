# Flight-status collection schedule

Why the FIS fetcher runs 15 times a day, why coverage is split into two tiers,
and what the numbers behind that are. Companion to `flightstatus/crontab` (the
schedule itself) and `flightstatus/fetch_flightstatus.py` (the run modes).

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
| **wide** (since 2026-10-06) | B789, A359 (787/A350) | ~78 | D-2…D+2 sweeps (+1 day), D+3/D+4 far |
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
changed four times and staleness statistics are meaningless across the seams:

| from | regime |
|---|---|
| 2026-07-15 | migration 009: per-pass history (before this, one row per day per flight — later runs overwrote earlier ones) |
| 2026-07-17…19 | **sweeps crashing** (zero-padded chained flight numbers, fixed in e9ec83e) — watch passes only |
| 2026-07-21 | far pass split out (50df930); first stable 8-slot regime |
| 2026-07-26 | this schedule: tiering + pulses |
| 2026-09-24 | 22:00 pulse also probes the deep tier at D+5..D+9 |
| 2026-10-06 | wide tier (787/A350): sweeps +1 day, far D+4 |
| 2026-10-12 | far pass moved from 03:30 to 11:20; 03:30 is a pulse, the 11:35 pulse is gone (the far pass asks its slices first) |

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
| 403 — the bot check, which the headed browser passes | 2,203 | 35,259 | 1,827 |
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
differently then.

## Known gaps

- **`batch_runs` has no pass-kind column.** Analyses infer which pass made an
  observation from the Berlin clock hour (`PASSES` in
  `tools/reassignment_timing.py`), which is fragile and got more so at 15 slots.
- **Truncation is invisible.** If `FIS_MAX_LOOKUPS` bites, the dropped work
  leaves no trace, so a missing row is ambiguous between "not queried" and
  "queried, not found". This is how the 07-16…07-19 holes formed.
- **B744 is still flying** (2 tails) but is not in `FIS_SEED_TYPES`, so B748→B744
  substitutions are only caught when they land on an already-catalogued number.
