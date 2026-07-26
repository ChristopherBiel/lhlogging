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
| **deep** | B748, A388 | ~33 | D-2…D+2 sweeps, D+3/D+4 far, **7 pulse passes/day at D+1/D+2** |
| **broad** | A359, B789 (+ strays) | ~89 | D-2…D+2 sweeps, single far look at D+3 |

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
- **the far pass carries the deep tier's D+1/D+2** as well. It is the only run
  between 03:00 and 05:00, so without that the pulse cadence would have a hole
  there.

Result for a deep-tier leg — 10 looks/day at D+1/D+2, max gap ~2.8h:

```
02:00p → 03:40far → 06:30lite → 09:00p → 11:35p → 14:10p → 16:45p → 19:20full → 22:00p → 00:10p → 02:00p
   1.7h      2.8h       2.5h      2.6h     2.6h     2.6h      2.6h       2.7h      2.2h      1.8h
```

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

## Regime boundaries

Any analysis of this data must filter on `flight_date`, because the cadence has
changed four times and staleness statistics are meaningless across the seams:

| from | regime |
|---|---|
| 2026-07-15 | migration 009: per-pass history (before this, one row per day per flight — later runs overwrote earlier ones) |
| 2026-07-17…19 | **sweeps crashing** (zero-padded chained flight numbers, fixed in e9ec83e) — watch passes only |
| 2026-07-21 | far pass split out (50df930); first stable 8-slot regime |
| 2026-07-26 | this schedule: tiering + pulses |

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

## Known gaps

- **`batch_runs` has no pass-kind column.** Analyses infer which pass made an
  observation from the Berlin clock hour (`PASSES` in
  `tools/reassignment_timing.py`), which is fragile and got more so at 15 slots.
- **Truncation is invisible.** If `FIS_MAX_LOOKUPS` bites, the dropped work
  leaves no trace, so a missing row is ambiguous between "not queried" and
  "queried, not found". This is how the 07-16…07-19 holes formed.
- **B744 is still flying** (2 tails) but is not in `FIS_SEED_TYPES`, so B748→B744
  substitutions are only caught when they land on an already-catalogued number.
