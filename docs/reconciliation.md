# Reconciliation pass — design (2026-07-03; R0 built 2026-07-06)

> **Status 2026-07-06 — R0 shipped** (`app/lhlogging/reconciler.py` +
> `tools/reconcile_shadow.py`; no schema, no writes, no cron):
> - Dual corpus gate green: `run_corpus.py --reconciler` **13/13** (both
>   xfail fixtures flip to pass via `expected_legs_reconciler`); online mode
>   unchanged (11/11 + 2 xfail).
> - Case regressions pass: D-ABYJ true rotation (no KLAX phantom, DLH457
>   attributed right), D-AIXM honest `DLH520 EDDM→UNKN + DLH521 ?→EDDM`
>   (modal-cruise callsign kills the DLH768 mis-attribution), D-AIML no
>   folding around the invisible DEN rotation.
> - 30-day offline shadow (338 aircraft, 37k legs): **87% CONFIRM**,
>   corrections 94/day, missed-leg inserts 57/day, phantom deletes 3.6/day.
>   Reconciled self-loops **11.1/day (all review-flagged)** vs the write-gate
>   target ≈0 — the residual is dark-outstation folding (zero-coverage
>   turnarounds at LIMF/LIEO-class stations and long-haul rotations, edges
>   at cruise). FIS cross-check **81%** vs 89% provisional baseline, gap
>   fully explained by that same folding + E13 upstream capture gaps.
> - Segmentation grew five physics rules beyond the original design, all
>   forced by real data: feed-staleness slack in the teleport test,
>   spatial-outlier + frozen-ghost prefilters, evidence-paired weak-stop
>   promotion, unflyable-gap splits (speed floor + endurance bound + one-
>   sided V), and boundary coalescing around dark turnarounds.
> - **Not write-ready** (by design at R0): proceed to R1 (migration 009 +
>   report-only cron) and iterate the dark-outstation classes against the
>   live shadow until the R2 thresholds hold.

## Why

The 2026-07 edge-case audit (`tools/EDGE_CASES.md`) showed the online detector's
remaining failures are **execution-model artifacts, not physics bugs**: it must
decide every 30 minutes on partial, late-arriving data and can never revise.
The offline replay — same `detector_core`, same data, but with hindsight —
produced the correct legs in every traced case where production produced
phantoms (D-ABYJ and D-AIML at KLAX, D-AIXM at EDDM). Hindsight is not a test
trick; it is the correct semantics once data has settled. The reconciliation
pass makes it a production layer.

**Principle: two zones.** The online detector keeps owning the *provisional
zone* (now → lag) so the dashboard still sees flights in near-real-time. A new
reconciler owns the *settled zone*: it re-derives legs from the full position
track and finalizes/corrects the provisional rows. Consumers keep reading one
`flights` table; settled rows are simply better.

```
positions ──► flight_detector (30min cron)  ──► provisional rows   (now .. now-LAG)
          └─► reconciler (hourly cron)      ──► finalized rows     (now-LAG .. now-WINDOW)
                                                 └─ route_enrichment fills only
                                                    what reconciliation left unknown
```

## The reconciler (`app/lhlogging/reconciler.py`)

- **Window:** `[now − RECON_WINDOW_H, now − RECON_LAG_H]`, defaults 72 h / 6 h
  (env-backed like the P-guards). 6 h lag is past any turnaround and past
  OpenSky's late inserts; 72 h ≪ the 30-day positions retention.
- **Per aircraft:** load the *full* track overlapping the window (extended to
  session boundaries so no leg is cut), segment once with hindsight — **not**
  the 30-min cron simulation. Single-pass segmentation over complete sessions
  is exactly the mode the replay proved clean.
- **Physical identity, not callsign identity:**
  - A leg boundary is a *confirmed landing* (k consecutive ground fixes,
    frozen-feed signature, or proximity descent — the existing
    `detector_core` primitives) or an implausible teleport (implied
    great-circle speed > 350 m/s ⇒ boundary + flag).
  - Callsign changes never create boundaries. A leg's callsign is the modal
    callsign of its cruise portion (crews key the next leg during descent —
    the ~57/day C6 class dissolves here by construction).
  - Corrupt-fix prefilter: drop fixes whose implied kinematics against both
    neighbors are impossible (kills the 1-in-30-days altglitch class).
- **Endpoint provenance:** every dep/arr is labeled `observed` (ground/low fix
  at the field), `inferred` (chain continuity: previous settled arrival),
  or `unknown` (honest UNKN). The reconciler never writes a guessed airport —
  guessing stays enrichment's job, and enrichment marks what it fills.

## Write model — the hard part (leg identity)

Constraints that make this tractable: `flights` has **no FK dependents**
(verified during the 07-02 purge); consumers query by time range + airports,
not by stored row references; `flight_date`/`duration_minutes` are GENERATED.

Matching hindsight legs → provisional rows per aircraft, by time overlap:

| Case | Action |
|---|---|
| 1:1 (endpoints agree) | confirm: set `reconciled_at`, provenance, clear `needs_review` |
| 1:1 (endpoints differ) | correct in place (keep the row's `icao24, first_seen` when the observed takeoff is within the same session; else update `first_seen` in place — no dependents) |
| 1:N (provisional split a real leg) | keep the max-overlap row, absorb/delete the fragments (the `repair_gap_splits` pattern) |
| N:1 (provisional merged two legs) | keep first, insert second |
| 0:1 (provisional phantom, e.g. P3 leftover) | delete |
| 1:0 (missed leg) | insert |

Rules: never touch a row whose `arrival_airport_icao IS NULL` (open — the
online detector owns it) and never touch anything younger than the lag.
Every applied change is logged with its evidence (the `prune_self_loops`
logging discipline); stats go to `batch_runs`.

## Schema & graceful degradation (migrations are manual here)

Migration 009: `reconciled_at timestamptz`, `dep_source text`,
`arr_source text` on `flights` (nullable, no backfill needed).

The code ships **before** the migration and degrades: an
`information_schema` check (the `airports_has_type` pattern in `db.py`)
switches the reconciler to report-only when the columns are missing, and
`RECONCILER_APPLY` (env, default `false`) gates writes even after the
migration — the same rollout shape as the P1–P5 guards.

`route_enrichment` ordering changes in `app/crontab`: reconciler runs before
enrichment; enrichment pass 2 additionally skips endpoints whose
`*_source = 'observed'` once the columns exist (a `coalesce(dep_source,'') <>
'observed'` clause — degrades to current behavior when NULL).

## Validation gates (all exist already)

1. **Dual corpus gate:** `run_corpus.py` grows a `--reconciler` mode driving
   fixtures through the hindsight segmenter. Acceptance: all 11 gate fixtures
   pass AND both xfail fixtures (`p3_approach_snap_edma`,
   `singleton_altglitch_eddf`) flip to pass; the keep-split diversions and
   `c6_callsign_flip_lqsa` (honest-UNKN pair when continuity is broken) must
   stay correct.
2. **Shadow mode:** run report-only in prod for ≥5 days; score the would-be
   table with `tools/audit_edge_cases.py`. Thresholds to enable writes:
   new self-loops ≈ 0/day, UNKN inflow at the true coverage baseline (C6
   class gone), FIS cross-check ≥ 90% MATCH on widebodies, zero regressions
   on the diversion fixtures.
3. **Case regression:** the three traced incidents (D-ABYJ, D-AIML, D-AIXM)
   must reconcile to their true rotations.

## Phasing

- **R0 (no schema, no writes):** `reconciler.py` segmentation core + offline
  driver in `tools/`; dual corpus gate; shadow report. Most of the code is
  adaptation of `detector_replay` + `repair_gap_splits` — small.
- **R1:** migration 009; apply-mode wiring behind `RECONCILER_APPLY=false`;
  crontab entry (report-only in prod = live shadow).
- **R2:** enable apply. Retire `prune_self_loops` inside the settled window;
  drop the Wave-2 interim P3 patches if present (they become redundant);
  leave the online C6 behavior as-is — its damage now heals in ≤ lag hours.
- **R3 (optional, later):** if double-bookkeeping annoys, shrink the online
  layer to an open-flight tracker and make the reconciler the sole writer of
  completed legs.

## Open questions (decide at R0 review)

1. **`first_seen` shifts:** update-in-place is proposed (no dependents today).
   If external consumers ever key on `(icao24, first_seen)`, add a stable
   `leg_id` in migration 009 instead — cheap to include preemptively.
2. **Dashboard boundary:** settled-vs-provisional could be surfaced (e.g.
   muted styling for unreconciled legs) — cosmetic, defer.
3. **Consensus circularity:** enrichment-inferred endpoints currently feed
   back into `seed_flight_routes` consensus. Provenance columns end this
   (seed only from `observed`); until R1, Wave 1c's recency window is the
   mitigation.
