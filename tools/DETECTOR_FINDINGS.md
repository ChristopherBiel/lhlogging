# Flight-detector failure analysis & offline replay harness

Grounded in the recorded `positions` stream (2,098,615 rows, 333 aircraft,
retained 2026‑05‑30 → 2026‑06‑29) and the 123,150‑row `flights` export.
All numbers below are reproducible with the tools in this directory.

## TL;DR

- The "single real flight logged as two rows" symptom is **dominated by a data-quality
  bug, not coverage gaps**: a *single spurious `on_ground=true` ADS-B sample at cruise
  speed* makes the detector "land" the aircraft at whatever airport is nearest (≤50 km),
  then re-open a phantom leg. **134/138** replayable splits are *continuous* (no real gap);
  **107/138 (77.5%)** are this cruise-snap.
- The screening heuristic (consensus-route mismatch) **over-counts**: **~15–21%** of the
  662 candidate pairs are *real* intermediate stops / diversions that the detector handled
  **correctly** (verified: D‑AIMH genuinely flew Munich→Mumbai→Delhi on 06‑12).
- A targeted guard — *distrust `on_ground=true` when velocity > ~80 m/s* — fixes
  **93/107** cruise-snaps with **0** false merges of genuine stops, measured by the harness.
- Two secondary modes surfaced: **dep==arr micro-flights** (touch-and-go re-departures) and
  **missed-departure `dep=None`** (aircraft first seen airborne at a window boundary).

## How detection actually works (why the symptom appears)

`flight_detector.py` is **incremental & stateful**: a cron runs every ~30 min over a rolling
60‑min `positions` lookback, carrying open-flight state in the `flights` table. A flight is
opened/extended/closed across many runs. This matters: **~half the split pairs have a
fragment gap < 8 min** (below the session-split threshold), so they *cannot* arise from a
single pass over the whole flight — they come from a false close in one run + a re-open in
the next. The replay harness simulates this windowed cadence (see below); a naive single
pass (as in `app/tools/compare_detector.py`) does **not** reproduce them.

## Failure-mode taxonomy (quantified)

Of 662 consensus-flagged candidate pairs (1.08% of all flights, 1,324 fragment rows,
278 aircraft, 453 callsigns; 508 with **both fragments `needs_review=false`** → silently
wrong; 93 phantoms inside the dashboard `_DIVERSION_AIRPORTS` set → false-incident risk).
**138** have retained positions and were replayed/classified (`classify_splits.py`):

| Mode | Count (in-window) | What it is | Bug? |
|---|---|---|---|
| **CRUISE_SNAP** | **107 (77.5%)** | one/few spurious `on_ground=true` samples at cruise (107 `ONGROUND_AT_SPEED`, 8 also `ALT_GLITCH` with impossible altitudes) → false air→ground transition → 50 km nearest-airport snap | **yes — the main bug** |
| **REAL_STOP** | 21 (15.2%) | aircraft genuinely landed at the "phantom" (sustained ≤15 m/s within 10 km of it) — real diversion/tech stop | **no — detector correct; heuristic false positive** |
| AMBIGUOUS | 8 (5.8%) | low/slow near boundary but no clean signature | partial |
| GAP_DESCENT | 2 (1.4%) | a real >8 min coverage gap during descent | edge |

`524/662` candidates predate the 30-day positions retention (can't replay), but their
phantoms are cruise-corridor/approach fields (ETOU ×94 near Frankfurt, CYxx on the Atlantic
approach, etc.) — same signature, so the cruise-snap share is likely similar fleet-wide.

### Mechanism (reconstructed from raw positions)

1. `_is_on_ground` (flight_detector.py:31) returns `True` **purely on the `on_ground` flag**
   with no velocity/altitude sanity check.
2. A corrupt sample (`on_ground=true` while at ~250 m/s, sometimes with a doubled/garbage
   altitude) therefore reads as "on ground" mid-cruise.
3. `_scan_for_arrival_after` sees a single air→ground transition and declares a **landing**.
4. `db.lookup_nearest_airport` snaps the cruise position to the nearest airport within the
   **50 km** default radius → a **phantom arrival**; `needs_review=false` because dep≠arr.
5. A later cron run re-opens a phantom leg via the **Case-2 missed-departure fallback**
   (`departure = previous arrival`), producing `phantom→real_arrival`.

Verified examples (raw samples in the commit message / fixtures):
- **D‑AIMH DLH425 KBOS→EDDM** (06‑29): `05:38:00 alt=22799 vel=256 on_ground=t` between two
  normal cruise samples 2 min apart → snap to **EGTE** (Exeter, 34 km).
- **D‑AIMA DLH459 KSFO→EDDM** (06‑06): `14:45:57 alt=12497 vel=253 on_ground=t` → **EDDK**
  (Cologne, 32 km); the real EDDM landing is right there at 15:28 (vel→5).
- **Counter-example (correct) D‑AIMH DLH762** (06‑12): real Munich→**Mumbai**→Delhi; VABB
  landing is genuine (descent 1737→46 m, touchdown vel 68, then `on_ground=t` at 4–6 m/s for
  10 min, 100‑min parked gap). The detector correctly split it — my heuristic false-flagged it.

### Secondary modes (not part of the 662, but seen in replay)

- **dep==arr micro-flights** — e.g. D‑AIMA `KLAX→KLAX` (06‑13, 5 min, `needs_review=t`): a
  spurious re-departure right after arrival (touch-and-go logic in Cases 1/3). ~459 self-loops
  across the replayed subset.
- **Missed-departure `dep=None`** — when an aircraft first appears airborne at a window
  boundary with only one sample, `_detect_departure` (needs ≥2) can't resolve the origin and
  leaves `dep=None` (e.g. `real_stop_mumbai` leg 1). Distinct from the snap bug.
- **UNKN/open ~11%** of legs — arrivals lost to sparse coverage, later force-closed
  (`_close_stale_flights`). Pre-existing, see the `flight-detection-data-quality` memory.

## The harness (this directory)

Pure-stdlib, Python 3.9+, no DB. A faithful **port** of `flight_detector.py` (validated:
reproduces the EGTE/EDDK/VABB production rows exactly) plus the **windowed cron simulation**.

| File | Purpose |
|---|---|
| `pull_positions.sh` | pull `positions` (+ `airports`) over ssh → `tmp/` (companion to `pull_data.sh`) |
| `_airports.py` | offline `lookup_nearest_airport` (haversine + hub-preference + 50 km cap) |
| `detector_replay.py` | **pure** detector core + windowed replay: `replay(positions, airports, cfg) → legs`. Experimental guards default OFF (faithful). |
| `find_gap_splits.py` | enumerate the 662 candidate pairs from the flights export |
| `classify_splits.py` | label each candidate CRUISE_SNAP / REAL_STOP / … from raw physics |
| `eval_detector.py` | replay the fleet subset under a `Config`; score merge/split outcome per label + global leg health |
| `build_corpus.py` / `run_corpus.py` | build & run the committed self-contained regression fixtures (`corpus/`) |

```bash
./tools/pull_positions.sh                 # one-time: corpus from prod (read-only)
PYTHONPATH=tools python3 tools/find_gap_splits.py
PYTHONPATH=tools python3 tools/classify_splits.py
PYTHONPATH=tools python3 tools/run_corpus.py                          # baseline (current detector)
PYTHONPATH=tools python3 tools/run_corpus.py --onground-max-speed 80  # a candidate change
PYTHONPATH=tools python3 tools/eval_detector.py --onground-max-speed 80
```

## Measured effect of candidate fixes (for the redesign)

Guard = `is_on_ground` distrusts `on_ground=true` when `velocity > threshold`.

| Config | cruise-snaps merged | genuine stops merged (bad) | gap-split pairs (replayed subset) | dep==arr | corpus |
|---|---|---|---|---|---|
| baseline | 1/107 | — | 128 | 459 | 2/7 |
| onground > 60 | 93/107 (0 still split) | 0 true | 25 | 499 | 6/7 |
| **onground > 80** | **93/107 (0 still split)** | **0 true** | **25** | **475** | **6/7** |
| onground > 100 | 93/107 | 0 true | 25 | 473 | 6/7 |
| landing_min_consecutive=3 | 75/107 | 0 true | 37 | 414 | (less effective) |

- **80 m/s (~155 kt)** is the sweet spot: above approach speed (≈72 m/s) so it can't disturb
  real taxi/touchdown, far below cruise (~250). Same fix as 60 with a smaller dep==arr side-effect.
- The 14 "merged-imperfect" snaps are flights whose *real* arrival was also lost to coverage —
  removing the phantom is still strictly better; they need the complementary missed-arrival work.
- The single flagged "regression" (D‑ABYL EDDF→KHHR→KLAX) is the KLAX arrival mis-snapped to
  adjacent Hawthorne (≈5 km) — a destination-vicinity artifact, not a real stop.

## Implemented

- **Shared pure core** `app/lhlogging/detector_core.py` — the case/state machine, DB-free
  (injected `store` + `nearest` + `DetectorConfig`). `flight_detector.py` and
  `tools/detector_replay.py` both call it (no second copy to drift); the duplicate
  `app/tools/compare_detector.py` was retired. Guards-off == historical behaviour (fleet parity:
  128 split pairs reproduced).
- **Guards** behind `config.py` env vars (defaults ON; disable live via env, e.g.
  `ONGROUND_MAX_SPEED_MS=0`): `ONGROUND_MAX_SPEED_MS=80`, `ONGROUND_MAX_ALTITUDE_M=6000` (P1),
  `MISSED_DEPARTURE_SNAP=true` (P3), `SCAN_ARRIVAL_MAX_KM=8` (P4); `LANDING_MIN_CONSECUTIVE` (P2)
  and `MIN_TURNAROUND_MIN` (P5) available, default off. Corpus 7/7 (P/R 1.00), 0 genuine-stop
  merges, split pairs 128->24.
- **Backfill** `app/tools/repair_gap_splits.py` (dry-run default): recent (<=30d) merges
  CRUISE_SNAP pairs (never REAL_STOP) from positions; `--flag-historical` sets needs_review on
  older fragments via `flight_routes`.
- **Consumer defenses** in `dashboard/app.py`: `_stitch_phantom_legs` collapses phantom splits in
  the `/api/schedule` overlay; the `/api/insights` diversions query excludes interior phantoms
  (same-callsign continuation within 6h). Route/rotation are already protected by the existing
  `NOT needs_review` scope + historical flagging.
