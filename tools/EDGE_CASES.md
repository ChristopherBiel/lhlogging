# Edge-case & failure-mode audit — flight/route detection pipeline (2026-07-02/03)

Grounded in fresh read-only pulls: `flights` (113,447 rows, post-purge),
`positions` (2,164,077 rows, 2026-06-02 → 07-02), `fis_export` (1,393 obs),
replayed with the production guard config. Every number below reproduces with:

```bash
tools/pull_data.sh && tools/pull_positions.sh && tools/pull_fis.sh
PYTHONPATH=tools python3 tools/audit_edge_cases.py presplit
PYTHONPATH=tools python3 tools/audit_edge_cases.py replay --tag prod
PYTHONPATH=tools python3 tools/audit_edge_cases.py replay --tag nop3
PYTHONPATH=tools python3 tools/audit_edge_cases.py e3 e2 e7 e6 e8 e10 census e12 fis
```

Per-candidate CSVs land in `tmp/audit_*.csv`. "Post-deploy" = first_seen ≥
2026-06-29 (the detector-overhaul deploy; ~3.5 days of the new regime at audit
time). Severity: **S1** silent wrong data (`needs_review=false`) · **S2**
flagged but unrecoverable · **S3** auto-healed · **S4** noise.

## TL;DR — the story changed

The plan's central hypothesis was *single false ADS-B samples*. The data says
that family is nearly dead: **post-deploy there were 0 cruise-snap splits and
exactly 1 genuinely spurious single-sample flight** in 30 days. The dominant
remaining failure is **compositional**, a cascade across three components that
are individually "working":

```
 arrival fix missing or leg closed on a frozen feed          (coverage reality)
   └─ open flight meets the NEXT leg's callsign mid-air      (C6, ~57/day)
        └─ C6 closes arr=UNKN at the successor's first fix,
           opens successor with dep=None, review=true
             └─ any later scan-close WIPES review back to false
                (close_scanned recomputes it from dep==arr only)
                  └─ route_enrichment backfills dep/arr from flight_routes
                     with NO dep≠arr guard and sometimes a STALE consensus
                       └─ silent EDDF==EDDF / EDDM==EDDM self-loops,
                          wrong callsign attribution, poisoned consensus
```

The detector's raw output is usually *honest* (UNKN / None); the silent wrong
rows are mostly **manufactured downstream** by enrichment + the review-flag
wipe. Plus one new, previously invisible mode: **whole rotations with zero
captured positions** (upstream, before the detector runs).

## Quantified findings

| ID | Mode | Post-deploy rate | Severity | Verdict |
|---|---|---|---|---|
| E1 | Cruise-snap splits (the old bug) | **0** real (2 flagged pairs are genuine diversions) | — | fixed by P1/P4 |
| E10 | C6 mid-air callsign flip → UNKN close + dep=None successor | **199 / 3.5d ≈ 57/day**, 199/200 verified airborne at handover | S2→S1 | **top ongoing failure** |
| E3 | dep==arr legs w/ airborne track | 140 post-deploy (104 CRUISE + 109 LOW total in window) | mixed | 3 sub-modes, see below |
| ─ E3a | Return leg, dep fabricated by enrichment (`RETURN_LEG_FAB_DEP`) | 81 total, 76 silent; 77/81 dep == successor-callsign's consensus dep | **S1** | enrichment bug |
| ─ E3b | Approach fragments (P3 opens at the destination) | ~82 (`OTHER`, first+last fix airborne near field, 77 flagged) | S4/S2 | P3 + ingest lag |
| ─ E3c | Arr contradicted by physics (`ARR_SNAP_WRONG`) | 39, all flagged; cruise-far ones = enrichment **arr**-backfill from stale consensus (e.g. DLH01Y 833 km away) | S2 | enrichment bug |
| E7 | P3 dep snapped to an overflown field | 75 P3-attributed legs/month in replay; **38 wrong dep, 22 silent**; picks are approach-corridor fields (LFPB↔LFPG, EDMA↔EDDM, LFMD↔LFMN, ETSN, EDTY…) | **S1** | P3 needs a trend/leftover guard |
| E2 | Genuinely spurious single airborne sample | **1** in 30d (D-AINV DLH3YY, 11,894 m fix while parked, 06-30) | S4 | rare — NOT the main bug |
| E6 | Wrong-field arrival snap | 26 confirmed MIS_SNAP all silent, but only **2 post-deploy** (OCN41B EDML↔EDDM, DLH584 HECA↔HEAZ) | S1 | mostly historical |
| E4 | UNKN arrivals | 234 post-deploy; only 1 had a consensus route to fill (enrichment keeps up); the bulk are **C6 events near the hubs**, not foreign coverage | S2 | reframed: recoverable |
| E9 | Stale-close / review queue | queue = 383: 231 UNKN(<20h) + 145 self-loops + 4 real + 3 ≥20h | S3/S4 | shrinks to ~0 if E10/E7 fixed |
| E8 | dep = previous-arrival poisoning | 2,644 raw candidates (140 post-deploy) — **screening only, unverified**; includes legit multi-route callsigns | ? | needs physics pass before claims |
| E12 | Consensus staleness / contest | 6 stale (recent modal ≠ all-time consensus, e.g. DLH01Y, DLH8PF), 15 contested | S1 via enrichment | feeds E3c |
| E13 | **Upstream capture gaps** (new) | 2 whole widebody rotations with zero positions in 4 days (D-AIMA 06-29 LAX, D-AIML 07-01 DEN) | invisible | monitor poller vs FIS |
| FIS | External-oracle agreement | 211/250 MATCH (84%); 5 post-deploy misses with data; 27 unknowable (purged pre-deploy rows); 4 no-ADSB | — | oracle works |

## Three fully-traced case studies

**1. D-AINM / DLH3MU→DLH6YX (06-30) — the C6+enrichment self-loop.**
Outbound EDDF→Sarajevo, no ground fixes at LQSA (arrival lost). Return climbs
out already squawking DLH6YX → C6 closes outbound `EDDF→UNKN rev=t` at the
return's first fix and opens the return `dep=None rev=t`. Return lands EDDF;
`close_scanned` recomputes review from `dep==arr` with `dep=None` → **review
wiped to f**. Enrichment then fills `dep` from DLH6YX's `flight_routes` entry
(EDDF) → **silent `EDDF==EDDF`**. Same shape recurs for the same callsigns
(3 tails hit on DLH6YX alone).

**2. D-AIXM / DLH520→DLH521→DLH768 (06-27/28) — wrong-callsign attribution.**
Return MMMX→EDDM first seen mid-climb over Mexico (C6-opened, dep=None,
cs=DLH521), sparse ocean fixes, then resurfaces *parked at EDDM with the next
leg's callsign DLH768 on the transponder*. The reappear-close
(detector_core.py:305–315) sets arr=EDDM (correct) but **overwrites the
callsign to DLH768** and computes review from dep=None → f. Enrichment fills
dep from DLH768's consensus (EDDM) → silent `EDDM==EDDM`, and the leg is now
attributed to a flight it wasn't.

**3. D-ABYJ / DLH456→457 at KLAX (07-01) — the "spurious sample" that wasn't.**
The prior interpretation (review-queue-triage memory) was wrong: the lone
632 m/130 m/s fix is the **real first fix of the DLH457 return departure**.
Actual chain: the EDDF→KLAX leg closed off a **frozen feed** (3 identical 30 m
fixes, never on_ground=true); one leftover frozen fix later formed a
single-fix session (window-boundary slicing) → **P3 opened a phantom KLAX leg
with review=f**; 152 min later the return's first fix arrived under DLH457 →
C6 closed the phantom `UNKN`; enrichment backfilled `UNKN→KLAX` → the
`KLAX==KLAX` row. The offline replay does **not** reproduce this (it sees
positions with perfect hindsight; see "harness fidelity" below) — prod's
ragged ingest does.

## Root causes (verified against current code)

| Code path | Defect |
|---|---|
| `route_enrichment.py:72–89` | backfill of dep (and arr) has **no `≠ other endpoint` guard** → manufactures dep==arr rows; uses all-time consensus even when recent legs disagree (6 stale callsigns) |
| `detector_core.py:225–234` (`close_scanned`) + close paths :296–298, :313–315, :352–355 | `needs_review` is **recomputed from dep==arr only**, silently discarding the review=true state set at open (C6/C2); 194/200 C6 successors ended review=f |
| `detector_core.py:313–315` | reappear-close **overwrites the leg callsign with the new session's callsign** — the next flight's — destroying attribution |
| `detector_core.py:343–356` (C6) | a mid-air callsign change is treated as flight boundary; in reality it's the crew keying the next leg (~57/day, consecutive flight numbers, 8–10 km altitude); the real landing goes to the successor row |
| `detector_core.py:277–282` (P3) | fires on any non-climbing first sighting <3000 m: approach fragments and leftover post-close fixes get a fabricated dep at the **full 50 km radius** with **review=False**; `MISSED_DEPARTURE_DISTANCE_KM` (config.py:65) was evidently meant to cap it and is wired to nothing |
| `flight_detector.py` cron × ingest | window-boundary re-slicing of late-arriving fixes creates single-fix sessions (the P3 fodder); prod rate ~10× the replay's |
| upstream (state_poller/OpenSky) | entire rotations occasionally never captured (E13) — no detector-level symptom at all |

## Screening-heuristic precision (the over-count discipline)

- Consensus split-pair heuristic (E1): post-deploy **0/2 true positives** —
  both hits (D-ABYS EDDF→SAAR→SAEZ, D-AIKS EDDF→KCLT→KRDU) are genuine
  diversions the detector handled correctly. The heuristic is now a
  diversion-finder, not a bug-finder.
- E8 inherited-dep heuristic: raw 2,644 is an **upper bound only** — consensus
  disagreement fires on legitimate multi-route callsigns; do the positions
  pass before quoting any number.
- prune_self_loops keep-rule: of 90 kept self-loops with ≤2 airborne fixes,
  89 contain *real* airborne fixes (approach/departure fragments of real
  flights) — the keep is "right" but the rows are still artifacts; only 1 was
  true sample corruption. The rule can't distinguish artifact rows from data
  it should protect; fixing the openers (P3/C6) is the real lever.

## Harness fidelity gap

Replay assumes every fix is available the moment it is captured. Production
ingests late; the detector re-slices sessions at window boundaries and meets
leftover single-fix sessions the replay never sees (~31/day LOW self-loops in
prod vs ~2.5/day in replay). Consequence: `p3_leftover_klax` passes in replay
while prod produced the phantom. Candidate follow-up: record insert-time in
future pulls (or model a lag distribution) and add it to `replay_aircraft`.

## Fix recommendations (ranked; NOT implemented — analysis was read-only)

1. **Enrichment guards** (SQL-only, kills most S1): refuse any backfill that
   would set dep==arr; skip callsigns whose recent modal route disagrees with
   `flight_routes` (staleness test from `audit_edge_cases.py e12`).
2. **Stop wiping review/callsign on close**: closes should OR the existing
   `needs_review` and never *replace* a non-empty callsign with the new
   session's on a reappear-close (keep both / flag instead).
3. **C6 stitching**: a mid-air callsign change with positional continuity is
   a callsign *update*, not a flight boundary — split at the actual landing
   instead. Kills ~57 UNKN/day and the whole cascade. (Cheaper interim: an
   offline stitcher in the prune_self_loops mold that back-fills `a.arr` from
   the successor's landing.)
4. **P3 tightening**: don't snap when the next fix shows descent (approach) or
   when `last_completed.arr` equals the candidate airport within ~30 min
   (leftover-fix signature); never set review=False from a single-sample
   inference; wire `MISSED_DEPARTURE_DISTANCE_KM` as the cap or delete it.
   Measure with `run_corpus` (xfail fixtures flip to pass) + `eval_detector`.
5. **E13 monitor**: alert when a FIS-confirmed rotation has zero positions.
6. Harness ingest-lag simulation (above), then re-derive E3b/E7 rates.
7. Leave `MIN_TURNAROUND_MIN` off (re-confirmed non-lever).

## Corpus fixtures added (tools/build_corpus.py)

`diversion_saar_saez`, `diversion_kclt_krdu` (keep-split controls),
`c6_callsign_flip_lqsa` (pins the honest UNKN pair — protects the EDDF
landing), `p3_leftover_klax` (control; see harness gap),
`p3_approach_snap_edma` **[xfail]**, `singleton_altglitch_eddf` **[xfail]**.
Gate: **11/11 pass + 2 xfail** under production flags. `run_corpus.py` now
understands `"xfail": true` (desired-behaviour fixtures for open bugs; an
XPASS prompts promotion into the gate).
