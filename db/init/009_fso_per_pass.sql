-- ============================================================
-- Per-pass flight-status history (FIS fetcher, multi-run days)
-- ============================================================
-- The fetcher now runs up to 7x/day (2 sweeps + same-day "watch" passes around
-- the departure banks). Under the old unique key
-- (observed_date, flight_date, airline, flight_number) every later run of the
-- same day overwrote the earlier one, destroying exactly the evidence the
-- watch passes exist to capture: *when* a same-day tail swap landed. Keying by
-- run instead makes the table append-per-pass: one row per (run, flight,
-- target date), so intra-day reassignments become visible as successive rows.
--
-- Readers that want "the latest view of a flight" must order by observed_at
-- DESC (not observed_date) — observed_at has always been maintained, so this
-- ordering is valid for pre-migration rows too. Pre-migration rows keep
-- run_id NULL; they were unique per day and stay unique.
--
-- APPLY ORDER: deploy the fetcher first (it detects this schema and falls
-- back to the daily-overwrite upsert while the old constraint is in place),
-- then apply this migration. Applying first would break the old fetcher,
-- whose ON CONFLICT targets the dropped constraint.
ALTER TABLE flight_status_observations
    ADD COLUMN IF NOT EXISTS run_id BIGINT REFERENCES batch_runs(id);

ALTER TABLE flight_status_observations
    DROP CONSTRAINT IF EXISTS fso_unique;

-- New-code rows always carry a run_id; NULLs (historic rows) never collide.
CREATE UNIQUE INDEX IF NOT EXISTS fso_unique_per_run
    ON flight_status_observations (run_id, flight_date, airline, flight_number);
