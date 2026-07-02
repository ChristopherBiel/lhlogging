-- ============================================================
-- Coverage metric on batch_runs (FIS fetcher continuity audit)
-- ============================================================
-- After each FIS sweep the fetcher reconstructs every A388/B748 tail's rotation
-- over the forecast window and records a coverage summary: how many fleet tails
-- were seen, how many reconstruct into a clean continuous rotation, and which
-- have airport-continuity gaps, time overlaps, or are absent entirely. Stored
-- here (JSONB) so the now-clean state is a monitored, queryable invariant —
-- alert when clean < seen or a tail goes absent.
ALTER TABLE batch_runs ADD COLUMN IF NOT EXISTS coverage JSONB;
