-- ============================================================
-- FIS flight-number catalog (which flights the nightly fetcher sweeps)
-- ============================================================
-- Persistent universe of LH widebody (A380/B748) flight numbers to query on the
-- FIS feed, replacing the reactive "numeric DLH callsigns seen in the last 2
-- days" seed. Repopulated idempotently at the start of each run from the union
-- of:
--   * fis_history — numbers FIS has ever returned operated by a known A388/B748
--     tail (robust: keys off the registration<->fleet join, so it also catches
--     the empty-`aircraftType` tactical A380 legs the type string misses);
--   * adsb_seed   — the legacy ADS-B seed (still first to see a brand-new
--     number before FIS history knows it);
--   * pairing     — the even<->odd sibling of every widebody-confirmed number
--     (an outbound N implies its return N+1), so turnaround legs the
--     `previousFlight` chain skips over — e.g. LH763 — get queried anyway.
--
-- Goal: catch ALL scheduled widebody legs. `status` / `consecutive_misses`
-- exist for the Phase-3 prune (retire numbers that stop returning a widebody);
-- Phase 1 leaves everything 'active'.
CREATE TABLE IF NOT EXISTS fis_flight_catalog (
    airline            VARCHAR(3)   NOT NULL DEFAULT 'LH',
    flight_number      VARCHAR(8)   NOT NULL,                   -- numeric part, e.g. '762'
    seed_type          VARCHAR(10),                             -- best-known widebody type: 'B748'/'A388'
    paired_number      VARCHAR(8),                              -- even<->odd sibling (return leg)
    status             VARCHAR(12)  NOT NULL DEFAULT 'active',  -- active|probation|retired
    source             VARCHAR(16),                             -- fis_history|pairing|adsb_seed|chain|curated
    last_widebody_date DATE,                                    -- last date FIS returned a widebody for it
    consecutive_misses INT          NOT NULL DEFAULT 0,
    first_added        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT fis_catalog_pk PRIMARY KEY (airline, flight_number)
);

CREATE INDEX IF NOT EXISTS idx_fis_catalog_status ON fis_flight_catalog (status);
