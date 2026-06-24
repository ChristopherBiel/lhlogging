-- ============================================================
-- Flight-status observations (source: lufthansa.com FIS endpoint)
-- ============================================================
-- Nightly snapshots from the public Lufthansa flight-status feed
-- (/service/api/fis/byflightnumber). For each seed flight number
-- (the B748/A388 flight numbers seen in the last ~2 days) we query a
-- few days ahead and record which airframe (tail) is assigned, the
-- scheduled route/times, and — crucially for the rotation model — the
-- aircraft's *previous* flight. Re-querying the same target date on
-- successive nights captures how the assignment firms up over time.
--
-- One row per (run date, target date, flight). Airports are IATA
-- 3-letter codes as returned by the feed (FRA/HND), not ICAO. The full
-- per-flight payload is kept in `raw` so we can backfill new columns
-- later without re-fetching.
CREATE TABLE IF NOT EXISTS flight_status_observations (
    id                 BIGSERIAL    PRIMARY KEY,
    observed_date      DATE         NOT NULL DEFAULT CURRENT_DATE,  -- nightly run date (container TZ)
    observed_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    flight_date        DATE         NOT NULL,                       -- queried target date
    airline            VARCHAR(3)   NOT NULL DEFAULT 'LH',
    flight_number      VARCHAR(8)   NOT NULL,                       -- numeric part, e.g. '716'
    seed_type          VARCHAR(10),                                 -- why we queried: 'B748' / 'A388'
    found              BOOLEAN      NOT NULL DEFAULT FALSE,          -- did the feed return a flight?
    registration       VARCHAR(12),                                 -- normalised tail, e.g. 'D-ABYN'
    aircraft_type      VARCHAR(40),                                 -- 'Boeing 747-8'
    aircraft_subtype   VARCHAR(12),                                 -- '74H'
    dep_airport_iata   VARCHAR(4),
    arr_airport_iata   VARCHAR(4),
    dep_scheduled      TIMESTAMPTZ,
    arr_scheduled      TIMESTAMPTZ,
    overall_status     VARCHAR(24),                                 -- 'ONTIME', 'DELAYED', ...
    prev_airline       VARCHAR(3),                                  -- previous flight of this airframe
    prev_flight_number VARCHAR(8),
    prev_flight_date   DATE,
    raw                JSONB,

    CONSTRAINT fso_unique UNIQUE (observed_date, flight_date, airline, flight_number)
);

CREATE INDEX IF NOT EXISTS idx_fso_registration  ON flight_status_observations (registration);
CREATE INDEX IF NOT EXISTS idx_fso_flight_date   ON flight_status_observations (flight_date);
CREATE INDEX IF NOT EXISTS idx_fso_flight_number ON flight_status_observations (airline, flight_number);
CREATE INDEX IF NOT EXISTS idx_fso_seed_type     ON flight_status_observations (seed_type);
