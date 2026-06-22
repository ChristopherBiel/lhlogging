-- ============================================================
-- Callsign → canonical route reference
-- ============================================================
-- Maps a flight-number callsign (e.g. DLH716) to its canonical
-- departure/arrival airports. Used to recover the route when the
-- flight detector fails to resolve dep/arr (common for long-haul
-- arrivals at poor-ADS-B-coverage destinations, which get stored as
-- EDDF→UNKN). Populated by tools/seed_flight_routes.py from a
-- consensus of clean flights plus a small curated override set.
CREATE TABLE IF NOT EXISTS flight_routes (
    callsign                VARCHAR(16) PRIMARY KEY,
    departure_airport_icao  CHAR(4)     NOT NULL,
    arrival_airport_icao    CHAR(4)     NOT NULL,
    source                  VARCHAR(16) NOT NULL DEFAULT 'consensus',
    support                 INTEGER     NOT NULL DEFAULT 0,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
