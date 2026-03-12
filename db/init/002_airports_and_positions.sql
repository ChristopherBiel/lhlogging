-- ============================================================
-- Extensions for airport distance lookups
-- ============================================================
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;


-- ============================================================
-- Airports (source: OurAirports, loaded by tools/load_airports.py)
-- ============================================================
CREATE TABLE IF NOT EXISTS airports (
    icao_code   CHAR(4)          PRIMARY KEY,
    name        VARCHAR(200),
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL
);


-- ============================================================
-- Positions table: add callsign column + performance indexes
-- ============================================================
ALTER TABLE positions ADD COLUMN IF NOT EXISTS callsign VARCHAR(10);

CREATE INDEX IF NOT EXISTS idx_positions_icao24_captured
    ON positions (icao24, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_positions_captured_at
    ON positions (captured_at);
