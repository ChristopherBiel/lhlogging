-- Add needs_review flag for flights where dep == arr (likely missed the real route)
ALTER TABLE flights ADD COLUMN IF NOT EXISTS needs_review BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_flights_needs_review ON flights (needs_review) WHERE needs_review = TRUE;

-- Add needs_review flag for aircraft with missing type/registration data
ALTER TABLE aircraft ADD COLUMN IF NOT EXISTS needs_review BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_aircraft_needs_review ON aircraft (needs_review) WHERE needs_review = TRUE;

-- Flag existing aircraft that have missing data
UPDATE aircraft SET needs_review = TRUE
WHERE aircraft_type IS NULL
   OR registration = UPPER(icao24);
