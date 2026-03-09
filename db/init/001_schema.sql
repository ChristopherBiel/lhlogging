-- ============================================================
-- Aircraft fleet (source: Planespotters.net)
-- ============================================================
CREATE TABLE IF NOT EXISTS aircraft (
    id               SERIAL PRIMARY KEY,
    icao24           CHAR(6)      NOT NULL,
    registration     VARCHAR(10)  NOT NULL,
    aircraft_type    VARCHAR(10),
    aircraft_subtype VARCHAR(50),
    airline_iata     VARCHAR(3)   NOT NULL DEFAULT 'LH',
    first_seen_date  DATE         NOT NULL DEFAULT CURRENT_DATE,
    last_seen_date   DATE,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT aircraft_icao24_unique UNIQUE (icao24)
);

CREATE INDEX IF NOT EXISTS idx_aircraft_icao24        ON aircraft (icao24);
CREATE INDEX IF NOT EXISTS idx_aircraft_is_active     ON aircraft (is_active);
CREATE INDEX IF NOT EXISTS idx_aircraft_registration  ON aircraft (registration);


-- ============================================================
-- Flight routes (source: OpenSky /flights/aircraft)
-- ============================================================
CREATE TABLE IF NOT EXISTS flights (
    id                     BIGSERIAL    PRIMARY KEY,
    icao24                 CHAR(6)      NOT NULL,
    callsign               VARCHAR(10),
    departure_airport_icao CHAR(4),
    arrival_airport_icao   CHAR(4),
    first_seen             TIMESTAMPTZ  NOT NULL,
    last_seen              TIMESTAMPTZ  NOT NULL,
    flight_date            DATE         GENERATED ALWAYS AS (CAST(first_seen AT TIME ZONE 'UTC' AS DATE)) STORED,
    duration_minutes       INTEGER      GENERATED ALWAYS AS (
                               EXTRACT(EPOCH FROM (last_seen - first_seen))::INTEGER / 60
                           ) STORED,
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT flights_icao24_first_seen_unique UNIQUE (icao24, first_seen),

    CONSTRAINT fk_flights_aircraft FOREIGN KEY (icao24)
        REFERENCES aircraft (icao24)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_flights_icao24      ON flights (icao24);
CREATE INDEX IF NOT EXISTS idx_flights_flight_date ON flights (flight_date);
CREATE INDEX IF NOT EXISTS idx_flights_departure   ON flights (departure_airport_icao);
CREATE INDEX IF NOT EXISTS idx_flights_arrival     ON flights (arrival_airport_icao);
CREATE INDEX IF NOT EXISTS idx_flights_callsign    ON flights (callsign);
CREATE INDEX IF NOT EXISTS idx_flights_route_date  ON flights (departure_airport_icao, arrival_airport_icao, flight_date);


-- ============================================================
-- Batch run audit log
-- ============================================================
CREATE TABLE IF NOT EXISTS batch_runs (
    id               BIGSERIAL    PRIMARY KEY,
    run_type         VARCHAR(20)  NOT NULL,
    started_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    aircraft_total   INTEGER,
    aircraft_ok      INTEGER,
    aircraft_error   INTEGER,
    flights_upserted INTEGER,
    error_detail     TEXT,
    status           VARCHAR(10)  NOT NULL DEFAULT 'running'
);


-- ============================================================
-- Future: position snapshots (schema stub, not yet populated)
-- ============================================================
CREATE TABLE IF NOT EXISTS positions (
    id          BIGSERIAL        PRIMARY KEY,
    icao24      CHAR(6)          NOT NULL,
    captured_at TIMESTAMPTZ      NOT NULL,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    altitude_m  REAL,
    velocity_ms REAL,
    heading     REAL,
    on_ground   BOOLEAN,

    CONSTRAINT positions_icao24_time_unique UNIQUE (icao24, captured_at),

    CONSTRAINT fk_positions_aircraft FOREIGN KEY (icao24)
        REFERENCES aircraft (icao24)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_positions_airborne
    ON positions (icao24, captured_at)
    WHERE on_ground = FALSE;
