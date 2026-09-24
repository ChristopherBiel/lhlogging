-- ============================================================
-- Leg layer over the FIS observations (booking planner foundation)
-- ============================================================
-- flight_status_observations holds one row per *look* (~1.7k/day, with the
-- raw payload). Every question the booking pages ask is about the *leg* —
-- which tail flew LH510 on a date, which tail was published 48h out, how
-- often that holds — and answering it from raw looks means scanning the whole
-- table on every request. `fis_legs` is the collapsed form: one row per
-- scheduled flight, rebuilt by the collector after every run
-- (flightstatus/legs.py; `fetch_flightstatus.py --rebuild-legs` for a full
-- backfill). It is derived data: dropping and rebuilding it loses nothing.
--
-- Times: FIS publishes airport-local wall clock stamped +0000. The *_local
-- columns keep that FIS clock (what the pages display); dep_utc is the true
-- UTC departure (German-hub anchor + block time, see legs.dep_utc), which is
-- what lead times are measured against.
CREATE TABLE IF NOT EXISTS fis_legs (
    flight_date        DATE         NOT NULL,
    airline            VARCHAR(3)   NOT NULL,
    flight_number      VARCHAR(8)   NOT NULL,
    dep_iata           VARCHAR(4),
    arr_iata           VARCHAR(4),
    dep_sched_local    TIMESTAMPTZ,
    arr_sched_local    TIMESTAMPTZ,
    dep_utc            TIMESTAMPTZ,
    duration_min       INTEGER,
    fleet_type         VARCHAR(10),                  -- what flew it, else what is published
    truth_tail         VARCHAR(12),                  -- tail in the ARRIVED/DIVERTED look
    truth_status       VARCHAR(24),
    cancelled          BOOLEAN      NOT NULL DEFAULT FALSE,
    latest_tail        VARCHAR(12),                  -- newest look, whatever it says
    latest_status      VARCHAR(24),
    latest_observed_at TIMESTAMPTZ,
    first_tail         VARCHAR(12),                  -- first pre-departure publication
    first_observed_at  TIMESTAMPTZ,
    first_lead_h       REAL,                         -- lead of our first pre-dep look
    n_obs              INTEGER      NOT NULL DEFAULT 0,
    n_changes          INTEGER      NOT NULL DEFAULT 0,
    n_distinct_tails   INTEGER      NOT NULL DEFAULT 0,
    -- published-tail history, one entry per change: [[lead_h, tail], ...]
    -- (descending lead). legs.tail_at() answers "tail at lead H" from it.
    timeline           JSONB        NOT NULL DEFAULT '[]'::jsonb,
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    PRIMARY KEY (flight_date, airline, flight_number)
);

CREATE INDEX IF NOT EXISTS idx_fis_legs_truth_tail  ON fis_legs (truth_tail);
CREATE INDEX IF NOT EXISTS idx_fis_legs_latest_tail ON fis_legs (latest_tail);
CREATE INDEX IF NOT EXISTS idx_fis_legs_type_date   ON fis_legs (fleet_type, flight_date);
CREATE INDEX IF NOT EXISTS idx_fis_legs_route       ON fis_legs (dep_iata, arr_iata);

-- Cabin history per airframe: every distinct (seat layout, Allegris, FIS
-- sub-type) a tail has been published with, and when. A retrofit shows up as
-- a second row with a later first_seen. Refreshed alongside fis_legs from the
-- same observations (raw->'aircraftInfo'); a one-off glitch row shows up as a
-- span of minutes, which readers can discount by its duration.
CREATE TABLE IF NOT EXISTS airframe_cabin (
    registration VARCHAR(12)  NOT NULL,
    seat_config  VARCHAR(40)  NOT NULL DEFAULT '',   -- e.g. F8C80E32M244
    allegris     BOOLEAN      NOT NULL DEFAULT FALSE,
    sub_type     VARCHAR(12)  NOT NULL DEFAULT '',   -- FIS aircraftSubType, e.g. 74H / 35S
    first_seen   TIMESTAMPTZ  NOT NULL,
    last_seen    TIMESTAMPTZ  NOT NULL,

    PRIMARY KEY (registration, seat_config, allegris, sub_type)
);
