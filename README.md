<div align="center">

# LH Fleet Logger

**Automated flight data collection for the entire Lufthansa fleet.**

[![Deploy](https://github.com/ChristopherBiel/lhlogging/actions/workflows/deploy.yml/badge.svg)](https://github.com/ChristopherBiel/lhlogging/actions/workflows/deploy.yml)
[![Dashboard](https://img.shields.io/website?url=https%3A%2F%2Flhlogging.biels.net&label=dashboard&up_message=online&down_message=offline)](https://lhlogging.biels.net)
[![Python](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16-4169E1?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![OpenSky](https://img.shields.io/badge/data-OpenSky%20Network-1a1a2e)](https://opensky-network.org/)
[![Credits/day](https://img.shields.io/badge/API%20cost-~2%2C928%20credits%2Fday-yellow)](#credit-budget)

Tracks every Lufthansa aircraft — from A320s to A380s — logging departure/arrival airports, flight times, and route data into a PostgreSQL database. A live monitoring dashboard gives you a bird's-eye view of fleet activity, route frequency, and system health.

[Live Dashboard](https://lhlogging.biels.net) · [Architecture](#architecture) · [Quick Start](#quick-start) · [Configuration](#configuration)

</div>

---

## How It Works

```
                                         ┌──────────────────┐
  ┌──────────────┐    /states/all        │                  │
  │  OpenSky API │◄───── live poll ──────│  State Poller    │──── every 2 min
  └──────────────┘    (all global        │                  │
                       aircraft)         └────────┬─────────┘
                                                  │ positions
                                         ┌────────▼─────────┐
                                         │                  │
                                         │ Flight Detector  │──── every 30 min
                                         │                  │
                                         └────────┬─────────┘
                                                  │ infers flights from
  ┌──────────────┐    Aircraft DB CSV             │ on_ground transitions
  │  OpenSky CSV │◄──────────────────────┐        │
  └──────────────┘                       │        │
                                  ┌──────▼────────▼─────────┐
                                  │       PostgreSQL          │
                                  │       - aircraft          │
                                  │       - positions         │
                                  │       - flights           │
                                  │       - airports          │
                                  │       - batch_runs        │
                                  └────────────┬─────────────┘
                                               │
                                      ┌────────▼────────┐
                                      │   Dashboard      │──── :8080
                                      │   (Flask)        │
                                      └─────────────────┘
```

### The Position-Snapshot Approach

The OpenSky `/states/all` endpoint returns live state vectors for all aircraft globally in a single API call. Every 2 minutes the state poller fetches this snapshot, filters it to the Lufthansa fleet, and stores it in the `positions` table.

The flight detector runs every 30 minutes and walks each aircraft's position history looking for ground/air transitions:
- **Ground → Air** = departure (airport identified from the last ground position lat/lon)
- **Air → Ground** = arrival (airport identified from the first ground position lat/lon)

Landing detection uses OpenSky's `on_ground` flag with a **velocity+altitude fallback**: if `on_ground` is false but velocity < 30 m/s and altitude < 300 m, the aircraft is treated as on the ground. This catches cases where OpenSky's flag is unreliable (e.g. aircraft clearly stationary at an airport but still reporting `on_ground=false`).

Flights that are still in progress are inserted immediately as pending records and updated when the aircraft lands — so a 14-hour flight to Buenos Aires is handled just as well as a 90-minute hop to Munich. Pending flights older than 24 hours are automatically closed (with arrival `UNKN`) and flagged for review, preventing outages from leaving flights stuck open forever.

**Dep == Arr detection:** When a flight's detected departure and arrival airport are the same (e.g. EDDF→EDDF), it usually means the system missed the real arrival and the subsequent departure, merging two separate flights into one. These flights are closed normally but flagged with `needs_review = TRUE` so they are excluded from statistics and can be corrected manually via the [review tool](#review-tool).

Airport identification uses the [OurAirports](https://ourairports.com/) dataset (~6,000 large/medium airports) stored locally, with nearest-neighbour lookup via PostgreSQL's `earthdistance` extension.

### Why Not Per-Aircraft Queries?

OpenSky's `/flights/aircraft` endpoint charges 30 credits per call. Querying all 400+ fleet aircraft twice daily would cost **24,000+ credits/day** — 6× the 4,000 credit budget. The `/states/all` live endpoint costs 4 credits per call (for a full-world request) regardless of how many aircraft are returned, making it scale-free.

---

## Architecture

```
lhlogging/
├── app/                            # Core application
│   ├── lhlogging/
│   │   ├── config.py               # Environment-based configuration
│   │   ├── db.py                   # PostgreSQL operations & upserts
│   │   ├── opensky.py              # OpenSky API client (OAuth2, retry, rate limiting)
│   │   ├── opensky_fleet.py        # Aircraft database CSV downloader
│   │   ├── planespotters.py        # Planespotters API client (fleet type enrichment)
│   │   ├── state_poller.py         # Every 2 min — snapshots live positions
│   │   ├── flight_detector.py      # Every 30 min — infers flights from positions
│   │   ├── fleet_discovery.py      # Every 30 min — discovers new aircraft via DLH callsigns
│   │   ├── positions_cleanup.py    # Daily — deletes old position snapshots
│   │   ├── fleet_refresh.py        # Weekly — updates type data, retires decommissioned aircraft
│   │   ├── route_enrichment.py     # Every 30 min — recovers dep/arr from callsign→route reference
│   │   └── utils.py                # Logging, retry decorator, rate limiter
│   ├── tools/
│   │   ├── load_airports.py        # One-off: populates airports table from OurAirports
│   │   ├── seed_flight_routes.py   # Builds callsign→route reference (consensus + curated)
│   │   ├── backfill_routes.py      # One-off: enriches all history from flight_routes
│   │   ├── eval_flightaware.py     # FlightAware AeroAPI evaluation + fleet rebuild tool
│   │   └── review.py               # SSH-based review tool for flagged aircraft and flights
│   ├── crontab                     # Cron schedule (runs inside Docker)
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard/                      # Monitoring UI + fleet-catching pages
│   ├── app.py                      # Flask app: / (monitor), /book, /schedule, /insights, /fleet
│   ├── Dockerfile
│   └── requirements.txt
├── db/
│   └── init/
│       ├── 001_schema.sql          # PostgreSQL schema (auto-applied on first run)
│       ├── 002_airports_and_positions.sql  # Airports table + indexes migration
│       ├── 003_flights_needs_review.sql    # needs_review flag for flights and aircraft
│       ├── 004_flight_routes.sql           # callsign→route reference table
│       └── 005_airports_type.sql           # airport size class (hub-preference lookup)
├── docker-compose.yml              # Three services: db, app, dashboard
└── .github/
    └── workflows/
        └── deploy.yml              # Push to main → auto-deploy via SSH
```

### Database Schema

| Table | Purpose |
|---|---|
| **aircraft** | Fleet registry — ICAO24, registration, type, active status, `needs_review` flag |
| **positions** | 2-minute position snapshots — lat/lon, altitude, on_ground, callsign |
| **flights** | Route log — airports, callsign, timestamps, auto-calculated duration, `needs_review` flag |
| **airports** | Static airport lookup — ICAO code, lat/lon (from OurAirports) |
| **batch_runs** | Audit trail — every job run with stats and error details |

Key design decisions:
- **Pending flights** — flights are inserted when a departure is detected with `arrival_airport_icao = NULL`, then updated when the aircraft lands. Handles flights of any duration.
- **Upsert on `(icao24, first_seen)`** — re-detecting an already-logged flight safely updates arrival info without creating duplicates.
- **Generated columns** — `flight_date` and `duration_minutes` are computed automatically from timestamps.
- **30-day position retention** — snapshots are cleaned up daily; only the derived `flights` records are kept permanently.
- **`needs_review` flags** — flights with dep == arr and aircraft with missing type/registration are flagged for manual review. Flagged records are excluded from all dashboard statistics but remain queryable for correction via the [review tool](#review-tool).

---

## Quick Start

### 1. Clone & configure

```bash
git clone git@github.com:ChristopherBiel/lhlogging.git && cd lhlogging
cp .env.example .env
# Edit .env with your credentials:
#   - POSTGRES_USER / POSTGRES_PASSWORD
#   - OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET (from opensky-network.org account settings)
```

### 2. Launch

```bash
docker compose up -d
```

This starts three containers:
- **db** — PostgreSQL 16 (schema auto-initialized from `db/init/`)
- **app** — Python 3.12 with cron for scheduled jobs
- **dashboard** — Flask app on port 8080

### 3. Load airport data (one-off)

```bash
docker compose exec app python tools/load_airports.py
```

This populates the `airports` table (~6,000 large/medium airports) used for identifying departure and arrival airports from lat/lon coordinates.

### 4. Verify

```bash
# Check services are healthy
docker compose ps

# Watch the state poller in action
docker compose logs -f app

# Open dashboard
open http://localhost:8080
```

The first state poller run happens within 2 minutes of startup. The first flight detections appear within 30 minutes (after enough position history has accumulated).

---

## Configuration

All settings are environment variables (via `.env`):

| Variable | Default | Description |
|---|---|---|
| `OPENSKY_CLIENT_ID` | *(required)* | OAuth2 client ID |
| `OPENSKY_CLIENT_SECRET` | *(required)* | OAuth2 client secret |
| `POSTGRES_USER` | *(required)* | Database user |
| `POSTGRES_PASSWORD` | *(required)* | Database password |
| `TRACK_AIRCRAFT_TYPES` | *(empty = all)* | Comma-separated ICAO type codes to filter (e.g. `A388,B748`) |
| `FLIGHT_DETECT_LOOKBACK_MINUTES` | `60` | How far back the detector scans for new departures (recommended: `90`) |
| `LANDING_VELOCITY_THRESHOLD_MS` | `30.0` | Velocity fallback: below this (m/s) + altitude threshold = on ground |
| `LANDING_ALTITUDE_THRESHOLD_M` | `300.0` | Altitude fallback: below this (m) + velocity threshold = on ground |
| `MISSED_DEPARTURE_ALTITUDE_M` | `3000.0` | High-confidence inferred departure if aircraft below this altitude |
| `MISSED_DEPARTURE_DISTANCE_KM` | `100.0` | High-confidence inferred departure if within this distance of last arrival |
| `MISSED_DEPARTURE_MAX_GAP_H` | `48` | Max hours since last landing to infer departure from that airport |
| `POSITIONS_RETENTION_DAYS` | `30` | How long position snapshots are kept |
| `AIRPORT_LOOKUP_RADIUS_KM` | `50.0` | Max distance for nearest-airport matching |
| `OPENSKY_REQUEST_DELAY_S` | `2.0` | Delay between API calls |
| `OPENSKY_RATELIMIT_BACKOFF_S` | `60` | Sleep time on HTTP 429 |
| `FLIGHTAWARE_API_KEY` | *(optional)* | For `tools/eval_flightaware.py` — fleet evaluation and rebuild |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

### Tracking Subsets

To track only specific aircraft types, set `TRACK_AIRCRAFT_TYPES`:

```bash
# Only A380s and 747-8s
TRACK_AIRCRAFT_TYPES=A388,B748

# All widebodies
TRACK_AIRCRAFT_TYPES=A332,A333,A343,A345,A346,A359,A35K,A388,B744,B748,B788,B789

# Everything (default)
TRACK_AIRCRAFT_TYPES=
```

---

## Scheduled Jobs

| Job | Schedule | What it does |
|---|---|---|
| **State Poller** | Every 2 min | Fetches `/states/all`, stores position snapshots for the LH fleet |
| **Flight Detector** | Every 30 min (at :15 and :45) | Detects flights from ground/air transitions (with velocity+altitude fallback), closes pending arrivals, auto-closes stale flights (>24h), infers missed departures for airborne aircraft with no open flight |
| **Route Enrichment** | Every 30 min (at :20 and :50) | Recovers missing/UNKN dep/arr (and clears `needs_review`) for recent flights from the `flight_routes` callsign→route reference; normalises EDFE→EDDF |
| **Fleet Discovery** | Every 30 min (at :00 and :30) | Discovers new aircraft via live DLH callsign matching (OpenSky + Planespotters) |
| **Positions Cleanup** | Daily at 04:00 UTC | Deletes position snapshots older than `POSITIONS_RETENTION_DAYS` |
| **Fleet Refresh** | Mondays at 02:00 UTC | Updates type data for existing fleet, retires decommissioned aircraft. Does **not** add new aircraft (that's fleet_discovery's job) |
| **Flight-Routes Seed** | Mondays at 02:30 UTC | Rebuilds the `flight_routes` callsign→route reference from a consensus of clean flights (plus curated overrides) |
| **Flight-Status Fetch** | 15 slots/day, Europe/Berlin ([schedule](docs/flightstatus_schedule.md)) | Pulls the public Lufthansa FIS feed for the catalogued widebody flight numbers (747-8, A380, 787, A350) across D-2…D+4; records assigned tail, route, scheduled times, and the airframe's previous flight into `flight_status_observations` (see below). Coverage is tiered: 747-8/A380 are re-checked every ~2.6h at D+1/D+2 so reassignments can be dated to a few hours |

---

## Flight-Status Collector

A separate `flightstatus` service (its own image — it needs a real browser)
captures the **planned airframe→route assignments** ahead of time, which the
ADS-B pipeline can only observe after the fact.

- **Source:** `https://www.lufthansa.com/service/api/fis/byflightnumber?flightNumber=LH716&date=YYYY-MM-DD`.
  Unlike the official Lufthansa Open API (which returns only the 3-letter
  aircraft *type*), this feed includes the **registration (tail)** and the
  aircraft's **previous flight** — enough to chain an airframe's rotation.
- **Bot protection:** the endpoint sits behind Imperva/Distil. `curl` and
  headless browsers are blocked; only a *real headed* Chromium gets through, so
  the service runs Playwright headed under Xvfb. Requests are paced gently
  (5–10 s) with a rate-limit backoff, one run at a time (flock) — a ~450-lookup
  sweep takes ~90 min, a 66-lookup pulse ~12 min.
- **Storage:** every pass appends a row per `(run, flight_date, flight_number)`,
  so **reassignments are preserved as a time series** — ordering a flight's rows
  by `observed_at` shows the plan firm up or swap. Full per-flight payload kept
  in `raw` JSONB.
- **Run modes** — `--dry-run` on any of them prints the planned lookups by lead
  offset without touching the browser or the DB:

  | mode | what it does | ~lookups |
  |---|---|---|
  | *(none)* | near sweep, D-2…D+2 — owns discovery, pairing, catalog lifecycle, `previousFlight` chaining, coverage audit | 400–470 |
  | `--far` | far pass, broad tier D+3 / deep tier D+1…D+4, read-only against the catalog | ~220 |
  | `--watch H` | re-check only flights departing within the next H hours | 5–50 |
  | `--pulse` | re-check only the deep tier at D+1/D+2, several times a day | ~66 |
  | `--flight LH716 --date …` | ad-hoc single lookup, prints JSON (`tools/fis_lookup.sh`) | 1 |
  | `--audit` | print the fleet-continuity coverage audit, no browser | 0 |

- **Tiering:** the *deep* tier (`FIS_DEEP_TYPES`, default `B748,A388`) is tracked
  at high cadence because reassignment hazard turns out to be nearly flat in
  lead time — more frequent looks buy far more than deeper ones. Tier membership
  comes from the modal *observed* fleet type per flight number, not from the
  catalog's `seed_type`. See [docs/flightstatus_schedule.md](docs/flightstatus_schedule.md)
  for the measurements behind the schedule and the request budget.
- **Config:** `FIS_SEED_TYPES` (default `B748,A388,B788,B789,B78X,A359,A35K`),
  `FIS_LOOKAHEAD_DAYS` (2), `FIS_BACKFILL_DAYS` (2), `FIS_FAR_MIN_DAYS`/`MAX_DAYS`
  (3/3), `FIS_FAR_DEEP_MAX_DAYS` (4), `FIS_DEEP_TYPES`, `FIS_PULSE_OFFSETS`
  (`1,2`), `FIS_DEEP_LOOKAHEAD_BONUS` (1), `FIS_SEED_LOOKBACK_DAYS` (2),
  `FIS_MAX_LOOKUPS` (700), `FIS_SESSION_LOOKUPS` (80),
  `FIS_REQUEST_DELAY_MIN_S`/`MAX_S`, `FIS_BLOCK_BACKOFF_S`. Ad-hoc single lookup:
  `docker compose exec flightstatus /app/run_nightly.sh --flight LH716 --date 2026-06-25`
  (set `NIGHTLY_JITTER=0` to skip the start jitter).

---

## Dashboard

The dashboard runs on port **8080**. The `/` monitor page auto-refreshes every
30 seconds; the fleet-catching pages below are driven by the nightly FIS
snapshots.

**`/` — Monitor** (auto-refreshing system + fleet overview):
- System health — last run status, result details, and timing for each job (state poller, flight detector, fleet discovery, fleet refresh)
- Fleet breakdown — active/retired aircraft counts by type
- Flight metrics — today, 7-day, and all-time counts (excludes `needs_review` flights)
- Daily trend chart — flights and unique callsigns over the last 14 days
- Recent errors — any job failures in the last 48 hours (hidden when all clear)
- Top routes — most frequent city pairs (30-day window)
- Fleet table — sortable/filterable list with a "Needs Review" checkbox to find aircraft missing type or registration data

### Fleet-catching pages

Built on the FIS snapshots, these help you *catch* a specific airframe — find
an upcoming flight, see which tail is published on it, and how likely that
assignment is to still hold by departure.

- **`/book` — Catch a Tail.** Find an upcoming flight three ways: **by tail**
  (every leg a registration is published on), **by route** (several alternative
  airports per side), or **by location** (pick a departure airport off a world
  map). Each result carries the currently published tail, its cabin config
  (First/Business seat counts, Allegris marker), and a measured hold
  probability — how often that assignment survives to departure, from the
  reassignment time series. Deep-linkable: `?reg=D-ABYN`, `?dep=FRA&arr=HND`,
  `?loc=KIX`.
- **`/schedule`** — per-airframe upcoming timeline from the latest snapshot of
  each flight, grouped by tail.
- **`/insights`** — descriptive, backward-looking analytics per aircraft
  type/family (747-8, A380, 787, A350): route frequency, rotation transitions,
  per-airframe profiles, and reassignment reliability.
- **`/fleet`** — the aircraft database: one row per airframe, drill into a tail's
  flight log and route history.

**Map mode note.** The `/book` world map is self-hosted: the land/border
outline and airport coordinates are inlined in `app.py` (regenerate with
`tools/build_book_map.py`) and served from `/book/world.json` and
`/api/book/map`. It deliberately loads no third-party map tiles, so no external
service sees a visitor — consistent with the Datenschutz page. An airport with
an upcoming departure but no known coordinates is listed under the map rather
than dropped, which is the cue to re-run the generator (`--extra <IATA>` forces
in a non-large field).

---

## Local Data Analysis

Offline tooling for digging into the real data with plain Python (no DB or extra
deps). `tools/pull_data.sh` pulls a fresh flights snapshot from the production DB
into `tmp/flights_export.csv` (gitignored); the analysis scripts read that file.

Configure the pull once via a gitignored `.env.local` in the repo root:

```bash
# .env.local
LHLOGGING_SSH=user@your-server         # ssh host/alias
LHLOGGING_REMOTE_DIR=/path/to/lhlogging # server project dir (has docker-compose.yml)
# LHLOGGING_DB_USER / LHLOGGING_DB_NAME  # optional, default: lhlogging
```

Then:

```bash
./tools/pull_data.sh                              # refresh tmp/flights_export.csv

python3 tools/analyze_rotation.py                 # rotation model + backtest (default D-ABYN)
python3 tools/analyze_rotation.py --reg D-ABYO --targets RJTT,SAEZ,FAOR
python3 tools/data_quality_report.py --type B748  # UNKN/EDFE/needs_review + callsign-resolution
python3 tools/explore_fleet.py --reg D-ABYN       # one tail's flight log + route mix
python3 tools/explore_fleet.py --route EDDF-FAOR  # who flies a route, and when
```

Routes are resolved through the same callsign reference the dashboard uses, so legs
the detector left as `UNKN` still show their real destination. `tools/_lhdata.py`
holds the shared loader/route logic.

---

## Deployment

Pushing to `main` triggers automatic deployment via GitHub Actions:

```
Push to main → SSH to production → git pull → docker compose up -d --build
```

Required GitHub secrets:
- `DEPLOY_HOST` — server IP/hostname
- `DEPLOY_USER` — SSH username
- `DEPLOY_SSH_KEY` — private SSH key
- `DEPLOY_PATH` — path to the repo on the server

### First-time setup on a new server

After the initial `docker compose up -d`, run the airport loader once:

```bash
docker compose exec app python tools/load_airports.py
```

### Migrating an existing deployment

Apply any new schema migrations manually before deploying the new image:

```bash
# Airports & positions (if not already applied)
ssh user@your-server "docker exec -i lhlogging-db-1 psql -U your_db_user -d lhlogging" \
  < db/init/002_airports_and_positions.sql

# needs_review flags for flights and aircraft
ssh user@your-server "docker exec -i lhlogging-db-1 psql -U your_db_user -d lhlogging" \
  < db/init/003_flights_needs_review.sql

# callsign→route reference table + airport size class
ssh user@your-server "docker exec -i lhlogging-db-1 psql -U your_db_user -d lhlogging" \
  < db/init/004_flight_routes.sql
ssh user@your-server "docker exec -i lhlogging-db-1 psql -U your_db_user -d lhlogging" \
  < db/init/005_airports_type.sql
```

Then deploy. The 003 migration also auto-flags existing aircraft that have missing type data or placeholder registrations.

The app degrades gracefully if `004`/`005` haven't been applied yet — the nearest-airport
lookup falls back to its plain form, route enrichment skips itself, and the dashboard's
747-8 page falls back to raw dep/arr — so deploy order isn't critical. Apply the migrations
to *enable* the features, then populate the new tables and backfill history (dry-run first
to preview the row counts):

```bash
# reload airports so the `type` column is populated (enables hub-preference lookup)
docker compose exec app python tools/load_airports.py

# build the callsign→route reference, then backfill all history
docker compose exec app python -m tools.seed_flight_routes --apply
docker compose exec app python -m tools.backfill_routes            # dry-run preview
docker compose exec app python -m tools.backfill_routes --apply    # apply
```

---

## Fleet Management

The fleet is managed through two complementary mechanisms:

- **Fleet Discovery** (every 30 min) — the sole path for adding new aircraft. Monitors live ADS-B data for DLH callsigns, discovers unknown aircraft, and enriches them via OpenSky CSV and Planespotters. Aircraft with missing type or placeholder registrations are auto-flagged `needs_review` for manual correction.
- **Fleet Refresh** (weekly) — updates type/subtype data for existing aircraft and retires those no longer in the OpenSky registry. Does **not** add new aircraft to prevent database bloat from the OpenSky CSV's broad registration-prefix matching. Clears the `needs_review` flag when enrichment fills in missing data.

**Why this separation matters:** The OpenSky CSV contains ~900+ aircraft matching `operatoricao=DLH` or `D-A*` registration prefix (including non-LH carriers like Condor, Eurowings). Allowing fleet_refresh to add aircraft would re-bloat the database. Fleet discovery uses callsign-based confirmation to ensure only genuine LH mainline aircraft are tracked.

### FlightAware AeroAPI Tool

The `tools/eval_flightaware.py` script uses the FlightAware AeroAPI (requires `FLIGHTAWARE_API_KEY` in `.env`) for fleet evaluation and one-off database rebuilds:

```bash
# Evaluate: compare FA data against current DB
docker compose exec app python tools/eval_flightaware.py

# Rebuild: truncate DB and seed with FA-confirmed D-A* aircraft
docker compose exec app python tools/eval_flightaware.py --rebuild-db

# Update: fill missing types and reactivate aircraft
docker compose exec app python tools/eval_flightaware.py --update-db
```

The rebuild mode cross-references FlightAware (source of truth for in-service aircraft) with the OpenSky CSV (source of ICAO24 hex codes needed for ADS-B tracking). Aircraft confirmed by FA but missing from the CSV are picked up by fleet_discovery within hours.

**Cost:** $10/month free credit as an ADS-B data contributor. A single evaluation run uses ~15 pages (~$0.75 estimated, though actual billing has shown $0.00).

---

## Credit Budget

| | Per call | Daily calls | Daily cost |
|---|---|---|---|
| State poller | 4 credits | 720 (every 2 min) | **~2,880 credits** |
| Fleet discovery | 1 credit | 48 (every 30 min) | **~48 credits** |
| Fleet refresh | ~free (CSV download) | 1/week | **~0** |
| **Total** | | | **~2,928 credits/day** |

This uses **~73%** of the 4,000 credit daily budget, leaving headroom for retries and rate-limit recovery.

---

## Review Tool

Aircraft and flights that need manual attention are flagged with `needs_review = TRUE` in the database. This happens automatically when:

- **Flights:** the detected departure and arrival airport are the same (dep == arr), indicating the system likely merged two separate flights
- **Aircraft:** the type is missing or the registration is a placeholder (hex code used as registration)

Flagged records are excluded from all dashboard statistics but remain visible in detail views. To correct them, use the markdown-based review tool via SSH — no web login required.

### Workflow

```bash
# 1. SSH into the VPS and export the review queue
docker exec -it <app-container> python -m tools.review export

# 2. Edit the generated markdown file
docker exec -it <app-container> nano /var/log/lhlogging/review.md

# 3. Apply your changes back to the database
docker exec -it <app-container> python -m tools.review apply
```

You can also specify a custom file path with `--file /path/to/review.md`.

### Review file format

The exported file looks like this:

```markdown
# Review Queue

Exported: 2026-03-14 15:30 UTC

Actions:
  PENDING  — skip, keep flagged (default)
  UPDATE   — apply your edits to the database and clear the flag
  DISMISS  — clear the flag without changing any data

## Aircraft

### 3c4a52

- action: PENDING
- registration: 3C4A52
- type:
- subtype:

## Flights

### flight-1234

- action: PENDING
- icao24: 3c4a52
- registration: D-AIMC
- callsign: DLH438
- dep: EDDF
- arr: EDDF
- time: 2026-03-14 08:12 — 2026-03-14 08:45 (33min)
```

To correct an aircraft, look it up on FlightRadar24, fill in the details, and change the action:

```markdown
- action: UPDATE
- registration: D-AXYZ
- type: A320
- subtype: A320-214
```

To accept a flight or aircraft as-is (just clear the flag without editing), change the action to `DISMISS`. Entries left as `PENDING` are skipped and will appear again on the next export.

---

<div align="center">

*Flight data provided by [The OpenSky Network](https://opensky-network.org/).*

[![Last Commit](https://img.shields.io/github/last-commit/ChristopherBiel/lhlogging?label=last%20commit)](https://github.com/ChristopherBiel/lhlogging/commits/main)

</div>
