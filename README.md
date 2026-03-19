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
│   │   └── utils.py                # Logging, retry decorator, rate limiter
│   ├── tools/
│   │   ├── load_airports.py        # One-off: populates airports table from OurAirports
│   │   ├── eval_flightaware.py     # FlightAware AeroAPI evaluation + fleet rebuild tool
│   │   └── review.py               # SSH-based review tool for flagged aircraft and flights
│   ├── crontab                     # Cron schedule (runs inside Docker)
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard/                      # Live monitoring UI
│   ├── app.py                      # Flask app with dark-themed SPA
│   ├── Dockerfile
│   └── requirements.txt
├── db/
│   └── init/
│       ├── 001_schema.sql          # PostgreSQL schema (auto-applied on first run)
│       ├── 002_airports_and_positions.sql  # Airports table + indexes migration
│       └── 003_flights_needs_review.sql    # needs_review flag for flights and aircraft
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
| **Fleet Discovery** | Every 30 min (at :00 and :30) | Discovers new aircraft via live DLH callsign matching (OpenSky + Planespotters) |
| **Positions Cleanup** | Daily at 04:00 UTC | Deletes position snapshots older than `POSITIONS_RETENTION_DAYS` |
| **Fleet Refresh** | Mondays at 02:00 UTC | Updates type data for existing fleet, retires decommissioned aircraft. Does **not** add new aircraft (that's fleet_discovery's job) |

---

## Dashboard

The monitoring dashboard runs on port **8080** and auto-refreshes every 30 seconds.

**What it shows:**
- System health — last run status, result details, and timing for each job (state poller, flight detector, fleet discovery, fleet refresh)
- Fleet breakdown — active/retired aircraft counts by type
- Flight metrics — today, 7-day, and all-time counts (excludes `needs_review` flights)
- Daily trend chart — flights and unique callsigns over the last 14 days
- Recent errors — any job failures in the last 48 hours (hidden when all clear)
- Top routes — most frequent city pairs (30-day window)
- Fleet table — sortable/filterable list with a "Needs Review" checkbox to find aircraft missing type or registration data

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
```

Then deploy. The 003 migration also auto-flags existing aircraft that have missing type data or placeholder registrations.

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
