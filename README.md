<div align="center">

# LH Fleet Logger

**Automated flight data collection for the entire Lufthansa fleet.**

[![Deploy](https://github.com/ChristopherBiel/lhlogging/actions/workflows/deploy.yml/badge.svg)](https://github.com/ChristopherBiel/lhlogging/actions/workflows/deploy.yml)
[![Dashboard](https://img.shields.io/website?url=https%3A%2F%2Flhlogging.biels.net&label=dashboard&up_message=online&down_message=offline)](https://lhlogging.biels.net)
[![Python](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16-4169E1?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![OpenSky](https://img.shields.io/badge/data-OpenSky%20Network-1a1a2e)](https://opensky-network.org/)
[![Credits/day](https://img.shields.io/badge/API%20cost-~300%20credits%2Fday-brightgreen)](#credit-budget)

Tracks every Lufthansa aircraft — from A320s to A380s — logging departure/arrival airports, flight times, and route data into a PostgreSQL database. A live monitoring dashboard gives you a bird's-eye view of fleet activity, route frequency, and system health.

[Live Dashboard](https://lhlogging.biels.net) · [Architecture](#architecture) · [Quick Start](#quick-start) · [Configuration](#configuration)

</div>

---

## How It Works

```
                                         ┌──────────────────┐
  ┌──────────────┐    /states/all        │                  │
  │  OpenSky API │◄───── live poll ──────│  State Poller    │──── every 5 min
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

The OpenSky `/states/all` endpoint returns live state vectors for all aircraft globally in a single API call. Every 5 minutes the state poller fetches this snapshot, filters it to the Lufthansa fleet, and stores it in the `positions` table.

The flight detector runs every 30 minutes and walks each aircraft's position history looking for `on_ground` transitions:
- **Ground → Air** = departure (airport identified from the last ground position lat/lon)
- **Air → Ground** = arrival (airport identified from the first ground position lat/lon)

Flights that are still in progress are inserted immediately as pending records and updated when the aircraft lands — so a 14-hour flight to Buenos Aires is handled just as well as a 90-minute hop to Munich.

Airport identification uses the [OurAirports](https://ourairports.com/) dataset (~6,000 large/medium airports) stored locally, with nearest-neighbour lookup via PostgreSQL's `earthdistance` extension.

### Why Not Per-Aircraft Queries?

OpenSky's `/flights/aircraft` endpoint charges 30 credits per call. Querying all 400+ fleet aircraft twice daily would cost **24,000+ credits/day** — 6× the 4,000 credit budget. The `/states/all` live endpoint costs a flat rate per call regardless of how many aircraft are returned, making it scale-free.

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
│   │   ├── state_poller.py         # Every 5 min — snapshots live positions
│   │   ├── flight_detector.py      # Every 30 min — infers flights from positions
│   │   ├── positions_cleanup.py    # Daily — deletes old position snapshots
│   │   ├── fleet_refresh.py        # Weekly — syncs fleet registry
│   │   └── utils.py                # Logging, retry decorator, rate limiter
│   ├── tools/
│   │   └── load_airports.py        # One-off: populates airports table from OurAirports
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
│       └── 002_airports_and_positions.sql  # Airports table + indexes migration
├── docker-compose.yml              # Three services: db, app, dashboard
└── .github/
    └── workflows/
        └── deploy.yml              # Push to main → auto-deploy via SSH
```

### Database Schema

| Table | Purpose |
|---|---|
| **aircraft** | Fleet registry — ICAO24, registration, type, active status |
| **positions** | 5-minute position snapshots — lat/lon, altitude, on_ground, callsign |
| **flights** | Route log — airports, callsign, timestamps, auto-calculated duration |
| **airports** | Static airport lookup — ICAO code, lat/lon (from OurAirports) |
| **batch_runs** | Audit trail — every job run with stats and error details |

Key design decisions:
- **Pending flights** — flights are inserted when a departure is detected with `arrival_airport_icao = NULL`, then updated when the aircraft lands. Handles flights of any duration.
- **Upsert on `(icao24, first_seen)`** — re-detecting an already-logged flight safely updates arrival info without creating duplicates.
- **Generated columns** — `flight_date` and `duration_minutes` are computed automatically from timestamps.
- **30-day position retention** — snapshots are cleaned up daily; only the derived `flights` records are kept permanently.

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

The first state poller run happens within 5 minutes of startup. The first flight detections appear within 35 minutes (after enough position history has accumulated).

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
| `FLIGHT_DETECT_LOOKBACK_MINUTES` | `60` | How far back the detector scans for new departures |
| `POSITIONS_RETENTION_DAYS` | `30` | How long position snapshots are kept |
| `AIRPORT_LOOKUP_RADIUS_KM` | `50.0` | Max distance for nearest-airport matching |
| `OPENSKY_REQUEST_DELAY_S` | `2.0` | Delay between API calls |
| `OPENSKY_RATELIMIT_BACKOFF_S` | `60` | Sleep time on HTTP 429 |
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
| **State Poller** | Every 5 min | Fetches `/states/all`, stores position snapshots for the LH fleet |
| **Flight Detector** | Every 30 min (at :15 and :45) | Detects flights from on_ground transitions, closes pending arrivals |
| **Positions Cleanup** | Daily at 04:00 UTC | Deletes position snapshots older than `POSITIONS_RETENTION_DAYS` |
| **Fleet Refresh** | Mondays at 02:00 UTC | Downloads OpenSky aircraft CSV, syncs fleet registry, retires removed aircraft |

---

## Dashboard

The monitoring dashboard runs on port **8080** and auto-refreshes every 30 seconds.

**What it shows:**
- System health — last run status for each job
- Fleet breakdown — active/retired aircraft counts by type
- Flight metrics — today, 7-day, and all-time counts
- Daily trend chart — flights and unique callsigns over the last 14 days
- Top routes — most frequent city pairs (30-day window)
- Batch run history — last 10 runs with status, stats, and error details

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

If upgrading from the old `/flights/all`-based approach, apply the schema migration manually before deploying the new image:

```bash
ssh user@your-server "docker exec -i lhlogging-db-1 psql -U your_db_user -d lhlogging" \
  < db/init/002_airports_and_positions.sql
```

Then deploy and run the airport loader.

---

## Credit Budget

| | Per call | Daily calls | Daily cost |
|---|---|---|---|
| State poller | 1 credit | 288 (every 5 min) | **~288 credits** |
| Fleet refresh | ~free (CSV download) | 1/week | **~0** |
| **Total** | | | **~288 credits/day** |

This uses **~7%** of the 4,000 credit daily budget, leaving ample headroom for retries and rate-limit recovery.

---

<div align="center">

*Flight data provided by [The OpenSky Network](https://opensky-network.org/).*

[![Last Commit](https://img.shields.io/github/last-commit/ChristopherBiel/lhlogging?label=last%20commit)](https://github.com/ChristopherBiel/lhlogging/commits/main)

</div>
