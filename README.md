<div align="center">

# LH Fleet Logger

**Automated flight data collection for the entire Lufthansa fleet.**

[![Deploy](https://github.com/ChristopherBiel/lhlogging/actions/workflows/deploy.yml/badge.svg)](https://github.com/ChristopherBiel/lhlogging/actions/workflows/deploy.yml)
[![Dashboard](https://img.shields.io/website?url=https%3A%2F%2Flhlogging.biels.net&label=dashboard&up_message=online&down_message=offline)](https://lhlogging.biels.net)
[![Python](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/postgresql-16-4169E1?logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![OpenSky](https://img.shields.io/badge/data-OpenSky%20Network-1a1a2e)](https://opensky-network.org/)
[![Credits/day](https://img.shields.io/badge/API%20cost-780%20credits%2Fday-brightgreen)](#credit-budget)

Tracks every Lufthansa aircraft — from A320s to A380s — logging departure/arrival airports, flight times, and route data into a PostgreSQL database. A live monitoring dashboard gives you a bird's-eye view of fleet activity, route frequency, and system health.

[Live Dashboard](https://lhlogging.biels.net) · [Architecture](#architecture) · [Quick Start](#quick-start) · [Configuration](#configuration)

</div>

---

## How It Works

```
                                         ┌─────────────────┐
  ┌──────────────┐    /flights/all       │                 │
  │  OpenSky API │◄────── 2h chunks ─────│   Route Logger  │──── twice daily
  └──────────────┘    (bulk global data)  │   (cron jobs)   │     03:00 & 15:00 UTC
                                         └────────┬────────┘
                                                  │ upsert
  ┌──────────────┐    Aircraft DB CSV    ┌────────▼────────┐
  │  OpenSky CSV │◄──────────────────────│  Fleet Refresh  │──── weekly (Mon 02:00)
  └──────────────┘                       └────────┬────────┘
                                                  │
                                         ┌────────▼────────┐
                                         │   PostgreSQL     │
                                         │   - aircraft     │
                                         │   - flights      │
                                         │   - batch_runs   │
                                         └────────┬────────┘
                                                  │
                                         ┌────────▼────────┐
                                         │   Dashboard      │──── :8080
                                         │   (Flask)        │
                                         └─────────────────┘
```

### The Credit-Efficient Approach

OpenSky charges **30 credits per API call** regardless of endpoint. Querying per-aircraft for a 300+ plane fleet would cost **9,000+ credits/day** — far exceeding the 4,000 credit daily budget.

Instead, the route logger uses the **bulk `/flights/all` endpoint**, fetching *all global flights* in 2-hour chunks and filtering to the Lufthansa fleet client-side:

| | Per-Aircraft (`/flights/aircraft`) | Bulk (`/flights/all`) |
|---|---|---|
| API calls for 300 aircraft | 300/run | 13/run |
| Daily cost (2 runs) | ~18,000 credits | **~780 credits** |
| Scales with fleet size? | Yes (linearly) | **No (fixed cost)** |

### Completeness Guarantee

Each run looks back **26 hours** with a 12-hour interval between runs, creating a **14-hour overlap** — longer than Lufthansa's longest route (FRA-EZE, ~13.5h). This ensures every flight is captured by at least one run, regardless of when it departed or arrived. Duplicates are handled by the database's `ON CONFLICT` upsert.

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
│   │   ├── planespotters.py        # Planespotters API client (alternate fleet source)
│   │   ├── route_logger.py         # Main batch job — bulk flight collection
│   │   ├── fleet_refresh.py        # Weekly fleet sync from OpenSky CSV
│   │   └── utils.py                # Logging, retry decorator, rate limiter
│   ├── crontab                     # Cron schedule (runs inside Docker)
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard/                      # Live monitoring UI
│   ├── app.py                      # Flask app with dark-themed SPA
│   ├── Dockerfile
│   └── requirements.txt
├── db/
│   └── init/
│       └── 001_schema.sql          # PostgreSQL schema (auto-applied on first run)
├── tools/
│   └── compare_methods.py          # Credit cost benchmarking tool
├── docker-compose.yml              # Three services: db, app, dashboard
└── .github/
    └── workflows/
        └── deploy.yml              # Push to main → auto-deploy via SSH
```

### Database Schema

| Table | Purpose |
|---|---|
| **aircraft** | Fleet registry — ICAO24, registration, type, active status |
| **flights** | Route log — airports, callsign, timestamps, auto-calculated duration |
| **batch_runs** | Audit trail — every job run with stats and error details |
| **positions** | *(stub)* Future: real-time position snapshots |

Key design decisions:
- **Upsert on `(icao24, first_seen)`** — overlapping query windows are safe; duplicates are merged, not doubled
- **Generated columns** — `flight_date` and `duration_minutes` are computed automatically from timestamps
- **Foreign key from flights → aircraft** — ensures data integrity

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
- **db** — PostgreSQL 16 (schema auto-initialized)
- **app** — Python 3.12 with cron for scheduled jobs
- **dashboard** — Flask app on port 8080

### 3. Verify

```bash
# Check services are healthy
docker compose ps

# Watch the route logger in action
docker compose logs -f app

# Open dashboard
open http://localhost:8080
```

The first route logger run occurs at the next scheduled time (03:00 or 15:00 UTC). To trigger a manual run:

```bash
docker compose exec app python -m lhlogging.route_logger
```

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
| `OPENSKY_LOOKBACK_HOURS` | `26` | Hours to look back each run |
| `OPENSKY_CHUNK_SIZE_S` | `7200` | Chunk size in seconds (2h = OpenSky max) |
| `OPENSKY_REQUEST_DELAY_S` | `2.0` | Delay between API calls |
| `OPENSKY_RATELIMIT_BACKOFF_S` | `60` | Sleep time on HTTP 429 |
| `BATCH_MAX_ERRORS_BEFORE_ABORT` | `50` | Error threshold to abort a run |
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
| **Route Logger** | Daily at 03:00 and 15:00 UTC | Fetches global flight data in 2h chunks, filters to LH fleet, upserts routes |
| **Fleet Refresh** | Mondays at 02:00 UTC | Downloads OpenSky aircraft CSV, syncs fleet registry, retires removed aircraft |

---

## Dashboard

The monitoring dashboard runs on port **8080** and auto-refreshes every 30 seconds.

**What it shows:**
- System health — last run status for route logger and fleet refresh
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

---

## Tools

### Credit Cost Comparison

`tools/compare_methods.py` benchmarks per-aircraft vs. bulk query approaches against the live API, measuring actual credit consumption:

```bash
python3 tools/compare_methods.py
```

---

## Credit Budget

With the bulk approach and default settings:

| | Per run | Daily (2 runs) | % of 4,000 budget |
|---|---|---|---|
| Route logger | 13 chunks x 30 cr = **390** | **780** | 19.5% |
| Fleet refresh | 1 CSV download (free) | **0** | 0% |
| **Total** | | **780** | **19.5%** |

Remaining daily budget: **3,220 credits** — available as buffer for retries, 429 recovery, or additional queries.

---

<div align="center">

*Flight data provided by [The OpenSky Network](https://opensky-network.org/).*

[![Last Commit](https://img.shields.io/github/last-commit/ChristopherBiel/lhlogging?label=last%20commit)](https://github.com/ChristopherBiel/lhlogging/commits/main)

</div>
