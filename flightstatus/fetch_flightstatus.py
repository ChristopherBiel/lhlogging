#!/usr/bin/env python3
"""
Nightly Lufthansa flight-status fetcher.

Pulls the public lufthansa.com FIS feed
(`/service/api/fis/byflightnumber?flightNumber=LH716&date=YYYY-MM-DD`) for the
catalogued widebody flight numbers (see `fis_flight_catalog` — history ∪ ADS-B
seed ∪ even/odd pairing) across a date window that spans a few days ahead, today,
*and* the last couple of days, and records the assigned airframe (tail), scheduled
route/times, status, and the aircraft's previous flight into
`flight_status_observations`. Forward dates are the provisional plan; the past
(backfill) dates return the actually-operated tail (ARRIVED) as ground truth.

The endpoint sits behind Imperva/Distil bot management, which blocks plain HTTP
clients and headless browsers. The only thing that gets through is a *real*
(headed) Chromium — here driven by Playwright and run under Xvfb (see
run_nightly.sh). We load the timetable page once to establish the session, then
issue same-origin `fetch()`es from inside the page.

Coverage is tiered by fleet type. The *deep* tier (B748 + A388) is tracked at
high cadence in the D+1/D+2 window so we can date a reassignment to a few hours
rather than a day; the *broad* tier (787/A350) keeps the full near window but
only one far look. Measured 2026-07-26: reassignment hazard barely varies with
lead time (~1-1.7 changes per 100 leg-hours from D0 out to D+5), so what limits
us is not how far ahead we look but how often — with two looks a day, changes
land in ~23h-wide brackets and the time of day they happen is unrecoverable.

Run modes:
  python fetch_flightstatus.py                 # near sweep (catalog x -BACKFILL..+LOOKAHEAD days,
                                                #   incl. D0; owns discovery/pairing/catalog lifecycle)
  python fetch_flightstatus.py --far           # far pass: catalog x D+FAR_MIN..D+FAR_MAX only,
                                                #   read-only against the catalog, run once in a quiet
                                                #   window since nothing here needs same-day freshness
  python fetch_flightstatus.py --watch 4.5     # watch pass: re-check flights departing in the next 4.5h
  python fetch_flightstatus.py --pulse         # pulse pass: deep-tier numbers x D+PULSE_OFFSETS only,
                                                #   ~7x/day, so deep-tier brackets stay under ~3h
  python fetch_flightstatus.py --flight LH716 --date 2026-06-25   # ad-hoc single lookup (prints JSON)
  python fetch_flightstatus.py --dry-run       # sweep/watch/far/pulse, print candidate set + plan, no writes

Must be run under a display (Xvfb): `xvfb-run -a python fetch_flightstatus.py`.
"""
import argparse
import json
import os
import random
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import psycopg
from playwright.sync_api import sync_playwright

# --- config (env-overridable, mirrors the lhlogging config style) -----------
DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "lhlogging")
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# Widebody scope: current fleet (B748/A388/B789/A359) plus the ICAO codes of
# variants on order (B788/B78X/A35K), so a new delivery is swept from its first
# ADS-B-logged flight without a config change. Unknown codes cost nothing.
SEED_TYPES = [t.strip() for t in os.environ.get(
    "FIS_SEED_TYPES", "B748,A388,B788,B789,B78X,A359,A35K").split(",") if t.strip()]
SEED_LOOKBACK_DAYS = int(os.environ.get("FIS_SEED_LOOKBACK_DAYS", "2"))
# Near sweep (05:45/18:15): D0 needs same-day freshness before the next
# departure bank, so this stays short enough to always finish with margin.
# Deeper lead times aren't time-critical — see FAR_MIN_DAYS/FAR_MAX_DAYS below,
# a separate unhurried pass that used to be crammed in here (2026-07-20: at
# LOOKAHEAD_DAYS=4 the catalog had grown past MAX_LOOKUPS, silently dropping
# all of D+4 and part of D+3 every run).
LOOKAHEAD_DAYS = int(os.environ.get("FIS_LOOKAHEAD_DAYS", "2"))
# Far pass (--far, run once nightly in the quiet window after the red-eye
# watch): re-checks D+FAR_MIN_DAYS..D+FAR_MAX_DAYS only. No discovery, no
# chaining, no catalog prune — those stay owned by the near sweeps, which run
# twice daily and keep the catalog current enough for this to just read it.
# Nothing depends on same-day freshness this far out, so it's free to take
# hours without risking the next scheduled slot. It also carries the deep tier's
# PULSE_OFFSETS: it is the only run in the 03:00-05:00 window, so without that
# the pulse cadence would have a hole there (see PULSE_OFFSETS below).
FAR_MIN_DAYS = int(os.environ.get("FIS_FAR_MIN_DAYS", "3"))
# Broad-tier ceiling. Was 5 until 2026-07-26: a change first seen at D+4 or D+5
# is the tail that ends up flying 0-5% of the time, so those lookups bought
# almost no label value and now fund the deep-tier pulses instead.
FAR_MAX_DAYS = int(os.environ.get("FIS_FAR_MAX_DAYS", "3"))
FAR_DEEP_MAX_DAYS = int(os.environ.get("FIS_FAR_DEEP_MAX_DAYS", "4"))

# --- tiering ----------------------------------------------------------------
# Deep tier: the types we want fine temporal resolution on. Everything else is
# the broad tier — full near window, one far look, no pulses.
DEEP_TYPES = [t.strip() for t in os.environ.get(
    "FIS_DEEP_TYPES", "B748,A388").split(",") if t.strip()]
# Which lead days the pulse passes re-check. D+1/D+2 is where the hazard is
# highest (1.37 and 1.68 changes per 100 leg-hours) and where a change still has
# a real chance of being the one that flies.
PULSE_OFFSETS = [int(d) for d in os.environ.get(
    "FIS_PULSE_OFFSETS", "1,2").split(",") if d.strip()]
# Extra lookahead day the deep tier gets on every near sweep, so both pulse
# offsets are refreshed by the sweeps too: lite (D+1) reaches D+2, full (D+2)
# reaches D+3. Keeping the sampling pattern identical across D+1 and D+2 matters
# — comparing their hazards is otherwise confounded by the sampling grid.
DEEP_LOOKAHEAD_BONUS = int(os.environ.get("FIS_DEEP_LOOKAHEAD_BONUS", "1"))
# How far back to look when deciding which numbers are deep-tier.
DEEP_TIER_DAYS = int(os.environ.get("FIS_DEEP_TIER_DAYS", "6"))
# Truth pass: also query the last N days so FIS returns the *actually-operated*
# tail (overallStatus ARRIVED) as ground truth for calibration. FIS only keeps a
# rolling few-days window of past flights, so the twice-daily job must catch them
# soon after operation. Already-settled (ARRIVED/CANCELLED) flights are skipped.
BACKFILL_DAYS = int(os.environ.get("FIS_BACKFILL_DAYS", "2"))
# A past flight in one of these states won't change tail again — don't re-query.
TERMINAL_STATUSES = ["ARRIVED", "CANCELLED"]
# A watch pass only re-checks flights that can still change tail — skip anything
# the latest snapshot already shows as airborne or done.
WATCH_SKIP_STATUSES = set(TERMINAL_STATUSES) | {"DEPARTED", "FLYING", "LANDED"}
# Distil throttles by request rate: ~2.5s pacing got ~70 lookups in before it
# started 403ing. This is a nightly job with a full hour to run, so we pace
# gently (a few hundred lookups still finish in ~15min) and, on a block, wait
# out the rate window rather than hammering with short retries.
REQUEST_DELAY_MIN_S = float(os.environ.get("FIS_REQUEST_DELAY_MIN_S", "5.0"))
REQUEST_DELAY_MAX_S = float(os.environ.get("FIS_REQUEST_DELAY_MAX_S", "10.0"))
MAX_FETCH_RETRIES = int(os.environ.get("FIS_MAX_FETCH_RETRIES", "3"))
BLOCK_BACKOFF_S = float(os.environ.get("FIS_BLOCK_BACKOFF_S", "45.0"))
# Rotation-chain expansion: many legs (esp. westbound MUC departures) fly under
# tactical callsigns (DLH3Y, DLH8P, …) so they never enter the ADS-B-derived
# seed. The FIS `previousFlight` field names them, so after the seed fetch we
# follow that chain a couple of hops to fill the gaps. Capped to stay gentle.
CHAIN_HOPS = int(os.environ.get("FIS_CHAIN_HOPS", "2"))
# Catalog sweep (120+ numbers) x (BACKFILL_DAYS + 1 + LOOKAHEAD_DAYS) date
# slices ≈ 600 steady-state for the near sweep now that D+3..FAR_MAX_DAYS is
# split off into its own unhurried --far pass (see below) — comfortably under
# this cap again. Work is priority-ordered by lead time, so if this cap does
# still bite it drops the least-valuable far-future lookups first.
MAX_LOOKUPS = int(os.environ.get("FIS_MAX_LOOKUPS", "700"))
# Distil caps successful lookups per browser session (~100-115). Recycle the
# browser context (fresh cf_clearance) every N lookups to stay under it — needed
# once chain expansion pushes a run past ~120 lookups.
SESSION_LOOKUPS = int(os.environ.get("FIS_SESSION_LOOKUPS", "80"))
# Catalog lifecycle: a catalogued number swept without returning a widebody
# accrues misses; at PROBATION it's flagged, at RETIRE it drops out of the sweep
# (any widebody hit — including one found via chaining — resets it to active).
# Keeps speculative pairing/seed additions from bloating the sweep forever. At
# twice-daily runs, RETIRE=8 ≈ 4 days of no widebody.
CATALOG_PROBATION_MISSES = int(os.environ.get("FIS_CATALOG_PROBATION_MISSES", "4"))
CATALOG_RETIRE_MISSES = int(os.environ.get("FIS_CATALOG_RETIRE_MISSES", "8"))

BASE = "https://www.lufthansa.com"
PAGE_URL = f"{BASE}/de/en/timetable-and-flight-status"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36"
)


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


# --- DB ---------------------------------------------------------------------
def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, autocommit=False,
    )


def seed_flight_numbers(conn: psycopg.Connection) -> list[dict]:
    """Distinct numeric DLH flight numbers flown by the seed types in the last N days.

    Callsigns like 'DLH716' map to flight number '716'; operational variants with
    letter suffixes ('DLH510D', 'DLH3W') are skipped — they don't resolve to a
    sellable flight number on the feed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT f.callsign, a.aircraft_type
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = ANY(%s)
              AND f.flight_date >= CURRENT_DATE - %s::int
              AND f.callsign ~ '^DLH[0-9]+$'
            ORDER BY f.callsign
            """,
            (SEED_TYPES, SEED_LOOKBACK_DAYS),
        )
        rows = cur.fetchall()
    # de-dup by flight number; keep first seen type
    seen: dict[str, dict] = {}
    for callsign, atype in rows:
        num = callsign.strip()[3:]  # strip 'DLH'
        if num and num not in seen:
            seen[num] = {"flight_number": num, "seed_type": atype}
    return list(seen.values())


# --- flight-number catalog (which flights to sweep) -------------------------
def refresh_catalog(conn: psycopg.Connection, adsb_seed: list[dict], commit: bool = True) -> bool:
    """Idempotently (re)populate `fis_flight_catalog` from every discovery source.

    Returns True if the catalog table exists and was refreshed; False if it's
    missing (migration 007 not yet applied) so the caller can fall back to the
    legacy ADS-B seed — DB migrations are manual, so this must degrade
    gracefully. When ``commit`` is False the writes are left in the open
    transaction (visible to reads in the same session) for the --dry-run diff,
    and the caller rolls back.
    """
    try:
        with conn.cursor() as cur:
            # Normalization pre-pass: FIS zero-pads flight numbers in
            # `previousFlight` ('096' for LH96), so a chained number can enter
            # the pipeline in a padded spelling next to its canonical one. Two
            # spellings of one number must not coexist in the catalog: the
            # pairing insert maps both onto the same sibling, and a single
            # INSERT .. ON CONFLICT may not touch a row twice (this killed
            # every sweep 07-17..07-19). Fold padded rows into the canonical
            # row, then drop/rename what's left — idempotent, no-op when clean.
            cur.execute(
                """
                UPDATE fis_flight_catalog c SET
                    seed_type          = COALESCE(c.seed_type, p.seed_type),
                    last_widebody_date = GREATEST(c.last_widebody_date, p.last_widebody_date),
                    updated_at         = NOW()
                FROM fis_flight_catalog p
                WHERE p.airline = c.airline
                  AND p.flight_number ~ '^0[0-9]*$'
                  AND (p.flight_number::int)::text = c.flight_number
                """
            )
            cur.execute(
                """
                DELETE FROM fis_flight_catalog p
                WHERE p.flight_number ~ '^0[0-9]*$'
                  AND EXISTS (SELECT 1 FROM fis_flight_catalog c
                              WHERE c.airline = p.airline
                                AND c.flight_number = (p.flight_number::int)::text)
                """
            )
            cur.execute(
                """
                UPDATE fis_flight_catalog SET
                    flight_number = (flight_number::int)::text, updated_at = NOW()
                WHERE flight_number ~ '^0[0-9]*$'
                """
            )
            cur.execute(
                """
                UPDATE fis_flight_catalog SET
                    paired_number = (paired_number::int)::text, updated_at = NOW()
                WHERE paired_number ~ '^0[0-9]*$'
                """
            )
            # Source A — FIS history: numbers ever operated by a known A388/B748
            # tail. Joining on registration (not the often-empty aircraftType
            # string) is the robust widebody signal — it catches the tactical
            # A380 MUC legs too. Numbers group by canonical ::int spelling so
            # legacy padded observation rows fold into one catalog entry.
            cur.execute(
                """
                INSERT INTO fis_flight_catalog
                    (airline, flight_number, seed_type, source, last_widebody_date)
                SELECT o.airline, (o.flight_number::int)::text, MIN(a.aircraft_type), 'fis_history', MAX(o.flight_date)
                FROM flight_status_observations o
                JOIN aircraft a ON a.registration = o.registration
                WHERE o.found AND a.aircraft_type = ANY(%s)
                  AND o.flight_number ~ '^[0-9]+$'
                GROUP BY o.airline, (o.flight_number::int)
                ON CONFLICT (airline, flight_number) DO UPDATE SET
                    seed_type          = COALESCE(EXCLUDED.seed_type, fis_flight_catalog.seed_type),
                    last_widebody_date = GREATEST(fis_flight_catalog.last_widebody_date, EXCLUDED.last_widebody_date),
                    updated_at         = NOW()
                -- status/consecutive_misses are owned by prune_catalog (the
                -- lifecycle pass); discovery must not reactivate a retired number.
                """,
                (SEED_TYPES,),
            )
            # Source B — legacy ADS-B seed: still first to catch a brand-new
            # number before FIS history has it.
            for s in adsb_seed:
                cur.execute(
                    """
                    INSERT INTO fis_flight_catalog (airline, flight_number, seed_type, source)
                    VALUES ('LH', %s, %s, 'adsb_seed')
                    ON CONFLICT (airline, flight_number) DO UPDATE SET
                        seed_type  = COALESCE(fis_flight_catalog.seed_type, EXCLUDED.seed_type),
                        updated_at = NOW()
                    """,
                    (s["flight_number"], s["seed_type"]),
                )
            # Source C — pairing: an outbound N implies its return sibling (N+1
            # for even, N-1 for odd). Only off widebody-confirmed numbers so we
            # don't pair charter/non-widebody noise. This is what makes skipped
            # turnaround legs (e.g. LH763) get queried without any chaining.
            # GROUP BY the canonical ::int so two spellings of one source
            # number can never propose the same target twice in this statement
            # (ON CONFLICT forbids double-touch — the 07-17 sweep outage).
            cur.execute(
                """
                INSERT INTO fis_flight_catalog
                    (airline, flight_number, seed_type, paired_number, source)
                SELECT c.airline,
                       CASE WHEN c.flight_number::int % 2 = 0
                            THEN (c.flight_number::int + 1)::text
                            ELSE (c.flight_number::int - 1)::text END,
                       MIN(c.seed_type), (c.flight_number::int)::text, 'pairing'
                FROM fis_flight_catalog c
                WHERE c.flight_number ~ '^[0-9]+$' AND c.last_widebody_date IS NOT NULL
                GROUP BY c.airline, (c.flight_number::int)
                ON CONFLICT (airline, flight_number) DO UPDATE SET
                    paired_number = COALESCE(fis_flight_catalog.paired_number, EXCLUDED.paired_number),
                    updated_at    = NOW()
                """
            )
            # Backfill paired_number on any rows still missing it.
            cur.execute(
                """
                UPDATE fis_flight_catalog SET paired_number =
                    CASE WHEN flight_number::int % 2 = 0
                         THEN (flight_number::int + 1)::text
                         ELSE (flight_number::int - 1)::text END
                WHERE flight_number ~ '^[0-9]+$' AND paired_number IS NULL
                """
            )
        if commit:
            conn.commit()
        return True
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        return False


def catalog_candidates(conn: psycopg.Connection) -> list[dict]:
    """Catalog flight numbers to sweep this run: everything not retired (active +
    probation — a probation number is still queried so a widebody hit can revive
    it). Retired numbers drop out until re-discovered via chaining."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT flight_number, seed_type FROM fis_flight_catalog
            WHERE airline = 'LH' AND status <> 'retired'
            ORDER BY flight_number::int
            """
        )
        return [{"flight_number": r[0], "seed_type": r[1]} for r in cur.fetchall()]


def deep_numbers(conn: psycopg.Connection) -> dict:
    """Flight numbers whose recent FIS history is dominated by a deep-tier type.

    Keyed off the tail actually observed (registration -> `aircraft`), not
    `fis_flight_catalog.seed_type`: that column records why a number was first
    added and has drifted badly — it currently labels *no* number A388, because
    those numbers entered the catalog under some other type's rotation.

    Modal rather than "ever seen": a number stays deep-tier while it is usually
    flown by a deep type, so a one-off A350 substitution on LH424 doesn't drop
    it out of the tier mid-week (and an A350 number that once had a 747 sub
    doesn't get pulled in). Returns {flight_number: fleet_type}; empty on any
    error, which degrades the tiering off rather than failing the run.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH modal AS (
                    SELECT o.flight_number, a.aircraft_type AS fleet_type,
                           row_number() OVER (PARTITION BY o.flight_number
                                              ORDER BY count(*) DESC, a.aircraft_type) AS rn
                    FROM flight_status_observations o
                    JOIN aircraft a ON a.registration = btrim(o.registration)
                    WHERE o.found
                      AND o.observed_at > NOW() - make_interval(days => %(days)s)
                    GROUP BY o.flight_number, a.aircraft_type
                )
                SELECT flight_number, fleet_type FROM modal
                WHERE rn = 1 AND fleet_type = ANY(%(types)s)
                """,
                {"days": DEEP_TIER_DAYS, "types": DEEP_TYPES},
            )
            return {r[0]: r[1] for r in cur.fetchall()}
    except Exception as e:  # noqa: BLE001 - tiering is an optimisation, never fatal
        conn.rollback()
        log(f"deep-tier query failed, treating every number as broad tier: {e}")
        return {}


def pulse_candidates(conn: psycopg.Connection, offsets: list) -> list[dict]:
    """(flight_number, date) work for a pulse pass: deep-tier catalog numbers
    across D+offset, minus anything already settled.

    Only numbers that are *both* catalogued (not retired) and deep-tier by
    observed type are pulsed, so the pass stays at a predictable ~30 lookups per
    offset. Discovery stays with the sweeps — a number the catalog has never
    seen cannot enter here."""
    deep = deep_numbers(conn)
    if not deep:
        return []
    try:
        catalogued = {c["flight_number"] for c in catalog_candidates(conn)}
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        catalogued = set(deep)  # migration 007 unapplied — pulse the tier as-is

    today = date.today()
    targets = [today + timedelta(days=d) for d in offsets]
    with conn.cursor() as cur:  # a cancelled leg won't get a tail again
        cur.execute(
            """
            SELECT DISTINCT ON (o.flight_date, o.flight_number)
                   o.flight_number, o.flight_date, o.overall_status
            FROM flight_status_observations o
            WHERE o.airline = 'LH' AND o.flight_date = ANY(%s)
            ORDER BY o.flight_date, o.flight_number, o.observed_at DESC
            """,
            (targets,),
        )
        settled = {(r[0], r[1]) for r in cur.fetchall()
                   if (r[2] or "").upper() in TERMINAL_STATUSES}

    # Nearest offset first, shuffled within it — same rationale as the sweep's
    # priority sort: the ordering that matters is by lead time, and shuffling
    # inside a band keeps 7 passes a day from replaying one fixed sequence.
    work = []
    for target in targets:
        batch = [{"flight_number": n, "flight_date": target, "seed_type": deep[n]}
                 for n in deep if n in catalogued and (n, target) not in settled]
        random.shuffle(batch)
        work.extend(batch)
    return work


def prune_catalog(conn: psycopg.Connection) -> None:
    """Lifecycle pass, run after each sweep. A number with recent widebody
    evidence (this run or the last, incl. via chaining) resets to active; one
    without accrues a miss and steps active→probation→retired. Retired numbers
    stop being swept, so speculative pairing/seed additions can't bloat the
    sweep forever. Skips gracefully if the catalog table is absent."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH recent_hits AS (
                    SELECT DISTINCT o.flight_number
                    FROM flight_status_observations o
                    JOIN aircraft a ON a.registration = o.registration
                    WHERE o.found AND a.aircraft_type = ANY(%(types)s)
                      AND o.observed_date >= CURRENT_DATE - 1
                )
                UPDATE fis_flight_catalog c SET
                    consecutive_misses = CASE WHEN c.flight_number IN (SELECT flight_number FROM recent_hits)
                                              THEN 0 ELSE c.consecutive_misses + 1 END,
                    status = CASE
                        WHEN c.flight_number IN (SELECT flight_number FROM recent_hits) THEN 'active'
                        WHEN c.consecutive_misses + 1 >= %(retire)s THEN 'retired'
                        WHEN c.consecutive_misses + 1 >= %(probation)s THEN 'probation'
                        ELSE c.status END,
                    updated_at = NOW()
                WHERE c.status <> 'retired' OR c.flight_number IN (SELECT flight_number FROM recent_hits)
                """,
                {"types": SEED_TYPES, "retire": CATALOG_RETIRE_MISSES,
                 "probation": CATALOG_PROBATION_MISSES},
            )
            cur.execute("SELECT status, count(*) FROM fis_flight_catalog GROUP BY status ORDER BY status")
            dist = {r[0]: r[1] for r in cur.fetchall()}
        conn.commit()
        log(f"catalog prune: {dist}")
    except psycopg.errors.UndefinedTable:
        conn.rollback()


def existing_truth(conn: psycopg.Connection, start: date, end_exclusive: date) -> set:
    """(flight_number, flight_date) pairs already captured with a terminal status,
    so the truth pass can skip re-querying flights whose tail is settled."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT flight_number, flight_date FROM flight_status_observations
            WHERE overall_status = ANY(%s) AND flight_date >= %s AND flight_date < %s
            """,
            (TERMINAL_STATUSES, start, end_exclusive),
        )
        return {(r[0], r[1]) for r in cur.fetchall()}


# --- coverage audit (continuity monitor) ------------------------------------
def coverage_audit(conn: psycopg.Connection, today: date, horizon_days: int) -> dict:
    """Reconstruct every fleet tail's forecast rotation and score its coverage.

    For each active A388/B748 tail, take the latest snapshot of each upcoming
    flight, order by scheduled departure, and check the rotation is physically
    coherent: each leg's arrival airport is the next leg's departure airport
    (no *gap* — a missing leg), and no two legs overlap in time (no *overlap* —
    a stale ghost or FIS double-booking). A tail is `clean` if it has neither.
    `absent` = active fleet tails with no upcoming leg at all (maintenance, or a
    coverage hole). Read-only; returns a summary dict and logs a one-line metric.
    """
    horizon_end = today + timedelta(days=horizon_days)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (o.flight_date, o.flight_number)
                btrim(o.registration), o.flight_number,
                o.dep_airport_iata, o.arr_airport_iata, o.dep_scheduled, o.arr_scheduled
            FROM flight_status_observations o
            JOIN aircraft a ON a.registration = o.registration
            WHERE o.found AND o.dep_scheduled IS NOT NULL AND o.arr_scheduled IS NOT NULL
              AND o.flight_date >= %s AND o.flight_date <= %s
              AND a.aircraft_type = ANY(%s)
            -- observed_at, not observed_date: with per-pass rows (009) a day
            -- holds several snapshots and "latest" must break intra-day ties.
            ORDER BY o.flight_date, o.flight_number, o.observed_at DESC
            """,
            (today, horizon_end, SEED_TYPES),
        )
        legrows = cur.fetchall()
        cur.execute(
            "SELECT DISTINCT btrim(registration) FROM aircraft "
            "WHERE aircraft_type = ANY(%s) AND is_active",
            (SEED_TYPES,),
        )
        roster = {r[0] for r in cur.fetchall()}

    bytail: dict[str, list] = defaultdict(list)
    for reg, fnum, dep, arr, dep_t, arr_t in legrows:
        bytail[reg].append({"fnum": fnum, "dep": dep, "arr": arr, "dep_t": dep_t, "arr_t": arr_t})

    gaps, overlaps = {}, {}
    for reg, legs in bytail.items():
        legs.sort(key=lambda l: l["dep_t"])
        tail_gaps = [f"{legs[i]['arr']}!={legs[i + 1]['dep']}"
                     for i in range(len(legs) - 1)
                     if legs[i]["arr"] != legs[i + 1]["dep"]]
        tail_overlaps = [f"LH{a['fnum']}xLH{b['fnum']}"
                         for i, a in enumerate(legs) for b in legs[i + 1:]
                         if a["dep_t"] < b["arr_t"] and b["dep_t"] < a["arr_t"]]
        if tail_gaps:
            gaps[reg] = tail_gaps
        if tail_overlaps:
            overlaps[reg] = tail_overlaps

    seen = set(bytail)
    clean = seen - set(gaps) - set(overlaps)
    absent = sorted(roster - seen)
    summary = {
        "window": [today.isoformat(), horizon_end.isoformat()],
        "fleet": len(roster), "seen": len(seen), "clean": len(clean),
        "gaps": gaps, "overlaps": overlaps, "absent": absent,
    }
    detail = ""
    if gaps:
        detail += f" | GAPS {gaps}"
    if overlaps:
        detail += f" | OVERLAPS {overlaps}"
    if absent:
        detail += f" | ABSENT {absent}"
    log(f"coverage[{today}..{horizon_end}]: fleet={len(roster)} seen={len(seen)} "
        f"clean={len(clean)} gaps={len(gaps)} overlaps={len(overlaps)} absent={len(absent)}{detail}")
    return summary


def store_coverage(conn: psycopg.Connection, run_id: int, summary: dict) -> None:
    """Persist the coverage summary on the batch_runs row; skip gracefully if the
    column isn't there yet (migration 008 unapplied)."""
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE batch_runs SET coverage = %s::jsonb WHERE id = %s",
                        (json.dumps(summary), run_id))
        conn.commit()
    except psycopg.errors.UndefinedColumn:
        conn.rollback()
        log("coverage column absent (apply migration 008) — summary not stored")


def per_pass_schema(conn: psycopg.Connection) -> bool:
    """True once migration 009 (run_id column, per-run unique key) is applied.

    Migrations are applied manually after deploys, so the fetcher must work
    against both shapes: per-run append rows when it can, the legacy
    one-row-per-day upsert otherwise.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'flight_status_observations' AND column_name = 'run_id'
            """
        )
        return cur.fetchone() is not None


def upsert_observation(conn: psycopg.Connection, obs: dict, run_id: int | None = None) -> None:
    """Record one observation. With migration 009 applied (run_id given), each
    run appends its own row so intra-day passes preserve swap history; before
    it, fall back to the legacy one-row-per-observed_date upsert."""
    if run_id is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO flight_status_observations
                    (run_id, observed_date, flight_date, airline, flight_number, seed_type, found,
                     registration, aircraft_type, aircraft_subtype,
                     dep_airport_iata, arr_airport_iata, dep_scheduled, arr_scheduled,
                     overall_status, prev_airline, prev_flight_number, prev_flight_date, raw)
                VALUES
                    (%(run_id)s, CURRENT_DATE, %(flight_date)s, %(airline)s, %(flight_number)s,
                     %(seed_type)s, %(found)s,
                     %(registration)s, %(aircraft_type)s, %(aircraft_subtype)s,
                     %(dep_airport_iata)s, %(arr_airport_iata)s, %(dep_scheduled)s, %(arr_scheduled)s,
                     %(overall_status)s, %(prev_airline)s, %(prev_flight_number)s, %(prev_flight_date)s,
                     %(raw)s)
                ON CONFLICT (run_id, flight_date, airline, flight_number) DO UPDATE SET
                    observed_at        = NOW(),
                    seed_type          = EXCLUDED.seed_type,
                    found              = EXCLUDED.found,
                    registration       = EXCLUDED.registration,
                    aircraft_type      = EXCLUDED.aircraft_type,
                    aircraft_subtype   = EXCLUDED.aircraft_subtype,
                    dep_airport_iata   = EXCLUDED.dep_airport_iata,
                    arr_airport_iata   = EXCLUDED.arr_airport_iata,
                    dep_scheduled      = EXCLUDED.dep_scheduled,
                    arr_scheduled      = EXCLUDED.arr_scheduled,
                    overall_status     = EXCLUDED.overall_status,
                    prev_airline       = EXCLUDED.prev_airline,
                    prev_flight_number = EXCLUDED.prev_flight_number,
                    prev_flight_date   = EXCLUDED.prev_flight_date,
                    raw                = EXCLUDED.raw
                WHERE EXCLUDED.found OR NOT flight_status_observations.found
                """,
                {**obs, "run_id": run_id},
            )
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO flight_status_observations
                (observed_date, flight_date, airline, flight_number, seed_type, found,
                 registration, aircraft_type, aircraft_subtype,
                 dep_airport_iata, arr_airport_iata, dep_scheduled, arr_scheduled,
                 overall_status, prev_airline, prev_flight_number, prev_flight_date, raw)
            VALUES
                (CURRENT_DATE, %(flight_date)s, %(airline)s, %(flight_number)s, %(seed_type)s, %(found)s,
                 %(registration)s, %(aircraft_type)s, %(aircraft_subtype)s,
                 %(dep_airport_iata)s, %(arr_airport_iata)s, %(dep_scheduled)s, %(arr_scheduled)s,
                 %(overall_status)s, %(prev_airline)s, %(prev_flight_number)s, %(prev_flight_date)s,
                 %(raw)s)
            ON CONFLICT (observed_date, flight_date, airline, flight_number) DO UPDATE SET
                observed_at        = NOW(),
                seed_type          = EXCLUDED.seed_type,
                found              = EXCLUDED.found,
                registration       = EXCLUDED.registration,
                aircraft_type      = EXCLUDED.aircraft_type,
                aircraft_subtype   = EXCLUDED.aircraft_subtype,
                dep_airport_iata   = EXCLUDED.dep_airport_iata,
                arr_airport_iata   = EXCLUDED.arr_airport_iata,
                dep_scheduled      = EXCLUDED.dep_scheduled,
                arr_scheduled      = EXCLUDED.arr_scheduled,
                overall_status     = EXCLUDED.overall_status,
                prev_airline       = EXCLUDED.prev_airline,
                prev_flight_number = EXCLUDED.prev_flight_number,
                prev_flight_date   = EXCLUDED.prev_flight_date,
                raw                = EXCLUDED.raw
            -- Legacy (pre-009) shape: one row per day. Don't let a blocked/
            -- not-found later run of the same day clobber a good assignment an
            -- earlier run already captured: only overwrite when the new row
            -- found a flight, or the existing row hadn't found one either.
            WHERE EXCLUDED.found OR NOT flight_status_observations.found
            """,
            obs,
        )


def log_batch_start(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO batch_runs (run_type, status) VALUES ('flightstatus', 'running') RETURNING id"
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def log_batch_finish(conn: psycopg.Connection, run_id: int, total: int, ok: int,
                     err: int, upserted: int, status: str, detail: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE batch_runs SET finished_at = NOW(), aircraft_total = %s,
                aircraft_ok = %s, aircraft_error = %s, flights_upserted = %s,
                error_detail = %s, status = %s
            WHERE id = %s
            """,
            (total, ok, err, upserted, detail, status, run_id),
        )
    conn.commit()


# --- parsing ----------------------------------------------------------------
def norm_flight_number(n: str | None) -> str | None:
    """Strip FIS's zero-padding ('096' -> '96') at the parse boundary, so a
    chained-in number never diverges from its canonical spelling downstream
    (catalog inserts, work-queue dedup) — the divergence that made the pairing
    insert propose the same row twice and crash every sweep (07-17..07-19)."""
    if n and n.isdigit():
        return str(int(n))
    return n


def norm_registration(reg: str | None) -> str | None:
    """'DABYN' -> 'D-ABYN' (German regs) to match the aircraft table; leave others as-is."""
    if not reg:
        return None
    reg = reg.strip().upper()
    if reg.startswith("D") and "-" not in reg and len(reg) >= 4:
        return "D-" + reg[1:]
    return reg


def _ts(s: str | None):
    """Parse a FIS timestamp like '2026-06-25T14:05:00.000+0000'.

    The offset has no colon, so datetime.fromisoformat rejects it on Python
    <3.11 (the collector image runs 3.10) — strptime with %z handles it.
    """
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _d(s: str | None):
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def parse_flight(payload: dict, flight_number: str, target: date, seed_type: str | None) -> dict:
    """Turn a FIS response into an observation row dict."""
    obs = {
        "flight_date": target, "airline": "LH", "flight_number": flight_number,
        "seed_type": seed_type, "found": False, "registration": None,
        "aircraft_type": None, "aircraft_subtype": None, "dep_airport_iata": None,
        "arr_airport_iata": None, "dep_scheduled": None, "arr_scheduled": None,
        "overall_status": None, "prev_airline": None, "prev_flight_number": None,
        "prev_flight_date": None, "raw": None,
    }
    flights = (payload or {}).get("flights") or []
    if not flights:
        return obs
    fl = flights[0]
    obs["found"] = True
    obs["raw"] = json.dumps(fl)

    ac = fl.get("aircraftInfo") or {}
    obs["registration"] = norm_registration(ac.get("aircraftRegistration"))
    obs["aircraft_type"] = ac.get("aircraftType")
    obs["aircraft_subtype"] = ac.get("aircraftSubType")

    details = fl.get("flightDetails") or {}
    prev = details.get("previousFlight") or {}
    obs["prev_airline"] = prev.get("operatingCarrier")
    obs["prev_flight_number"] = norm_flight_number(prev.get("operatingCarrierFlightNumber"))
    obs["prev_flight_date"] = _d(prev.get("flightDate"))

    legs = fl.get("legs") or []
    if legs:
        leg = legs[0]
        obs["overall_status"] = leg.get("overallStatus")
        dep = leg.get("departure") or {}
        arr = leg.get("arrival") or {}
        obs["dep_airport_iata"] = dep.get("departureAirport")
        obs["arr_airport_iata"] = arr.get("arrivalAirport")
        obs["dep_scheduled"] = _ts(dep.get("departureScheduledTime"))
        obs["arr_scheduled"] = _ts(arr.get("arrivalScheduledTime"))
    return obs


# --- browser fetch ----------------------------------------------------------
_FETCH_JS = """
async (url) => {
  const r = await fetch(url, { headers: {
    'Accept': 'application/json, text/plain, */*',
    'X-Portal': 'LH', 'X-Portal-Site': 'DE', 'X-Portal-Language': 'en'
  }, credentials: 'include' });
  return { status: r.status, ct: r.headers.get('content-type') || '', body: await r.text() };
}
"""


def seed_session(page) -> None:
    """Load the timetable page to establish the Distil/CF session.

    The page nav itself often returns 403, which is fine — the subsequent
    same-origin API fetches still clear the bot check from a real headed browser.
    """
    try:
        resp = page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)
        log(f"session nav: {resp.status if resp else '?'}")
    except Exception as e:
        log(f"session nav error (continuing): {e}")
    page.wait_for_timeout(4000)


def _open_session(browser):
    """Open a fresh browser context (new cf_clearance) and prime it. Returns
    (context, page). Recreating the context resets Distil's per-session budget."""
    ctx = browser.new_context(locale="en-US", user_agent=USER_AGENT)
    ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
    page = ctx.new_page()
    seed_session(page)
    return ctx, page


def fetch_one(sess, flight_number: str, target: date, reset_cb):
    """Fetch one flight/date; returns parsed JSON dict or None if blocked.

    `sess` is a mutable {"page": ...} holder; on a block we wait then call
    `reset_cb()` to swap in a *fresh* session (the effective cure for Distil's
    per-session cap) before retrying.

    Two failure modes share the same wait-and-reset retry: an HTTP block (Distil
    403 / non-JSON in the response), and a *thrown* `page.evaluate` error — the
    in-page `fetch()` rejecting with `TypeError: Failed to fetch` when the
    document context is torn down (Imperva redirect/interstitial), the renderer
    crashes, or the network blips. Catching the latter keeps one bad lookup from
    aborting the whole batch (it propagated to run_batch and killed the run).
    """
    fn = f"LH{flight_number}"
    url = f"/service/api/fis/byflightnumber?flightNumber={fn}&date={target.isoformat()}"
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        try:
            res = sess["page"].evaluate(_FETCH_JS, url)
            status, ct = res.get("status"), res.get("ct", "")
            if status == 200 and "json" in ct:
                try:
                    return json.loads(res["body"])
                except json.JSONDecodeError:
                    log(f"  {fn} {target}: 200 but bad JSON")
                    return None
            if status == 404 and "json" in ct:
                # A JSON 404 is FIS answering authoritatively "no such flight"
                # (charter/ferry numbers like LH9910 that entered the catalog via
                # the ADS-B seed). Not a block: record a not-found so the catalog
                # lifecycle retires the number, and don't burn retries/backoffs
                # on it (3 numbers x 7 slices was costing ~24 min per sweep).
                log(f"  {fn} {target}: 404 — not a FIS flight, recording not-found")
                return {}
            log(f"  {fn} {target}: HTTP {status} ({ct[:40]}) attempt {attempt}/{MAX_FETCH_RETRIES}")
        except Exception as e:
            # in-page fetch rejected / evaluate threw (context destroyed, renderer
            # crash, network blip) — recoverable; fall through to the reset+retry
            first = str(e).splitlines()[0][:80]
            log(f"  {fn} {target}: evaluate error ({first}) attempt {attempt}/{MAX_FETCH_RETRIES}")
        if attempt < MAX_FETCH_RETRIES:
            time.sleep(BLOCK_BACKOFF_S)
            reset_cb()  # fresh session — the real fix for a per-session block
    return None


# --- runners ----------------------------------------------------------------
def run_batch(dry_run: bool = False, far: bool = False) -> int:
    conn = connect()
    try:
        adsb_seed = seed_flight_numbers(conn)
    except Exception as e:
        log(f"seed query failed: {e}")
        conn.close()
        return 1

    if far:
        # Far pass: read-only against the catalog — discovery, pairing, and
        # the retire/probation lifecycle stay owned by the near sweeps (which
        # run twice daily and keep it current enough for this to just read).
        # Same degrade-gracefully fallback as the near path if migration 007
        # isn't applied yet.
        try:
            candidates = catalog_candidates(conn)
            using_catalog = True
            source_label = "catalog (far, read-only)"
        except psycopg.errors.UndefinedTable:
            conn.rollback()
            candidates = adsb_seed
            using_catalog = False
            source_label = "adsb-seed (far, catalog table absent)"
    else:
        # The catalog is the primary source; the ADS-B seed folds into it. If the
        # catalog table isn't there yet (migration 007 unapplied), fall back to the
        # seed alone so the job keeps running.
        using_catalog = refresh_catalog(conn, adsb_seed, commit=not dry_run)
        if using_catalog:
            candidates = catalog_candidates(conn)
            source_label = "catalog"
        else:
            candidates = adsb_seed
            source_label = "adsb-seed (catalog table absent)"
    if not candidates:
        log("no candidate flight numbers — nothing to do")
        conn.close()
        return 0

    today = date.today()
    # Target dates are per-tier: the deep tier gets an extra lookahead day on the
    # near sweeps and, on the far pass, the pulse offsets too (nothing else runs
    # between 03:00 and 05:00, so the pulse cadence would otherwise have a hole).
    deep = deep_numbers(conn)
    if far:
        # No backfill/truth (only future dates) and no D0 (the near sweeps and
        # watches already keep today fresh).
        past: list[date] = []
        broad_future = [today + timedelta(days=d)
                        for d in range(FAR_MIN_DAYS, FAR_MAX_DAYS + 1)]
        deep_future = [today + timedelta(days=d)
                       for d in range(FAR_MIN_DAYS, FAR_DEEP_MAX_DAYS + 1)]
        deep_future += [today + timedelta(days=d) for d in PULSE_OFFSETS]
        base: list[date] = []
    else:
        broad_future = [today + timedelta(days=d) for d in range(1, LOOKAHEAD_DAYS + 1)]
        deep_future = [today + timedelta(days=d)
                       for d in range(1, LOOKAHEAD_DAYS + DEEP_LOOKAHEAD_BONUS + 1)]
        past = [today - timedelta(days=d) for d in range(1, BACKFILL_DAYS + 1)]  # truth pass
        # Sweep D0 (today) too: same-day tail swaps only show up on today's slice, so
        # skipping it lets an already-captured assignment go stale. It's a forecast
        # day (not settled), so it's never truth-skipped and — offset 0 — is queried
        # first under the priority sort.
        base = [today, *past]

    def targets_for(number: str) -> list:
        return sorted(set(base) | set(deep_future if number in deep else broad_future))

    # Chained legs may reference any day in the swept window; the union spans D0,
    # so it already bounds chain reachability.
    targets = sorted(set(base) | set(broad_future) | set(deep_future))
    window_dates = set(targets)

    # Truth pass: skip past (number, date) pairs already settled — their tail
    # won't change, so re-querying them wastes lookups. Always empty in far
    # mode (no past dates there).
    have_truth = existing_truth(conn, min(past), today) if past else set()

    # Far mode never chains — discovery is the near sweeps' job (see above).
    chain_hops = 0 if far else CHAIN_HOPS
    n_deep = sum(1 for s in candidates if s["flight_number"] in deep)
    log(f"source={source_label}: {len(candidates)} flight numbers x dates "
        f"{targets[0]}..{targets[-1]} "
        f"(backfill={BACKFILL_DAYS if not far else 0}, "
        f"lookahead={LOOKAHEAD_DAYS if not far else f'{FAR_MIN_DAYS}-{FAR_MAX_DAYS}'}, "
        f"chain_hops={chain_hops}, "
        f"deep={n_deep}/{len(candidates)} to {max(deep_future) if deep_future else '-'})")
    log("flight numbers: " + ", ".join(f"LH{s['flight_number']}({s['seed_type']})" for s in candidates))

    # Work queue of (flight_number, date, seed_type, hop), deduped by (num, date).
    # Base entries are hop 0; previousFlight discoveries are enqueued at hop+1.
    queued = set()
    work = []
    skipped_truth = 0
    for s in candidates:
        for t in targets_for(s["flight_number"]):
            key = (s["flight_number"], t)
            if key in queued:
                continue
            if t < today and key in have_truth:  # already-settled past flight
                skipped_truth += 1
                continue
            queued.add(key)
            work.append((s["flight_number"], t, s["seed_type"], 0))
    # Priority: nearest dates first (small |offset|), shuffled within each lead-
    # time band so the request pattern still varies. If MAX_LOOKUPS bites, the
    # drops are then the least-valuable far-future lookups.
    random.shuffle(work)
    work.sort(key=lambda w: abs((w[1] - today).days))

    if dry_run:
        n_truth = sum(1 for w in work if w[1] < today)
        by_offset = defaultdict(int)
        for w in work:
            by_offset[(w[1] - today).days] += 1
        log(f"dry-run: {len(work)} planned lookups ({n_truth} truth / {len(work) - n_truth} forecast, "
            f"{sum(1 for w in work if w[0] in deep)} deep-tier); "
            f"{skipped_truth} past pairs skipped as already-settled")
        log("dry-run: by lead offset: "
            + ", ".join(f"D{o:+d}={by_offset[o]}" for o in sorted(by_offset)))
        log("dry-run: not launching browser or writing to DB")
        conn.rollback()  # discard the uncommitted catalog refresh
        conn.close()
        return 0

    per_pass = per_pass_schema(conn)
    run_id = log_batch_start(conn)
    obs_run_id = run_id if per_pass else None  # None -> legacy daily upsert
    total = ok = err = upserted = chained = resets = 0
    status, detail = "ok", None
    summary = None
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            sess = {}

            def open_session():
                old = sess.get("ctx")
                if old:
                    try:
                        old.close()
                    except Exception:
                        pass
                sess["ctx"], sess["page"] = _open_session(browser)
                sess["n"] = 0

            open_session()

            while work and total < MAX_LOOKUPS:
                flight_number, target, seed_type, hop = work.pop(0)
                # recycle the session before it hits Distil's per-session cap
                if sess["n"] >= SESSION_LOOKUPS:
                    log(f"  recycling session after {sess['n']} lookups")
                    resets += 1
                    open_session()
                total += 1
                payload = fetch_one(sess, flight_number, target, open_session)
                sess["n"] += 1
                if payload is None:
                    err += 1
                    obs = parse_flight({}, flight_number, target, seed_type)
                    upsert_observation(conn, obs, obs_run_id)
                    conn.commit()
                else:
                    obs = parse_flight(payload, flight_number, target, seed_type)
                    upsert_observation(conn, obs, obs_run_id)
                    conn.commit()
                    upserted += 1
                    if obs["found"]:
                        ok += 1
                        prev = (f"{obs['prev_airline']}{obs['prev_flight_number']}@{obs['prev_flight_date']}"
                                if obs["prev_flight_number"] else "n/a")
                        log(f"  {'> ' * hop}LH{flight_number} {target}: {obs['registration']} "
                            f"{obs['dep_airport_iata']}->{obs['arr_airport_iata']} prev={prev}")
                        # rotation-chain expansion: follow previousFlight to fill
                        # legs (often tactically-flown) that the seed never caught
                        pn, pd = obs["prev_flight_number"], obs["prev_flight_date"]
                        if (hop < chain_hops and obs["prev_airline"] == "LH"
                                and pn and pn.isdigit() and pd in window_dates
                                and (pn, pd) not in queued and len(queued) < MAX_LOOKUPS
                                and not (pd < today and (pn, pd) in have_truth)):
                            queued.add((pn, pd))
                            work.append((pn, pd, seed_type, hop + 1))
                            chained += 1
                time.sleep(random.uniform(REQUEST_DELAY_MIN_S, REQUEST_DELAY_MAX_S))

            if sess.get("ctx"):
                try:
                    sess["ctx"].close()
                except Exception:
                    pass
            browser.close()
        # catalog lifecycle: retire numbers that stopped returning a widebody.
        # Owned by the near sweeps only — the far pass just reads the catalog.
        if using_catalog and not far:
            try:
                prune_catalog(conn)
            except Exception as e:
                log(f"catalog prune error (non-fatal): {e}")
        # coverage audit (monitor) — read-only; must never fail the run. Always
        # spans through FAR_MAX_DAYS so gap/overlap detection reflects the full
        # near+far horizon regardless of which pass triggers it.
        try:
            summary = coverage_audit(conn, today, max(LOOKAHEAD_DAYS, FAR_MAX_DAYS))
        except Exception as e:
            log(f"coverage audit error (non-fatal): {e}")
    except Exception as e:
        status, detail = "error", str(e)
        log(f"batch error: {e}")
    finally:
        log_batch_finish(conn, run_id, total, ok, err, upserted, status, detail)
        if summary is not None:
            store_coverage(conn, run_id, summary)
        conn.close()
    log(f"done: {total} lookups ({chained} via chain, {resets} session recycles), "
        f"{ok} found, {err} blocked/missing, {upserted} rows upserted")
    return 0 if status == "ok" else 1


def watch_candidates(conn: psycopg.Connection, horizon_hours: float) -> list[dict]:
    """Flights that could still change tail before pushback: the latest found
    snapshot per (flight_date, flight_number) whose scheduled departure lies in
    [now - 45min, now + horizon), soonest first. The small lookback keeps a
    delayed flight in scope; anything the latest snapshot already shows as
    airborne or done is dropped. Watch passes only re-check known flights —
    discovery of new numbers stays with the sweeps."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM (
                SELECT DISTINCT ON (o.flight_date, o.airline, o.flight_number)
                    o.flight_number, o.flight_date, o.seed_type,
                    o.dep_scheduled, o.overall_status
                FROM flight_status_observations o
                WHERE o.found
                  AND o.airline = 'LH'
                  AND o.dep_scheduled >= NOW() - INTERVAL '45 minutes'
                  AND o.dep_scheduled <  NOW() + make_interval(mins => %s)
                ORDER BY o.flight_date, o.airline, o.flight_number, o.observed_at DESC
            ) latest
            ORDER BY dep_scheduled
            """,
            (int(horizon_hours * 60),),
        )
        rows = cur.fetchall()
    return [
        {"flight_number": r[0], "flight_date": r[1], "seed_type": r[2], "dep": r[3]}
        for r in rows
        if (r[4] or "").upper() not in WATCH_SKIP_STATUSES
    ]


def run_light_pass(label: str, work: list, conn: psycopg.Connection) -> int:
    """Fetch a pre-computed (flight_number, flight_date) work list and store it.

    The shared body of the two light passes — watch and pulse. Neither refreshes
    or prunes the catalog, chains, or runs the coverage audit: those belong to
    the sweeps. Both stay small enough for a single Distil session, so the only
    difference between them is how `work` was chosen."""
    per_pass = per_pass_schema(conn)
    run_id = log_batch_start(conn)
    obs_run_id = run_id if per_pass else None  # None -> legacy daily upsert
    total = ok = err = upserted = resets = 0
    status, detail = "ok", None
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            sess = {}

            def open_session():
                old = sess.get("ctx")
                if old:
                    try:
                        old.close()
                    except Exception:
                        pass
                sess["ctx"], sess["page"] = _open_session(browser)
                sess["n"] = 0

            open_session()
            for w in work[:MAX_LOOKUPS]:
                if sess["n"] >= SESSION_LOOKUPS:
                    log(f"  recycling session after {sess['n']} lookups")
                    resets += 1
                    open_session()
                total += 1
                payload = fetch_one(sess, w["flight_number"], w["flight_date"], open_session)
                sess["n"] += 1
                obs = parse_flight(payload or {}, w["flight_number"], w["flight_date"], w["seed_type"])
                upsert_observation(conn, obs, obs_run_id)
                conn.commit()
                if payload is None:
                    err += 1
                else:
                    upserted += 1
                    if obs["found"]:
                        ok += 1
                        log(f"  LH{w['flight_number']} {w['flight_date']}: {obs['registration']} "
                            f"{obs['dep_airport_iata']}->{obs['arr_airport_iata']} {obs['overall_status']}")
                time.sleep(random.uniform(REQUEST_DELAY_MIN_S, REQUEST_DELAY_MAX_S))

            if sess.get("ctx"):
                try:
                    sess["ctx"].close()
                except Exception:
                    pass
            browser.close()
    except Exception as e:
        status, detail = "error", str(e)
        log(f"{label} error: {e}")
    finally:
        log_batch_finish(conn, run_id, total, ok, err, upserted, status, detail)
        conn.close()
    log(f"{label} done: {total} lookups ({resets} session recycles), "
        f"{ok} found, {err} blocked/missing, {upserted} rows upserted")
    return 0 if status == "ok" else 1


def run_watch(horizon_hours: float, dry_run: bool = False) -> int:
    """Light same-day pass: re-check only the flights departing within the next
    few hours, so a late tail swap is caught while catching it still has value.
    Typically 15-45 lookups."""
    conn = connect()
    try:
        work = watch_candidates(conn, horizon_hours)
    except Exception as e:
        log(f"watch candidate query failed: {e}")
        conn.close()
        return 1
    if not work:
        log(f"watch[{horizon_hours}h]: no re-checkable departures in window — nothing to do")
        conn.close()
        return 0
    log(f"watch[{horizon_hours}h]: {len(work)} departures: "
        + ", ".join(f"LH{w['flight_number']}@{w['dep']:%H:%M}Z" for w in work))
    if dry_run:
        log("dry-run: not launching browser or writing to DB")
        conn.close()
        return 0
    return run_light_pass(f"watch[{horizon_hours}h]", work, conn)


def run_pulse(dry_run: bool = False) -> int:
    """Deep-tier high-cadence pass: re-check B748/A388 legs at D+1/D+2 only.

    Run ~7x/day between the sweeps. The sweeps see those slices twice a day,
    which leaves ~12h between looks — wide enough that a reassignment can only
    be placed within half a day, and far too wide to tell what time of day it
    happened. Interleaving these keeps every gap under ~3h for the two types we
    study closely, at ~30 lookups per offset."""
    conn = connect()
    try:
        work = pulse_candidates(conn, PULSE_OFFSETS)
    except Exception as e:
        log(f"pulse candidate query failed: {e}")
        conn.close()
        return 1
    if not work:
        log("pulse: no deep-tier candidates — nothing to do "
            f"(types={','.join(DEEP_TYPES)}, offsets={PULSE_OFFSETS})")
        conn.close()
        return 0
    offsets = sorted({(w["flight_date"] - date.today()).days for w in work})
    log(f"pulse: {len(work)} lookups, {len(work) // max(len(offsets), 1)} numbers x "
        f"D{'/D'.join(f'+{o}' for o in offsets)} "
        f"(deep types: {','.join(DEEP_TYPES)})")
    if dry_run:
        log("dry-run: " + ", ".join(f"LH{w['flight_number']}@{w['flight_date']}"
                                    for w in work[:40])
            + (" ..." if len(work) > 40 else ""))
        log("dry-run: not launching browser or writing to DB")
        conn.close()
        return 0
    return run_light_pass("pulse", work, conn)


def run_single(flight: str, target: str) -> int:
    """Ad-hoc single lookup; prints the parsed observation + raw payload as JSON."""
    fn = re.sub(r"^LH", "", flight.strip().upper())
    tgt = date.fromisoformat(target)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        ctx, page = _open_session(browser)
        sess = {"ctx": ctx, "page": page, "n": 0}

        def reset():
            try:
                sess["ctx"].close()
            except Exception:
                pass
            sess["ctx"], sess["page"] = _open_session(browser)
            sess["n"] = 0

        payload = fetch_one(sess, fn, tgt, reset)
        browser.close()
    if payload is None:
        log(f"LH{fn} {tgt}: blocked or no response")
        return 1
    obs = parse_flight(payload, fn, tgt, None)
    obs_print = {k: (v.isoformat() if isinstance(v, (date, datetime)) else v)
                 for k, v in obs.items() if k != "raw"}
    print(json.dumps({"observation": obs_print, "raw": payload}, indent=2, ensure_ascii=False))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Lufthansa FIS flight-status fetcher")
    ap.add_argument("--flight", help="ad-hoc: flight number, e.g. LH716")
    ap.add_argument("--date", help="ad-hoc: target date YYYY-MM-DD")
    ap.add_argument("--watch", type=float, metavar="HOURS",
                    help="watch pass: re-check flights departing within the next HOURS "
                         "(no catalog refresh/prune, no chaining)")
    ap.add_argument("--far", action="store_true",
                    help=f"far pass: re-check D+{FAR_MIN_DAYS}..D+{FAR_MAX_DAYS} only "
                         f"(deep tier to D+{FAR_DEEP_MAX_DAYS}, plus its pulse offsets; "
                         "no catalog refresh/prune, no chaining) — the near sweep covers "
                         f"D-{BACKFILL_DAYS}..D+{LOOKAHEAD_DAYS} and owns discovery")
    ap.add_argument("--pulse", action="store_true",
                    help=f"pulse pass: deep tier ({','.join(DEEP_TYPES)}) x "
                         f"D{'/D'.join(f'+{o}' for o in PULSE_OFFSETS)} only, run several "
                         "times a day to keep those brackets under ~3h")
    ap.add_argument("--dry-run", action="store_true",
                    help="sweep/watch/far/pulse: print the planned lookups only, no browser/DB writes")
    ap.add_argument("--audit", action="store_true",
                    help="print the coverage audit for the current window and exit (no browser)")
    args = ap.parse_args()

    if args.watch is not None:
        if args.flight or args.date or args.far or args.pulse:
            ap.error("--watch cannot be combined with --flight/--date/--far/--pulse")
        if args.watch <= 0:
            ap.error("--watch requires a positive number of hours")
        return run_watch(args.watch, dry_run=args.dry_run)
    if args.pulse:
        if args.flight or args.date or args.far:
            ap.error("--pulse cannot be combined with --flight/--date/--far")
        return run_pulse(dry_run=args.dry_run)
    if args.far:
        if args.flight or args.date:
            ap.error("--far cannot be combined with --flight/--date")
        return run_batch(dry_run=args.dry_run, far=True)
    if args.audit:
        conn = connect()
        try:
            summary = coverage_audit(conn, date.today(), max(LOOKAHEAD_DAYS, FAR_MAX_DAYS))
            print(json.dumps(summary, indent=2, default=str))
        finally:
            conn.close()
        return 0
    if args.flight or args.date:
        if not (args.flight and args.date):
            ap.error("--flight and --date must be given together")
        return run_single(args.flight, args.date)
    return run_batch(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
