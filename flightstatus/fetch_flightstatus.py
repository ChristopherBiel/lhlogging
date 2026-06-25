#!/usr/bin/env python3
"""
Nightly Lufthansa flight-status fetcher.

Pulls the public lufthansa.com FIS feed
(`/service/api/fis/byflightnumber?flightNumber=LH716&date=YYYY-MM-DD`) for the
B748 and A388 flight numbers seen in the last couple of days, a few days ahead,
and records the assigned airframe (tail), scheduled route/times, status, and the
aircraft's previous flight into `flight_status_observations`.

The endpoint sits behind Imperva/Distil bot management, which blocks plain HTTP
clients and headless browsers. The only thing that gets through is a *real*
(headed) Chromium — here driven by Playwright and run under Xvfb (see
run_nightly.sh). We load the timetable page once to establish the session, then
issue same-origin `fetch()`es from inside the page.

Run modes:
  python fetch_flightstatus.py                 # nightly batch (seed from DB, +1..+LOOKAHEAD days)
  python fetch_flightstatus.py --flight LH716 --date 2026-06-25   # ad-hoc single lookup (prints JSON)
  python fetch_flightstatus.py --dry-run       # batch, print seed + would-fetch, no browser/DB writes

Must be run under a display (Xvfb): `xvfb-run -a python fetch_flightstatus.py`.
"""
import argparse
import json
import os
import random
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone

import psycopg
from playwright.sync_api import sync_playwright

# --- config (env-overridable, mirrors the lhlogging config style) -----------
DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "lhlogging")
DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]

SEED_TYPES = [t.strip() for t in os.environ.get("FIS_SEED_TYPES", "B748,A388").split(",") if t.strip()]
SEED_LOOKBACK_DAYS = int(os.environ.get("FIS_SEED_LOOKBACK_DAYS", "2"))
LOOKAHEAD_DAYS = int(os.environ.get("FIS_LOOKAHEAD_DAYS", "4"))
# Distil throttles by request rate: ~2.5s pacing got ~70 lookups in before it
# started 403ing. This is a nightly job with a full hour to run, so we pace
# gently (a few hundred lookups still finish in ~15min) and, on a block, wait
# out the rate window rather than hammering with short retries.
REQUEST_DELAY_MIN_S = float(os.environ.get("FIS_REQUEST_DELAY_MIN_S", "5.0"))
REQUEST_DELAY_MAX_S = float(os.environ.get("FIS_REQUEST_DELAY_MAX_S", "10.0"))
MAX_FETCH_RETRIES = int(os.environ.get("FIS_MAX_FETCH_RETRIES", "3"))
BLOCK_BACKOFF_S = float(os.environ.get("FIS_BLOCK_BACKOFF_S", "45.0"))

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


def upsert_observation(conn: psycopg.Connection, obs: dict) -> None:
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
    obs["prev_flight_number"] = prev.get("operatingCarrierFlightNumber")
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


def fetch_one(page, flight_number: str, target: date):
    """Fetch one flight/date with retries; returns parsed JSON dict or None if blocked."""
    fn = f"LH{flight_number}"
    url = f"/service/api/fis/byflightnumber?flightNumber={fn}&date={target.isoformat()}"
    for attempt in range(1, MAX_FETCH_RETRIES + 1):
        res = page.evaluate(_FETCH_JS, url)
        status, ct = res.get("status"), res.get("ct", "")
        if status == 200 and "json" in ct:
            try:
                return json.loads(res["body"])
            except json.JSONDecodeError:
                log(f"  {fn} {target}: 200 but bad JSON")
                return None
        # blocked (distil 403) or unexpected — re-seed and retry
        log(f"  {fn} {target}: HTTP {status} ({ct[:40]}) attempt {attempt}/{MAX_FETCH_RETRIES}")
        if attempt < MAX_FETCH_RETRIES:
            # likely a rate-limit block — wait out the window, then re-seed
            time.sleep(BLOCK_BACKOFF_S)
            seed_session(page)
    return None


# --- runners ----------------------------------------------------------------
def run_batch(dry_run: bool = False) -> int:
    conn = connect()
    try:
        seed = seed_flight_numbers(conn)
    except Exception as e:
        log(f"seed query failed: {e}")
        conn.close()
        return 1
    if not seed:
        log("no seed flight numbers (no recent B748/A388 flights?) — nothing to do")
        conn.close()
        return 0

    targets = [date.today() + timedelta(days=d) for d in range(1, LOOKAHEAD_DAYS + 1)]
    jobs = [(s["flight_number"], t, s["seed_type"]) for s in seed for t in targets]
    log(f"seed: {len(seed)} flight numbers x {len(targets)} days = {len(jobs)} lookups "
        f"(types={SEED_TYPES}, lookahead={LOOKAHEAD_DAYS})")
    log("flight numbers: " + ", ".join(f"LH{s['flight_number']}({s['seed_type']})" for s in seed))

    if dry_run:
        log("dry-run: not launching browser or writing to DB")
        conn.close()
        return 0

    run_id = log_batch_start(conn)
    total = len(jobs)
    ok = err = upserted = 0
    status, detail = "ok", None
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            ctx = browser.new_context(locale="en-US", user_agent=USER_AGENT)
            ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            )
            page = ctx.new_page()
            seed_session(page)

            for flight_number, target, seed_type in jobs:
                payload = fetch_one(page, flight_number, target)
                if payload is None:
                    err += 1
                    # still record the miss so we know we tried
                    obs = parse_flight({}, flight_number, target, seed_type)
                    upsert_observation(conn, obs)
                    conn.commit()
                else:
                    obs = parse_flight(payload, flight_number, target, seed_type)
                    upsert_observation(conn, obs)
                    conn.commit()
                    upserted += 1
                    if obs["found"]:
                        ok += 1
                        prev = (f"{obs['prev_airline']}{obs['prev_flight_number']}@{obs['prev_flight_date']}"
                                if obs["prev_flight_number"] else "n/a")
                        log(f"  LH{flight_number} {target}: {obs['registration']} "
                            f"{obs['dep_airport_iata']}->{obs['arr_airport_iata']} prev={prev}")
                time.sleep(random.uniform(REQUEST_DELAY_MIN_S, REQUEST_DELAY_MAX_S))

            browser.close()
    except Exception as e:
        status, detail = "error", str(e)
        log(f"batch error: {e}")
    finally:
        log_batch_finish(conn, run_id, total, ok, err, upserted, status, detail)
        conn.close()
    log(f"done: {ok} found, {err} blocked/missing, {upserted} rows upserted")
    return 0 if status == "ok" else 1


def run_single(flight: str, target: str) -> int:
    """Ad-hoc single lookup; prints the parsed observation + raw payload as JSON."""
    fn = re.sub(r"^LH", "", flight.strip().upper())
    tgt = date.fromisoformat(target)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(locale="en-US", user_agent=USER_AGENT)
        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = ctx.new_page()
        seed_session(page)
        payload = fetch_one(page, fn, tgt)
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
    ap.add_argument("--dry-run", action="store_true", help="batch: print seed only")
    args = ap.parse_args()

    if args.flight or args.date:
        if not (args.flight and args.date):
            ap.error("--flight and --date must be given together")
        return run_single(args.flight, args.date)
    return run_batch(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
