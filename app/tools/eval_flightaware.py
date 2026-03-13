"""
FlightAware AeroAPI evaluation and fleet rebuild script.

Queries the /operators/DLH/flights endpoint to get all currently active
Lufthansa flights, extracts unique aircraft (registration + type), and
compares against the current database.

Modes:
  (default)        — evaluate only, print report
  --update-db      — fill missing types + reactivate inactive aircraft
  --rebuild-db     — TRUNCATE all tables, then seed DB with FA-confirmed
                     D-A* aircraft (cross-referenced with OpenSky CSV for
                     ICAO24 hex codes)

Usage:
    python tools/eval_flightaware.py                # evaluate only
    python tools/eval_flightaware.py --update-db    # fill types + reactivate
    python tools/eval_flightaware.py --rebuild-db   # nuke + rebuild from FA data
"""
import argparse
import sys
import time

import requests

from lhlogging import config, db
from lhlogging.opensky_fleet import OpenSkyFleetClient
from lhlogging.utils import setup_logging

AEROAPI_BASE = "https://aeroapi.flightaware.com/aeroapi"
OPERATOR_CODE = "DLH"
MAINLINE_PREFIX = "D-A"


def get_api_key() -> str:
    key = config._optional("FLIGHTAWARE_API_KEY", "")
    if not key:
        print("ERROR: Set FLIGHTAWARE_API_KEY in your .env file")
        sys.exit(1)
    return key


def fetch_account_usage(session: requests.Session, logger) -> dict | None:
    """Check current account usage (free endpoint)."""
    try:
        resp = session.get(f"{AEROAPI_BASE}/account/usage")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Could not fetch account usage: {e}")
        return None


def fetch_operator_flights(session: requests.Session, logger, max_pages: int = 15) -> list[dict]:
    """Fetch pages of /operators/{code}/flights with rate-limit handling."""
    flights = []
    url = f"{AEROAPI_BASE}/operators/{OPERATOR_CODE}/flights"
    pages = 0
    # Personal tier: 10 requests/minute → need ~7s between requests to be safe
    page_delay = 7.0

    while url:
        logger.info(f"Fetching page {pages + 1}...")

        for attempt in range(3):
            resp = session.get(url)
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning(f"Rate limited (429), waiting {wait}s before retry...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            logger.critical("Rate limited 3 times in a row, aborting")
            resp.raise_for_status()

        data = resp.json()

        # Response has 'scheduled', 'arrivals', 'enroute' lists (15 items each)
        page_flights = (
            data.get("arrivals", [])
            + data.get("enroute", [])
            + data.get("scheduled", [])
        )

        flights.extend(page_flights)
        pages += 1

        # Stop if page returned no flights
        if not page_flights:
            logger.info("Empty page — stopping pagination")
            break

        # Stop at max pages to control cost
        if pages >= max_pages:
            logger.info(f"Reached max pages ({max_pages}) — stopping to conserve credits")
            break

        # Follow cursor-based pagination
        links = data.get("links")
        if links and links.get("next"):
            url = AEROAPI_BASE + links["next"]
            logger.info(f"  Got {len(page_flights)} flights, waiting {page_delay}s for rate limit...")
            time.sleep(page_delay)
        else:
            url = None

    logger.info(f"Fetched {len(flights)} flight records across {pages} pages")
    return flights


def extract_aircraft(flights: list[dict], logger) -> dict[str, dict]:
    """Extract unique aircraft from flight records, keyed by registration."""
    aircraft = {}
    no_reg = 0

    for f in flights:
        reg = (f.get("registration") or "").strip().upper()
        if not reg:
            no_reg += 1
            continue

        ac_type = (f.get("aircraft_type") or "").strip().upper()

        if reg not in aircraft:
            aircraft[reg] = {
                "registration": reg,
                "aircraft_type": ac_type or None,
                "flight_count": 1,
                "sample_callsign": (f.get("ident_icao") or f.get("ident") or ""),
                "sample_origin": (f.get("origin", {}) or {}).get("code_icao", ""),
                "sample_destination": (f.get("destination", {}) or {}).get("code_icao", ""),
            }
        else:
            aircraft[reg]["flight_count"] += 1

    logger.info(
        f"Extracted {len(aircraft)} unique aircraft from flights "
        f"({no_reg} flights had no registration)"
    )
    return aircraft


def filter_mainline(fa_aircraft: dict[str, dict], logger) -> dict[str, dict]:
    """Keep only D-A* registered aircraft (LH mainline)."""
    mainline = {
        reg: ac for reg, ac in fa_aircraft.items()
        if reg.startswith(MAINLINE_PREFIX)
    }
    excluded = len(fa_aircraft) - len(mainline)
    if excluded:
        logger.info(
            f"Filtered to {len(mainline)} mainline (D-A*) aircraft, "
            f"excluded {excluded} non-mainline"
        )
    return mainline


def compare_with_db(fa_aircraft: dict[str, dict], logger) -> dict:
    """Compare FlightAware aircraft against the database."""
    conn = db.get_connection()

    # Get ALL aircraft from DB (active + inactive)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT icao24, registration, aircraft_type, aircraft_subtype, is_active "
            "FROM aircraft ORDER BY registration"
        )
        rows = cur.fetchall()

    db_by_reg = {}
    db_by_icao24 = {}
    for r in rows:
        entry = {
            "icao24": r[0].strip(),
            "registration": (r[1] or "").strip().upper(),
            "aircraft_type": (r[2] or "").strip().upper() if r[2] else None,
            "aircraft_subtype": r[3],
            "is_active": r[4],
        }
        if entry["registration"]:
            db_by_reg[entry["registration"]] = entry
        db_by_icao24[entry["icao24"]] = entry

    total_db = len(rows)
    active_db = sum(1 for r in rows if r[4])
    inactive_db = total_db - active_db

    # --- Analysis ---
    fa_regs = set(fa_aircraft.keys())
    db_regs = set(db_by_reg.keys())

    in_fa_not_db = fa_regs - db_regs
    in_both = fa_regs & db_regs

    # Type mismatches
    type_mismatches = []
    for reg in sorted(in_both):
        fa_type = fa_aircraft[reg]["aircraft_type"]
        db_type = db_by_reg[reg]["aircraft_type"]
        if fa_type and db_type and fa_type != db_type:
            type_mismatches.append({
                "registration": reg,
                "fa_type": fa_type,
                "db_type": db_type,
                "db_subtype": db_by_reg[reg]["aircraft_subtype"],
            })

    # DB aircraft with missing type that FA could fill
    type_fills = []
    for reg in sorted(in_both):
        fa_type = fa_aircraft[reg]["aircraft_type"]
        db_type = db_by_reg[reg]["aircraft_type"]
        if fa_type and not db_type:
            type_fills.append({
                "registration": reg,
                "fa_type": fa_type,
                "icao24": db_by_reg[reg]["icao24"],
            })

    # Inactive in DB but flying in FA
    reactivations = []
    for reg in sorted(in_both):
        if not db_by_reg[reg]["is_active"]:
            reactivations.append({
                "registration": reg,
                "icao24": db_by_reg[reg]["icao24"],
                "fa_type": fa_aircraft[reg]["aircraft_type"],
            })

    conn.close()

    return {
        "total_db": total_db,
        "active_db": active_db,
        "inactive_db": inactive_db,
        "fa_unique_aircraft": len(fa_aircraft),
        "in_fa_not_db": sorted(in_fa_not_db),
        "in_both": len(in_both),
        "type_mismatches": type_mismatches,
        "type_fills": type_fills,
        "reactivations": reactivations,
        "fa_aircraft": fa_aircraft,
        "db_by_reg": db_by_reg,
    }


def print_report(result: dict, usage_before: dict | None, usage_after: dict | None, logger):
    """Print the evaluation report."""
    print("\n" + "=" * 70)
    print("  FLIGHTAWARE AEROAPI EVALUATION REPORT")
    print("=" * 70)

    # --- Cost ---
    if usage_before and usage_after:
        try:
            before_cost = usage_before.get("total_cost", 0)
            after_cost = usage_after.get("total_cost", 0)
            query_cost = after_cost - before_cost
            pages_used = usage_after.get("total_pages", 0) - usage_before.get("total_pages", 0)
            print(f"\n--- API Cost ---")
            print(f"  This query cost:    ${query_cost:.4f}")
            print(f"  Pages fetched:      {pages_used}")
            print(f"  Total spent (month):${after_cost:.4f} / $10.00")
            print(f"  Remaining:          ${10.0 - after_cost:.4f}")
        except (TypeError, KeyError):
            print(f"\n--- API Cost ---")
            print(f"  Usage before: {usage_before}")
            print(f"  Usage after:  {usage_after}")
    elif usage_after:
        print(f"\n--- API Cost ---")
        print(f"  Usage data: {usage_after}")

    # --- Coverage ---
    print(f"\n--- Coverage ---")
    print(f"  DB total aircraft:          {result['total_db']}")
    print(f"    Active:                   {result['active_db']}")
    print(f"    Inactive:                 {result['inactive_db']}")
    print(f"  FA unique aircraft seen:    {result['fa_unique_aircraft']}")
    print(f"  Overlap (in both):          {result['in_both']}")

    # --- New aircraft FA found ---
    new = result["in_fa_not_db"]
    print(f"\n--- New Aircraft (in FA, not in DB): {len(new)} ---")
    if new:
        for reg in new[:30]:
            fa = result["fa_aircraft"][reg]
            print(f"  {reg:>8}  type={fa['aircraft_type'] or '?':>5}  "
                  f"callsign={fa['sample_callsign']:<10}  "
                  f"route={fa['sample_origin']}->{fa['sample_destination']}")
        if len(new) > 30:
            print(f"  ... and {len(new) - 30} more")

    # --- Type mismatches ---
    mismatches = result["type_mismatches"]
    print(f"\n--- Type Mismatches: {len(mismatches)} ---")
    for m in mismatches[:20]:
        print(f"  {m['registration']:>8}  FA={m['fa_type']:>5}  DB={m['db_type']:>5}  "
              f"(DB subtype: {m['db_subtype'] or 'none'})")
    if len(mismatches) > 20:
        print(f"  ... and {len(mismatches) - 20} more")

    # --- Missing types we could fill ---
    fills = result["type_fills"]
    print(f"\n--- DB Aircraft Missing Type (FA can fill): {len(fills)} ---")
    for f in fills[:20]:
        print(f"  {f['registration']:>8}  icao24={f['icao24']}  FA type={f['fa_type']}")
    if len(fills) > 20:
        print(f"  ... and {len(fills) - 20} more")

    # --- Reactivations ---
    reacts = result["reactivations"]
    print(f"\n--- Inactive in DB but Flying (reactivate?): {len(reacts)} ---")
    for r in reacts[:20]:
        print(f"  {r['registration']:>8}  icao24={r['icao24']}  type={r['fa_type'] or '?'}")
    if len(reacts) > 20:
        print(f"  ... and {len(reacts) - 20} more")

    print("\n" + "=" * 70)


def update_database(result: dict, logger):
    """Upsert newly discovered aircraft and fill missing types."""
    conn = db.get_connection()
    filled = 0
    reactivated = 0

    new_regs = result["in_fa_not_db"]
    if new_regs:
        logger.warning(
            f"{len(new_regs)} aircraft found in FA but not in DB. "
            f"Cannot auto-insert because FlightAware does not provide ICAO24 hex codes. "
            f"These need to be resolved via OpenSky CSV or manual lookup."
        )

    # Fill missing types
    for f in result["type_fills"]:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE aircraft SET aircraft_type = %s, updated_at = NOW() "
                    "WHERE icao24 = %s AND (aircraft_type IS NULL OR aircraft_type = '')",
                    (f["fa_type"], f["icao24"]),
                )
                if cur.rowcount > 0:
                    filled += 1
                    logger.info(f"Filled type for {f['registration']}: {f['fa_type']}")
        except Exception as e:
            logger.error(f"Failed to update type for {f['registration']}: {e}")
            conn.rollback()

    # Reactivate aircraft that are flying but marked inactive
    for r in result["reactivations"]:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE aircraft SET is_active = TRUE, updated_at = NOW() "
                    "WHERE icao24 = %s AND is_active = FALSE",
                    (r["icao24"],),
                )
                if cur.rowcount > 0:
                    reactivated += 1
                    logger.info(f"Reactivated {r['registration']} ({r['icao24']})")
        except Exception as e:
            logger.error(f"Failed to reactivate {r['registration']}: {e}")
            conn.rollback()

    conn.commit()
    conn.close()

    logger.info(f"DB update done — filled types: {filled}, reactivated: {reactivated}")
    print(f"\n--- DB Updates Applied ---")
    print(f"  Types filled:     {filled}")
    print(f"  Reactivated:      {reactivated}")
    print(f"  New (need icao24): {len(new_regs)} (logged, not inserted)")


def rebuild_database(fa_aircraft: dict[str, dict], logger):
    """
    Truncate all tables and rebuild from FA-confirmed D-A* aircraft.

    Cross-references with OpenSky CSV to get ICAO24 hex codes (required as DB
    primary key). Aircraft not found in the CSV are inserted using FA type data
    with a placeholder ICAO24 derived from registration.
    """
    # Step 1: Filter to mainline only
    mainline = filter_mainline(fa_aircraft, logger)
    if not mainline:
        logger.critical("No mainline aircraft to insert — aborting rebuild")
        return

    # Step 2: Download OpenSky CSV to get icao24 → registration mappings
    logger.info("Downloading OpenSky CSV to resolve ICAO24 hex codes...")
    csv_client = OpenSkyFleetClient(logger)

    # Build a registration → CSV data lookup from the full DLH fleet in CSV
    csv_fleet = csv_client.get_airline_fleet(
        operator_icao="DLH",
        registration_prefixes=("D-A",),
    )
    csv_by_reg = {ac["registration"]: ac for ac in csv_fleet}
    logger.info(f"OpenSky CSV has {len(csv_by_reg)} D-A*/DLH aircraft")

    # Step 3: Merge FA + CSV data
    merged = []
    resolved = 0
    fa_only = 0

    for reg, fa in sorted(mainline.items()):
        csv_ac = csv_by_reg.get(reg)
        if csv_ac:
            # Use CSV for icao24 + subtype, prefer FA for type (more accurate)
            merged.append({
                "icao24": csv_ac["icao24"],
                "registration": reg,
                "aircraft_type": fa["aircraft_type"] or csv_ac["aircraft_type"],
                "aircraft_subtype": csv_ac["aircraft_subtype"],
            })
            resolved += 1
        else:
            # Not in CSV — log it but skip (no icao24 = can't track via OpenSky)
            logger.warning(
                f"  {reg} ({fa['aircraft_type'] or '?'}) — not in OpenSky CSV, skipping "
                f"(no ICAO24 hex = can't track via ADS-B)"
            )
            fa_only += 1

    # Also add CSV aircraft that weren't in the FA snapshot (not flying right now)
    fa_regs = set(mainline.keys())
    csv_extras = 0
    for reg, csv_ac in sorted(csv_by_reg.items()):
        if reg not in fa_regs and reg.startswith(MAINLINE_PREFIX):
            merged.append(csv_ac)
            csv_extras += 1

    logger.info(
        f"Merged fleet: {len(merged)} aircraft "
        f"({resolved} FA+CSV, {csv_extras} CSV-only, {fa_only} FA-only skipped)"
    )

    # Step 4: Truncate and rebuild
    conn = db.get_connection()

    logger.info("TRUNCATING all tables...")
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE positions, flights, batch_runs, aircraft RESTART IDENTITY CASCADE"
        )
    conn.commit()
    logger.info("Tables truncated")

    # Step 5: Insert all aircraft
    inserted = 0
    errors = 0
    for ac in merged:
        try:
            db.upsert_aircraft(conn, ac)
            inserted += 1
        except Exception as e:
            logger.error(f"Failed to insert {ac['registration']}: {e}")
            conn.rollback()
            errors += 1

    conn.commit()
    conn.close()

    print(f"\n{'=' * 70}")
    print(f"  DATABASE REBUILD COMPLETE")
    print(f"{'=' * 70}")
    print(f"  FA mainline aircraft:     {len(mainline)}")
    print(f"  Resolved via OpenSky CSV: {resolved}")
    print(f"  CSV-only (not flying now):{csv_extras}")
    print(f"  Skipped (no ICAO24):      {fa_only}")
    print(f"  Total inserted:           {inserted}")
    print(f"  Errors:                   {errors}")
    print(f"{'=' * 70}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate FlightAware AeroAPI for fleet data")
    parser.add_argument("--update-db", action="store_true",
                        help="Apply improvements to the database (fill types, reactivate)")
    parser.add_argument("--rebuild-db", action="store_true",
                        help="TRUNCATE all tables and rebuild from FA + OpenSky CSV data")
    args = parser.parse_args()

    logger = setup_logging("eval_flightaware")
    api_key = get_api_key()

    session = requests.Session()
    session.headers["x-apikey"] = api_key

    # Check usage before
    logger.info("Checking account usage before query...")
    usage_before = fetch_account_usage(session, logger)
    if usage_before:
        logger.info(f"Account usage: {usage_before}")

    # Fetch operator flights
    logger.info(f"Fetching all flights for operator {OPERATOR_CODE}...")
    try:
        flights = fetch_operator_flights(session, logger)
    except requests.HTTPError as e:
        logger.critical(f"API request failed: {e}")
        if e.response is not None:
            logger.critical(f"Response body: {e.response.text}")
        return 1

    # Extract unique aircraft
    fa_aircraft = extract_aircraft(flights, logger)

    # Check usage after
    usage_after = fetch_account_usage(session, logger)

    if args.rebuild_db:
        # Rebuild mode — no comparison needed, just nuke and rebuild
        logger.info("REBUILD MODE — will truncate DB and rebuild from FA + CSV data")
        rebuild_database(fa_aircraft, logger)
    else:
        # Compare with database
        logger.info("Comparing with database...")
        result = compare_with_db(fa_aircraft, logger)
        print_report(result, usage_before, usage_after, logger)

        if args.update_db:
            logger.info("Applying database updates...")
            update_database(result, logger)

    # Print final usage
    if usage_before and usage_after:
        try:
            before_cost = usage_before.get("total_cost", 0)
            after_cost = usage_after.get("total_cost", 0)
            print(f"\nAPI cost for this run: ${after_cost - before_cost:.4f}")
        except (TypeError, KeyError):
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
