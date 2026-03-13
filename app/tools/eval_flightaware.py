"""
FlightAware AeroAPI evaluation script.

Queries the /operators/DLH/flights endpoint to get all currently active
Lufthansa flights, extracts unique aircraft (registration + type), and
compares against the current database.

Reports:
  - API cost and data quality
  - Aircraft in FA but missing from DB
  - Aircraft in DB but not seen in FA (expected — not all fly at once)
  - Type/registration mismatches between FA and DB
  - Option to upsert newly discovered aircraft into the DB

Usage:
    python tools/eval_flightaware.py                # evaluate only
    python tools/eval_flightaware.py --update-db    # evaluate + upsert new aircraft
"""
import argparse
import sys
import time

import requests

from lhlogging import config, db
from lhlogging.utils import setup_logging

AEROAPI_BASE = "https://aeroapi.flightaware.com/aeroapi"
OPERATOR_CODE = "DLH"


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


def fetch_operator_flights(session: requests.Session, logger) -> list[dict]:
    """Fetch all pages of /operators/{code}/flights with rate-limit handling."""
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

        page_flights = data.get("arrivals", []) + data.get("departures", []) + data.get("flights", [])
        flights.extend(page_flights)
        pages += 1

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
    upserted = 0
    filled = 0
    reactivated = 0

    # We cannot upsert FA-only aircraft directly because we don't have icao24.
    # FA gives us registration but not ICAO24 hex code.
    # We can only fill types and reactivate for aircraft already in the DB.
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate FlightAware AeroAPI for fleet data")
    parser.add_argument("--update-db", action="store_true",
                        help="Apply improvements to the database (fill types, reactivate)")
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

    # Compare with database
    logger.info("Comparing with database...")
    result = compare_with_db(fa_aircraft, logger)

    # Print report
    print_report(result, usage_before, usage_after, logger)

    # Optionally update DB
    if args.update_db:
        logger.info("Applying database updates...")
        update_database(result, logger)

    return 0


if __name__ == "__main__":
    sys.exit(main())
