"""
Fleet discovery — runs periodically via cron.

Fetches all live state vectors from OpenSky and looks for aircraft transmitting
callsigns that match the airline prefix (e.g. 'DLH') but are not yet in the
aircraft database. New aircraft are looked up in the OpenSky CSV for
registration and type data, then inserted into the database.

This complements the weekly fleet_refresh, which seeds the database from the
CSV's operatoricao field. Discovery catches aircraft that the CSV has with a
blank or incorrect operator code.

Usage:
    python -m lhlogging.fleet_discovery
"""
import sys

from lhlogging import config, db
from lhlogging.opensky import OpenSkyClient, OpenSkyError
from lhlogging.opensky_fleet import OpenSkyFleetClient, OpenSkyFleetError
from lhlogging.planespotters import PlanespottersClient, PlanespottersError, PlanespottersRateLimitError
from lhlogging.utils import setup_logging


def main() -> int:
    logger = setup_logging("fleet_discovery")
    logger.info("Fleet discovery starting")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    run_id = db.log_batch_start(conn, "fleet_discovery")
    stats = {"ok": 0, "error": 0, "flights_upserted": 0, "status": "ok", "error_detail": None}

    # --- Step 1: Fetch all live states and filter by callsign prefix ---
    prefix = config.AIRLINE_CALLSIGN_PREFIX
    logger.info(f"Fetching live states with callsign prefix '{prefix}'")

    client = OpenSkyClient(logger)
    try:
        states = client.get_states_by_callsign_prefix(prefix)
    except OpenSkyError as e:
        logger.critical(f"Failed to fetch states: {e}")
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    live_icao24s = {s["icao24"] for s in states}
    logger.info(f"Found {len(live_icao24s)} aircraft airborne with '{prefix}*' callsigns")

    if not live_icao24s:
        logger.info("No aircraft found — nothing to discover")
        stats["aircraft_total"] = 0
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 0

    # --- Step 2: Find which are not already in the database ---
    known = db.get_active_aircraft(conn)
    known_icao24s = {a["icao24"].strip() for a in known}

    new_icao24s = live_icao24s - known_icao24s
    logger.info(
        f"{len(known_icao24s)} aircraft already in DB, "
        f"{len(new_icao24s)} new aircraft to discover"
    )

    if not new_icao24s:
        logger.info("No new aircraft discovered")
        stats["aircraft_total"] = 0
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 0

    # --- Step 3: Look up new aircraft in the OpenSky CSV ---
    csv_client = OpenSkyFleetClient(logger)
    try:
        csv_data = csv_client.get_aircraft_by_icao24s(new_icao24s)
    except OpenSkyFleetError as e:
        logger.warning(f"OpenSky CSV lookup failed: {e} — will insert with limited data")
        csv_data = {}

    # --- Step 4: Enrich with Planespotters for aircraft not in CSV ---
    missing_from_csv = new_icao24s - set(csv_data.keys())
    if missing_from_csv:
        logger.info(
            f"{len(missing_from_csv)} aircraft not in OpenSky CSV, "
            "trying Planespotters for type data"
        )
        ps_client = PlanespottersClient(logger)
        for icao24 in missing_from_csv:
            try:
                ps_data = ps_client.get_aircraft(icao24)
                if ps_data:
                    csv_data[icao24] = {
                        "icao24": icao24,
                        "registration": icao24.upper(),
                        "aircraft_type": ps_data.get("aircraft_type"),
                        "aircraft_subtype": ps_data.get("aircraft_subtype"),
                    }
            except PlanespottersRateLimitError:
                logger.warning("Planespotters rate limit — stopping enrichment")
                break
            except PlanespottersError as e:
                logger.warning(f"Planespotters lookup failed for {icao24}: {e}")

    # --- Step 5: Upsert discovered aircraft ---
    for icao24 in new_icao24s:
        aircraft = csv_data.get(icao24)
        if not aircraft:
            # Bare minimum: we know the hex code, use it as placeholder registration
            aircraft = {
                "icao24": icao24,
                "registration": icao24.upper(),
                "aircraft_type": None,
                "aircraft_subtype": None,
            }

        # Flag for review if type or real registration is missing
        aircraft["needs_review"] = (
            not aircraft.get("aircraft_type")
            or aircraft.get("registration", "").upper() == icao24.upper()
        )

        try:
            db.upsert_aircraft(conn, aircraft)
            stats["ok"] += 1
            logger.info(
                f"Discovered: {icao24} → {aircraft.get('registration')} "
                f"({aircraft.get('aircraft_type') or 'unknown type'})"
            )
        except Exception as e:
            logger.error(f"Failed to upsert {icao24}: {e}")
            stats["error"] += 1
            conn.rollback()

    conn.commit()

    stats["aircraft_total"] = len(new_icao24s)
    logger.info(
        f"Fleet discovery done — "
        f"discovered: {stats['ok']}, errors: {stats['error']}"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
