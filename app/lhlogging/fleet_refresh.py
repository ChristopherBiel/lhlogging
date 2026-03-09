"""
Weekly entrypoint: refreshes the Lufthansa fleet from Planespotters.net.

Usage:
    python -m lhlogging.fleet_refresh
"""
import sys

from lhlogging import config, db
from lhlogging.planespotters import PlanespottersClient, PlanespottersDataError, PlanespottersError
from lhlogging.utils import setup_logging

# Minimum plausible fleet size — guards against retiring everything on an API failure
_MIN_FLEET_SANITY = 50


def main() -> int:
    logger = setup_logging("fleet_refresh")
    logger.info("Fleet refresh starting")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    run_id = db.log_batch_start(conn, "fleet_refresh")
    stats = {"ok": 0, "error": 0, "flights_upserted": 0, "status": "ok", "error_detail": None}

    try:
        client = PlanespottersClient(logger)
        api_fleet = client.get_airline_fleet(config.PLANESPOTTERS_AIRLINE_ICAO)
    except PlanespottersError as e:
        logger.critical(f"Failed to fetch fleet from Planespotters: {e}")
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    if len(api_fleet) < _MIN_FLEET_SANITY:
        msg = (
            f"Planespotters returned only {len(api_fleet)} aircraft "
            f"(minimum expected: {_MIN_FLEET_SANITY}). "
            "Aborting to avoid incorrectly retiring active aircraft."
        )
        logger.critical(msg)
        stats["status"] = "error"
        stats["error_detail"] = msg
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    # --- Upsert all aircraft from API ---
    api_icao24_set: set[str] = set()
    new_count = 0
    for aircraft in api_fleet:
        try:
            db.upsert_aircraft(conn, aircraft)
            api_icao24_set.add(aircraft["icao24"])
            stats["ok"] += 1
        except Exception as e:
            logger.error(f"Failed to upsert {aircraft}: {e}")
            stats["error"] += 1
            conn.rollback()
    conn.commit()

    # --- Retire aircraft no longer in the API response ---
    db_fleet = db.get_active_aircraft(conn)
    db_icao24_set = {a["icao24"] for a in db_fleet}
    retired = db_icao24_set - api_icao24_set

    for icao24 in retired:
        try:
            db.mark_aircraft_retired(conn, icao24)
            logger.warning(f"Retired aircraft no longer in Planespotters fleet: {icao24}")
        except Exception as e:
            logger.error(f"Failed to retire {icao24}: {e}")
    conn.commit()

    stats["aircraft_total"] = len(api_fleet)
    logger.info(
        f"Fleet refresh done — "
        f"upserted: {stats['ok']}, errors: {stats['error']}, retired: {len(retired)}"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
