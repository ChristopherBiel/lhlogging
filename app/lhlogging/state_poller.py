"""
State poller — runs every 5 minutes via cron.
Calls OpenSky /states/all, filters to the active fleet client-side,
and stores position snapshots in the positions table.

Usage:
    python -m lhlogging.state_poller
"""
import sys

from lhlogging import config, db
from lhlogging.opensky import OpenSkyClient, OpenSkyError
from lhlogging.utils import setup_logging


def main() -> int:
    logger = setup_logging("state_poller")
    logger.info("State poller starting")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    run_id = db.log_batch_start(conn, "state_poller")
    stats = {
        "ok": 0,
        "error": 0,
        "flights_upserted": 0,
        "status": "ok",
        "error_detail": None,
        "aircraft_total": 0,
    }

    try:
        aircraft_list = db.get_active_aircraft(conn, config.TRACK_AIRCRAFT_TYPES)
    except Exception as e:
        logger.critical(f"Cannot fetch aircraft list: {e}")
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    fleet_icao24s = {a["icao24"].strip() for a in aircraft_list}
    stats["aircraft_total"] = len(aircraft_list)
    logger.info(f"Fleet: {len(aircraft_list)} active aircraft")

    client = OpenSkyClient(logger)
    try:
        states = client.get_states_all(fleet_icao24s)
    except OpenSkyError as e:
        logger.critical(f"Failed to fetch states: {e}")
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        stats["error"] = 1
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    # Filter out entries with no position fix
    valid = [s for s in states if s.get("latitude") is not None]
    skipped = len(states) - len(valid)
    if skipped:
        logger.info(f"Skipped {skipped} states with no position fix")

    try:
        inserted = db.insert_positions(conn, valid)
        conn.commit()
    except Exception as e:
        logger.critical(f"DB error inserting positions: {e}")
        conn.rollback()
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        stats["error"] = 1
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    stats["ok"] = len(valid)
    stats["flights_upserted"] = inserted
    logger.info(
        f"State poller done — {len(valid)} aircraft seen, {inserted} positions inserted"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
