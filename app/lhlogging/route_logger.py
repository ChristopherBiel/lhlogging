"""
Route logger entrypoint — runs twice daily via cron (06:00 and 18:00 UTC).
Queries OpenSky for completed flights for each active LH aircraft over the
last OPENSKY_LOOKBACK_HOURS (default 36h), upserts into the flights table.

Usage:
    python -m lhlogging.route_logger
"""
import sys
import time

from lhlogging import config, db
from lhlogging.opensky import OpenSkyClient, OpenSkyError
from lhlogging.utils import RateLimiter, setup_logging


def main() -> int:
    logger = setup_logging("route_logger")
    logger.info("Route logger starting")
    if config.TRACK_AIRCRAFT_TYPES:
        logger.info(f"Aircraft type filter active: {sorted(config.TRACK_AIRCRAFT_TYPES)}")
    else:
        logger.info("No aircraft type filter — tracking all active aircraft")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    run_id = db.log_batch_start(conn, "route_logger")
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

    stats["aircraft_total"] = len(aircraft_list)
    logger.info(f"Processing {len(aircraft_list)} active aircraft")

    now_unix = int(time.time())
    begin_unix = now_unix - (config.OPENSKY_LOOKBACK_HOURS * 3600)

    client = OpenSkyClient(logger)
    rate_limiter = RateLimiter(config.OPENSKY_REQUEST_DELAY_S)

    for aircraft in aircraft_list:
        icao24 = aircraft["icao24"]
        reg = aircraft["registration"]

        rate_limiter.wait()

        try:
            flights = client.get_flights_for_aircraft(icao24, begin_unix, now_unix)
        except OpenSkyError as e:
            logger.error(f"OpenSky error for {icao24} ({reg}): {e}")
            stats["error"] += 1
            if stats["error"] >= config.BATCH_MAX_ERRORS_BEFORE_ABORT:
                msg = f"Reached {config.BATCH_MAX_ERRORS_BEFORE_ABORT} errors — aborting batch"
                logger.critical(msg)
                stats["status"] = "error"
                stats["error_detail"] = msg
                break
            continue

        inserted = 0
        try:
            for flight in flights:
                db.upsert_flight(conn, flight)
                inserted += 1
            conn.commit()
        except Exception as e:
            logger.error(f"DB error for {icao24} ({reg}): {e}")
            conn.rollback()
            stats["error"] += 1
            continue

        stats["ok"] += 1
        stats["flights_upserted"] += inserted

        if inserted:
            logger.debug(f"{icao24} ({reg}): {inserted} flights upserted")

    logger.info(
        f"Route logger done — "
        f"aircraft ok: {stats['ok']}, errors: {stats['error']}, "
        f"flights upserted: {stats['flights_upserted']}"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0 if stats["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
