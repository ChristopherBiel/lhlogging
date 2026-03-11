"""
Route logger entrypoint — runs twice daily via cron (03:00 and 15:00 UTC).
Queries OpenSky /flights/all in 2-hour chunks over the last
OPENSKY_LOOKBACK_HOURS (default 26h), filters to the active LH fleet
client-side, and upserts matched flights into the database.

Uses the bulk /flights/all endpoint instead of per-aircraft queries to
keep credit usage fixed regardless of fleet size (~780 credits/day for
two runs with 26h lookback each, vs. 30 credits × N_aircraft per run).

Usage:
    python -m lhlogging.route_logger
"""
import sys
import time
from datetime import datetime, timezone

from lhlogging import config, db
from lhlogging.opensky import OpenSkyClient, OpenSkyError
from lhlogging.utils import RateLimiter, setup_logging


def _fmt_ts(unix: int) -> str:
    return datetime.fromtimestamp(unix, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def main() -> int:
    logger = setup_logging("route_logger")
    logger.info("Route logger starting (bulk /flights/all method)")
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

    fleet_icao24s = {a["icao24"] for a in aircraft_list}
    stats["aircraft_total"] = len(aircraft_list)
    logger.info(f"Fleet: {len(aircraft_list)} active aircraft loaded for filtering")

    now_unix = int(time.time())
    begin_unix = now_unix - (config.OPENSKY_LOOKBACK_HOURS * 3600)
    logger.info(f"Query window: {_fmt_ts(begin_unix)} → {_fmt_ts(now_unix)} ({config.OPENSKY_LOOKBACK_HOURS}h)")

    chunk_size = config.OPENSKY_CHUNK_SIZE_S
    num_chunks = -(-((now_unix - begin_unix)) // chunk_size)  # ceil division
    logger.info(f"Fetching {num_chunks} chunks of {chunk_size // 3600}h each (est. {num_chunks * 30} credits)")

    client = OpenSkyClient(logger)
    rate_limiter = RateLimiter(config.OPENSKY_REQUEST_DELAY_S)
    chunk_start = begin_unix

    while chunk_start < now_unix:
        chunk_end = min(chunk_start + chunk_size, now_unix)

        rate_limiter.wait()

        try:
            flights = client.get_flights_all(chunk_start, chunk_end, fleet_icao24s)
        except OpenSkyError as e:
            logger.error(f"Chunk {_fmt_ts(chunk_start)}–{_fmt_ts(chunk_end)} failed: {e}")
            stats["error"] += 1
            if stats["error"] >= config.BATCH_MAX_ERRORS_BEFORE_ABORT:
                msg = f"Reached {config.BATCH_MAX_ERRORS_BEFORE_ABORT} chunk errors — aborting batch"
                logger.critical(msg)
                stats["status"] = "error"
                stats["error_detail"] = msg
                break
            chunk_start += chunk_size
            continue

        inserted = 0
        try:
            for flight in flights:
                db.upsert_flight(conn, flight)
                inserted += 1
            conn.commit()
        except Exception as e:
            logger.error(f"DB error for chunk {_fmt_ts(chunk_start)}–{_fmt_ts(chunk_end)}: {e}")
            conn.rollback()
            stats["error"] += 1
            chunk_start += chunk_size
            continue

        stats["ok"] += 1
        stats["flights_upserted"] += inserted
        logger.info(
            f"Chunk {_fmt_ts(chunk_start)}–{_fmt_ts(chunk_end)}: "
            f"{len(flights)} fleet flights, {inserted} upserted"
        )

        chunk_start += chunk_size

    logger.info(
        f"Route logger done — "
        f"chunks ok: {stats['ok']}, chunk errors: {stats['error']}, "
        f"flights upserted: {stats['flights_upserted']}"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0 if stats["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
