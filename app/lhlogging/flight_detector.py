"""
Flight detector — runs every 30 minutes via cron.
Reads position snapshots from the positions table and detects flights
by looking for on_ground transitions (ground→air = departure, air→ground = arrival).

Departures within the lookback window are inserted as pending flights
(arrival_airport_icao = NULL).  On subsequent runs, pending flights are
closed when the aircraft is seen on the ground again — regardless of
how long the flight lasted.

Usage:
    python -m lhlogging.flight_detector
"""
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from lhlogging import config, db
from lhlogging.utils import setup_logging


def _detect_departures(conn, grouped: dict, logger) -> int:
    """
    Walk each aircraft's position sequence looking for on_ground transitions.
    Insert complete flights and pending departures.
    Returns the number of flights upserted.
    """
    count = 0

    for icao24, positions in grouped.items():
        departure = None
        prev = None

        for pos in positions:
            if pos["on_ground"] is None:
                continue

            if prev is not None:
                # Ground → Air: departure
                if prev["on_ground"] and not pos["on_ground"]:
                    departure = {
                        "first_seen": prev["captured_at"],
                        "dep_lat": prev["latitude"],
                        "dep_lon": prev["longitude"],
                        "callsign": pos["callsign"] or prev["callsign"],
                    }

                # Air → Ground: arrival (only if we have a matching departure)
                elif not prev["on_ground"] and pos["on_ground"] and departure:
                    dep_icao = db.lookup_nearest_airport(
                        conn, departure["dep_lat"], departure["dep_lon"]
                    )
                    arr_icao = db.lookup_nearest_airport(
                        conn, pos["latitude"], pos["longitude"]
                    )
                    # dep == arr means we likely missed the real route;
                    # close it but flag for manual review
                    review = bool(
                        dep_icao and arr_icao and dep_icao == arr_icao
                    )
                    if review:
                        logger.info(
                            f"Flagging {icao24} for review: dep==arr ({dep_icao})"
                        )

                    flight = {
                        "icao24": icao24,
                        "callsign": departure["callsign"] or pos["callsign"],
                        "dep": dep_icao,
                        "arr": arr_icao,
                        "first_seen": departure["first_seen"],
                        "last_seen": pos["captured_at"],
                        "needs_review": review,
                    }
                    db.upsert_flight(conn, flight)
                    count += 1
                    logger.info(
                        f"Flight {icao24} {dep_icao or '?'}→{arr_icao or '?'} "
                        f"({departure['first_seen'].strftime('%H:%M')}–"
                        f"{pos['captured_at'].strftime('%H:%M')})"
                    )
                    departure = None

            prev = pos

        # Open departure with no landing yet — insert as pending
        if departure:
            dep_icao = db.lookup_nearest_airport(
                conn, departure["dep_lat"], departure["dep_lon"]
            )
            latest = positions[-1]
            flight = {
                "icao24": icao24,
                "callsign": departure["callsign"],
                "dep": dep_icao,
                "arr": None,
                "first_seen": departure["first_seen"],
                "last_seen": latest["captured_at"],
                "needs_review": False,
            }
            db.upsert_flight(conn, flight)
            count += 1
            logger.info(
                f"Pending departure {icao24} {dep_icao or '?'}→? "
                f"(since {departure['first_seen'].strftime('%H:%M')})"
            )

    return count


def _close_stale_flights(conn, logger, max_age_hours: int = 24) -> int:
    """
    Close pending flights that have been open longer than max_age_hours.
    These are flights where we missed the arrival (e.g. due to a polling outage).
    Closed with arrival 'UNKN' and flagged for review.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE flights
            SET arrival_airport_icao = 'UNKN',
                needs_review = TRUE
            WHERE arrival_airport_icao IS NULL
              AND first_seen < NOW() - make_interval(hours => %s)
            RETURNING icao24, callsign, departure_airport_icao, first_seen
            """,
            (max_age_hours,),
        )
        rows = cur.fetchall()

    for r in rows:
        logger.warning(
            f"Closed stale flight {r[0].strip()} {r[2] or '?'}→UNKN "
            f"(callsign={r[1] or '?'}, departed {r[3].strftime('%Y-%m-%d %H:%M')})"
        )

    return len(rows)


def _close_pending_flights(conn, logger) -> int:
    """
    Find flights with no arrival yet. Check if the aircraft has landed
    (most recent position on_ground=True) and close them.
    Also updates last_seen for still-airborne flights.
    Returns the number of flights closed.
    """
    open_flights = db.get_open_flights(conn)
    if not open_flights:
        return 0

    icao24s = [f["icao24"] for f in open_flights]
    latest_positions = db.get_latest_positions(conn, icao24s)

    closed = 0
    for flight in open_flights:
        icao24 = flight["icao24"]
        pos = latest_positions.get(icao24.strip())
        if not pos:
            continue

        if pos["on_ground"]:
            arr_icao = db.lookup_nearest_airport(
                conn, pos["latitude"], pos["longitude"]
            )
            dep_icao = flight.get("departure_airport_icao")

            # dep == arr means we likely missed the real route;
            # close it but flag for manual review
            review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
            if review:
                logger.info(
                    f"Flagging {icao24} for review: dep==arr ({dep_icao})"
                )

            db.update_open_flight(
                conn,
                icao24,
                flight["first_seen"],
                pos["captured_at"],
                arr=arr_icao,
                callsign=pos["callsign"],
                needs_review=review,
            )
            closed += 1
            logger.info(
                f"Closed flight {icao24} → {arr_icao or '?'} "
                f"(landed {pos['captured_at'].strftime('%H:%M')})"
            )
        else:
            # Still airborne — keep last_seen fresh
            db.update_open_flight(
                conn,
                icao24,
                flight["first_seen"],
                pos["captured_at"],
                callsign=pos["callsign"],
            )

    return closed


def main() -> int:
    logger = setup_logging("flight_detector")
    logger.info("Flight detector starting")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    run_id = db.log_batch_start(conn, "flight_detector")
    stats = {
        "ok": 0,
        "error": 0,
        "flights_upserted": 0,
        "status": "ok",
        "error_detail": None,
        "aircraft_total": 0,
    }

    # --- Part 1: detect new departures in the lookback window ---
    since = datetime.now(timezone.utc) - timedelta(
        minutes=config.FLIGHT_DETECT_LOOKBACK_MINUTES
    )
    try:
        positions = db.get_positions_since(conn, since)
    except Exception as e:
        logger.critical(f"Cannot fetch positions: {e}")
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    grouped: dict[str, list[dict]] = defaultdict(list)
    for p in positions:
        grouped[p["icao24"].strip()].append(p)

    stats["aircraft_total"] = len(grouped)
    logger.info(
        f"Loaded {len(positions)} positions for {len(grouped)} aircraft "
        f"(lookback {config.FLIGHT_DETECT_LOOKBACK_MINUTES}m)"
    )

    try:
        new_flights = _detect_departures(conn, grouped, logger)
    except Exception as e:
        logger.error(f"Error detecting departures: {e}")
        conn.rollback()
        stats["error"] += 1
        new_flights = 0

    # --- Part 2: close pending flights ---
    try:
        closed = _close_pending_flights(conn, logger)
    except Exception as e:
        logger.error(f"Error closing pending flights: {e}")
        conn.rollback()
        stats["error"] += 1
        closed = 0

    # --- Part 3: close stale flights (open > 24h, likely missed arrival) ---
    try:
        stale = _close_stale_flights(conn, logger)
    except Exception as e:
        logger.error(f"Error closing stale flights: {e}")
        conn.rollback()
        stats["error"] += 1
        stale = 0

    try:
        conn.commit()
    except Exception as e:
        logger.critical(f"Commit failed: {e}")
        conn.rollback()
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    stats["ok"] = new_flights + closed + stale
    stats["flights_upserted"] = new_flights + closed + stale
    logger.info(
        f"Flight detector done — {new_flights} new/pending, {closed} closed, {stale} stale"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
