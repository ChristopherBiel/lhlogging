"""
Flight detector — runs every 30 minutes via cron.
Reads position snapshots from the positions table and detects flights
by looking for on_ground transitions (ground→air = departure, air→ground = arrival).

When OpenSky's on_ground flag is unreliable, a velocity+altitude fallback
is used: an aircraft with velocity < 30 m/s and altitude < 300 m is treated
as on the ground.

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


def _is_on_ground(pos: dict) -> bool | None:
    """
    Determine if an aircraft is on the ground using on_ground flag with
    a velocity+altitude fallback for when OpenSky's flag is unreliable.

    Returns True (on ground), False (airborne), or None (indeterminate).
    """
    if pos["on_ground"] is True:
        return True
    if pos["on_ground"] is None:
        return None
    # on_ground is False — check velocity+altitude fallback
    vel = pos.get("velocity_ms")
    alt = pos.get("altitude_m")
    if vel is not None and alt is not None:
        if vel < config.LANDING_VELOCITY_THRESHOLD_MS and alt < config.LANDING_ALTITUDE_THRESHOLD_M:
            return True
    return False


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
        prev_ground = None

        for pos in positions:
            cur_ground = _is_on_ground(pos)
            if cur_ground is None:
                continue

            if prev is not None and prev_ground is not None:
                # Ground → Air: departure
                if prev_ground and not cur_ground:
                    departure = {
                        "first_seen": prev["captured_at"],
                        "dep_lat": prev["latitude"],
                        "dep_lon": prev["longitude"],
                        "callsign": pos["callsign"] or prev["callsign"],
                    }

                # Air → Ground: arrival (only if we have a matching departure)
                elif not prev_ground and cur_ground and departure:
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
            prev_ground = cur_ground

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

    Before defaulting to 'UNKN', checks if the aircraft's last position is
    low altitude near an airport — if so, uses that airport as the arrival.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT icao24, first_seen, callsign, departure_airport_icao
            FROM flights
            WHERE arrival_airport_icao IS NULL
              AND first_seen < NOW() - make_interval(hours => %s)
            """,
            (max_age_hours,),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    stale_flights = [
        {
            "icao24": r[0],
            "first_seen": r[1],
            "callsign": r[2],
            "departure_airport_icao": r[3],
        }
        for r in rows
    ]

    icao24s = [f["icao24"] for f in stale_flights]
    latest_positions = db.get_latest_positions(conn, icao24s)

    closed = 0
    for flight in stale_flights:
        icao24 = flight["icao24"]
        pos = latest_positions.get(icao24.strip())

        arr_icao = None
        if pos:
            alt = pos.get("altitude_m")
            if alt is not None and alt < config.PROXIMITY_LANDING_ALTITUDE_M:
                arr_icao = db.lookup_nearest_airport(
                    conn,
                    pos["latitude"],
                    pos["longitude"],
                    max_km=config.PROXIMITY_LANDING_RADIUS_KM,
                )

        if arr_icao:
            logger.info(
                f"Stale flight {icao24.strip()}: last position near {arr_icao} "
                f"(alt={pos.get('altitude_m')}), using as arrival"
            )
        else:
            arr_icao = "UNKN"
            logger.warning(
                f"Closed stale flight {icao24.strip()} "
                f"{flight['departure_airport_icao'] or '?'}→UNKN "
                f"(callsign={flight['callsign'] or '?'}, "
                f"departed {flight['first_seen'].strftime('%Y-%m-%d %H:%M')})"
            )

        last_seen = pos["captured_at"] if pos else flight["first_seen"]
        db.update_open_flight(
            conn,
            icao24,
            flight["first_seen"],
            last_seen,
            arr=arr_icao,
            callsign=pos["callsign"] if pos else flight["callsign"],
            needs_review=True,
        )
        closed += 1

    return closed


def _close_pending_flights(conn, logger) -> int:
    """
    Find flights with no arrival yet. Check if the aircraft has landed
    (on_ground flag or velocity+altitude fallback) and close them.
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

        on_ground = _is_on_ground(pos)
        if on_ground:
            # Log when the fallback triggered (on_ground was False but vel+alt say landed)
            if not pos["on_ground"]:
                logger.info(
                    f"Landing detected via velocity+altitude fallback for {icao24} "
                    f"(vel={pos.get('velocity_ms')}, alt={pos.get('altitude_m')})"
                )

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
            # Not detected as on-ground — check proximity-based landing fallback.
            # If positions stopped arriving (stale) and last position is low altitude
            # near an airport, the aircraft likely landed but we missed the on_ground state.
            stale_cutoff = datetime.now(timezone.utc) - timedelta(
                minutes=config.PROXIMITY_LANDING_MIN_STALE_MINUTES
            )
            alt = pos.get("altitude_m")
            if (
                pos["captured_at"] < stale_cutoff
                and alt is not None
                and alt < config.PROXIMITY_LANDING_ALTITUDE_M
            ):
                arr_icao = db.lookup_nearest_airport(
                    conn,
                    pos["latitude"],
                    pos["longitude"],
                    max_km=config.PROXIMITY_LANDING_RADIUS_KM,
                )
                if arr_icao:
                    dep_icao = flight.get("departure_airport_icao")
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
                        f"Landing inferred via proximity for {icao24} → {arr_icao} "
                        f"(alt={alt}, stale since {pos['captured_at'].strftime('%H:%M')})"
                    )
                    continue

            # Still airborne — keep last_seen fresh
            db.update_open_flight(
                conn,
                icao24,
                flight["first_seen"],
                pos["captured_at"],
                callsign=pos["callsign"],
            )

    return closed


def _infer_missed_departures(conn, logger) -> int:
    """
    Find aircraft that are airborne (recent position with on_ground=False)
    but have no open flight. These are missed departures — the ground→air
    transition was not captured (e.g. poor ADS-B coverage at departure airport).

    High confidence (altitude < 3000m or within 100km of last arrival):
        Create pending flight with last arrival airport as departure.
    Low confidence (at cruise altitude, far from last airport):
        Create pending flight with unknown departure, flagged for review.

    Returns the number of inferred departures.
    """
    # Get all aircraft with an open flight — we skip these
    open_flights = db.get_open_flights(conn)
    has_open_flight = {f["icao24"].strip() for f in open_flights}

    # Get all active aircraft and their latest positions
    active = db.get_active_aircraft(conn)
    all_icao24s = [a["icao24"] for a in active]
    latest = db.get_latest_positions(conn, all_icao24s)

    # Find aircraft that are airborne but have no open flight.
    # Only consider positions from the last hour to avoid acting on stale data.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
    airborne_no_flight = []
    for icao24, pos in latest.items():
        icao24_stripped = icao24.strip()
        if icao24_stripped in has_open_flight:
            continue
        if pos["captured_at"] < cutoff:
            continue
        ground = _is_on_ground(pos)
        if ground is False:
            airborne_no_flight.append((icao24_stripped, pos))

    if not airborne_no_flight:
        return 0

    logger.info(f"Found {len(airborne_no_flight)} aircraft airborne with no open flight")

    inferred = 0
    for icao24, pos in airborne_no_flight:
        # Look up the most recent completed flight for this aircraft
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT arrival_airport_icao, last_seen
                FROM flights
                WHERE icao24 = %s
                  AND arrival_airport_icao IS NOT NULL
                  AND arrival_airport_icao != 'UNKN'
                ORDER BY last_seen DESC
                LIMIT 1
                """,
                (icao24,),
            )
            row = cur.fetchone()

        if not row:
            # No completed flight history — can't infer departure
            logger.info(
                f"  {icao24}: airborne, no open flight, no completed flight history — "
                f"creating pending flight with unknown departure"
            )
            db.upsert_flight(conn, {
                "icao24": icao24,
                "callsign": pos["callsign"],
                "dep": None,
                "arr": None,
                "first_seen": pos["captured_at"],
                "last_seen": pos["captured_at"],
                "needs_review": True,
            })
            inferred += 1
            continue

        last_arr_icao = row[0].strip()
        last_landed = row[1]

        # Don't infer if the last flight was too long ago
        gap = pos["captured_at"] - last_landed
        max_gap = timedelta(hours=config.MISSED_DEPARTURE_MAX_GAP_H)
        if gap > max_gap:
            logger.info(
                f"  {icao24}: airborne, last landed {last_arr_icao} "
                f"{gap.total_seconds() / 3600:.0f}h ago — too old, skipping"
            )
            continue

        # Use the first position after the last landing for confidence checks.
        # By the time this runs, the latest position (pos) is at cruise altitude
        # far from the departure airport. The first position is near the airport
        # at low altitude, giving much better confidence.
        first_pos = db.get_first_position_since(conn, icao24, last_landed)
        if not first_pos:
            first_pos = pos  # fallback to latest

        # Check confidence using first_pos (near departure)
        alt = first_pos.get("altitude_m")
        low_altitude = alt is not None and alt < config.MISSED_DEPARTURE_ALTITUDE_M

        # Get distance from last arrival airport using first_pos
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT earth_distance(
                    ll_to_earth(a.latitude, a.longitude),
                    ll_to_earth(%s, %s)
                ) / 1000.0 AS dist_km
                FROM airports a
                WHERE a.icao_code = %s
                """,
                (first_pos["latitude"], first_pos["longitude"], last_arr_icao),
            )
            dist_row = cur.fetchone()

        near_airport = (
            dist_row is not None
            and dist_row[0] < config.MISSED_DEPARTURE_DISTANCE_KM
        )

        callsign = pos["callsign"] or first_pos["callsign"]

        if low_altitude or near_airport:
            # High confidence — use last arrival as departure
            logger.info(
                f"  {icao24}: inferred departure from {last_arr_icao} "
                f"(alt={alt}, near={near_airport}, gap={gap.total_seconds() / 3600:.1f}h)"
            )
            db.upsert_flight(conn, {
                "icao24": icao24,
                "callsign": callsign,
                "dep": last_arr_icao,
                "arr": None,
                "first_seen": first_pos["captured_at"],
                "last_seen": pos["captured_at"],
                "needs_review": False,
            })
        else:
            # Low confidence — unknown departure, flagged for review
            logger.info(
                f"  {icao24}: airborne far from {last_arr_icao} "
                f"(alt={alt}, dist={dist_row[0]:.0f}km, gap={gap.total_seconds() / 3600:.1f}h) "
                f"— unknown departure, flagged for review"
            )
            db.upsert_flight(conn, {
                "icao24": icao24,
                "callsign": callsign,
                "dep": None,
                "arr": None,
                "first_seen": first_pos["captured_at"],
                "last_seen": pos["captured_at"],
                "needs_review": True,
            })

        inferred += 1

    return inferred


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

    # --- Part 4: infer missed departures ---
    try:
        inferred = _infer_missed_departures(conn, logger)
    except Exception as e:
        logger.error(f"Error inferring missed departures: {e}")
        conn.rollback()
        stats["error"] += 1
        inferred = 0

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

    total = new_flights + closed + stale + inferred
    stats["ok"] = total
    stats["flights_upserted"] = total
    logger.info(
        f"Flight detector done — {new_flights} new/pending, {closed} closed, "
        f"{stale} stale, {inferred} inferred departures"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
