"""
Flight detector — runs every 30 minutes via cron.
Reads position snapshots from the positions table and detects flights
using a session-based approach.

A session is a contiguous sequence of ADS-B positions for one aircraft
with no gaps (a gap of more than 2x the poll interval ends a session).
At each session boundary the detector evaluates:
  - Session end: did the aircraft land? (altitude, velocity, proximity, on_ground)
  - Session start: is there an open flight? does this look like a departure?

Six cases at session start:
  1. No open flight, on ground         → scan session for departure
  2. No open flight, airborne          → infer missed departure
  3. Open flight, on ground, same cs   → landing detected, close flight
  4. Open flight, on ground, diff cs   → landing detected, close flight
  5. Open flight, airborne, same cs    → coverage gap, continue flight
  6. Open flight, airborne, diff cs    → close old (UNKN), open new (UNKN)

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


def _split_sessions(positions: list[dict]) -> list[list[dict]]:
    """Split a single aircraft's positions into sessions based on time gaps.

    A session boundary exists when the gap between consecutive positions
    exceeds 2x the poll interval, indicating at least one missed sample.
    """
    if not positions:
        return []

    gap_threshold = timedelta(minutes=2 * config.STATE_POLL_INTERVAL_MINUTES)
    sessions: list[list[dict]] = [[positions[0]]]

    for i in range(1, len(positions)):
        gap = positions[i]["captured_at"] - positions[i - 1]["captured_at"]
        if gap > gap_threshold:
            sessions.append([positions[i]])
        else:
            sessions[-1].append(positions[i])

    return sessions


def _detect_landing(positions: list[dict], conn) -> dict | None:
    """Analyze the last ~5 positions of a session for landing indicators.

    Returns {"lat": ..., "lon": ..., "captured_at": ...} if landing detected,
    or None if the aircraft appears still airborne.
    """
    if not positions:
        return None

    last = positions[-1]

    # Direct on-ground detection
    if _is_on_ground(last):
        return {
            "lat": last["latitude"],
            "lon": last["longitude"],
            "captured_at": last["captured_at"],
        }

    # Trend-based: descent + deceleration ending at low altitude near an airport
    if len(positions) >= 2:
        altitudes = [p["altitude_m"] for p in positions if p.get("altitude_m") is not None]
        velocities = [p["velocity_ms"] for p in positions if p.get("velocity_ms") is not None]

        descending = len(altitudes) >= 2 and altitudes[-1] < altitudes[0]
        decelerating = len(velocities) >= 2 and velocities[-1] < velocities[0]
        low_altitude = (
            last.get("altitude_m") is not None
            and last["altitude_m"] < config.PROXIMITY_LANDING_ALTITUDE_M
        )

        if descending and low_altitude:
            airport = db.lookup_nearest_airport(
                conn, last["latitude"], last["longitude"],
                max_km=config.PROXIMITY_LANDING_RADIUS_KM,
            )
            if airport and decelerating:
                return {
                    "lat": last["latitude"],
                    "lon": last["longitude"],
                    "captured_at": last["captured_at"],
                }

    return None


def _detect_departure(positions: list[dict]) -> dict | None:
    """Analyze the first ~5 positions of a session for departure indicators.

    Returns {"lat": ..., "lon": ..., "captured_at": ...} if a departure
    pattern is detected, or None if the aircraft was already at cruise.
    """
    if not positions:
        return None

    first = positions[0]

    # Direct on-ground detection — aircraft is still at the airport
    if _is_on_ground(first):
        return {
            "lat": first["latitude"],
            "lon": first["longitude"],
            "captured_at": first["captured_at"],
        }

    # Trend-based: climbing + accelerating from low altitude
    if len(positions) >= 2:
        altitudes = [p["altitude_m"] for p in positions if p.get("altitude_m") is not None]

        climbing = len(altitudes) >= 2 and altitudes[-1] > altitudes[0]
        low_start = (
            first.get("altitude_m") is not None
            and first["altitude_m"] < config.MISSED_DEPARTURE_ALTITUDE_M
        )

        if climbing and low_start:
            return {
                "lat": first["latitude"],
                "lon": first["longitude"],
                "captured_at": first["captured_at"],
            }

    return None


def _get_session_callsign(session: list[dict]) -> str | None:
    """Return the first non-null callsign in a session, stripped."""
    for pos in session:
        cs = pos.get("callsign")
        if cs and cs.strip():
            return cs.strip()
    return None


def _callsigns_match(cs_a: str | None, cs_b: str | None) -> bool:
    """Compare two callsigns. If either is None, treat as matching (not enough info)."""
    if cs_a is None or cs_b is None:
        return True
    return cs_a == cs_b


def _scan_for_departure(session: list[dict]) -> dict | None:
    """Scan a session for a ground→air transition. Returns departure info or None."""
    prev_ground = None
    prev_pos = None
    for pos in session:
        cur_ground = _is_on_ground(pos)
        if cur_ground is None:
            continue
        if prev_ground is True and cur_ground is False:
            return {
                "lat": prev_pos["latitude"],
                "lon": prev_pos["longitude"],
                "captured_at": prev_pos["captured_at"],
            }
        prev_ground = cur_ground
        prev_pos = pos
    return None


def _scan_for_arrival_after(session: list[dict], after: datetime) -> dict | None:
    """Scan a session for an air→ground transition after the given timestamp."""
    prev_ground = None
    for pos in session:
        cur_ground = _is_on_ground(pos)
        if cur_ground is None:
            continue
        if pos["captured_at"] <= after:
            prev_ground = cur_ground
            continue
        if prev_ground is False and cur_ground is True:
            return {
                "lat": pos["latitude"],
                "lon": pos["longitude"],
                "captured_at": pos["captured_at"],
            }
        prev_ground = cur_ground
    return None


def _make_open_flight(icao24, callsign, dep, first_seen, last_seen) -> dict:
    """Create a normalized open flight dict for tracking within _process_aircraft."""
    return {
        "icao24": icao24,
        "callsign": callsign,
        "departure_airport_icao": dep,
        "first_seen": first_seen,
    }


def _open_new_flight(conn, icao24, callsign, dep, first_seen, last_seen,
                     needs_review, logger) -> dict:
    """Insert a new flight and return a normalized open flight dict."""
    db.upsert_flight(conn, {
        "icao24": icao24,
        "callsign": callsign,
        "dep": dep,
        "arr": None,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "needs_review": needs_review,
    })
    logger.info(
        f"Opened flight {icao24} from {dep or 'UNKNOWN'} "
        f"(cs={callsign or '?'}, review={needs_review})"
    )
    return _make_open_flight(icao24, callsign, dep, first_seen, last_seen)


def _close_flight(conn, open_flight, last_seen, arr, callsign, needs_review, logger):
    """Close an open flight with the given arrival info."""
    db.update_open_flight(
        conn,
        open_flight["icao24"],
        open_flight["first_seen"],
        last_seen,
        arr=arr,
        callsign=callsign,
        needs_review=needs_review,
    )
    dep = open_flight.get("departure_airport_icao") or "?"
    logger.info(
        f"Closed flight {open_flight['icao24']} {dep}→{arr or '?'} "
        f"(cs={callsign or '?'}, review={needs_review})"
    )


def _process_aircraft(
    conn,
    icao24: str,
    sessions: list[list[dict]],
    open_flight: dict | None,
    last_completed: dict | None,
    logger,
) -> int:
    """
    Unified session-walk for one aircraft.

    Walks sessions in chronological order. At each session boundary,
    evaluates the session end (landing?) and session start (which of the
    6 cases applies?). Also scans within sessions for transitions.

    Returns the number of flight records created or closed.
    """
    count = 0
    prev_session = None

    for session in sessions:
        if not session:
            continue

        first_pos = session[0]
        session_cs = _get_session_callsign(session)
        starts_on_ground = _is_on_ground(first_pos)

        # --- Evaluate session end (previous session's tail) ---
        # Before classifying the new session start, check if the previous
        # session's tail indicates a landing. This must happen first so that
        # session-start classification sees the correct open_flight state.
        if open_flight and prev_session:
            tail = prev_session[-5:]
            landing_info = _detect_landing(tail, conn)
            if landing_info:
                arr_icao = db.lookup_nearest_airport(
                    conn, landing_info["lat"], landing_info["lon"]
                )
                dep_icao = open_flight.get("departure_airport_icao")
                review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                flight_cs = (open_flight.get("callsign") or "").strip() or None
                _close_flight(
                    conn, open_flight, landing_info["captured_at"],
                    arr=arr_icao, callsign=flight_cs,
                    needs_review=review, logger=logger,
                )
                count += 1
                open_flight = None

        # --- Evaluate session start ---

        if open_flight is None:
            # Skip sessions already covered by a recently completed flight.
            # This prevents reprocessing positions from overlapping lookback windows.
            if (
                last_completed
                and last_completed["last_seen"] >= first_pos["captured_at"]
            ):
                continue

            if starts_on_ground is True:
                # CASE 1: No open flight, on ground.
                # Scan session for a ground→air departure transition.
                dep_result = _scan_for_departure(session)
                if dep_result:
                    dep_icao = db.lookup_nearest_airport(
                        conn, dep_result["lat"], dep_result["lon"]
                    )
                    open_flight = _open_new_flight(
                        conn, icao24, session_cs, dep_icao,
                        dep_result["captured_at"], session[-1]["captured_at"],
                        needs_review=False, logger=logger,
                    )
                    count += 1

                    # Check if the session also contains a landing (short flight)
                    arr_result = _scan_for_arrival_after(
                        session, dep_result["captured_at"]
                    )
                    if arr_result:
                        arr_icao = db.lookup_nearest_airport(
                            conn, arr_result["lat"], arr_result["lon"]
                        )
                        review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                        _close_flight(
                            conn, open_flight, arr_result["captured_at"],
                            arr=arr_icao, callsign=session_cs,
                            needs_review=review, logger=logger,
                        )
                        open_flight = None

            elif starts_on_ground is False:
                # CASE 2: No open flight, airborne. Missed departure.
                dep_info = _detect_departure(session[:5])
                dep_icao = None
                review = True
                if dep_info:
                    dep_icao = db.lookup_nearest_airport(
                        conn, dep_info["lat"], dep_info["lon"]
                    )
                    if dep_icao:
                        review = False

                open_flight = _open_new_flight(
                    conn, icao24, session_cs, dep_icao,
                    session[0]["captured_at"], session[-1]["captured_at"],
                    needs_review=review, logger=logger,
                )
                count += 1

                # Check if session also contains a landing
                arr_result = _scan_for_arrival_after(
                    session, session[0]["captured_at"]
                )
                if arr_result:
                    arr_icao = db.lookup_nearest_airport(
                        conn, arr_result["lat"], arr_result["lon"]
                    )
                    review_arr = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                    _close_flight(
                        conn, open_flight, arr_result["captured_at"],
                        arr=arr_icao, callsign=session_cs,
                        needs_review=review or review_arr, logger=logger,
                    )
                    open_flight = None

            # starts_on_ground is None (indeterminate) — skip

        else:
            # We have an open flight
            flight_cs = (open_flight.get("callsign") or "").strip() or None
            cs_match = _callsigns_match(flight_cs, session_cs)

            if starts_on_ground is True:
                # CASE 3 (same cs) / CASE 4 (diff cs): Landing detected.
                # Use the last ~5 positions before this session for arrival
                # airport resolution (descent trend).
                landing_positions = db.get_positions_for_aircraft_before(
                    conn, icao24, first_pos["captured_at"], limit=5
                )
                landing_info = _detect_landing(landing_positions, conn)

                if landing_info:
                    arr_icao = db.lookup_nearest_airport(
                        conn, landing_info["lat"], landing_info["lon"]
                    )
                    last_seen = landing_info["captured_at"]
                else:
                    # Fallback: aircraft is on ground now, use current position
                    arr_icao = db.lookup_nearest_airport(
                        conn, first_pos["latitude"], first_pos["longitude"]
                    )
                    last_seen = first_pos["captured_at"]

                dep_icao = open_flight.get("departure_airport_icao")
                review = bool(dep_icao and arr_icao and dep_icao == arr_icao)

                _close_flight(
                    conn, open_flight, last_seen,
                    arr=arr_icao, callsign=session_cs or flight_cs,
                    needs_review=review, logger=logger,
                )
                count += 1
                open_flight = None

                # After closing, scan this session for a new departure
                dep_result = _scan_for_departure(session)
                if dep_result:
                    new_dep_icao = db.lookup_nearest_airport(
                        conn, dep_result["lat"], dep_result["lon"]
                    )
                    new_cs = _get_session_callsign(
                        [p for p in session if p["captured_at"] >= dep_result["captured_at"]]
                    ) or session_cs
                    open_flight = _open_new_flight(
                        conn, icao24, new_cs, new_dep_icao,
                        dep_result["captured_at"], session[-1]["captured_at"],
                        needs_review=False, logger=logger,
                    )
                    count += 1

                    # Check for arrival within the same session
                    arr_result = _scan_for_arrival_after(
                        session, dep_result["captured_at"]
                    )
                    if arr_result:
                        arr_icao2 = db.lookup_nearest_airport(
                            conn, arr_result["lat"], arr_result["lon"]
                        )
                        review2 = bool(
                            new_dep_icao and arr_icao2 and new_dep_icao == arr_icao2
                        )
                        _close_flight(
                            conn, open_flight, arr_result["captured_at"],
                            arr=arr_icao2, callsign=new_cs,
                            needs_review=review2, logger=logger,
                        )
                        open_flight = None

            elif starts_on_ground is False:
                if cs_match:
                    # CASE 5: Coverage gap, same callsign. Continue flight.
                    db.update_open_flight(
                        conn, icao24, open_flight["first_seen"],
                        session[-1]["captured_at"],
                        callsign=session_cs or flight_cs,
                    )

                    # Scan session for a landing
                    arr_result = _scan_for_arrival_after(
                        session, session[0]["captured_at"]
                    )
                    if arr_result:
                        arr_icao = db.lookup_nearest_airport(
                            conn, arr_result["lat"], arr_result["lon"]
                        )
                        dep_icao = open_flight.get("departure_airport_icao")
                        review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                        _close_flight(
                            conn, open_flight, arr_result["captured_at"],
                            arr=arr_icao, callsign=session_cs or flight_cs,
                            needs_review=review, logger=logger,
                        )
                        count += 1
                        open_flight = None

                else:
                    # CASE 6: Different callsign, airborne. Close old, open new.
                    _close_flight(
                        conn, open_flight, session[0]["captured_at"],
                        arr="UNKN", callsign=flight_cs,
                        needs_review=True, logger=logger,
                    )
                    count += 1

                    open_flight = _open_new_flight(
                        conn, icao24, session_cs, None,
                        session[0]["captured_at"], session[-1]["captured_at"],
                        needs_review=True, logger=logger,
                    )
                    count += 1

                    # Scan session for a landing
                    arr_result = _scan_for_arrival_after(
                        session, session[0]["captured_at"]
                    )
                    if arr_result:
                        arr_icao = db.lookup_nearest_airport(
                            conn, arr_result["lat"], arr_result["lon"]
                        )
                        _close_flight(
                            conn, open_flight, arr_result["captured_at"],
                            arr=arr_icao, callsign=session_cs,
                            needs_review=True, logger=logger,
                        )
                        open_flight = None

            # starts_on_ground is None — update last_seen conservatively
            elif starts_on_ground is None and open_flight:
                db.update_open_flight(
                    conn, icao24, open_flight["first_seen"],
                    session[-1]["captured_at"],
                    callsign=session_cs or flight_cs,
                )

        prev_session = session

    # After all sessions: if flight is still open, update last_seen to latest position
    if open_flight and sessions and sessions[-1]:
        latest = sessions[-1][-1]
        flight_cs = (open_flight.get("callsign") or "").strip() or None
        latest_cs = _get_session_callsign(sessions[-1])

        # Evaluate the end of the last session for landing
        tail = sessions[-1][-5:] if len(sessions[-1]) >= 5 else sessions[-1]
        landing_info = _detect_landing(tail, conn)
        if landing_info:
            arr_icao = db.lookup_nearest_airport(
                conn, landing_info["lat"], landing_info["lon"]
            )
            dep_icao = open_flight.get("departure_airport_icao")
            review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
            _close_flight(
                conn, open_flight, landing_info["captured_at"],
                arr=arr_icao, callsign=latest_cs or flight_cs,
                needs_review=review, logger=logger,
            )
            count += 1
        else:
            db.update_open_flight(
                conn, icao24, open_flight["first_seen"],
                latest["captured_at"],
                callsign=latest_cs or flight_cs,
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

    # Load positions in the lookback window
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

    # Group by aircraft
    grouped: dict[str, list[dict]] = defaultdict(list)
    for p in positions:
        grouped[p["icao24"].strip()].append(p)

    stats["aircraft_total"] = len(grouped)
    logger.info(
        f"Loaded {len(positions)} positions for {len(grouped)} aircraft "
        f"(lookback {config.FLIGHT_DETECT_LOOKBACK_MINUTES}m)"
    )

    # Load all open flights, indexed by icao24
    open_flights_list = db.get_open_flights(conn)
    open_flights_map = {f["icao24"].strip(): f for f in open_flights_list}

    # Load last completed flight per active aircraft (to skip already-processed sessions)
    active_icao24s = list(grouped.keys())
    last_completed_map = db.get_last_completed_flights(conn, active_icao24s)

    # Process each active aircraft
    total = 0
    for icao24, acft_positions in grouped.items():
        sessions = _split_sessions(acft_positions)
        open_flight = open_flights_map.get(icao24)
        last_completed = last_completed_map.get(icao24)
        try:
            n = _process_aircraft(
                conn, icao24, sessions, open_flight, last_completed, logger
            )
            total += n
        except Exception as e:
            logger.error(f"Error processing {icao24}: {e}")
            conn.rollback()
            stats["error"] += 1

    # Close stale flights (open > 24h)
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

    stats["ok"] = total + stale
    stats["flights_upserted"] = total + stale
    logger.info(
        f"Flight detector done — {total} flights processed, {stale} stale closed"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
