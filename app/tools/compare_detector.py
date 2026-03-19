"""
Compare old vs new flight detection for specific aircraft.

Replays all position data for the given aircraft through the new session-based
flight detector and prints the results side-by-side with existing flight records.

Does NOT modify the database.

Usage:
    python -m tools.compare_detector 3c4dc1              # single aircraft
    python -m tools.compare_detector 3c4dc1 3c4b26 3c6587  # multiple
    python -m tools.compare_detector --since 2026-03-15 3c4dc1  # limit time range
"""
import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from lhlogging import config, db
from lhlogging.flight_detector import (
    _split_sessions,
    _detect_landing,
    _detect_departure,
    _get_session_callsign,
    _is_on_ground,
    _scan_for_departure,
    _scan_for_arrival_after,
    _callsigns_match,
)
from lhlogging.utils import setup_logging


def _get_all_positions(conn, icao24: str, since: datetime) -> list[dict]:
    """Fetch all positions for an aircraft since a given time."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT icao24, callsign, captured_at, latitude, longitude,
                   on_ground, velocity_ms, altitude_m
            FROM positions
            WHERE icao24 = %s AND captured_at >= %s
            ORDER BY captured_at
            """,
            (icao24, since),
        )
        rows = cur.fetchall()
    return [
        {
            "icao24": r[0],
            "callsign": r[1],
            "captured_at": r[2],
            "latitude": r[3],
            "longitude": r[4],
            "on_ground": r[5],
            "velocity_ms": r[6],
            "altitude_m": r[7],
        }
        for r in rows
    ]


def _get_existing_flights(conn, icao24: str, since: datetime) -> list[dict]:
    """Fetch all existing flight records for an aircraft since a given time."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT icao24, callsign, departure_airport_icao, arrival_airport_icao,
                   first_seen, last_seen, needs_review
            FROM flights
            WHERE icao24 = %s AND first_seen >= %s
            ORDER BY first_seen
            """,
            (icao24, since),
        )
        rows = cur.fetchall()
    return [
        {
            "icao24": r[0],
            "callsign": r[1],
            "departure_airport_icao": r[2],
            "arrival_airport_icao": r[3],
            "first_seen": r[4],
            "last_seen": r[5],
            "needs_review": r[6],
        }
        for r in rows
    ]


def _simulate_detector(conn, icao24: str, positions: list[dict], logger) -> list[dict]:
    """
    Run the new session-based detection logic on all positions for one aircraft.

    Returns a list of detected flights (dicts with dep, arr, first_seen, last_seen, etc).
    Does NOT write to the database.
    """
    if not positions:
        return []

    # Split ALL positions into sessions using the full timeline
    gap_threshold = timedelta(minutes=2 * config.STATE_POLL_INTERVAL_MINUTES)
    sessions: list[list[dict]] = [[positions[0]]]
    for i in range(1, len(positions)):
        gap = positions[i]["captured_at"] - positions[i - 1]["captured_at"]
        if gap > gap_threshold:
            sessions.append([positions[i]])
        else:
            sessions[-1].append(positions[i])

    logger.info(f"  {icao24}: {len(positions)} positions, {len(sessions)} sessions")

    flights = []
    open_flight = None
    prev_session = None

    for session in sessions:
        if not session:
            continue

        first_pos = session[0]
        session_cs = _get_session_callsign(session)
        starts_on_ground = _is_on_ground(first_pos)

        # Evaluate previous session's tail for landing before classifying new session
        if open_flight and prev_session:
            tail = prev_session[-5:]
            landing_info = _detect_landing(tail, conn)
            if landing_info:
                arr_icao = db.lookup_nearest_airport(
                    conn, landing_info["lat"], landing_info["lon"]
                )
                dep_icao = open_flight.get("departure_airport_icao")
                review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                open_flight["arrival_airport_icao"] = arr_icao
                open_flight["last_seen"] = landing_info["captured_at"]
                open_flight["needs_review"] = review
                open_flight["case"] += "+land"
                flights.append(open_flight)
                open_flight = None

        if open_flight is None:
            if starts_on_ground is True:
                # CASE 1: scan for departure
                dep_result = _scan_for_departure(session)
                if dep_result:
                    dep_icao = db.lookup_nearest_airport(
                        conn, dep_result["lat"], dep_result["lon"]
                    )
                    open_flight = {
                        "callsign": session_cs,
                        "departure_airport_icao": dep_icao,
                        "first_seen": dep_result["captured_at"],
                        "last_seen": session[-1]["captured_at"],
                        "needs_review": False,
                        "case": "1",
                    }

                    arr_result = _scan_for_arrival_after(session, dep_result["captured_at"])
                    if arr_result:
                        arr_icao = db.lookup_nearest_airport(
                            conn, arr_result["lat"], arr_result["lon"]
                        )
                        review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                        open_flight["arrival_airport_icao"] = arr_icao
                        open_flight["last_seen"] = arr_result["captured_at"]
                        open_flight["needs_review"] = review
                        flights.append(open_flight)
                        open_flight = None

            elif starts_on_ground is False:
                # CASE 2: missed departure
                dep_info = _detect_departure(session[:5])
                dep_icao = None
                review = True
                if dep_info:
                    dep_icao = db.lookup_nearest_airport(
                        conn, dep_info["lat"], dep_info["lon"]
                    )
                    if dep_icao:
                        review = False

                open_flight = {
                    "callsign": session_cs,
                    "departure_airport_icao": dep_icao,
                    "first_seen": session[0]["captured_at"],
                    "last_seen": session[-1]["captured_at"],
                    "needs_review": review,
                    "case": "2",
                }

                arr_result = _scan_for_arrival_after(session, session[0]["captured_at"])
                if arr_result:
                    arr_icao = db.lookup_nearest_airport(
                        conn, arr_result["lat"], arr_result["lon"]
                    )
                    review_arr = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                    open_flight["arrival_airport_icao"] = arr_icao
                    open_flight["last_seen"] = arr_result["captured_at"]
                    open_flight["needs_review"] = review or review_arr
                    flights.append(open_flight)
                    open_flight = None

        else:
            flight_cs = (open_flight.get("callsign") or "").strip() or None
            cs_match = _callsigns_match(flight_cs, session_cs)

            if starts_on_ground is True:
                # CASE 3/4: landing
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
                    arr_icao = db.lookup_nearest_airport(
                        conn, first_pos["latitude"], first_pos["longitude"]
                    )
                    last_seen = first_pos["captured_at"]

                dep_icao = open_flight.get("departure_airport_icao")
                review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                open_flight["arrival_airport_icao"] = arr_icao
                open_flight["last_seen"] = last_seen
                open_flight["needs_review"] = review
                open_flight["case"] += "+3/4"
                flights.append(open_flight)
                open_flight = None

                # Scan for new departure in this session
                dep_result = _scan_for_departure(session)
                if dep_result:
                    new_dep_icao = db.lookup_nearest_airport(
                        conn, dep_result["lat"], dep_result["lon"]
                    )
                    new_cs = _get_session_callsign(
                        [p for p in session if p["captured_at"] >= dep_result["captured_at"]]
                    ) or session_cs
                    open_flight = {
                        "callsign": new_cs,
                        "departure_airport_icao": new_dep_icao,
                        "first_seen": dep_result["captured_at"],
                        "last_seen": session[-1]["captured_at"],
                        "needs_review": False,
                        "case": "1(after3/4)",
                    }

                    arr_result = _scan_for_arrival_after(session, dep_result["captured_at"])
                    if arr_result:
                        arr_icao2 = db.lookup_nearest_airport(
                            conn, arr_result["lat"], arr_result["lon"]
                        )
                        review2 = bool(new_dep_icao and arr_icao2 and new_dep_icao == arr_icao2)
                        open_flight["arrival_airport_icao"] = arr_icao2
                        open_flight["last_seen"] = arr_result["captured_at"]
                        open_flight["needs_review"] = review2
                        flights.append(open_flight)
                        open_flight = None

            elif starts_on_ground is False:
                if cs_match:
                    # CASE 5: coverage gap
                    open_flight["last_seen"] = session[-1]["captured_at"]

                    arr_result = _scan_for_arrival_after(session, session[0]["captured_at"])
                    if arr_result:
                        arr_icao = db.lookup_nearest_airport(
                            conn, arr_result["lat"], arr_result["lon"]
                        )
                        dep_icao = open_flight.get("departure_airport_icao")
                        review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
                        open_flight["arrival_airport_icao"] = arr_icao
                        open_flight["last_seen"] = arr_result["captured_at"]
                        open_flight["needs_review"] = review
                        open_flight["case"] += "+5"
                        flights.append(open_flight)
                        open_flight = None
                else:
                    # CASE 6: different callsign
                    open_flight["arrival_airport_icao"] = "UNKN"
                    open_flight["last_seen"] = session[0]["captured_at"]
                    open_flight["needs_review"] = True
                    open_flight["case"] += "+6"
                    flights.append(open_flight)

                    open_flight = {
                        "callsign": session_cs,
                        "departure_airport_icao": None,
                        "first_seen": session[0]["captured_at"],
                        "last_seen": session[-1]["captured_at"],
                        "needs_review": True,
                        "case": "6",
                    }

                    arr_result = _scan_for_arrival_after(session, session[0]["captured_at"])
                    if arr_result:
                        arr_icao = db.lookup_nearest_airport(
                            conn, arr_result["lat"], arr_result["lon"]
                        )
                        open_flight["arrival_airport_icao"] = arr_icao
                        open_flight["last_seen"] = arr_result["captured_at"]
                        flights.append(open_flight)
                        open_flight = None

        prev_session = session

    # Evaluate tail of last session
    if open_flight and sessions and sessions[-1]:
        tail = sessions[-1][-5:] if len(sessions[-1]) >= 5 else sessions[-1]
        landing_info = _detect_landing(tail, conn)
        if landing_info:
            arr_icao = db.lookup_nearest_airport(
                conn, landing_info["lat"], landing_info["lon"]
            )
            dep_icao = open_flight.get("departure_airport_icao")
            review = bool(dep_icao and arr_icao and dep_icao == arr_icao)
            open_flight["arrival_airport_icao"] = arr_icao
            open_flight["last_seen"] = landing_info["captured_at"]
            open_flight["needs_review"] = review
            flights.append(open_flight)
        else:
            open_flight["arrival_airport_icao"] = None
            flights.append(open_flight)

    return flights


def _fmt_time(dt: datetime | None) -> str:
    if dt is None:
        return "           —"
    return dt.strftime("%m-%d %H:%M")


def _fmt_flight(f: dict, show_case: bool = False) -> str:
    dep = (f.get("departure_airport_icao") or "????").ljust(4)
    arr = (f.get("arrival_airport_icao") or " ?? ").ljust(4)
    cs = (f.get("callsign") or "?").ljust(7)
    first = _fmt_time(f.get("first_seen"))
    last = _fmt_time(f.get("last_seen"))
    review = " [R]" if f.get("needs_review") else "    "
    case = f" (case {f['case']})" if show_case and f.get("case") else ""
    return f"  {cs} {dep}→{arr} {first} – {last}{review}{case}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare old vs new flight detection for specific aircraft"
    )
    parser.add_argument("icao24s", nargs="+", help="ICAO24 hex codes to analyze")
    parser.add_argument(
        "--since",
        default=None,
        help="Start date (YYYY-MM-DD), default: 30 days ago",
    )
    args = parser.parse_args()

    logger = setup_logging("compare_detector")

    since = (
        datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if args.since
        else datetime.now(timezone.utc) - timedelta(days=30)
    )

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    for icao24 in args.icao24s:
        icao24 = icao24.strip().lower()
        print(f"\n{'='*72}")
        print(f"  Aircraft: {icao24}")
        print(f"{'='*72}")

        # Get existing flight records
        existing = _get_existing_flights(conn, icao24, since)

        # Get all positions and simulate
        positions = _get_all_positions(conn, icao24, since)
        simulated = _simulate_detector(conn, icao24, positions, logger)

        print(f"\n  EXISTING FLIGHTS ({len(existing)}):")
        if not existing:
            print("    (none)")
        for f in existing:
            print(_fmt_flight(f))

        print(f"\n  NEW DETECTOR ({len(simulated)}):")
        if not simulated:
            print("    (none)")
        for f in simulated:
            print(_fmt_flight(f, show_case=True))

        # Highlight differences
        print(f"\n  COMPARISON:")
        old_count = len(existing)
        new_count = len(simulated)
        if old_count != new_count:
            print(f"    Flight count: {old_count} existing vs {new_count} new")

        old_review = sum(1 for f in existing if f.get("needs_review"))
        new_review = sum(1 for f in simulated if f.get("needs_review"))
        print(f"    Needs review: {old_review} existing vs {new_review} new")

        old_dep_arr_same = sum(
            1 for f in existing
            if f.get("departure_airport_icao") and f.get("arrival_airport_icao")
            and f["departure_airport_icao"] == f["arrival_airport_icao"]
            and f["arrival_airport_icao"] != "UNKN"
        )
        new_dep_arr_same = sum(
            1 for f in simulated
            if f.get("departure_airport_icao") and f.get("arrival_airport_icao")
            and f["departure_airport_icao"] == f["arrival_airport_icao"]
            and f["arrival_airport_icao"] != "UNKN"
        )
        if old_dep_arr_same or new_dep_arr_same:
            print(f"    dep==arr flights: {old_dep_arr_same} existing vs {new_dep_arr_same} new")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
