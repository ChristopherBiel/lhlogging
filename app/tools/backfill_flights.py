"""
One-off tool: re-evaluate pending flights using the full position history.

For each open flight (arrival_airport_icao IS NULL), walks all positions
from first_seen onward to find:
  1. The first landing → closes the original flight with the correct arrival
  2. Any subsequent departure/arrival pairs → inserts them as new flights

Uses the same velocity+altitude fallback as the regular flight detector.

Usage:
    python -m tools.backfill_flights              # dry-run (default)
    python -m tools.backfill_flights --apply       # apply changes to the DB
"""
import argparse
import sys
from collections import defaultdict

from lhlogging import db, config
from lhlogging.flight_detector import _is_on_ground
from lhlogging.utils import setup_logging


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill pending flights from position history")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    args = parser.parse_args()

    logger = setup_logging("backfill_flights")
    dry_run = not args.apply
    logger.info(f"Backfill starting ({'DRY RUN' if dry_run else 'APPLY MODE'})")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    open_flights = db.get_open_flights(conn)
    logger.info(f"Found {len(open_flights)} open flights")

    if not open_flights:
        conn.close()
        return 0

    # Group open flights by icao24 (an aircraft could have multiple open flights)
    flights_by_icao = defaultdict(list)
    for f in open_flights:
        flights_by_icao[f["icao24"].strip()].append(f)

    closed = 0
    new_flights = 0

    for icao24, flights in flights_by_icao.items():
        # Sort by first_seen to process oldest first
        flights.sort(key=lambda f: f["first_seen"])
        earliest = flights[0]["first_seen"]

        # Fetch all positions from the earliest departure onward
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT icao24, callsign, captured_at, latitude, longitude,
                       on_ground, velocity_ms, altitude_m
                FROM positions
                WHERE icao24 = %s AND captured_at >= %s
                ORDER BY captured_at
                """,
                (icao24, earliest),
            )
            rows = cur.fetchall()

        positions = [
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

        if not positions:
            logger.info(f"  {icao24}: no positions found after {earliest}")
            continue

        logger.info(f"  {icao24}: {len(positions)} positions, {len(flights)} open flight(s)")

        # Walk positions and detect transitions
        # We process each open flight: find its landing, then detect any new flights after
        for flight in flights:
            first_seen = flight["first_seen"]
            dep_icao = flight.get("departure_airport_icao")

            # Find positions after this flight's departure
            flight_positions = [p for p in positions if p["captured_at"] >= first_seen]
            if not flight_positions:
                continue

            # Walk to find the first landing
            prev = None
            prev_ground = None
            landing_pos = None

            for pos in flight_positions:
                cur_ground = _is_on_ground(pos)
                if cur_ground is None:
                    continue

                if prev is not None and prev_ground is not None:
                    if not prev_ground and cur_ground:
                        # Air → Ground: found the landing
                        landing_pos = pos
                        break

                prev = pos
                prev_ground = cur_ground

            if landing_pos:
                arr_icao = db.lookup_nearest_airport(
                    conn, landing_pos["latitude"], landing_pos["longitude"]
                )
                review = bool(dep_icao and arr_icao and dep_icao == arr_icao)

                logger.info(
                    f"    Close: {dep_icao or '?'}→{arr_icao or '?'} "
                    f"({first_seen.strftime('%m-%d %H:%M')}–"
                    f"{landing_pos['captured_at'].strftime('%m-%d %H:%M')})"
                    f"{' [fallback]' if not landing_pos['on_ground'] else ''}"
                    f"{' [review: dep==arr]' if review else ''}"
                )

                if not dry_run:
                    db.update_open_flight(
                        conn,
                        icao24,
                        first_seen,
                        landing_pos["captured_at"],
                        arr=arr_icao,
                        callsign=landing_pos["callsign"] or flight.get("callsign"),
                        needs_review=review,
                    )
                closed += 1

                # Now look for subsequent departures after this landing
                after_landing = [
                    p for p in flight_positions
                    if p["captured_at"] > landing_pos["captured_at"]
                ]

                departure = None
                prev = None
                prev_ground = None

                for pos in after_landing:
                    cur_ground = _is_on_ground(pos)
                    if cur_ground is None:
                        continue

                    if prev is not None and prev_ground is not None:
                        # Ground → Air: new departure
                        if prev_ground and not cur_ground:
                            departure = {
                                "first_seen": prev["captured_at"],
                                "dep_lat": prev["latitude"],
                                "dep_lon": prev["longitude"],
                                "callsign": pos["callsign"] or prev["callsign"],
                            }

                        # Air → Ground: complete a new flight
                        elif not prev_ground and cur_ground and departure:
                            new_dep = db.lookup_nearest_airport(
                                conn, departure["dep_lat"], departure["dep_lon"]
                            )
                            new_arr = db.lookup_nearest_airport(
                                conn, pos["latitude"], pos["longitude"]
                            )
                            new_review = bool(new_dep and new_arr and new_dep == new_arr)

                            logger.info(
                                f"    New flight: {new_dep or '?'}→{new_arr or '?'} "
                                f"({departure['first_seen'].strftime('%m-%d %H:%M')}–"
                                f"{pos['captured_at'].strftime('%m-%d %H:%M')})"
                                f"{' [review: dep==arr]' if new_review else ''}"
                            )

                            if not dry_run:
                                db.upsert_flight(conn, {
                                    "icao24": icao24,
                                    "callsign": departure["callsign"] or pos["callsign"],
                                    "dep": new_dep,
                                    "arr": new_arr,
                                    "first_seen": departure["first_seen"],
                                    "last_seen": pos["captured_at"],
                                    "needs_review": new_review,
                                })
                            new_flights += 1
                            departure = None

                    prev = pos
                    prev_ground = cur_ground

                # If there's an open departure after the landing, insert as pending
                if departure:
                    new_dep = db.lookup_nearest_airport(
                        conn, departure["dep_lat"], departure["dep_lon"]
                    )
                    latest = after_landing[-1]

                    logger.info(
                        f"    New pending: {new_dep or '?'}→? "
                        f"(since {departure['first_seen'].strftime('%m-%d %H:%M')})"
                    )

                    if not dry_run:
                        db.upsert_flight(conn, {
                            "icao24": icao24,
                            "callsign": departure["callsign"],
                            "dep": new_dep,
                            "arr": None,
                            "first_seen": departure["first_seen"],
                            "last_seen": latest["captured_at"],
                            "needs_review": False,
                        })
                    new_flights += 1

            else:
                logger.info(
                    f"    No landing found for {dep_icao or '?'}→? "
                    f"(since {first_seen.strftime('%m-%d %H:%M')}) — still airborne?"
                )

    if not dry_run:
        conn.commit()

    logger.info(
        f"Backfill {'would apply' if dry_run else 'applied'}: "
        f"{closed} closed, {new_flights} new flights"
    )
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
