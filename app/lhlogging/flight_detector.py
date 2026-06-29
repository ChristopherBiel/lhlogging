"""
Flight detector — runs every 30 minutes via cron.
Reads position snapshots from the positions table and detects flights
using a session-based approach.

The case/state-machine logic lives in lhlogging.detector_core (shared, pure,
DB-free) so it is exercised identically by the offline replay harness
(tools/detector_replay.py). This module is the production wiring: it loads
positions, provides a DB-backed store + nearest-airport lookup, runs the core
per aircraft over the rolling lookback window, and closes stale flights.

A session is a contiguous sequence of ADS-B positions for one aircraft with no
gaps (a gap of more than 4x the poll interval ends a session). See
detector_core.process_window for the six session-start cases and the
robustness guards (configured via config.py / env).

Usage:
    python -m lhlogging.flight_detector
"""
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from lhlogging import config, db
from lhlogging import detector_core as core
from lhlogging.utils import setup_logging


def _build_config() -> core.DetectorConfig:
    """Build the detector core config from env-backed config.py values."""
    return core.DetectorConfig(
        state_poll_interval_min=config.STATE_POLL_INTERVAL_MINUTES,
        landing_velocity_threshold_ms=config.LANDING_VELOCITY_THRESHOLD_MS,
        landing_altitude_threshold_m=config.LANDING_ALTITUDE_THRESHOLD_M,
        missed_departure_altitude_m=config.MISSED_DEPARTURE_ALTITUDE_M,
        missed_departure_max_gap_h=config.MISSED_DEPARTURE_MAX_GAP_H,
        airport_lookup_radius_km=config.AIRPORT_LOOKUP_RADIUS_KM,
        proximity_landing_altitude_m=config.PROXIMITY_LANDING_ALTITUDE_M,
        proximity_landing_radius_km=config.PROXIMITY_LANDING_RADIUS_KM,
        onground_max_speed_ms=config.ONGROUND_MAX_SPEED_MS,
        onground_max_altitude_m=config.ONGROUND_MAX_ALTITUDE_M,
        landing_min_consecutive=config.LANDING_MIN_CONSECUTIVE,
        missed_departure_snap=config.MISSED_DEPARTURE_SNAP,
        scan_arrival_max_km=config.SCAN_ARRIVAL_MAX_KM,
        min_turnaround_min=config.MIN_TURNAROUND_MIN,
    )


# Default config for module-level helpers (env is read at import time).
_DEFAULT_CFG = _build_config()


def _is_on_ground(pos: dict) -> bool | None:
    """Back-compat shim (app/tools/backfill_flights.py) — delegates to the core."""
    return core.is_on_ground(pos, _DEFAULT_CFG)


class _DbStore:
    """The flights-table store the detector core writes through (DB-backed).

    Mirrors db.upsert_flight / db.update_open_flight / get_positions_for_aircraft_before
    and preserves the operational "Opened/Closed flight" logging.
    """

    def __init__(self, conn, icao24, logger):
        self.conn = conn
        self.icao24 = icao24
        self.logger = logger
        self.count = 0

    def positions_before(self, before, limit):
        return db.get_positions_for_aircraft_before(self.conn, self.icao24, before, limit)

    def upsert(self, callsign, dep, arr, first_seen, last_seen, needs_review, origin=""):
        db.upsert_flight(self.conn, {
            "icao24": self.icao24, "callsign": callsign, "dep": dep, "arr": arr,
            "first_seen": first_seen, "last_seen": last_seen, "needs_review": needs_review,
        })
        self.count += 1
        self.logger.info(
            f"Opened flight {self.icao24} from {dep or 'UNKNOWN'} "
            f"(cs={callsign or '?'}, review={needs_review})"
        )
        return {"icao24": self.icao24, "callsign": callsign,
                "departure_airport_icao": dep, "first_seen": first_seen}

    def update_open(self, first_seen, last_seen, arr=None, callsign=None, needs_review=False):
        db.update_open_flight(
            self.conn, self.icao24, first_seen, last_seen,
            arr=arr, callsign=callsign, needs_review=needs_review,
        )
        if arr:
            self.count += 1
            self.logger.info(
                f"Closed flight {self.icao24} →{arr} "
                f"(cs={callsign or '?'}, review={needs_review})"
            )


def _process_aircraft(conn, icao24, sessions, open_flight, last_completed, logger, cfg) -> int:
    """Run the detector core for one aircraft over one lookback window.

    open_flight/last_completed are the dicts from db.get_open_flights /
    db.get_last_completed_flights (the core reads only the keys it needs).
    Returns the number of flight rows opened/closed (for stats).
    """
    store = _DbStore(conn, icao24, logger)

    def nearest(lat, lon, max_km=None):
        return db.lookup_nearest_airport(conn, lat, lon, max_km=max_km)

    core.process_window(store, icao24, sessions, open_flight, last_completed, nearest, cfg)
    return store.count


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
    cfg = _build_config()

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
        sessions = core.split_sessions(acft_positions, cfg)
        open_flight = open_flights_map.get(icao24)
        last_completed = last_completed_map.get(icao24)
        try:
            n = _process_aircraft(
                conn, icao24, sessions, open_flight, last_completed, logger, cfg
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
