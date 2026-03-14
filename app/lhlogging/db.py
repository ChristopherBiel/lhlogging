from datetime import datetime, timezone
from typing import Any

import psycopg

from lhlogging import config


def get_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        autocommit=False,
    )


def get_active_aircraft(
    conn: psycopg.Connection, type_filter: frozenset[str] | None = None
) -> list[dict]:
    with conn.cursor() as cur:
        if type_filter:
            cur.execute(
                "SELECT icao24, registration, aircraft_type FROM aircraft"
                " WHERE is_active = TRUE AND aircraft_type = ANY(%s) ORDER BY icao24",
                (list(type_filter),),
            )
        else:
            cur.execute(
                "SELECT icao24, registration, aircraft_type FROM aircraft"
                " WHERE is_active = TRUE ORDER BY icao24"
            )
        rows = cur.fetchall()
    return [{"icao24": r[0], "registration": r[1], "aircraft_type": r[2]} for r in rows]


def upsert_aircraft(conn: psycopg.Connection, aircraft: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO aircraft (icao24, registration, aircraft_type, aircraft_subtype,
                                  is_active, needs_review, updated_at)
            VALUES (%(icao24)s, %(registration)s, %(aircraft_type)s, %(aircraft_subtype)s,
                    TRUE, %(needs_review)s, NOW())
            ON CONFLICT (icao24) DO UPDATE SET
                registration     = EXCLUDED.registration,
                aircraft_type    = EXCLUDED.aircraft_type,
                aircraft_subtype = EXCLUDED.aircraft_subtype,
                is_active        = TRUE,
                needs_review     = EXCLUDED.needs_review,
                updated_at       = NOW()
            """,
            aircraft,
        )


def mark_aircraft_retired(conn: psycopg.Connection, icao24: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE aircraft
            SET is_active = FALSE, last_seen_date = CURRENT_DATE, updated_at = NOW()
            WHERE icao24 = %s
            """,
            (icao24,),
        )


def upsert_flight(conn: psycopg.Connection, flight: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO flights
                (icao24, callsign, departure_airport_icao, arrival_airport_icao,
                 first_seen, last_seen, needs_review)
            VALUES
                (%(icao24)s, %(callsign)s, %(dep)s, %(arr)s,
                 %(first_seen)s, %(last_seen)s, %(needs_review)s)
            ON CONFLICT (icao24, first_seen) DO UPDATE SET
                callsign               = EXCLUDED.callsign,
                arrival_airport_icao   = EXCLUDED.arrival_airport_icao,
                last_seen              = EXCLUDED.last_seen,
                needs_review           = EXCLUDED.needs_review
            """,
            flight,
        )


def insert_positions(conn: psycopg.Connection, snapshots: list[dict]) -> int:
    """Bulk-insert position snapshots. Skips duplicates via ON CONFLICT DO NOTHING."""
    if not snapshots:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for s in snapshots:
            cur.execute(
                """
                INSERT INTO positions
                    (icao24, callsign, captured_at, latitude, longitude,
                     altitude_m, velocity_ms, heading, on_ground)
                VALUES
                    (%(icao24)s, %(callsign)s, %(captured_at)s, %(latitude)s, %(longitude)s,
                     %(altitude_m)s, %(velocity_ms)s, %(heading)s, %(on_ground)s)
                ON CONFLICT (icao24, captured_at) DO NOTHING
                """,
                s,
            )
            inserted += cur.rowcount
    return inserted


def get_positions_since(
    conn: psycopg.Connection, since: datetime
) -> list[dict]:
    """Return all positions since the given timestamp, ordered for transition detection."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT icao24, callsign, captured_at, latitude, longitude, on_ground
            FROM positions
            WHERE captured_at >= %s
            ORDER BY icao24, captured_at
            """,
            (since,),
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
        }
        for r in rows
    ]


def get_open_flights(conn: psycopg.Connection) -> list[dict]:
    """Return flights that have no arrival airport yet (still in progress)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT icao24, first_seen, callsign, departure_airport_icao
            FROM flights
            WHERE arrival_airport_icao IS NULL
            """
        )
        rows = cur.fetchall()
    return [
        {
            "icao24": r[0],
            "first_seen": r[1],
            "callsign": r[2],
            "departure_airport_icao": r[3],
        }
        for r in rows
    ]


def get_latest_positions(
    conn: psycopg.Connection, icao24s: list[str]
) -> dict[str, dict]:
    """Return the most recent position for each of the given icao24s."""
    if not icao24s:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (icao24)
                icao24, captured_at, latitude, longitude, on_ground, callsign
            FROM positions
            WHERE icao24 = ANY(%s)
            ORDER BY icao24, captured_at DESC
            """,
            (icao24s,),
        )
        rows = cur.fetchall()
    return {
        r[0]: {
            "captured_at": r[1],
            "latitude": r[2],
            "longitude": r[3],
            "on_ground": r[4],
            "callsign": r[5],
        }
        for r in rows
    }


def update_open_flight(
    conn: psycopg.Connection,
    icao24: str,
    first_seen: datetime,
    last_seen: datetime,
    arr: str | None = None,
    callsign: str | None = None,
    needs_review: bool = False,
) -> None:
    """Update a pending flight. Only touches flights where arrival is still NULL."""
    with conn.cursor() as cur:
        if arr:
            cur.execute(
                """
                UPDATE flights SET
                    arrival_airport_icao = %s,
                    last_seen = %s,
                    callsign = COALESCE(%s, callsign),
                    needs_review = %s
                WHERE icao24 = %s AND first_seen = %s
                    AND arrival_airport_icao IS NULL
                """,
                (arr, last_seen, callsign, needs_review, icao24, first_seen),
            )
        else:
            cur.execute(
                """
                UPDATE flights SET
                    last_seen = %s,
                    callsign = COALESCE(%s, callsign)
                WHERE icao24 = %s AND first_seen = %s
                    AND arrival_airport_icao IS NULL
                """,
                (last_seen, callsign, icao24, first_seen),
            )


def lookup_nearest_airport(
    conn: psycopg.Connection, lat: float, lon: float, max_km: float | None = None
) -> str | None:
    """Find the nearest airport to the given lat/lon using earthdistance."""
    if lat is None or lon is None:
        return None
    if max_km is None:
        max_km = config.AIRPORT_LOOKUP_RADIUS_KM
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT icao_code,
                   earth_distance(
                       ll_to_earth(latitude, longitude),
                       ll_to_earth(%s, %s)
                   ) / 1000.0 AS dist_km
            FROM airports
            ORDER BY earth_distance(
                ll_to_earth(latitude, longitude),
                ll_to_earth(%s, %s)
            )
            LIMIT 1
            """,
            (lat, lon, lat, lon),
        )
        r = cur.fetchone()
    if not r or r[1] > max_km:
        return None
    return r[0].strip()


def delete_positions_before(conn: psycopg.Connection, before: datetime) -> int:
    """Delete position snapshots older than the given timestamp. Returns rows deleted."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM positions WHERE captured_at < %s", (before,))
        return cur.rowcount


def log_batch_start(conn: psycopg.Connection, run_type: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO batch_runs (run_type, status) VALUES (%s, 'running') RETURNING id",
            (run_type,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def log_batch_finish(conn: psycopg.Connection, run_id: int, stats: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE batch_runs SET
                finished_at      = NOW(),
                aircraft_total   = %(aircraft_total)s,
                aircraft_ok      = %(aircraft_ok)s,
                aircraft_error   = %(aircraft_error)s,
                flights_upserted = %(flights_upserted)s,
                error_detail     = %(error_detail)s,
                status           = %(status)s
            WHERE id = %(run_id)s
            """,
            {
                "run_id": run_id,
                "aircraft_total": stats.get("aircraft_total"),
                "aircraft_ok": stats.get("ok", 0),
                "aircraft_error": stats.get("error", 0),
                "flights_upserted": stats.get("flights_upserted", 0),
                "error_detail": stats.get("error_detail"),
                "status": stats.get("status", "ok"),
            },
        )
    conn.commit()
