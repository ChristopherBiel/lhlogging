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


def get_active_aircraft(conn: psycopg.Connection) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT icao24, registration, aircraft_type FROM aircraft WHERE is_active = TRUE ORDER BY icao24"
        )
        rows = cur.fetchall()
    return [{"icao24": r[0], "registration": r[1], "aircraft_type": r[2]} for r in rows]


def upsert_aircraft(conn: psycopg.Connection, aircraft: dict) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO aircraft (icao24, registration, aircraft_type, aircraft_subtype, is_active, updated_at)
            VALUES (%(icao24)s, %(registration)s, %(aircraft_type)s, %(aircraft_subtype)s, TRUE, NOW())
            ON CONFLICT (icao24) DO UPDATE SET
                registration     = EXCLUDED.registration,
                aircraft_type    = EXCLUDED.aircraft_type,
                aircraft_subtype = EXCLUDED.aircraft_subtype,
                is_active        = TRUE,
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
                (icao24, callsign, departure_airport_icao, arrival_airport_icao, first_seen, last_seen)
            VALUES
                (%(icao24)s, %(callsign)s, %(dep)s, %(arr)s, %(first_seen)s, %(last_seen)s)
            ON CONFLICT (icao24, first_seen) DO UPDATE SET
                callsign               = EXCLUDED.callsign,
                arrival_airport_icao   = EXCLUDED.arrival_airport_icao,
                last_seen              = EXCLUDED.last_seen
            """,
            flight,
        )


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
