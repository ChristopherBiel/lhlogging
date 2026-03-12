"""
One-off script: downloads the OurAirports dataset and populates the airports table.
Run after applying the 002_airports_and_positions.sql migration.

Usage:
    cd /path/to/lhlogging
    python tools/load_airports.py
"""
import csv
import io
import sys

import requests

from lhlogging import db
from lhlogging.utils import setup_logging

_CSV_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
_INCLUDE_TYPES = {"large_airport", "medium_airport"}


def main() -> int:
    logger = setup_logging("load_airports")
    logger.info(f"Downloading airport data from {_CSV_URL}")

    try:
        resp = requests.get(_CSV_URL, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.critical(f"Download failed: {e}")
        return 1

    logger.info(f"Downloaded {len(resp.content) / 1024:.0f} KB, parsing...")

    reader = csv.DictReader(io.StringIO(resp.text))
    airports = []
    for row in reader:
        ap_type = (row.get("type") or "").strip()
        if ap_type not in _INCLUDE_TYPES:
            continue

        icao_code = (row.get("ident") or "").strip().upper()
        if len(icao_code) != 4:
            continue

        try:
            lat = float(row["latitude_deg"])
            lon = float(row["longitude_deg"])
        except (ValueError, KeyError):
            continue

        name = (row.get("name") or "").strip()[:200]
        airports.append((icao_code, name, lat, lon))

    logger.info(f"Parsed {len(airports)} large/medium airports")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    with conn.cursor() as cur:
        for icao_code, name, lat, lon in airports:
            cur.execute(
                """
                INSERT INTO airports (icao_code, name, latitude, longitude)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (icao_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude
                """,
                (icao_code, name, lat, lon),
            )
    conn.commit()
    conn.close()

    logger.info(f"Loaded {len(airports)} airports into database")
    return 0


if __name__ == "__main__":
    sys.exit(main())
