"""
Downloads the OpenSky aircraft database CSV and returns all aircraft
for a given operator ICAO code (e.g. 'DLH' for Lufthansa).

CSV source: https://opensky-network.org/datasets/metadata/aircraftDatabase.csv
Columns relevant to us: icao24, registration, typecode, operatoricao, model
"""
import csv
import io
import logging

import requests

from lhlogging.utils import make_retry

_CSV_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"


class OpenSkyFleetError(Exception):
    pass


class OpenSkyFleetClient:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._retry = make_retry(logger)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "LHLogging/0.1 (flight data research)"

    def get_airline_fleet(self, operator_icao: str) -> list[dict]:
        """
        Downloads the full OpenSky aircraft DB CSV and filters by operatoricao.
        Returns list of dicts: {icao24, registration, aircraft_type, aircraft_subtype}.
        """
        self._logger.info(f"Downloading OpenSky aircraft database from {_CSV_URL}")

        @self._retry
        def _fetch() -> bytes:
            try:
                resp = self._session.get(_CSV_URL, timeout=120, allow_redirects=True)
            except requests.RequestException as e:
                raise OpenSkyFleetError(f"Download failed: {e}") from e
            if not resp.ok:
                raise OpenSkyFleetError(f"HTTP {resp.status_code} fetching aircraft DB")
            return resp.content

        raw = _fetch()
        self._logger.info(f"Downloaded {len(raw) / 1024 / 1024:.1f} MB, parsing...")

        aircraft = []
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8", errors="replace")))
        for row in reader:
            if (row.get("operatoricao") or "").strip().upper() != operator_icao.upper():
                continue
            parsed = self._parse_row(row)
            if parsed:
                aircraft.append(parsed)

        self._logger.info(f"Found {len(aircraft)} aircraft for operator {operator_icao}")
        return aircraft

    def _parse_row(self, row: dict) -> dict | None:
        icao24 = (row.get("icao24") or "").strip().lower()
        registration = (row.get("registration") or "").strip().upper()

        if not icao24 or not registration:
            return None

        # typecode is the ICAO type designator e.g. "A359", "B748"
        aircraft_type = (row.get("typecode") or "").strip().upper() or None
        # model is the fuller name e.g. "Airbus A350-941"
        aircraft_subtype = (row.get("model") or "").strip() or None

        return {
            "icao24": icao24,
            "registration": registration,
            "aircraft_type": aircraft_type,
            "aircraft_subtype": aircraft_subtype,
        }
