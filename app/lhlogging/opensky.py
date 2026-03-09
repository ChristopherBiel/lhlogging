import logging
from datetime import datetime, timezone

import requests

from lhlogging import config
from lhlogging.utils import make_retry


class OpenSkyError(Exception):
    pass


class OpenSkyClient:
    """Fetches flight data from the OpenSky Network API."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._session = requests.Session()
        self._session.auth = (config.OPENSKY_USER, config.OPENSKY_PASS)
        self._session.headers["User-Agent"] = "LHLogging/0.1 (flight data research)"
        self._retry = make_retry(logger)

    def get_flights_for_aircraft(
        self, icao24: str, begin_unix: int, end_unix: int
    ) -> list[dict]:
        """
        Returns completed flights for a single aircraft within the given Unix time window.
        OpenSky only returns completed flights (both departure and arrival known).
        Returns [] if the aircraft has no completed flights in the window.
        """
        url = f"{config.OPENSKY_BASE_URL}/flights/aircraft"
        params = {"icao24": icao24, "begin": begin_unix, "end": end_unix}

        @self._retry
        def _fetch():
            try:
                resp = self._session.get(url, params=params, timeout=30)
            except requests.RequestException as e:
                raise OpenSkyError(f"Request failed for {icao24}: {e}") from e

            if resp.status_code == 401:
                raise OpenSkyError("OpenSky authentication failed — check credentials")
            if resp.status_code == 404:
                # Some ICAO24 codes return 404 if no data exists; treat as empty
                return []
            if resp.status_code == 429:
                raise OpenSkyError(f"OpenSky rate limit hit (429) for {icao24}")
            if not resp.ok:
                raise OpenSkyError(f"HTTP {resp.status_code} from OpenSky for {icao24}: {resp.text[:200]}")

            raw = resp.json()
            if raw is None:
                return []
            return [self._parse_flight(f) for f in raw if f]

        return _fetch()

    def _parse_flight(self, raw: dict) -> dict:
        icao24 = (raw.get("icao24") or "").strip().lower()

        callsign = (raw.get("callsign") or "").strip().upper() or None

        dep = (raw.get("estDepartureAirport") or "").strip().upper() or None
        arr = (raw.get("estArrivalAirport") or "").strip().upper() or None

        first_seen = datetime.fromtimestamp(raw["firstSeen"], tz=timezone.utc)
        last_seen = datetime.fromtimestamp(raw["lastSeen"], tz=timezone.utc)

        return {
            "icao24": icao24,
            "callsign": callsign,
            "dep": dep,
            "arr": arr,
            "first_seen": first_seen,
            "last_seen": last_seen,
        }
