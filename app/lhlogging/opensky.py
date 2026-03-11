import logging
import time
from datetime import datetime, timezone

import requests

from lhlogging import config
from lhlogging.utils import make_retry


class OpenSkyError(Exception):
    pass


class OpenSkyClient:
    """Fetches flight data from the OpenSky Network API using OAuth2 client credentials."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "LHLogging/0.1 (flight data research)"
        self._retry = make_retry(logger)
        self._access_token: str | None = None
        self._token_expires_at: float = 0.0

    def _ensure_token(self) -> None:
        """Fetch or refresh the OAuth2 bearer token if missing or expired."""
        if self._access_token and time.monotonic() < self._token_expires_at - 30:
            return

        self._logger.debug("Fetching OpenSky OAuth2 token")
        try:
            resp = requests.post(
                config.OPENSKY_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": config.OPENSKY_CLIENT_ID,
                    "client_secret": config.OPENSKY_CLIENT_SECRET,
                },
                timeout=15,
            )
        except requests.RequestException as e:
            raise OpenSkyError(f"Token request failed: {e}") from e

        if not resp.ok:
            raise OpenSkyError(
                f"OpenSky token fetch failed (HTTP {resp.status_code}): {resp.text[:200]}"
            )

        data = resp.json()
        self._access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        self._token_expires_at = time.monotonic() + expires_in
        self._session.headers["Authorization"] = f"Bearer {self._access_token}"
        self._logger.debug(f"OpenSky token obtained, expires in {expires_in}s")

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
            self._ensure_token()
            try:
                resp = self._session.get(url, params=params, timeout=30)
            except requests.RequestException as e:
                raise OpenSkyError(f"Request failed for {icao24}: {e}") from e

            if resp.status_code == 401:
                # Token may have just expired — clear it and let retry re-fetch
                self._access_token = None
                raise OpenSkyError(f"OpenSky 401 for {icao24} — token may have expired")
            if resp.status_code == 404:
                return []
            if resp.status_code == 429:
                self._logger.warning(
                    f"OpenSky rate limit (429) for {icao24} — sleeping {config.OPENSKY_RATELIMIT_BACKOFF_S}s"
                )
                time.sleep(config.OPENSKY_RATELIMIT_BACKOFF_S)
                raise OpenSkyError(f"OpenSky rate limit hit (429) for {icao24}")
            if not resp.ok:
                raise OpenSkyError(
                    f"HTTP {resp.status_code} from OpenSky for {icao24}: {resp.text[:200]}"
                )

            raw = resp.json()
            if raw is None:
                return []
            return [self._parse_flight(f) for f in raw if f]

        return _fetch()

    def get_flights_all(
        self, begin_unix: int, end_unix: int, fleet_icao24s: set[str]
    ) -> list[dict]:
        """
        Fetch all global flights from /flights/all for one time chunk (max 2h).
        Filters to fleet_icao24s client-side.
        Returns parsed flights for matching aircraft only.
        """
        url = f"{config.OPENSKY_BASE_URL}/flights/all"
        params = {"begin": begin_unix, "end": end_unix}

        @self._retry
        def _fetch():
            self._ensure_token()
            try:
                resp = self._session.get(url, params=params, timeout=120)
            except requests.RequestException as e:
                raise OpenSkyError(f"Request failed for /flights/all: {e}") from e

            if resp.status_code == 401:
                self._access_token = None
                raise OpenSkyError("OpenSky 401 — token may have expired")
            if resp.status_code == 404:
                return []
            if resp.status_code == 429:
                self._logger.warning(
                    f"OpenSky rate limit (429) — sleeping {config.OPENSKY_RATELIMIT_BACKOFF_S}s"
                )
                time.sleep(config.OPENSKY_RATELIMIT_BACKOFF_S)
                raise OpenSkyError("OpenSky rate limit hit (429)")
            if not resp.ok:
                raise OpenSkyError(
                    f"HTTP {resp.status_code} from /flights/all: {resp.text[:200]}"
                )

            raw = resp.json()
            if raw is None:
                return []

            return [
                self._parse_flight(f)
                for f in raw
                if f and (f.get("icao24") or "").strip().lower() in fleet_icao24s
            ]

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
