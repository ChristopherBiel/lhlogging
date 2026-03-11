import logging

import requests

from lhlogging import config
from lhlogging.utils import RateLimiter


class PlanespottersError(Exception):
    pass


class PlanespottersRateLimitError(PlanespottersError):
    pass


class PlanespottersDataError(PlanespottersError):
    pass


class PlanespottersClient:
    """Fetches per-aircraft type data from api.planespotters.net."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._rate_limiter = RateLimiter(config.PLANESPOTTERS_REQUEST_DELAY_S)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "LHLogging/0.1 (flight data research)"

    def get_aircraft(self, icao24: str) -> dict | None:
        """
        Look up a single aircraft by ICAO24 hex code.
        Returns a dict with aircraft_type and aircraft_subtype, or None if not found.
        """
        url = f"{config.PLANESPOTTERS_BASE_URL}/hex/{icao24}"
        self._rate_limiter.wait()
        try:
            resp = self._session.get(url, timeout=30)
        except requests.RequestException as e:
            raise PlanespottersError(f"Request failed: {e}") from e

        if resp.status_code == 404:
            return None
        if resp.status_code == 429:
            raise PlanespottersRateLimitError("Rate limited by Planespotters")
        if not resp.ok:
            raise PlanespottersError(f"HTTP {resp.status_code} from Planespotters: {resp.text[:200]}")

        data = resp.json()

        # Response may be a single aircraft dict or a list; use first record
        if isinstance(data, list):
            if not data:
                return None
            raw = data[0]
        else:
            raw = data

        return self._parse_aircraft(raw)

    def _parse_aircraft(self, raw: dict) -> dict | None:
        # Type info: "t" is the ICAO type code, "type"/"mdl"/"model" is the full name
        aircraft_type = (raw.get("t") or raw.get("icaoAircraftClass") or "").strip().upper() or None
        aircraft_subtype = (raw.get("type") or raw.get("mdl") or raw.get("model") or "").strip() or None

        if not aircraft_type and not aircraft_subtype:
            return None

        return {
            "aircraft_type": aircraft_type,
            "aircraft_subtype": aircraft_subtype,
        }
