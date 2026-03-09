import logging
import time

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
    """Fetches airline fleet data from api.planespotters.net."""

    # The public endpoint for fleet lookup by airline ICAO
    _FLEET_URL = "https://api.planespotters.net/pub/flights/hex/{airline_icao}"

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._rate_limiter = RateLimiter(config.PLANESPOTTERS_REQUEST_DELAY_S)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "LHLogging/0.1 (flight data research)"

    def get_airline_fleet(self, airline_icao: str) -> list[dict]:
        """
        Returns a list of aircraft dicts for the given airline ICAO code (e.g. 'DLH').
        Each dict has: icao24, registration, aircraft_type, aircraft_subtype.
        """
        url = f"https://api.planespotters.net/pub/flights/hex/{airline_icao}"
        aircraft = []
        page = 1

        while True:
            self._rate_limiter.wait()
            try:
                resp = self._session.get(url, params={"page": page}, timeout=30)
            except requests.RequestException as e:
                raise PlanespottersError(f"Request failed: {e}") from e

            if resp.status_code == 429:
                raise PlanespottersRateLimitError("Rate limited by Planespotters")
            if not resp.ok:
                raise PlanespottersError(f"HTTP {resp.status_code} from Planespotters: {resp.text[:200]}")

            data = resp.json()

            # Planespotters returns {"ac": [...], "total": N} or similar — handle both shapes
            records = data if isinstance(data, list) else data.get("ac") or data.get("aircraft") or []

            if not records:
                break

            for raw in records:
                parsed = self._parse_aircraft(raw)
                if parsed:
                    aircraft.append(parsed)

            # Stop if we got fewer records than a full page (no more pages)
            # or if the API doesn't paginate (all results in one response)
            if isinstance(data, list) or len(records) < 100:
                break

            page += 1
            self._logger.debug(f"Planespotters page {page}, {len(aircraft)} aircraft so far")

        self._logger.info(f"Planespotters returned {len(aircraft)} aircraft for {airline_icao}")
        return aircraft

    def _parse_aircraft(self, raw: dict) -> dict | None:
        icao24 = (raw.get("icao24") or raw.get("hex") or "").strip().lower()
        registration = (raw.get("r") or raw.get("registration") or "").strip().upper()

        if not icao24 or not registration:
            return None

        # Type info may be nested under "t" (ICAO type) or "type"/"desc"
        aircraft_type = (raw.get("t") or raw.get("icaoAircraftClass") or "").strip().upper() or None
        aircraft_subtype = (raw.get("type") or raw.get("mdl") or raw.get("model") or "").strip() or None

        return {
            "icao24": icao24,
            "registration": registration,
            "aircraft_type": aircraft_type,
            "aircraft_subtype": aircraft_subtype,
        }
