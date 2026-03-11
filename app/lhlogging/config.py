import os
from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Required environment variable '{name}' is not set.")
    return val


def _optional(name: str, default: str) -> str:
    return os.environ.get(name, default)


# Database
DB_HOST: str = _optional("DB_HOST", "db")
DB_PORT: int = int(_optional("DB_PORT", "5432"))
DB_NAME: str = _optional("DB_NAME", "lhlogging")
DB_USER: str = _require("POSTGRES_USER")
DB_PASSWORD: str = _require("POSTGRES_PASSWORD")

# OpenSky
OPENSKY_CLIENT_ID: str = _require("OPENSKY_CLIENT_ID")
OPENSKY_CLIENT_SECRET: str = _require("OPENSKY_CLIENT_SECRET")
OPENSKY_BASE_URL: str = _optional("OPENSKY_BASE_URL", "https://opensky-network.org/api")
OPENSKY_TOKEN_URL: str = _optional(
    "OPENSKY_TOKEN_URL",
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
)

# Fleet
PLANESPOTTERS_AIRLINE_ICAO: str = _optional("AIRLINE_OPERATOR_ICAO", "DLH")
PLANESPOTTERS_BASE_URL: str = _optional(
    "PLANESPOTTERS_BASE_URL", "https://api.planespotters.net/pub/flights"
)
PLANESPOTTERS_REQUEST_DELAY_S: float = float(_optional("PLANESPOTTERS_REQUEST_DELAY_S", "1.0"))

# Tuning
OPENSKY_REQUEST_DELAY_S: float = float(_optional("OPENSKY_REQUEST_DELAY_S", "2.0"))
OPENSKY_RATELIMIT_BACKOFF_S: int = int(_optional("OPENSKY_RATELIMIT_BACKOFF_S", "60"))
OPENSKY_LOOKBACK_HOURS: int = int(_optional("OPENSKY_LOOKBACK_HOURS", "26"))
OPENSKY_CHUNK_SIZE_S: int = int(_optional("OPENSKY_CHUNK_SIZE_S", str(2 * 3600)))  # 2h
BATCH_MAX_ERRORS_BEFORE_ABORT: int = int(_optional("BATCH_MAX_ERRORS_BEFORE_ABORT", "50"))

# Aircraft type filter (comma-separated ICAO type codes; empty = track all)
_TRACK_TYPES_RAW: str = _optional("TRACK_AIRCRAFT_TYPES", "")
TRACK_AIRCRAFT_TYPES: frozenset[str] | None = (
    frozenset(t.strip().upper() for t in _TRACK_TYPES_RAW.split(",") if t.strip()) or None
)

# Logging
LOG_DIR: str = _optional("LOG_DIR", "/var/log/lhlogging")
LOG_LEVEL: str = _optional("LOG_LEVEL", "INFO")
