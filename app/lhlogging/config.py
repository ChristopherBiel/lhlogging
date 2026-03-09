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

# Tuning
OPENSKY_REQUEST_DELAY_S: float = float(_optional("OPENSKY_REQUEST_DELAY_S", "0.5"))
OPENSKY_LOOKBACK_HOURS: int = int(_optional("OPENSKY_LOOKBACK_HOURS", "36"))
BATCH_MAX_ERRORS_BEFORE_ABORT: int = int(_optional("BATCH_MAX_ERRORS_BEFORE_ABORT", "50"))

# Logging
LOG_DIR: str = _optional("LOG_DIR", "/var/log/lhlogging")
LOG_LEVEL: str = _optional("LOG_LEVEL", "INFO")
