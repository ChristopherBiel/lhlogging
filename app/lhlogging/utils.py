import logging
import os
import time
from logging.handlers import RotatingFileHandler

import tenacity

from lhlogging import config


def setup_logging(run_type: str) -> logging.Logger:
    os.makedirs(config.LOG_DIR, exist_ok=True)

    logger = logging.getLogger(run_type)
    logger.setLevel(getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    fmt.converter = time.gmtime  # UTC timestamps in logs

    # Rotating file handler (10 MB × 5 backups)
    file_handler = RotatingFileHandler(
        os.path.join(config.LOG_DIR, f"{run_type}.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
    )
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


def make_retry(logger: logging.Logger):
    """Return a tenacity retry decorator that logs each attempt."""
    return tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=5, max=60),
        stop=tenacity.stop_after_attempt(3),
        reraise=True,
        before_sleep=lambda rs: logger.warning(
            f"Retry {rs.attempt_number} after error: {rs.outcome.exception()}"
        ),
    )


class RateLimiter:
    """Ensures at least `delay_s` seconds between successive calls to wait()."""

    def __init__(self, delay_s: float) -> None:
        self._delay = delay_s
        self._last = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last
        remaining = self._delay - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last = time.monotonic()
