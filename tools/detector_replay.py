"""
Offline replay of the flight detector — runs the SAME core as production
(app/lhlogging/detector_core.py) without a database, so detection can be
reconstructed, scored, and iterated on locally.

This module is now just the offline glue around that core:
  - load positions from the CSV export,
  - an in-memory FlightStore (mirrors the flights-table I/O the core needs),
  - the windowed cron simulation (every cadence_min over a detect_lookback_min
    rolling window), which is what reproduces the incremental behaviour that a
    single pass cannot.

The case/state-machine logic lives ONCE in detector_core; there is no second
copy to drift. Swap a modified DetectorConfig in to measure a change with
tools/eval_detector.py or tools/run_corpus.py.

Pure stdlib (Python 3.9+).
"""
from __future__ import annotations

import bisect
import csv
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

# Import the production core (stdlib-only; does NOT pull in config/db).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))
from lhlogging import detector_core as core  # noqa: E402
from lhlogging.detector_core import DetectorConfig  # noqa: E402

from _airports import Airports  # noqa: E402

# Back-compat alias: callers build configs as detector_replay.Config(...).
Config = DetectorConfig


# --------------------------------------------------------------------------
# Cron-schedule knobs (not part of the detector core — they describe how
# production *invokes* the core, and how the replay simulates that).
# --------------------------------------------------------------------------
CADENCE_MIN = 30
LOOKBACK_MIN = 60
STALE_MAX_AGE_H = 24


# --------------------------------------------------------------------------
# Position loading
# --------------------------------------------------------------------------
def _parse_ts(s):
    s = s.strip().replace(" ", "T", 1)
    if s.endswith("+00"):
        s = s[:-3] + "+00:00"
    return datetime.fromisoformat(s)


def _parse_bool(s):
    s = (s or "").strip()
    return True if s == "t" else False if s == "f" else None


def _parse_float(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def load_positions(path, icao24s=None):
    """Load positions_export.csv into {icao24: [pos dicts sorted by time]}."""
    want = set(icao24s) if icao24s else None
    out = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            icao = r["icao24"].strip()
            if want is not None and icao not in want:
                continue
            out.setdefault(icao, []).append({
                "icao24": icao,
                "callsign": r["callsign"],
                "captured_at": _parse_ts(r["captured_at"]),
                "latitude": _parse_float(r["latitude"]),
                "longitude": _parse_float(r["longitude"]),
                "altitude_m": _parse_float(r["altitude_m"]),
                "velocity_ms": _parse_float(r["velocity_ms"]),
                "on_ground": _parse_bool(r["on_ground"]),
            })
    for icao in out:
        out[icao].sort(key=lambda p: p["captured_at"])
    return out


# --------------------------------------------------------------------------
# In-memory stand-in for the flights table (the Store the core writes through)
# --------------------------------------------------------------------------
@dataclass
class Leg:
    icao24: str
    callsign: str | None
    departure_airport_icao: str | None
    arrival_airport_icao: str | None
    first_seen: datetime
    last_seen: datetime
    needs_review: bool = False
    origin: str = ""


class FlightStore:
    def __init__(self, icao24, all_positions):
        self.icao24 = icao24
        self.legs: list[Leg] = []
        self._all = all_positions
        self._times = [p["captured_at"] for p in all_positions]

    def _handle(self, leg):
        return {"icao24": leg.icao24, "callsign": leg.callsign,
                "departure_airport_icao": leg.departure_airport_icao,
                "first_seen": leg.first_seen}

    # --- reads ---
    def positions_before(self, before, limit):
        i = bisect.bisect_left(self._times, before)
        return self._all[max(0, i - limit):i]

    def open_flight(self):
        opens = [l for l in self.legs if l.arrival_airport_icao is None]
        return self._handle(opens[-1]) if opens else None

    def last_completed(self):
        done = [l for l in self.legs if l.arrival_airport_icao is not None]
        if not done:
            return None
        l = max(done, key=lambda x: x.last_seen)
        return {"last_seen": l.last_seen, "arrival_airport_icao": l.arrival_airport_icao}

    def latest_position_upto(self, t):
        i = bisect.bisect_right(self._times, t)
        return self._all[i - 1] if i > 0 else None

    # --- writes (db.upsert_flight / db.update_open_flight semantics) ---
    def upsert(self, callsign, dep, arr, first_seen, last_seen, needs_review, origin=""):
        for l in self.legs:
            if l.first_seen == first_seen:
                l.callsign, l.arrival_airport_icao = callsign, arr
                l.last_seen, l.needs_review = last_seen, needs_review
                return self._handle(l)
        l = Leg(self.icao24, callsign, dep, arr, first_seen, last_seen, needs_review, origin)
        self.legs.append(l)
        return self._handle(l)

    def update_open(self, first_seen, last_seen, arr=None, callsign=None, needs_review=False):
        for l in self.legs:
            if l.first_seen == first_seen and l.arrival_airport_icao is None:
                if arr:
                    l.arrival_airport_icao, l.needs_review = arr, needs_review
                l.last_seen = last_seen
                if callsign is not None:
                    l.callsign = callsign
                return


def _close_stale(store, t_now, nearest, cfg):
    """Port of _close_stale_flights: close flights open > STALE_MAX_AGE_H."""
    of = store.open_flight()
    if not of or of["first_seen"] >= t_now - timedelta(hours=STALE_MAX_AGE_H):
        return
    pos = store.latest_position_upto(t_now)
    arr = None
    if pos and pos.get("altitude_m") is not None and pos["altitude_m"] < cfg.proximity_landing_altitude_m:
        arr = nearest(pos["latitude"], pos["longitude"], max_km=cfg.proximity_landing_radius_km)
    last_seen = pos["captured_at"] if pos else of["first_seen"]
    store.update_open(of["first_seen"], last_seen, arr=arr or "UNKN",
                      callsign=(pos["callsign"].strip() if pos and pos.get("callsign") else of["callsign"]),
                      needs_review=True)


# --------------------------------------------------------------------------
# Windowed cron simulation for one aircraft → list[Leg]
# --------------------------------------------------------------------------
def replay_aircraft(positions, airports, cfg=DetectorConfig()):
    if not positions:
        return []
    store = FlightStore(positions[0]["icao24"], positions)
    nearest = airports.nearest
    lookback = timedelta(minutes=LOOKBACK_MIN)
    step = timedelta(minutes=CADENCE_MIN)
    times = store._times

    t = times[0].replace(second=0, microsecond=0)
    t -= timedelta(minutes=t.minute % CADENCE_MIN)
    t_end = times[-1] + lookback + step
    while t <= t_end:
        lo = bisect.bisect_left(times, t - lookback)
        hi = bisect.bisect_right(times, t)
        window = positions[lo:hi]
        if window:
            core.process_window(store, store.icao24, core.split_sessions(window, cfg),
                                store.open_flight(), store.last_completed(), nearest, cfg)
        _close_stale(store, t, nearest, cfg)
        t += step
    return store.legs


def replay(positions_by_icao, airports, cfg=DetectorConfig()):
    return {icao: replay_aircraft(pos, airports, cfg) for icao, pos in positions_by_icao.items()}
