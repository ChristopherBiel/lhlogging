"""
Pure flight-detection core — the single source of truth for the case/state
machine, shared by the production detector (app/lhlogging/flight_detector.py,
DB-backed) and the offline replay harness (tools/detector_replay.py, in-memory).

Importing this module pulls in **stdlib only** — NOT lhlogging.config or
lhlogging.db — so it loads on the harness (Python 3.9, no DB) and in production
(Python 3.10+) alike. All I/O is injected:

  - ``nearest(lat, lon, max_km=None) -> icao|None``  (airport lookup)
  - a ``store`` with:
        positions_before(before, limit) -> list[pos]      # last N before a time
        upsert(callsign, dep, arr, first_seen, last_seen,
               needs_review, origin="") -> handle          # insert/update a leg
        update_open(first_seen, last_seen, arr=None,
                    callsign=None, needs_review=False)      # update an open leg

A ``handle`` is a dict {icao24, callsign, departure_airport_icao, first_seen};
``open_flight`` and ``last_completed`` passed to process_window are dicts with
those keys / {last_seen, arrival_airport_icao}.

Tunables live on DetectorConfig. The "guard" fields default to OFF so this core
reproduces the historical detector exactly; production turns them on via env
(config.py), and the harness measures each one before it ships.

A position is a dict: icao24, callsign, captured_at (aware datetime),
latitude, longitude, altitude_m, velocity_ms, on_ground (bool|None).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class DetectorConfig:
    # --- historical production values (app/lhlogging/config.py) ---
    state_poll_interval_min: int = 2            # gap_threshold = 4x = 8 min
    landing_velocity_threshold_ms: float = 30.0
    landing_altitude_threshold_m: float = 300.0
    missed_departure_altitude_m: float = 3000.0
    missed_departure_max_gap_h: int = 12
    airport_lookup_radius_km: float = 50.0
    proximity_landing_altitude_m: float = 500.0
    proximity_landing_radius_km: float = 8.0

    # --- robustness guards (0 / 1 / False == OFF == historical behaviour) ---
    # P1: distrust on_ground=true when moving faster than this (cruise-snap) ...
    onground_max_speed_ms: float = 0.0
    # ... or reporting on_ground=true above this altitude (alt-glitch). 0 = off.
    onground_max_altitude_m: float = 0.0
    # P2: an air->ground transition only counts as a landing if followed by this
    # many consecutive on-ground samples. 1 = off.
    landing_min_consecutive: int = 1
    # P3: resolve a missed departure (first sighting airborne) from a single
    # low-altitude sample near an airport, instead of needing a 2-point climb.
    missed_departure_snap: bool = False
    # P4: cap the snap radius for SCAN-derived arrivals (within-session
    # air->ground). 0 = use airport_lookup_radius_km (50 km). A cruise position
    # is rarely within ~8 km of a field, so a tight cap refuses phantom snaps.
    scan_arrival_max_km: float = 0.0
    # P5: suppress re-opening a leg at the airport just landed at, within this
    # many minutes (dep==arr micro-flights / touch-and-go). 0 = off.
    min_turnaround_min: int = 0

    @property
    def gap_threshold(self) -> timedelta:
        return timedelta(minutes=4 * self.state_poll_interval_min)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def is_on_ground(pos, cfg):
    """True (ground) / False (airborne) / None (indeterminate)."""
    if pos["on_ground"] is True:
        # P1 — a transponder reporting on-ground while fast or high is a corrupt
        # cruise sample, not a landing.
        if cfg.onground_max_speed_ms > 0:
            v = pos.get("velocity_ms")
            if v is not None and v > cfg.onground_max_speed_ms:
                return False
        if cfg.onground_max_altitude_m > 0:
            a = pos.get("altitude_m")
            if a is not None and a > cfg.onground_max_altitude_m:
                return False
        return True
    if pos["on_ground"] is None:
        return None
    v = pos.get("velocity_ms")
    a = pos.get("altitude_m")
    if v is not None and a is not None:
        if v < cfg.landing_velocity_threshold_ms and a < cfg.landing_altitude_threshold_m:
            return True
    return False


def split_sessions(positions, cfg):
    if not positions:
        return []
    gap = cfg.gap_threshold
    sessions = [[positions[0]]]
    for i in range(1, len(positions)):
        if positions[i]["captured_at"] - positions[i - 1]["captured_at"] > gap:
            sessions.append([positions[i]])
        else:
            sessions[-1].append(positions[i])
    return sessions


def detect_landing(positions, nearest, cfg):
    if not positions:
        return None
    last = positions[-1]
    if is_on_ground(last, cfg):
        return {"lat": last["latitude"], "lon": last["longitude"], "captured_at": last["captured_at"]}
    last_alt = last.get("altitude_m")
    if last_alt is None or last_alt >= cfg.proximity_landing_altitude_m:
        return None
    altitudes = [p["altitude_m"] for p in positions if p.get("altitude_m") is not None]
    descending = len(altitudes) >= 2 and altitudes[-1] < altitudes[0]
    frozen = False
    if len(positions) >= 3:
        frozen = all(
            p.get("latitude") == last["latitude"]
            and p.get("longitude") == last["longitude"]
            and p.get("altitude_m") == last_alt
            for p in positions[-3:]
        )
    if descending or frozen:
        if nearest(last["latitude"], last["longitude"], max_km=cfg.proximity_landing_radius_km):
            return {"lat": last["latitude"], "lon": last["longitude"], "captured_at": last["captured_at"]}
    return None


def detect_departure(positions, cfg):
    if not positions:
        return None
    first = positions[0]
    if is_on_ground(first, cfg):
        return {"lat": first["latitude"], "lon": first["longitude"], "captured_at": first["captured_at"]}
    if len(positions) >= 2:
        altitudes = [p["altitude_m"] for p in positions if p.get("altitude_m") is not None]
        climbing = len(altitudes) >= 2 and altitudes[-1] > altitudes[0]
        low_start = first.get("altitude_m") is not None and first["altitude_m"] < cfg.missed_departure_altitude_m
        if climbing and low_start:
            return {"lat": first["latitude"], "lon": first["longitude"], "captured_at": first["captured_at"]}
    return None


def get_session_callsign(session):
    for pos in session:
        cs = pos.get("callsign")
        if cs and cs.strip():
            return cs.strip()
    return None


def callsigns_match(a, b):
    if a is None or b is None:
        return True
    return a == b


def scan_for_departure(session, cfg):
    prev_ground, prev_pos = None, None
    for pos in session:
        cur = is_on_ground(pos, cfg)
        if cur is None:
            continue
        if prev_ground is True and cur is False:
            return {"lat": prev_pos["latitude"], "lon": prev_pos["longitude"], "captured_at": prev_pos["captured_at"]}
        prev_ground, prev_pos = cur, pos
    return None


def scan_for_arrival_after(session, after, cfg):
    prev_ground = None
    for i, pos in enumerate(session):
        cur = is_on_ground(pos, cfg)
        if cur is None:
            continue
        if pos["captured_at"] <= after:
            prev_ground = cur
            continue
        if prev_ground is False and cur is True and _confirms_landing(session, i, cfg):
            return {"lat": pos["latitude"], "lon": pos["longitude"], "captured_at": pos["captured_at"]}
        prev_ground = cur
    return None


def _confirms_landing(session, i, cfg):
    """P2: require landing_min_consecutive on-ground samples from index i.
    Default (1) is always True == historical behaviour."""
    need = cfg.landing_min_consecutive
    if need <= 1:
        return True
    seen = 0
    for pos in session[i:]:
        g = is_on_ground(pos, cfg)
        if g is None:
            continue
        if g is True:
            seen += 1
            if seen >= need:
                return True
        else:
            return False
    return False  # ran out before confirming — wait for the next window


# --------------------------------------------------------------------------
# One cron run for one aircraft (the former _process_aircraft body)
# --------------------------------------------------------------------------
def process_window(store, icao24, sessions, open_flight, last_completed, nearest, cfg):
    """Walk the sessions of one lookback window, mutating ``store``.

    open_flight: handle dict | None.  last_completed: dict | None.
    """
    def scan_nearest(lat, lon):
        # P4: tighten the snap radius for scan-derived arrivals only.
        mx = cfg.scan_arrival_max_km if cfg.scan_arrival_max_km > 0 else None
        return nearest(lat, lon, max_km=mx)

    def close_scanned(open_flight_handle, arr_result, cs, dep_for_review):
        """Close ``open_flight_handle`` at a scan-derived arrival, honoring P4.
        Returns True if it closed (caller should set open_flight=None)."""
        arr = scan_nearest(arr_result["lat"], arr_result["lon"])
        if not arr:
            return False  # not near a field (P4) — not a real landing; stay open
        review = bool(dep_for_review and arr and dep_for_review == arr)
        store.update_open(open_flight_handle["first_seen"], arr_result["captured_at"],
                          arr=arr, callsign=cs, needs_review=review)
        return True

    prev_session = None
    for session in sessions:
        if not session:
            continue
        first_pos = session[0]
        session_cs = get_session_callsign(session)
        starts_on_ground = is_on_ground(first_pos, cfg)

        # --- session end: did the previous session's tail land? ---
        if open_flight and prev_session:
            landing = detect_landing(prev_session[-15:], nearest, cfg)
            if landing:
                arr = nearest(landing["lat"], landing["lon"])
                dep = open_flight.get("departure_airport_icao")
                review = bool(dep and arr and dep == arr)
                cs = (open_flight.get("callsign") or "").strip() or None
                store.update_open(open_flight["first_seen"], landing["captured_at"],
                                  arr=arr, callsign=cs, needs_review=review)
                open_flight = None

        if open_flight is None:
            if last_completed and last_completed["last_seen"] >= first_pos["captured_at"]:
                continue
            if starts_on_ground is True:
                dep_result = scan_for_departure(session, cfg)
                if dep_result:
                    dep = nearest(dep_result["lat"], dep_result["lon"])
                    of = store.upsert(session_cs, dep, None, dep_result["captured_at"],
                                      session[-1]["captured_at"], False, origin="C1")
                    open_flight = of
                    arr_result = scan_for_arrival_after(session, dep_result["captured_at"], cfg)
                    if arr_result and close_scanned(of, arr_result, session_cs, dep):
                        open_flight = None
            elif starts_on_ground is False:
                dep_info = detect_departure(session[:5], cfg)
                dep, review = None, True
                if dep_info:
                    dep = nearest(dep_info["lat"], dep_info["lon"])
                    if dep:
                        review = False
                # P3: snap a missed departure from a single low sample near a field.
                if not dep and cfg.missed_departure_snap:
                    fp = session[0]
                    if fp.get("altitude_m") is not None and fp["altitude_m"] < cfg.missed_departure_altitude_m:
                        cand = nearest(fp["latitude"], fp["longitude"])
                        if cand:
                            dep, review = cand, False
                if not dep and last_completed:
                    gap = first_pos["captured_at"] - last_completed["last_seen"]
                    if gap <= timedelta(hours=cfg.missed_departure_max_gap_h):
                        dep = (last_completed.get("arrival_airport_icao") or "").strip() or None
                        if dep and dep != "UNKN":
                            review = False
                of = store.upsert(session_cs, dep, None, session[0]["captured_at"],
                                  session[-1]["captured_at"], review, origin="C2")
                open_flight = of
                arr_result = scan_for_arrival_after(session, session[0]["captured_at"], cfg)
                if arr_result:
                    arr = scan_nearest(arr_result["lat"], arr_result["lon"])
                    if arr:
                        review_arr = bool(dep and arr and dep == arr)
                        store.update_open(of["first_seen"], arr_result["captured_at"],
                                          arr=arr, callsign=session_cs, needs_review=review or review_arr)
                        open_flight = None
            # None -> skip
        else:
            flight_cs = (open_flight.get("callsign") or "").strip() or None
            cs_match = callsigns_match(flight_cs, session_cs)
            if starts_on_ground is True:
                landing = detect_landing(store.positions_before(first_pos["captured_at"], 15), nearest, cfg)
                if landing:
                    arr = nearest(landing["lat"], landing["lon"])
                    last_seen = landing["captured_at"]
                else:
                    arr = nearest(first_pos["latitude"], first_pos["longitude"])
                    last_seen = first_pos["captured_at"]
                dep = open_flight.get("departure_airport_icao")
                review = bool(dep and arr and dep == arr)
                store.update_open(open_flight["first_seen"], last_seen, arr=arr,
                                  callsign=session_cs or flight_cs, needs_review=review)
                open_flight = None
                dep_result = scan_for_departure(session, cfg)
                if dep_result:
                    # P5: don't re-open at the airport we just landed at (micro-flight).
                    new_dep = nearest(dep_result["lat"], dep_result["lon"])
                    if not (cfg.min_turnaround_min and new_dep and arr and new_dep == arr
                            and (dep_result["captured_at"] - last_seen)
                            <= timedelta(minutes=cfg.min_turnaround_min)):
                        new_cs = get_session_callsign(
                            [p for p in session if p["captured_at"] >= dep_result["captured_at"]]
                        ) or session_cs
                        of = store.upsert(new_cs, new_dep, None, dep_result["captured_at"],
                                          session[-1]["captured_at"], False, origin="C3/4-redep")
                        open_flight = of
                        arr_result = scan_for_arrival_after(session, dep_result["captured_at"], cfg)
                        if arr_result and close_scanned(of, arr_result, new_cs, new_dep):
                            open_flight = None
            elif starts_on_ground is False:
                if cs_match:
                    store.update_open(open_flight["first_seen"], session[-1]["captured_at"],
                                      callsign=session_cs or flight_cs)
                    arr_result = scan_for_arrival_after(session, session[0]["captured_at"], cfg)
                    if arr_result and close_scanned(
                        open_flight, arr_result, session_cs or flight_cs,
                        open_flight.get("departure_airport_icao")
                    ):
                        open_flight = None
                else:
                    store.update_open(open_flight["first_seen"], session[0]["captured_at"],
                                      arr="UNKN", callsign=flight_cs, needs_review=True)
                    open_flight = None
                    of = store.upsert(session_cs, None, None, session[0]["captured_at"],
                                      session[-1]["captured_at"], True, origin="C6")
                    open_flight = of
                    arr_result = scan_for_arrival_after(session, session[0]["captured_at"], cfg)
                    if arr_result:
                        arr = scan_nearest(arr_result["lat"], arr_result["lon"])
                        if arr:
                            store.update_open(of["first_seen"], arr_result["captured_at"],
                                              arr=arr, callsign=session_cs, needs_review=True)
                            open_flight = None
            elif starts_on_ground is None:
                store.update_open(open_flight["first_seen"], session[-1]["captured_at"],
                                  callsign=session_cs or flight_cs)
        prev_session = session

    # end-of-window: close the last session's tail if it landed, else extend
    if open_flight and sessions and sessions[-1]:
        latest = sessions[-1][-1]
        latest_cs = get_session_callsign(sessions[-1])
        flight_cs = (open_flight.get("callsign") or "").strip() or None
        landing = detect_landing(sessions[-1][-15:], nearest, cfg)
        if landing:
            arr = nearest(landing["lat"], landing["lon"])
            dep = open_flight.get("departure_airport_icao")
            review = bool(dep and arr and dep == arr)
            store.update_open(open_flight["first_seen"], landing["captured_at"],
                              arr=arr, callsign=latest_cs or flight_cs, needs_review=review)
        else:
            store.update_open(open_flight["first_seen"], latest["captured_at"],
                              callsign=latest_cs or flight_cs)
