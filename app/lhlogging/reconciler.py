"""
Hindsight re-segmentation of settled position tracks — the reconciliation
pass core (docs/reconciliation.md, phase R0).

The online detector must decide every 30 minutes on partial, late-arriving
data and can never revise; its remaining failure modes are execution-model
artifacts (tools/EDGE_CASES.md). This module segments a COMPLETE track with
hindsight instead:

  - physical identity, not callsign identity: leg boundaries are confirmed
    stops (parked ground blocks), gap-landings (descend-low .. gap .. climb-
    out), or implausible teleports — a mid-air callsign change never splits
    a leg. A leg's callsign is the modal callsign of its cruise portion
    (crews key the next leg's callsign during descent — the C6 class).
  - corrupt fixes are prefiltered by kinematics against both neighbors
    (ghost/duplicate cruise fixes after landing, parked altitude glitches).
  - every endpoint carries provenance: "observed" (ground/low fix at the
    field), "inferred" (chain continuity or near-field low approach/climb),
    or unknown (honest UNKN/None + needs_review). The reconciler never
    writes a guessed airport — guessing stays route_enrichment's job.

Like detector_core, this module is stdlib-only (no lhlogging.config, no db):
all I/O is injected. ``nearest(lat, lon, max_km=None) -> icao|None`` is the
same airport lookup the detector uses. R0 ships no write path — the offline
driver (tools/reconcile_shadow.py) and the corpus gate (tools/run_corpus.py
--reconciler) consume the segmentation output directly.

A position is the detector_core dict: icao24, callsign, captured_at (aware
datetime), latitude, longitude, altitude_m, velocity_ms, on_ground.
"""
from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field as dc_field
from datetime import datetime, timedelta

from .detector_core import DetectorConfig, is_on_ground

_EARTH_KM = 6371.0088


def _hav_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * _EARTH_KM * math.asin(math.sqrt(a))


def _default_detector_cfg():
    # Production P1 values: distrust on_ground=true while fast or high.
    return DetectorConfig(onground_max_speed_ms=80.0, onground_max_altitude_m=6000.0)


@dataclass(frozen=True)
class ReconcilerConfig:
    detector: DetectorConfig = dc_field(default_factory=_default_detector_cfg)
    # corrupt-fix prefilter: a short run of fixes is dropped when the track
    # jumps impossibly into AND out of it while the surrounding track is
    # mutually consistent (ghost cruise duplicates, teleporting singletons).
    teleport_speed_ms: float = 350.0       # > any airliner ground speed
    teleport_min_km: float = 30.0          # ignore jitter-scale jumps
    stale_floor_s: float = 600.0           # feed staleness: a position may lag
    #                                        this far behind its captured_at
    ghost_run_max_fixes: int = 5
    ghost_run_max_min: float = 15.0
    # stops (parked ground blocks)
    stop_min_span_min: float = 8.0         # block duration that proves a stop
    stop_min_ground_fixes: int = 4         # ... or this many ground fixes
    stop_chain_km: float = 5.0             # ground fixes chain into one block
    stop_noise_radius_km: float = 8.0      # airborne blips within this of both
    #                                        same-field stops are glitch noise
    # gap-landings (the stop happened inside a coverage gap)
    gap_evidence_min: float = 20.0         # gap length needing edge evidence
    gap_vshape_min: float = 30.0           # gap length for pure V-shape splits
    gap_leg_min: float = 45.0              # gap that can hide a WHOLE leg
    #                                        (landing + turnaround + takeoff)
    gap_grounded_min: float = 120.0        # a gap this long whose implied
    min_airborne_speed_ms: float = 80.0    # speed is below sustainable flight
    #                                        cannot have been flown: stop inside
    max_airborne_gap_h: float = 16.0       # no airliner stays up this long:
    #                                        a longer gap always hides a stop
    vshape_max_alt_m: float = 6000.0
    observed_max_alt_m: float = 500.0      # ≤ this near a field = observed
    observed_max_km: float = 8.0           #   (the proximity-landing envelope)
    inferred_max_alt_m: float = 3000.0     # ≤ this + moving the right way +
    inferred_max_km: float = 25.0          #   near a field = inferred
    # callsign attribution
    cruise_callsign_alt_m: float = 6000.0
    descent_callsign_alt_m: float = 3000.0


@dataclass
class RecLeg:
    icao24: str
    callsign: str | None
    departure_airport_icao: str | None     # None = unknown (honest)
    arrival_airport_icao: str | None       # "UNKN" = finalized-unknown; None = open edge
    first_seen: datetime
    last_seen: datetime
    dep_source: str | None = None          # observed | inferred | None
    arr_source: str | None = None
    needs_review: bool = False
    flags: list = dc_field(default_factory=list)
    n_fixes: int = 0
    n_airborne: int = 0


# --------------------------------------------------------------------------
# Corrupt-fix prefilter
# --------------------------------------------------------------------------
def _is_jump(a, b, cfg):
    """Impossible transition between consecutive fixes: teleport or alt spike.

    Horizontal speed is judged against max(dt, stale_floor): the feed serves
    positions that lag captured_at by minutes, so short-dt pairs routinely
    imply 400+ m/s on perfectly healthy cruise tracks.
    """
    dt = (b["captured_at"] - a["captured_at"]).total_seconds()
    if dt <= 0:
        return False
    d_km = _hav_km(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
    if (d_km > cfg.teleport_min_km
            and d_km * 1000 / (dt + cfg.stale_floor_s) > cfg.teleport_speed_ms):
        return True
    aa, ab = a.get("altitude_m"), b.get("altitude_m")
    if aa is not None and ab is not None:
        if abs(ab - aa) > 3000 and abs(ab - aa) / dt > 50:  # > 50 m/s sustained
            return True
    return False


def _drop_spatial_outliers(track, cfg):
    """Drop single fixes that sit far from BOTH neighbors while the neighbors
    sit close to each other — a ghost/duplicate served between healthy fixes
    (e.g. a stale cruise position re-emitted after landing). dt-independent:
    the geometry alone is impossible."""
    if len(track) < 3:
        return list(track), []
    kept, dropped = [track[0]], []
    for prev, p, nxt in zip(track, track[1:], track[2:]):
        d_in = _hav_km(prev["latitude"], prev["longitude"], p["latitude"], p["longitude"])
        d_out = _hav_km(p["latitude"], p["longitude"], nxt["latitude"], nxt["longitude"])
        d_bridge = _hav_km(prev["latitude"], prev["longitude"], nxt["latitude"], nxt["longitude"])
        if (d_in > cfg.teleport_min_km and d_out > cfg.teleport_min_km
                and d_bridge < min(d_in, d_out) / 4):
            dropped.append(p)
        else:
            kept.append(p)
    kept.append(track[-1])
    return kept, dropped


def prefilter_corrupt(track, cfg):
    """Drop short runs the track jumps impossibly into and out of.

    Returns (kept_track, dropped_fixes). Also collapses runs of identical
    (lat, lon, altitude) fixes — the frozen-feed signature — to first + last,
    so leftover frozen duplicates can't seed phantom legs downstream.
    """
    if len(track) < 2:
        return list(track), []
    track, dropped0 = _drop_spatial_outliers(track, cfg)
    # frozen-feed dedupe: keep the first and latest fix of an identical run
    dedup = [track[0]]
    for p in track[1:]:
        q = dedup[-1]
        same = (p["latitude"] == q["latitude"] and p["longitude"] == q["longitude"]
                and p.get("altitude_m") == q.get("altitude_m"))
        prev_same = (len(dedup) >= 2
                     and dedup[-2]["latitude"] == p["latitude"]
                     and dedup[-2]["longitude"] == p["longitude"]
                     and dedup[-2].get("altitude_m") == p.get("altitude_m"))
        if same and prev_same:
            dedup[-1] = p
        else:
            dedup.append(p)

    dropped = list(dropped0)
    kept = dedup
    for _ in range(3):             # ghosts can shadow ghosts; converge fast
        runs = [[kept[0]]]
        for a, b in zip(kept, kept[1:]):
            if _is_jump(a, b, cfg):
                runs.append([b])
            else:
                runs[-1].append(b)
        if len(runs) == 1:
            break
        out, changed = [], False
        for i, run in enumerate(runs):
            small = (len(run) <= cfg.ghost_run_max_fixes and
                     (run[-1]["captured_at"] - run[0]["captured_at"])
                     <= timedelta(minutes=cfg.ghost_run_max_min))
            prev_run = runs[i - 1] if i > 0 else None
            next_run = runs[i + 1] if i + 1 < len(runs) else None
            neighbors_agree = (prev_run is None or next_run is None or
                               not _is_jump(prev_run[-1], next_run[0], cfg))
            bigger_neighbor = ((prev_run and len(prev_run) > len(run)) or
                               (next_run and len(next_run) > len(run)))
            if small and bigger_neighbor and neighbors_agree:
                dropped.extend(run)
                changed = True
            else:
                out.extend(run)
        kept = out
        if not changed or len(kept) < 2:
            break
    return kept, dropped


# --------------------------------------------------------------------------
# Stops (parked ground blocks)
# --------------------------------------------------------------------------
def _trend(track, i, side):
    """Altitude trend at track[i] against its same-side neighbors: 'desc' /
    'climb' / None. Scans past missing and unchanged altitudes (stale feed
    repeats the same level for several fixes) up to a few samples out."""
    a = track[i].get("altitude_m")
    if a is None:
        return None
    j = i - 1 if side == "before" else i + 1
    step = -1 if side == "before" else 1
    for _ in range(6):
        if not 0 <= j < len(track):
            return None
        b = track[j].get("altitude_m")
        if b is not None and b != a:
            if side == "before":
                return "desc" if a < b else "climb"
            return "climb" if b > a else "desc"
        j += step
    return None


def _ground_blocks(track, gclass, cfg):
    blocks = []
    cur = None
    for i, (p, g) in enumerate(zip(track, gclass)):
        if g is True:
            if cur and _hav_km(p["latitude"], p["longitude"], cur["lat"], cur["lon"]) <= cfg.stop_chain_km:
                cur["i1"], cur["n"] = i, cur["n"] + 1
                cur["lat"], cur["lon"] = p["latitude"], p["longitude"]
            else:
                if cur:
                    blocks.append(cur)
                cur = {"i0": i, "i1": i, "n": 1,
                       "lat": p["latitude"], "lon": p["longitude"]}
        elif g is False and cur is not None:
            blocks.append(cur)
            cur = None
        # g None: indeterminate — neither extends nor breaks the block
    if cur:
        blocks.append(cur)
    return blocks


def _is_stop(track, gclass, b, cfg):
    """A ground block is a confirmed stop if it is long/dense enough, sits at
    the track edge, or is bracketed by landing + takeoff evidence (a quick or
    transponder-dark turnaround at a thin-coverage outstation).

    The bracketing evidence needs the right TRENDS, not just low altitude: a
    stale parked fix re-served after departure would otherwise be promoted
    into a phantom stop by the (climbing) fixes around it."""
    t0, t1 = track[b["i0"]]["captured_at"], track[b["i1"]]["captured_at"]
    if (t1 - t0).total_seconds() / 60 >= cfg.stop_min_span_min:
        return True
    if b["n"] >= cfg.stop_min_ground_fixes:
        return True
    if b["i0"] == 0 or b["i1"] == len(track) - 1:
        return True
    # weak block: promote when ARRIVAL-side evidence (a low descending
    # approach, or a dark gap — transponder off means parked) pairs with
    # DEPARTURE-side evidence (a low climbing takeoff, or another dark gap).
    # A stale re-served ground fix cannot show both: it is embedded in
    # continuous airborne track on at least one side.
    landed = departed = False
    for j in range(b["i0"] - 1, -1, -1):
        if gclass[j] is False:
            p = track[j]
            alt = p.get("altitude_m")
            approach = (alt is not None and alt <= cfg.inferred_max_alt_m
                        and _trend(track, j, "before") == "desc")
            gap_before = ((t0 - p["captured_at"]).total_seconds() / 60
                          >= cfg.gap_evidence_min)
            landed = approach or gap_before
            break
    else:
        landed = True                      # no airborne history before the block
    for j in range(b["i1"] + 1, len(track)):
        if gclass[j] is False:
            p = track[j]
            alt = p.get("altitude_m")
            # generous ceiling: thin-coverage stations reacquire the climb-out
            # high (Alps/Sardinia); the anti-stale burden is on the landed side
            takeoff = (alt is not None and alt <= cfg.vshape_max_alt_m
                       and _trend(track, j, "after") == "climb")
            gap_after = ((p["captured_at"] - t1).total_seconds() / 60
                         >= cfg.gap_evidence_min)
            departed = takeoff or gap_after
            break
    return landed and departed


@dataclass
class _Stop:
    i0: int
    i1: int
    t_in: datetime              # landing side (first ground fix)
    t_out: datetime             # departure side (last ground fix)
    lat: float
    lon: float
    n_ground: int


def _find_stops(track, gclass, cfg):
    stops = []
    for b in _ground_blocks(track, gclass, cfg):
        if _is_stop(track, gclass, b, cfg):
            stops.append(_Stop(b["i0"], b["i1"],
                               track[b["i0"]]["captured_at"], track[b["i1"]]["captured_at"],
                               b["lat"], b["lon"], b["n"]))
    # merge same-field stops whose interstitial "airborne" fixes are noise:
    # either they never leave the field (parked altitude glitch), or they are
    # a frozen ghost track — fixes that REPORT flying speed while barely
    # moving positionally (the feed re-serving a stale track between two
    # parked blocks; seen as 20-min "flights" pinned at 3048 m near EDDF)
    merged = []
    for s in stops:
        if merged:
            prev = merged[-1]
            between = track[prev.i1 + 1:s.i0]
            near = all(_hav_km(p["latitude"], p["longitude"], prev.lat, prev.lon)
                       <= cfg.stop_noise_radius_km for p in between)
            ghost = False
            if between and not near:
                vels = sorted(p["velocity_ms"] for p in between
                              if p.get("velocity_ms") and p["velocity_ms"] >= 50)
                if vels:
                    # median implied speed across consecutive pairs (robust to
                    # dark gaps, unlike total-path-over-total-span): a frozen
                    # ghost "flies" at ~0 m/s positionally while reporting 150
                    pair_speeds = sorted(
                        _hav_km(a["latitude"], a["longitude"],
                                b["latitude"], b["longitude"]) * 1000
                        / max((b["captured_at"] - a["captured_at"]).total_seconds(), 1)
                        for a, b in zip(between, between[1:]))
                    med_pair = pair_speeds[len(pair_speeds) // 2] if pair_speeds else 0.0
                    med_vel = vels[len(vels) // 2]
                    ghost = med_pair < 0.25 * med_vel
            if _hav_km(prev.lat, prev.lon, s.lat, s.lon) <= cfg.stop_chain_km and (near or ghost):
                prev.i1, prev.t_out = s.i1, s.t_out
                prev.lat, prev.lon = s.lat, s.lon
                prev.n_ground += s.n_ground
                continue
        merged.append(s)
    return merged


# --------------------------------------------------------------------------
# Boundaries inside airborne spans: gap-landings and teleports
# --------------------------------------------------------------------------
def _endpoint_evidence(track, i, nearest, cfg, want_trend):
    """(field, source) a gap-edge fix supports: observed / inferred / nothing."""
    p = track[i]
    alt = p.get("altitude_m")
    if alt is not None and alt <= cfg.observed_max_alt_m:
        f = nearest(p["latitude"], p["longitude"], max_km=cfg.observed_max_km)
        if f:
            return f, "observed"
    if (alt is not None and alt <= cfg.inferred_max_alt_m
            and _trend(track, i, "before" if want_trend == "desc" else "after") == want_trend):
        f = nearest(p["latitude"], p["longitude"], max_km=cfg.inferred_max_km)
        if f:
            return f, "inferred"
    return None, None


def _gap_boundaries(track, gclass, nearest, cfg, stop_fields):
    """Boundary descriptors for gaps/teleports between consecutive fixes that
    the stop anchors don't already explain. ``stop_fields`` maps a track index
    inside a confirmed stop to that stop's resolved field."""
    out = []
    for i in range(len(track) - 1):
        a, b = track[i], track[i + 1]
        a_ground, b_ground = gclass[i] is True, gclass[i + 1] is True
        if a_ground and b_ground:
            continue                       # inside / between ground blocks
        gap_min = (b["captured_at"] - a["captured_at"]).total_seconds() / 60
        if gap_min < cfg.gap_evidence_min:
            continue                       # ordinary sampling cadence
        if i in stop_fields or i + 1 in stop_fields:
            # long gap into/out of a confirmed stop — normally the stop is the
            # anchor, UNLESS (a) the airborne side shows a landing/takeoff at
            # a DIFFERENT field and the gap can hide a whole leg (a rotation
            # lived inside it), or (b) the gap is physically un-flyable (an
            # entire dark leg to/from an uncovered outstation) — then split
            # honestly with UNKN. A low fix near the stop's own field is just
            # the ordinary departure/arrival.
            if gap_min < cfg.gap_leg_min:
                continue
            d_km = _hav_km(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
            if d_km <= cfg.stop_noise_radius_km:
                continue
            grounded = ((gap_min >= cfg.gap_grounded_min
                         and d_km * 1000 / (gap_min * 60) < cfg.min_airborne_speed_ms)
                        or gap_min >= cfg.max_airborne_gap_h * 60)
            if b_ground:                   # air .. gap .. (distant) stop
                stop_f = stop_fields.get(i + 1)
                arr_f, arr_src = _endpoint_evidence(track, i, nearest, cfg, "desc")
                if arr_f and arr_f != stop_f:
                    out.append({"i_left": i, "i_right": i + 1, "kind": "gap",
                                "arr": arr_f, "arr_src": arr_src,
                                "dep": arr_f, "dep_src": "inferred",
                                "asserts_leg": True})
                elif grounded and not arr_f:
                    out.append({"i_left": i, "i_right": i + 1, "kind": "gap",
                                "arr": "UNKN", "arr_src": None,
                                "dep": None, "dep_src": None,
                                "asserts_leg": True})
            else:                          # (distant) stop .. gap .. air
                stop_f = stop_fields.get(i)
                dep_f, dep_src = _endpoint_evidence(track, i + 1, nearest, cfg, "climb")
                if dep_f and dep_f != stop_f:
                    out.append({"i_left": i, "i_right": i + 1, "kind": "gap",
                                "arr": dep_f, "arr_src": "inferred",
                                "dep": dep_f, "dep_src": dep_src,
                                "asserts_leg": True})
                elif grounded and not dep_f:
                    out.append({"i_left": i, "i_right": i + 1, "kind": "gap",
                                "arr": "UNKN", "arr_src": None,
                                "dep": None, "dep_src": None,
                                "asserts_leg": True})
            continue
        if a_ground or b_ground:
            continue                       # weak non-stop block edges: leave alone
        if _is_jump(a, b, cfg):
            out.append({"i_left": i, "i_right": i + 1, "kind": "teleport",
                        "arr": "UNKN", "arr_src": None, "dep": None, "dep_src": None})
            continue
        arr_f, arr_src = _endpoint_evidence(track, i, nearest, cfg, "desc")
        dep_f, dep_src = _endpoint_evidence(track, i + 1, nearest, cfg, "climb")
        vshape = (gap_min >= cfg.gap_vshape_min
                  and a.get("altitude_m") is not None and b.get("altitude_m") is not None
                  and a["altitude_m"] <= cfg.vshape_max_alt_m
                  and b["altitude_m"] <= cfg.vshape_max_alt_m
                  and _trend(track, i, "before") == "desc"
                  and _trend(track, i + 1, "after") == "climb")
        # or the aircraft cannot have spent the whole gap airborne: hours of
        # gap covering almost no ground (an outstation with zero coverage —
        # both edges may sit at cruise, e.g. overnights at dark outstations)
        d_km = _hav_km(a["latitude"], a["longitude"], b["latitude"], b["longitude"])
        grounded = ((gap_min >= cfg.gap_grounded_min
                     and d_km * 1000 / (gap_min * 60) < cfg.min_airborne_speed_ms)
                    or gap_min >= cfg.max_airborne_gap_h * 60)
        # or a ONE-SIDED V at leg scale: vanishing in a committed descent (or
        # appearing in a low climb-out) with hours of gap means a landing
        # (takeoff) happened inside it even if the far edge is at cruise —
        # the FRA..NBO shape: lost at 5,800 m descending 60 km out,
        # reacquired 3,200 km into the return flight.
        long_gap = gap_min >= cfg.gap_grounded_min
        v_left = (long_gap and a.get("altitude_m") is not None
                  and a["altitude_m"] <= cfg.vshape_max_alt_m
                  and _trend(track, i, "before") == "desc")
        v_right = (long_gap and b.get("altitude_m") is not None
                   and b["altitude_m"] <= cfg.vshape_max_alt_m
                   and _trend(track, i + 1, "after") == "climb")
        # fire only on decisive evidence: an OBSERVED-grade landing, any
        # takeoff signature (low + climbing after the gap), a V-shape, or a
        # physically un-flyable gap. A lone inferred-grade descent must NOT
        # split an approach that merely has a coverage hiccup.
        if arr_src == "observed" or dep_f or vshape or grounded or v_left or v_right:
            out.append({"i_left": i, "i_right": i + 1, "kind": "gap",
                        "arr": arr_f or "UNKN", "arr_src": arr_src,
                        "dep": dep_f, "dep_src": dep_src})
    return out


# --------------------------------------------------------------------------
# Leg assembly
# --------------------------------------------------------------------------
def _modal_callsign(track, lo, hi, cfg):
    """Modal callsign of the cruise portion of track[lo..hi] (inclusive).
    Falls back to non-descent fixes, then to any fix with a callsign."""
    for min_alt in (cfg.cruise_callsign_alt_m, cfg.descent_callsign_alt_m, None):
        votes = Counter()
        for p in track[lo:hi + 1]:
            cs = (p.get("callsign") or "").strip()
            alt = p.get("altitude_m")
            if cs and (min_alt is None or (alt is not None and alt >= min_alt)):
                votes[cs] += 1
        if votes:
            return votes.most_common(1)[0][0]
    return None


def reconcile_track(positions, nearest, cfg=None):
    """Segment one aircraft's full track with hindsight -> list[RecLeg].

    ``positions`` must be time-sorted. The caller owns window/lag trimming
    (never feed the live edge younger than the reconciliation lag if the
    result is meant to be settled). The final leg is left OPEN (arr None)
    when the track ends airborne — the online detector owns the live edge.
    """
    cfg = cfg or ReconcilerConfig()
    dcfg = cfg.detector
    if not positions:
        return []
    icao24 = positions[0]["icao24"]

    track, dropped = prefilter_corrupt(list(positions), cfg)
    if len(track) < 2:
        return []
    gclass = [is_on_ground(p, dcfg) for p in track]
    stops = _find_stops(track, gclass, cfg)

    anchors = []
    stop_fields = {}
    for s in stops:
        f = nearest(s.lat, s.lon)
        for i in range(s.i0, s.i1 + 1):
            stop_fields[i] = f
        anchors.append({"i_left": s.i0, "i_right": s.i1, "kind": "stop",
                        "arr": f or "UNKN", "arr_src": "observed" if f else None,
                        "dep": f, "dep_src": "observed" if f else None,
                        "t_in": s.t_in, "t_out": s.t_out,
                        "lat": s.lat, "lon": s.lon})
    anchors.extend(_gap_boundaries(track, gclass, nearest, cfg, stop_fields))
    anchors.sort(key=lambda x: x["i_left"])

    # endurance backstop: no airliner stays airborne past max_airborne_gap_h.
    # An inter-anchor span longer than that hides at least one stop even when
    # every individual gap looked flyable — split at the largest internal gap
    # (honest UNKN/None unless the edges carry evidence), repeat until sane.
    for _ in range(8):
        spans = []
        edges = ([{"i_right": 0}] + anchors + [{"i_left": len(track) - 1}])
        for left, right in zip(edges, edges[1:]):
            lo, hi = left.get("i_right", 0), right.get("i_left", len(track) - 1)
            if hi <= lo:
                continue
            if ((track[hi]["captured_at"] - track[lo]["captured_at"])
                    > timedelta(hours=cfg.max_airborne_gap_h)):
                spans.append((lo, hi))
        if not spans:
            break
        added = False
        for lo, hi in spans:
            j = max(range(lo, hi), key=lambda k: track[k + 1]["captured_at"] - track[k]["captured_at"])
            gap_min = (track[j + 1]["captured_at"] - track[j]["captured_at"]).total_seconds() / 60
            if gap_min < cfg.gap_evidence_min:
                continue
            arr_f, arr_src = _endpoint_evidence(track, j, nearest, cfg, "desc")
            dep_f, dep_src = _endpoint_evidence(track, j + 1, nearest, cfg, "climb")
            anchors.append({"i_left": j, "i_right": j + 1, "kind": "gap",
                            "arr": arr_f or "UNKN", "arr_src": arr_src,
                            "dep": dep_f, "dep_src": dep_src})
            added = True
        if not added:
            break
        anchors.sort(key=lambda x: x["i_left"])

    # coalesce boundaries that crowd one physical stop: several gap signals
    # (edge evidence, V-shapes, the endurance backstop) can fire around the
    # same dark turnaround and would sandwich micro-legs between them. Within
    # a 15-minute window they describe ONE stop: keep the earliest arr side
    # and the latest dep side; a confirmed stop anchor absorbs gap signals.
    coalesced = []
    for a in anchors:
        if coalesced:
            prev = coalesced[-1]
            t_prev = track[min(prev["i_right"], len(track) - 1)]["captured_at"]
            t_cur = track[a["i_left"]]["captured_at"]
            if (t_cur - t_prev) < timedelta(minutes=15):
                if a["kind"] == "stop" and prev["kind"] != "stop":
                    a = dict(a)
                    a["i_left"] = min(a["i_left"], prev["i_left"])
                    a["t_in"] = min(a["t_in"], track[prev["i_left"]]["captured_at"])
                    coalesced[-1] = a          # the stop wins, keep its fields
                    continue
                if prev["kind"] == "stop":
                    # stop absorbs the gap signal; the departure edge moves to
                    # the far side of the absorbed gap (a dark parked period —
                    # possibly hiding an E13 uncaptured rotation, which no
                    # amount of segmentation can conjure back)
                    prev["i_right"] = max(prev["i_right"], a["i_right"])
                    prev["t_out"] = max(prev["t_out"],
                                        track[min(prev["i_right"], len(track) - 1)]["captured_at"])
                    continue
                prev["i_right"] = a["i_right"]  # gap+gap: arr from first,
                prev["dep"] = a.get("dep")      # dep from last
                prev["dep_src"] = a.get("dep_src")
                continue
        coalesced.append(a)
    anchors = coalesced

    legs = []
    # track edges: honor landing/takeoff evidence at the cut (the slice may
    # start mid-climb or end mid-final); otherwise dep=None / arr=None (open)
    dep0 = src0 = arrN = srcN = None
    if gclass[0] is not True:
        dep0, src0 = _endpoint_evidence(track, 0, nearest, cfg, "climb")
    if gclass[-1] is not True:
        arrN, srcN = _endpoint_evidence(track, len(track) - 1, nearest, cfg, "desc")
    edge_start = {"i_left": -1, "i_right": 0, "kind": "edge", "dep": dep0, "dep_src": src0}
    edge_end = {"i_left": len(track) - 1, "i_right": len(track), "kind": "edge",
                "arr": arrN, "arr_src": srcN}
    bounds = [edge_start] + anchors + [edge_end]
    dropped_times = [(p["captured_at"]) for p in dropped]
    for left, right in zip(bounds, bounds[1:]):
        lo = left["i_right"] if left["kind"] in ("stop", "edge") else left["i_right"]
        hi = right["i_left"]
        if left["kind"] == "gap" or left["kind"] == "teleport":
            lo = left["i_right"]
        if right["kind"] in ("gap", "teleport"):
            hi = right["i_left"]
        seg = track[max(lo, 0):hi + 1] if hi >= lo else []
        n_air = sum(1 for g in gclass[max(lo, 0):hi + 1] if g is False)
        asserted = left.get("asserts_leg") or right.get("asserts_leg") \
            or left["kind"] in ("gap", "teleport") or right["kind"] in ("gap", "teleport")
        if not seg and not asserted:
            continue
        if not asserted and n_air == 0:
            # ground shuffle between anchors: real only if it changed airports
            if not (left.get("dep") and right.get("arr") and left["dep"] != right["arr"]):
                continue
        first_seen = left.get("t_out") or (seg[0]["captured_at"] if seg
                                           else track[max(lo - 1, 0)]["captured_at"])
        last_seen = right.get("t_in") or (seg[-1]["captured_at"] if seg
                                          else track[min(hi + 1, len(track) - 1)]["captured_at"])
        flags = []
        if left["kind"] == "teleport" or right["kind"] == "teleport":
            flags.append("teleport")
        n_drop = sum(1 for t in dropped_times if first_seen <= t <= last_seen)
        if n_drop:
            flags.append(f"prefiltered={n_drop}")
        legs.append(RecLeg(
            icao24=icao24,
            callsign=_modal_callsign(track, max(lo, 0), hi, cfg) if seg else None,
            departure_airport_icao=left.get("dep"),
            arrival_airport_icao=right.get("arr"),
            first_seen=first_seen, last_seen=last_seen,
            dep_source=left.get("dep_src"), arr_source=right.get("arr_src"),
            flags=flags, n_fixes=len(seg), n_airborne=n_air,
        ))

    # chain-continuity inference: an unknown dep after an observed arrival is
    # that arrival (the aircraft was on the ground there — physical identity)
    for prev, leg in zip(legs, legs[1:]):
        if (leg.departure_airport_icao is None
                and prev.arrival_airport_icao not in (None, "UNKN")
                and prev.arr_source == "observed"):
            leg.departure_airport_icao = prev.arrival_airport_icao
            leg.dep_source = "inferred"

    for leg in legs:
        leg.needs_review = bool(
            leg.departure_airport_icao is None
            or leg.arrival_airport_icao == "UNKN"
            or "teleport" in leg.flags
            or (leg.departure_airport_icao is not None
                and leg.departure_airport_icao == leg.arrival_airport_icao)
        )
    return legs
