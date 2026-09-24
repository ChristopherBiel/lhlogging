"""
Leg layer — collapse the per-pass FIS observations of one scheduled flight
(flight_date, airline, flight_number) into a single outcome row: which tail
actually flew it (truth), which tails were published at which lead time, and
how often it changed. It is the single source of truth for that logic, shared
by the collector (fetch_flightstatus.py fills the `fis_legs` table from it after
every run) and the offline tools (tools/build_leg_outcomes.py and friends
replay it over a CSV export) — same pattern as app/lhlogging/detector_core.py.

Stdlib only, and nothing here touches a database: it has to load both in the
collector image (Python 3.10) and in the local harness (Python 3.9).

An observation is a dict with at least:
    observed_at        aware datetime (true UTC) of the look
    found              bool
    registration       normalised tail ('D-ABYN') or '' / None
    dep_scheduled      FIS scheduled departure — airport-LOCAL wall clock
    arr_scheduled      stamped +0000 (FIS publishes no offset), see dep_utc()
    dep_airport_iata, arr_airport_iata, overall_status, seed_type
    fleet_type         ICAO type via the `aircraft` table ('' when unknown)
    fis_type           FIS free-text aircraftType (fallback for fleet_type)
    flight_duration    ISO-8601 block time from the payload ('PT12H40M'), optional
"""
from __future__ import annotations

import collections
import statistics
from datetime import timedelta, timezone
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")

# Lead times (hours before departure) the outcome row snapshots the published
# tail at. Dense near departure, where the looks are dense too; out to 216h
# (D+9) because FIS publishes that far and the deep tier is probed there.
BANDS = [216, 192, 168, 144, 120, 96, 72, 48, 36, 24, 18, 12, 6, 3]

# Fleet types the booking tool is about (the FIS catalog also drags in
# narrowbody substitutions and chained short-haul legs, which are not).
WIDEBODY = ("B748", "A388", "B789", "B788", "B78X", "A359", "A35K")

# FIS free-text aircraftType -> fleet code, for the few tails the local
# `aircraft` table has never seen (new deliveries, wet-lease).
FIS_TYPE_MAP = {
    "boeing 747-8": "B748", "airbus a380-800": "A388", "boeing 787-9": "B789",
    "boeing 787-8": "B788", "boeing 787-10": "B78X", "boeing 787": "B789",
    "airbus a350-900": "A359", "airbus a350-1000": "A35K", "airbus a350": "A359",
}

TERMINAL = ("ARRIVED", "DIVERTED")  # the tail in these rows actually flew it

# Every LH widebody leg touches one of these; they share the Frankfurt clock.
# Mirrors _DE_HUBS in dashboard/app.py.
DE_HUBS = {"FRA", "MUC", "DUS", "BER", "HAM", "STR", "CGN", "NUE", "LEJ", "TXL"}


def iso_duration_min(s):
    """'PT12H40M' / 'PT2H' / 'PT55M' -> minutes; None when absent/unparseable."""
    if not s or not s.startswith("PT"):
        return None
    h = m = 0
    num = ""
    for ch in s[2:]:
        if ch.isdigit():
            num += ch
        elif ch == "H":
            h, num = int(num or 0), ""
        elif ch == "M":
            m, num = int(num or 0), ""
    return h * 60 + m


def _berlin_to_utc(local_stamped):
    """A local Frankfurt wall-clock time that FIS stamped +0000 -> true UTC.

    Read the wall clock in UTC first: a driver may hand the value back in its
    session time zone, and the digits FIS meant are the +0000 ones."""
    wall = local_stamped.astimezone(timezone.utc).replace(tzinfo=None)
    return wall.replace(tzinfo=BERLIN).astimezone(timezone.utc)


def dep_utc(dep_sched, arr_sched, dep, arr, duration_min):
    """True UTC departure of a FIS leg.

    FIS times are the airport's local wall clock stamped +0000, and the payload
    carries no offset. Every widebody leg touches a German hub, so anchor on it:
    a hub departure converts directly; a hub arrival converts and backs off the
    block time. A leg touching no German hub (rare triangle legs) keeps the FIS
    clock, i.e. is off by the outstation's UTC offset.
    """
    if dep_sched is None:
        return None
    if dep in DE_HUBS:
        return _berlin_to_utc(dep_sched)
    if arr in DE_HUBS and arr_sched is not None and duration_min:
        return _berlin_to_utc(arr_sched) - timedelta(minutes=duration_min)
    return dep_sched


def fleet_type(row):
    t = (row.get("fleet_type") or "").strip().upper()
    if t:
        return t
    return FIS_TYPE_MAP.get((row.get("fis_type") or "").strip().lower(), "")


def modal(values):
    """Most common value, ties broken by last occurrence (latest plan wins)."""
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    counts = collections.Counter(vals)
    best = max(counts.values())
    for v in reversed(vals):
        if counts[v] == best:
            return v


def tail_at(timeline, first_lead_h, lead_h):
    """Tail published at `lead_h` hours before departure, from a collapsed
    timeline [[lead_h, tail], ...] (descending lead, one entry per change).
    None when our first look came later than that."""
    if first_lead_h is None or first_lead_h < lead_h:
        return None
    tail = None
    for lead, reg in timeline:
        if lead >= lead_h:
            tail = reg
        else:
            break
    return tail


def build_leg(key, obs):
    """Collapse one leg's snapshot sequence into an outcome row + change rows.

    The row carries both the analysis columns the offline tools report on
    (tail_at_<H>h / hold_<H>h per BANDS, churn, sampling density, settle
    point) and the columns `fis_legs` stores (latest/first look, collapsed
    timeline). Returns (None, []) for a leg FIS never returned.
    """
    flight_date, airline, flight_number = key
    obs = sorted(obs, key=lambda r: r["observed_at"])
    found = [r for r in obs if r["found"]]
    if not found:
        return None, []

    dep_ap = modal([r["dep_airport_iata"] for r in found])
    arr_ap = modal([r["arr_airport_iata"] for r in found])
    duration = modal([iso_duration_min(r.get("flight_duration")) for r in found])

    # Lead-time reference: the modal scheduled departure across all looks. A
    # single re-timed snapshot (or a post-hoc row) can't shift the whole leg.
    dep_local = modal([r["dep_scheduled"] for r in found])
    if dep_local is None:
        return None, []
    arr_local = modal([r.get("arr_scheduled") for r in found])
    dep_ref = dep_utc(dep_local, arr_local, dep_ap, arr_ap, duration)

    for r in found:
        r["lead_h"] = (dep_ref - r["observed_at"]).total_seconds() / 3600.0

    # Truth = the tail in a terminal snapshot. Only legs that have one are
    # labelable; the rest are still-in-the-future legs (useful as features).
    terminal = [r for r in found if (r["overall_status"] or "").upper() in TERMINAL
                and r["registration"]]
    truth = terminal[-1] if terminal else None
    cancelled = any((r["overall_status"] or "").upper() == "CANCELLED" for r in found)

    pre = [r for r in found if r["lead_h"] > 0 and r["registration"]]
    post_dep_only = bool(truth) and not pre
    latest = found[-1]

    row = {
        "flight_date": flight_date, "airline": airline, "flight_number": flight_number,
        "dep_airport": dep_ap,
        "arr_airport": arr_ap,
        "dep_scheduled_utc": dep_ref.isoformat(),
        "dep_hour_berlin": dep_ref.astimezone(BERLIN).hour,
        "dep_dow": dep_ref.astimezone(BERLIN).isoweekday(),
        "seed_type": modal([r["seed_type"] for r in found]),
        "truth_tail": truth["registration"] if truth else "",
        "truth_status": (truth["overall_status"] or "").upper() if truth else "",
        "cancelled": int(cancelled),
        "n_obs": len(found),
        "n_obs_pre_dep": len(pre),
        "post_dep_only": int(post_dep_only),
    }

    # Type of the leg: what actually flew it, else what is currently published.
    types = [fleet_type(r) for r in pre if fleet_type(r)]
    truth_type = fleet_type(truth) if truth else ""
    row["fleet_type"] = truth_type or (types[-1] if types else "")
    row["type_changed"] = int(bool(truth_type) and bool(types)
                              and any(t != truth_type for t in types))

    # --- lead-time bands: the tail as it was published at each horizon -------
    for b in BANDS:
        at = [r for r in pre if r["lead_h"] >= b]
        snap = at[-1] if at else None
        row["tail_at_%dh" % b] = snap["registration"] if snap else ""
        row["stale_%dh" % b] = round(snap["lead_h"] - b, 1) if snap else ""
        if snap and truth:
            row["hold_%dh" % b] = int(snap["registration"] == truth["registration"])
        else:
            row["hold_%dh" % b] = ""

    # --- churn --------------------------------------------------------------
    seq = [r["registration"] for r in pre]
    changes = []
    for i in range(1, len(pre)):
        if pre[i]["registration"] == pre[i - 1]["registration"]:
            continue
        a, b = pre[i - 1], pre[i]
        changes.append({
            "flight_date": flight_date, "airline": airline, "flight_number": flight_number,
            "dep_airport": row["dep_airport"], "arr_airport": row["arr_airport"],
            "fleet_type": row["fleet_type"],
            "dep_scheduled_utc": dep_ref.isoformat(),
            "from_tail": a["registration"], "to_tail": b["registration"],
            "from_type": fleet_type(a), "to_type": fleet_type(b),
            # the change happened somewhere in this interval — we cannot know where
            "seen_after_utc": a["observed_at"].isoformat(),
            "seen_before_utc": b["observed_at"].isoformat(),
            "lead_h_hi": round(a["lead_h"], 2),   # lead at the last look showing the old tail
            "lead_h_lo": round(b["lead_h"], 2),   # lead at the look that revealed the new one
            "bracket_h": round(a["lead_h"] - b["lead_h"], 2),
            "is_final": int(bool(truth) and b["registration"] == truth["registration"]),
        })
    row["n_changes"] = len(changes)
    row["n_distinct_tails"] = len(set(seq))
    row["first_tail"] = seq[0] if seq else ""
    row["first_lead_h"] = round(pre[0]["lead_h"], 1) if pre else ""
    row["last_tail"] = seq[-1] if seq else ""
    row["last_lead_h"] = round(pre[-1]["lead_h"], 1) if pre else ""
    row["reassigned"] = int(bool(truth) and bool(seq) and seq[-1] != truth["registration"])
    row["missed_pre_dep"] = row["reassigned"]  # change we only learned post-departure

    gaps = [(pre[i]["observed_at"] - pre[i - 1]["observed_at"]).total_seconds() / 3600.0
            for i in range(1, len(pre))]
    row["median_gap_h"] = round(statistics.median(gaps), 1) if gaps else ""
    row["max_gap_h"] = round(max(gaps), 1) if gaps else ""

    # --- settle point: when did the truth tail arrive and stay? -------------
    row["settle_lead_h"] = row["settle_bracket_h"] = row["settle_censored"] = ""
    if truth and pre:
        tail = truth["registration"]
        idx = len(pre)
        while idx > 0 and pre[idx - 1]["registration"] == tail:
            idx -= 1
        if idx < len(pre):  # the truth tail is published in some pre-dep look
            row["settle_lead_h"] = round(pre[idx]["lead_h"], 1)
            if idx == 0:
                row["settle_censored"] = 1  # already right at our first look
                row["settle_bracket_h"] = ""
            else:
                row["settle_censored"] = 0
                row["settle_bracket_h"] = round(pre[idx - 1]["lead_h"] - pre[idx]["lead_h"], 2)
        else:
            row["settle_lead_h"] = 0  # never published before departure
            row["settle_censored"] = 0

    # --- storage columns (fis_legs) -----------------------------------------
    # The published-tail history, collapsed to one entry per change: enough to
    # answer "which tail was published at lead H" (tail_at) without the raw looks.
    timeline = []
    for r in pre:
        if not timeline or timeline[-1][1] != r["registration"]:
            timeline.append([round(r["lead_h"], 2), r["registration"]])
    row.update({
        "dep_sched_local": dep_local.isoformat(),
        "arr_sched_local": arr_local.isoformat() if arr_local else "",
        "duration_min": duration if duration is not None else "",
        "latest_tail": latest["registration"] or "",
        "latest_status": (latest["overall_status"] or "").upper(),
        "latest_observed_at": latest["observed_at"].isoformat(),
        "first_observed_at": found[0]["observed_at"].isoformat(),
        "timeline": timeline,
    })
    return row, changes
