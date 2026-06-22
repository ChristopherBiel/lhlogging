"""
Shared helpers for local analysis of a flights CSV export.

Pure stdlib — reads the CSV produced by tools/pull_data.sh. Mirrors the
canonical-route logic the dashboard/ingestion use (callsign consensus +
curated overrides, EDFE→EDDF) so offline analysis matches production.
"""
import collections
import csv
from datetime import date
from pathlib import Path

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "tmp" / "flights_export.csv"

BAD = ("", "UNKN")
NORM_ALIAS = {"EDFE": "EDDF"}  # Egelsbach GA strip → Frankfurt hub

# Curated callsign→route overrides for scheduled routes that never get a clean
# detection (arrival lost to sparse ADS-B coverage). Keep in sync with
# app/tools/seed_flight_routes.py.
CURATED = {"DLH572": ("EDDF", "FAOR"), "DLH573": ("FAOR", "EDDF")}

# Arrival ICAO → coarse "turn type" used by the rotation model.
TURN_MAP = {
    "RJTT": "HND", "SAEZ": "EZE", "FAOR": "JNB",
    "KLAX": "USW", "KSFO": "USW",
    "KORD": "USE", "KBOS": "USE", "KIAD": "USE", "KEWR": "USE", "KMIA": "USE", "KIAH": "USE",
    "MMMX": "MEX", "SBGR": "GRU", "SBGL": "GRU",
}


def norm_ap(code):
    """Normalize an airport code; None for unresolved (UNKN/empty)."""
    code = (code or "").strip().upper()
    if not code or code == "UNKN":
        return None
    return NORM_ALIAS.get(code, code)


def turn_type(arr):
    return TURN_MAP.get(arr, "OTHER")


def load(path=DEFAULT_CSV):
    """Load the export rows; exits with a hint if it's missing."""
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"No export at {path} — run tools/pull_data.sh first.")
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def build_callsign_routes(rows, min_support=3):
    """callsign → consensus (dep, arr) from clean legs, plus curated overrides."""
    cons = collections.defaultdict(collections.Counter)
    for r in rows:
        cs = (r["callsign"] or "").strip()
        dep, arr = norm_ap(r["departure_airport_icao"]), norm_ap(r["arrival_airport_icao"])
        if cs and dep and arr and dep != arr:
            cons[cs][(dep, arr)] += 1
    cmap = {
        cs: c.most_common(1)[0][0]
        for cs, c in cons.items()
        if c.most_common(1)[0][1] >= min_support
    }
    cmap.update(CURATED)
    return cmap


def canon_route(row, cmap):
    """Resolve a flight's (dep, arr) via the callsign map, falling back to raw fields."""
    cs = (row["callsign"] or "").strip()
    if cs in cmap:
        return cmap[cs]
    return (norm_ap(row["departure_airport_icao"]), norm_ap(row["arrival_airport_icao"]))


def by_registration(rows, aircraft_type=None):
    """Group rows by registration (optionally filtered to a type), ordered by first_seen."""
    out = collections.defaultdict(list)
    for r in rows:
        if aircraft_type and r["aircraft_type"] != aircraft_type:
            continue
        out[r["registration"]].append(r)
    for reg in out:
        out[reg].sort(key=lambda r: r["first_seen"])
    return out


def outbound_turns(flights, cmap, origin="EDDF"):
    """Chronological [(date, turn_type)] of departures from `origin`, deduped per (day, type)."""
    seq, seen = [], set()
    for r in flights:
        dep, arr = canon_route(r, cmap)
        if dep == origin and arr and arr != origin:
            key = (date.fromisoformat(r["flight_date"]), turn_type(arr))
            if key in seen:
                continue
            seen.add(key)
            seq.append(key)
    return seq
