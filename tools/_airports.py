"""
Offline nearest-airport lookup — a faithful port of db.lookup_nearest_airport
for replaying the flight detector without a database.

Production resolves the nearest airport with PostgreSQL earthdistance
(ll_to_earth / earth_distance, a spherical model). Haversine matches that to
well under a kilometre at these ranges, so offline replay assigns the same
airports. The hub-preference rule (prefer a large_airport within
HUB_PREFERENCE_RADIUS_KM over a nearer GA/medium field) is reproduced exactly.

Pure stdlib (Python 3.9+). Reads tmp/airports_export.csv by default
(tools/pull_positions.sh), or a committed corpus copy via load(path).
"""
import csv
import math
from pathlib import Path

# Defaults mirror app/lhlogging/config.py.
AIRPORT_LOOKUP_RADIUS_KM = 50.0
HUB_PREFERENCE_RADIUS_KM = 15.0

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "tmp" / "airports_export.csv"


def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0088  # mean Earth radius (km), matches earthdistance's sphere
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


class Airports:
    """In-memory airport table with a nearest-airport query + hub preference."""

    def __init__(self, rows):
        # rows: list of (icao_code, type, lat, lon)
        self._rows = rows
        self._large = [r for r in rows if (r[1] or "").strip() == "large_airport"]

    @classmethod
    def load(cls, path=DEFAULT_CSV):
        path = Path(path)
        if not path.exists():
            raise SystemExit(
                f"No airports export at {path} — run tools/pull_positions.sh "
                f"(or pass the committed corpus copy)."
            )
        rows = []
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                try:
                    rows.append((
                        r["icao_code"].strip(), (r.get("type") or "").strip(),
                        float(r["latitude"]), float(r["longitude"]),
                    ))
                except (ValueError, KeyError):
                    continue
        return cls(rows)

    def _nearest(self, lat, lon, candidates):
        best_code, best_type, best_d = None, None, None
        for code, typ, la, lo in candidates:
            d = _haversine_km(lat, lon, la, lo)
            if best_d is None or d < best_d:
                best_code, best_type, best_d = code, typ, d
        return best_code, best_type, best_d

    def nearest(self, lat, lon, max_km=None):
        """Return the nearest airport ICAO, or None — mirrors lookup_nearest_airport.

        With the hub-preference rule: if the nearest field is not a
        large_airport but a large_airport lies within HUB_PREFERENCE_RADIUS_KM,
        the hub is returned instead.
        """
        if lat is None or lon is None:
            return None
        if max_km is None:
            max_km = AIRPORT_LOOKUP_RADIUS_KM
        code, typ, dist = self._nearest(lat, lon, self._rows)
        if code is None or dist > max_km:
            return None
        if typ != "large_airport":
            hub_code, _, hub_dist = self._nearest(lat, lon, self._large)
            if hub_code is not None and hub_dist <= HUB_PREFERENCE_RADIUS_KM:
                return hub_code
        return code
