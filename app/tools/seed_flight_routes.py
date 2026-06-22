"""
Seed/refresh the flight_routes reference (callsign → canonical route).

Builds a consensus departure/arrival per callsign from "clean" flights
(both airports resolved, not UNKN, dep != arr; the Egelsbach GA strip EDFE
is normalised to the Frankfurt hub EDDF), keeping the most common pairing
when it has enough support. A small CURATED set is then layered on top for
scheduled routes that never get a clean detection — e.g. Johannesburg
(DLH572/573), whose arrival is almost always lost to poor ADS-B coverage and
stored as EDDF→UNKN.

The flight detector and the dashboard use this table to recover routes that
position-based detection could not resolve.

Usage:
    python -m tools.seed_flight_routes              # dry-run (default)
    python -m tools.seed_flight_routes --apply       # write to the DB
"""
import argparse
import sys
from collections import Counter, defaultdict

from lhlogging import db
from lhlogging.utils import setup_logging

# Minimum number of clean observations before a consensus pairing is trusted.
MIN_SUPPORT = 3

# Frankfurt-Egelsbach (GA field ~7 km from EDDF) — airliners that snap here
# are really at the Frankfurt hub.
_ALIAS = {"EDFE": "EDDF"}

# Curated overrides for scheduled routes that never produce a clean detection
# (arrival lost to sparse ADS-B coverage). Layered on top of consensus.
CURATED: dict[str, tuple[str, str]] = {
    "DLH572": ("EDDF", "FAOR"),  # FRA → JNB  (return DLH573)
    "DLH573": ("FAOR", "EDDF"),  # JNB → FRA
}


def _norm(code: str | None) -> str | None:
    code = (code or "").strip().upper()
    if not code or code == "UNKN":
        return None
    return _ALIAS.get(code, code)


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed the flight_routes reference table")
    parser.add_argument("--apply", action="store_true", help="Write to the DB (default is dry-run)")
    args = parser.parse_args()

    logger = setup_logging("seed_flight_routes")
    dry_run = not args.apply
    logger.info(f"Seeding flight_routes ({'DRY RUN' if dry_run else 'APPLY MODE'})")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT TRIM(callsign), departure_airport_icao, arrival_airport_icao
            FROM flights
            WHERE callsign IS NOT NULL AND TRIM(callsign) <> ''
            """
        )
        rows = cur.fetchall()

    # Tally clean (dep, arr) pairings per callsign.
    tally: dict[str, Counter] = defaultdict(Counter)
    for cs, dep, arr in rows:
        dep, arr = _norm(dep), _norm(arr)
        if dep and arr and dep != arr:
            tally[cs][(dep, arr)] += 1

    routes: dict[str, dict] = {}
    for cs, counter in tally.items():
        (dep, arr), support = counter.most_common(1)[0]
        if support >= MIN_SUPPORT:
            routes[cs] = {"dep": dep, "arr": arr, "source": "consensus", "support": support}

    # Curated overrides win (and record their observed support if any).
    for cs, (dep, arr) in CURATED.items():
        support = tally.get(cs, Counter()).get((dep, arr), 0)
        routes[cs] = {"dep": dep, "arr": arr, "source": "curated", "support": support}

    logger.info(
        f"Resolved {len(routes)} callsign routes "
        f"({sum(1 for r in routes.values() if r['source'] == 'curated')} curated, "
        f"from {len(rows)} flight rows)"
    )
    for cs in sorted(CURATED):
        r = routes[cs]
        logger.info(f"  curated {cs}: {r['dep']}→{r['arr']} (clean support={r['support']})")

    if not dry_run:
        with conn.cursor() as cur:
            for cs, r in routes.items():
                cur.execute(
                    """
                    INSERT INTO flight_routes
                        (callsign, departure_airport_icao, arrival_airport_icao, source, support, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (callsign) DO UPDATE SET
                        departure_airport_icao = EXCLUDED.departure_airport_icao,
                        arrival_airport_icao   = EXCLUDED.arrival_airport_icao,
                        source                 = EXCLUDED.source,
                        support                = EXCLUDED.support,
                        updated_at             = NOW()
                    """,
                    (cs, r["dep"], r["arr"], r["source"], r["support"]),
                )
        conn.commit()

    logger.info(f"{'Would write' if dry_run else 'Wrote'} {len(routes)} flight_routes rows")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
