"""
One-off tool: enrich the entire flights history from the flight_routes
reference (callsign → canonical route).

Normalises EDFE→EDDF, fills missing/UNKN departure & arrival airports from the
callsign reference, and clears needs_review on flights that then match their
reference route exactly. Same logic as the recurring lhlogging.route_enrichment
job, but over all history rather than a recent window.

Run tools/seed_flight_routes.py first so flight_routes is populated.

Usage:
    python -m tools.backfill_routes              # dry-run (default)
    python -m tools.backfill_routes --apply       # write to the DB
"""
import argparse
import sys

from lhlogging import db, route_enrichment
from lhlogging.utils import setup_logging


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill all flights from flight_routes")
    parser.add_argument("--apply", action="store_true", help="Write to the DB (default is dry-run)")
    args = parser.parse_args()

    logger = setup_logging("backfill_routes")
    logger.info(f"Route backfill starting ({'APPLY MODE' if args.apply else 'DRY RUN'})")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    try:
        stats = route_enrichment.enrich(conn, apply=args.apply, since=None)
    except Exception as e:
        logger.critical(f"Backfill failed: {e}")
        conn.rollback()
        conn.close()
        return 1

    for name, n in stats.items():
        logger.info(f"  {name}: {n}")
    logger.info(
        f"Backfill {'applied' if args.apply else 'would apply'}: "
        f"{sum(stats.values())} total changes"
    )
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
