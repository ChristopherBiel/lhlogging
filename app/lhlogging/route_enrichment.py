"""
Route enrichment — recover dep/arr that position-based detection could not
resolve, using the callsign → canonical route reference (flight_routes).

Three set-based passes (run in order within one transaction, so later passes
see earlier changes):
  1. Normalise the Egelsbach GA strip EDFE → the Frankfurt hub EDDF (an
     airliner is never really at Egelsbach).
  2. Fill a missing/UNKN departure or arrival from flight_routes, keyed on
     callsign. Robust for long-haul legs whose arrival was lost to sparse
     ADS-B coverage and stored as EDDF→UNKN.
  3. Clear needs_review on flights that now match their reference route
     exactly (both airports known, dep ≠ arr) — high-confidence resolutions.

Note: flight_date and duration_minutes are GENERATED from first_seen/last_seen,
so a stale-closed leg keeps its inflated duration; only the airports are fixed.

Used as a recurring job (recent window, see main()) and by the one-off
tools/backfill_routes.py (full history).
"""
import argparse
import sys
from datetime import datetime, timedelta, timezone

from lhlogging import db
from lhlogging.utils import setup_logging

# How far back the recurring job re-checks each run (long-haul legs are
# stale-closed to UNKN ~24h after departure, so a few days covers them).
RECENT_WINDOW_DAYS = 4

_BAD = ("", "UNKN")


def _since_clause(since, alias="f"):
    """Return (sql_fragment, params) restricting to first_seen >= since."""
    if since is None:
        return "", []
    return f" AND {alias}.first_seen >= %s", [since]


def enrich(conn, apply: bool, since=None) -> dict:
    """Run the enrichment passes. Commits if apply, else rolls back.

    Returns a dict of rows affected per pass (exact, since dry-run executes the
    same UPDATEs and rolls them back).
    """
    stats: dict[str, int] = {}
    sc, sp = _since_clause(since)
    # EDFE passes operate on the bare table, alias still works for the clause.

    with conn.cursor() as cur:
        # 1. EDFE → EDDF (departure, then arrival).
        cur.execute(
            "UPDATE flights AS f SET departure_airport_icao = 'EDDF' "
            "WHERE btrim(f.departure_airport_icao) = 'EDFE'" + sc,
            sp,
        )
        stats["edfe_departure"] = cur.rowcount
        cur.execute(
            "UPDATE flights AS f SET arrival_airport_icao = 'EDDF' "
            "WHERE btrim(f.arrival_airport_icao) = 'EDFE'" + sc,
            sp,
        )
        stats["edfe_arrival"] = cur.rowcount

        # 2. Backfill missing/UNKN departure, then arrival, from the reference.
        #    Only touch CLOSED flights (arrival already set) — a NULL arrival
        #    means the flight is still in progress and the detector owns it;
        #    filling it here would "land" an airborne aircraft and block the
        #    detector's close (which matches arrival IS NULL).
        cur.execute(
            "UPDATE flights AS f SET departure_airport_icao = fr.departure_airport_icao "
            "FROM flight_routes fr "
            "WHERE fr.callsign = btrim(f.callsign) "
            "  AND f.arrival_airport_icao IS NOT NULL "
            "  AND (f.departure_airport_icao IS NULL "
            "       OR btrim(f.departure_airport_icao) = ANY(%s))" + sc,
            [list(_BAD)] + sp,
        )
        stats["backfill_departure"] = cur.rowcount
        cur.execute(
            "UPDATE flights AS f SET arrival_airport_icao = fr.arrival_airport_icao "
            "FROM flight_routes fr "
            "WHERE fr.callsign = btrim(f.callsign) "
            "  AND f.arrival_airport_icao IS NOT NULL "
            "  AND btrim(f.arrival_airport_icao) = ANY(%s)" + sc,
            [list(_BAD)] + sp,
        )
        stats["backfill_arrival"] = cur.rowcount

        # 3. Clear needs_review where the flight now matches its reference route.
        cur.execute(
            "UPDATE flights AS f SET needs_review = FALSE "
            "FROM flight_routes fr "
            "WHERE fr.callsign = btrim(f.callsign) "
            "  AND f.needs_review = TRUE "
            "  AND f.departure_airport_icao IS NOT NULL "
            "  AND btrim(f.departure_airport_icao) <> ALL(%s) "
            "  AND f.arrival_airport_icao IS NOT NULL "
            "  AND btrim(f.arrival_airport_icao) <> ALL(%s) "
            "  AND btrim(f.departure_airport_icao) <> btrim(f.arrival_airport_icao) "
            "  AND btrim(f.departure_airport_icao) = btrim(fr.departure_airport_icao) "
            "  AND btrim(f.arrival_airport_icao) = btrim(fr.arrival_airport_icao)" + sc,
            [list(_BAD), list(_BAD)] + sp,
        )
        stats["cleared_review"] = cur.rowcount

    if apply:
        conn.commit()
    else:
        conn.rollback()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich recent flights from flight_routes")
    parser.add_argument("--dry-run", action="store_true", help="Report only; do not write")
    parser.add_argument("--window-days", type=int, default=RECENT_WINDOW_DAYS,
                        help=f"How far back to re-check (default {RECENT_WINDOW_DAYS})")
    args = parser.parse_args()

    logger = setup_logging("route_enrichment")
    apply = not args.dry_run
    since = datetime.now(timezone.utc) - timedelta(days=args.window_days)
    logger.info(
        f"Route enrichment starting ({'APPLY' if apply else 'DRY RUN'}, "
        f"since {since:%Y-%m-%d})"
    )

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    # Skip gracefully if the reference table isn't created yet (migration 004
    # not applied) — avoids erroring every cycle before the schema catches up.
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.flight_routes')")
        if cur.fetchone()[0] is None:
            logger.warning("flight_routes table not found — skipping (apply migration 004)")
            conn.close()
            return 0

    run_id = db.log_batch_start(conn, "route_enrichment")
    try:
        stats = enrich(conn, apply=apply, since=since)
    except Exception as e:
        logger.error(f"Enrichment failed: {e}")
        conn.rollback()
        db.log_batch_finish(conn, run_id, {"status": "error", "error_detail": str(e)})
        conn.close()
        return 1

    total = sum(stats.values())
    logger.info(
        f"Route enrichment {'applied' if apply else 'would apply'}: "
        f"{total} changes — {stats}"
    )
    db.log_batch_finish(conn, run_id, {"flights_upserted": total, "status": "ok"})
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
