"""
Seed/refresh the flight_routes reference (callsign → canonical route).

Builds a consensus departure/arrival per callsign from "clean" flights
(both airports resolved, not UNKN, dep != arr; the Egelsbach GA strip EDFE
is normalised to the Frankfurt hub EDDF), keeping the most common pairing
when it has enough support. A small CURATED set is then layered on top for
scheduled routes that never get a clean detection — e.g. Johannesburg
(DLH572/573), whose arrival is almost always lost to poor ADS-B coverage and
stored as EDDF→UNKN.

Freshness rules (2026-07, see tools/EDGE_CASES.md — stale consensus made
route_enrichment backfill retired routes):
  * If a callsign has enough RECENT support (--recent-days, default 60), the
    recent window decides — a seasonally re-pointed callsign follows its new
    route instead of being outvoted by history.
  * A CONTESTED callsign (runner-up >= half of the winner within the deciding
    window) is dropped entirely — no fill is better than a coin-flip fill.
  * Consensus rows for callsigns that no longer resolve are DELETED (curated
    rows are never deleted), so enrichment stops using them.

The flight detector and the dashboard use this table to recover routes that
position-based detection could not resolve.

Usage:
    python -m tools.seed_flight_routes              # dry-run (default)
    python -m tools.seed_flight_routes --apply       # write to the DB
"""
import argparse
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from lhlogging import db
from lhlogging.utils import setup_logging

# Minimum number of clean observations before a consensus pairing is trusted.
MIN_SUPPORT = 3

# A pairing is contested when the runner-up has at least this share of the
# winner's support (within the window that decides) — then we trust nothing.
CONTESTED_RATIO = 0.5

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
    parser.add_argument("--recent-days", type=int, default=60,
                        help="Recency window that outvotes all-time history (default 60)")
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
            SELECT TRIM(callsign), departure_airport_icao, arrival_airport_icao, first_seen
            FROM flights
            WHERE callsign IS NOT NULL AND TRIM(callsign) <> ''
            """
        )
        rows = cur.fetchall()

    # Tally clean (dep, arr) pairings per callsign — all-time and recent.
    recent_cutoff = datetime.now(timezone.utc) - timedelta(days=args.recent_days)
    tally: dict[str, Counter] = defaultdict(Counter)
    recent: dict[str, Counter] = defaultdict(Counter)
    for cs, dep, arr, first_seen in rows:
        dep, arr = _norm(dep), _norm(arr)
        if dep and arr and dep != arr:
            tally[cs][(dep, arr)] += 1
            if first_seen is not None and first_seen >= recent_cutoff:
                recent[cs][(dep, arr)] += 1

    routes: dict[str, dict] = {}
    contested: list[str] = []
    corrected: list[str] = []
    for cs, counter in tally.items():
        # the recent window decides when it has enough support on its own
        deciding = recent[cs] if recent[cs].most_common(1) and \
            recent[cs].most_common(1)[0][1] >= MIN_SUPPORT else counter
        top = deciding.most_common(2)
        (dep, arr), support = top[0]
        if support < MIN_SUPPORT:
            continue
        if len(top) > 1 and top[1][1] >= CONTESTED_RATIO * support:
            contested.append(f"{cs} {dep}→{arr} x{support} vs "
                             f"{top[1][0][0]}→{top[1][0][1]} x{top[1][1]}")
            continue
        if deciding is not counter and counter.most_common(1)[0][0] != (dep, arr):
            corrected.append(f"{cs}: all-time {counter.most_common(1)[0][0]} "
                             f"→ recent {dep}→{arr} (x{support})")
        routes[cs] = {"dep": dep, "arr": arr, "source": "consensus", "support": support}

    for line in corrected:
        logger.info(f"  recency correction: {line}")
    if contested:
        logger.info(f"  skipped {len(contested)} contested callsigns "
                    f"(runner-up >= {CONTESTED_RATIO:.0%} of winner): "
                    + "; ".join(contested[:8]))

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

    # Existing consensus rows that no longer resolve (retired route, went
    # contested, or support evaporated) get DELETED so enrichment stops
    # filling from them. Curated rows are never deleted.
    with conn.cursor() as cur:
        cur.execute("SELECT callsign FROM flight_routes WHERE source = 'consensus'")
        stale = sorted({r[0] for r in cur.fetchall()} - set(routes))
    if stale:
        logger.info(f"  {'would retire' if dry_run else 'retiring'} {len(stale)} "
                    f"stale consensus rows: {', '.join(stale[:10])}"
                    + (" …" if len(stale) > 10 else ""))

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
            if stale:
                cur.execute(
                    "DELETE FROM flight_routes WHERE source = 'consensus' AND callsign = ANY(%s)",
                    (stale,),
                )
        conn.commit()

    logger.info(f"{'Would write' if dry_run else 'Wrote'} {len(routes)} flight_routes rows")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
