"""
Repair gap-split flight pairs created by the on_ground cruise-snap bug
(one real flight stored as A→phantom + phantom→B). Dry-run by default, like
tools/seed_flight_routes.py and lhlogging/route_enrichment.py.

A split pair = consecutive legs of one aircraft + same callsign where
leg1.arr == leg2.dep and that shared airport is NOT an endpoint of the
callsign's reference route (flight_routes). For each candidate two modes:

  --window-days N (default 30, positions retained): classify the boundary from
     raw positions (the same physics as tools/classify_splits.py):
       CRUISE_SNAP  airborne-by-physics at the phantom, no parked samples near
                    it  -> MERGE: leg1.arr=leg2.arr, leg1.last_seen=leg2.last_seen,
                    delete leg2.
       REAL_STOP    >=2 near-stationary (vel<15 m/s) samples within 10km of the
                    phantom -> leave (the aircraft genuinely landed there).
       AMBIGUOUS    -> flag needs_review on both, don't merge.

  --flag-historical: for splits OLDER than the retention window (no positions to
     verify), set needs_review=true on both fragments via flight_routes. Never
     merges (can't confirm), never deletes. Reversible.

Requires the flight_routes table (migration 004); skips gracefully if absent.

Usage:
    python -m tools.repair_gap_splits                       # dry-run, recent
    python -m tools.repair_gap_splits --apply               # merge recent snaps
    python -m tools.repair_gap_splits --flag-historical --apply
"""
import argparse
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from lhlogging import config, db
from lhlogging.utils import setup_logging

_BAD = ("", "UNKN")
_ALIAS = {"EDFE": "EDDF"}


def _norm(code):
    code = (code or "").strip().upper()
    if not code or code == "UNKN":
        return None
    return _ALIAS.get(code, code)


def _haversine_km(la1, lo1, la2, lo2):
    import math
    R = 6371.0088
    p = math.radians
    a = (math.sin((p(la2) - p(la1)) / 2) ** 2
         + math.cos(p(la1)) * math.cos(p(la2)) * math.sin((p(lo2) - p(lo1)) / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def _load_routes(conn):
    """callsign -> {endpoints} from flight_routes, or None if the table is absent."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('public.flight_routes')")
        if cur.fetchone()[0] is None:
            return None
        cur.execute("SELECT btrim(callsign), departure_airport_icao, arrival_airport_icao FROM flight_routes")
        out = {}
        for cs, dep, arr in cur.fetchall():
            out[cs] = {_norm(dep), _norm(arr)} - {None}
        return out


def _find_pairs(conn, since):
    """Return candidate split pairs: (icao24, cs, l1, l2) dicts since `since`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT icao24, btrim(callsign), departure_airport_icao, arrival_airport_icao,
                   first_seen, last_seen
            FROM flights
            WHERE callsign IS NOT NULL AND btrim(callsign) <> ''
              AND first_seen >= %s
            ORDER BY icao24, first_seen
            """,
            (since,),
        )
        rows = cur.fetchall()
    by_ac = defaultdict(list)
    for r in rows:
        by_ac[r[0]].append({"icao24": r[0], "cs": r[1], "dep": r[2], "arr": r[3],
                             "first_seen": r[4], "last_seen": r[5]})
    pairs = []
    for legs in by_ac.values():
        for l1, l2 in zip(legs, legs[1:]):
            if l1["cs"] != l2["cs"]:
                continue
            shared = _norm(l1["arr"])
            if not shared or shared != _norm(l2["dep"]):
                continue
            pairs.append((shared, l1, l2))
    return pairs


def _airport_ll(conn, icao):
    with conn.cursor() as cur:
        cur.execute("SELECT latitude, longitude FROM airports WHERE btrim(icao_code) = %s", (icao,))
        r = cur.fetchone()
    return (float(r[0]), float(r[1])) if r else None


def _classify(conn, icao24, boundary, phantom_ll):
    """CRUISE_SNAP / REAL_STOP / AMBIGUOUS / NO_DATA from positions near the boundary."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT captured_at, latitude, longitude, altitude_m, velocity_ms, on_ground
            FROM positions
            WHERE icao24 = %s AND captured_at BETWEEN %s AND %s
            ORDER BY captured_at
            """,
            (icao24, boundary - timedelta(minutes=20), boundary + timedelta(minutes=20)),
        )
        near = cur.fetchall()
    if not near:
        return "NO_DATA"
    trig = min(near, key=lambda r: abs((r[0] - boundary).total_seconds()))
    _, _, _, alt, vel, _ = trig
    # parked (vel<15, or on_ground with null vel) within 10km of the phantom
    if phantom_ll:
        run = best = 0
        for _, la, lo, _, v, g in near:
            parked = (v is not None and v < 15) or (v is None and g is True)
            near_ph = la is not None and _haversine_km(la, lo, phantom_ll[0], phantom_ll[1]) <= 10
            run = run + 1 if (parked and near_ph) else 0
            best = max(best, run)
        if best >= 2:
            return "REAL_STOP"
    airborne = (vel is not None and vel > 80) or (alt is not None and alt > 3000)
    return "CRUISE_SNAP" if airborne else "AMBIGUOUS"


def _merge(conn, l1, l2, apply, logger):
    """Merge a cruise-snap pair: extend leg1 to leg2's arrival, delete leg2."""
    arr = l2["arr"]
    review = bool(_norm(l1["dep"]) and _norm(arr) and _norm(l1["dep"]) == _norm(arr))
    logger.info(
        f"  MERGE {l1['icao24']} {l1['cs']}: {l1['dep']}→[{l1['arr']}]→{arr}  "
        f"=> {l1['dep']}→{arr} (review={review}); delete leg2 @ {l2['first_seen']:%m-%d %H:%M}"
    )
    if not apply:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE flights SET arrival_airport_icao = %s, last_seen = %s, needs_review = %s
            WHERE icao24 = %s AND first_seen = %s
            """,
            (arr, l2["last_seen"], review, l1["icao24"], l1["first_seen"]),
        )
        cur.execute(
            "DELETE FROM flights WHERE icao24 = %s AND first_seen = %s",
            (l2["icao24"], l2["first_seen"]),
        )


def _flag(conn, l1, l2, apply):
    if not apply:
        return
    with conn.cursor() as cur:
        for leg in (l1, l2):
            cur.execute(
                "UPDATE flights SET needs_review = TRUE WHERE icao24 = %s AND first_seen = %s",
                (leg["icao24"], leg["first_seen"]),
            )


def main() -> int:
    ap = argparse.ArgumentParser(description="Repair gap-split flight pairs")
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    ap.add_argument("--window-days", type=int, default=config.POSITIONS_RETENTION_DAYS,
                    help="Recent window to re-detect from positions (default = retention)")
    ap.add_argument("--flag-historical", action="store_true",
                    help="Flag (needs_review) splits older than the window instead of merging")
    args = ap.parse_args()

    logger = setup_logging("repair_gap_splits")
    apply = args.apply
    logger.info(f"repair_gap_splits ({'APPLY' if apply else 'DRY RUN'}, "
                f"{'historical-flag' if args.flag_historical else 'recent-merge'})")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    routes = _load_routes(conn)
    if routes is None:
        logger.warning("flight_routes table not found — cannot classify phantoms; "
                       "apply migration 004 + seed_flight_routes. Skipping.")
        conn.close()
        return 0

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.window_days)

    def is_phantom(shared, cs):
        ep = routes.get(cs)
        return ep is not None and shared not in ep

    counts = defaultdict(int)
    if args.flag_historical:
        # Splits older than the retention window: flag both fragments.
        pairs = _find_pairs(conn, since=now - timedelta(days=3650))
        for shared, l1, l2 in pairs:
            if l1["first_seen"] >= cutoff or not is_phantom(shared, l1["cs"]):
                continue
            counts["flagged"] += 1
            logger.info(f"  FLAG {l1['icao24']} {l1['cs']} {l1['dep']}→[{shared}]→{l2['arr']} "
                        f"@ {l1['first_seen']:%Y-%m-%d}")
            _flag(conn, l1, l2, apply)
    else:
        pairs = _find_pairs(conn, since=cutoff)
        ll_cache = {}
        for shared, l1, l2 in pairs:
            if not is_phantom(shared, l1["cs"]):
                continue
            if shared not in ll_cache:
                ll_cache[shared] = _airport_ll(conn, shared)
            cat = _classify(conn, l1["icao24"], l1["last_seen"], ll_cache[shared])
            counts[cat] += 1
            if cat == "CRUISE_SNAP":
                _merge(conn, l1, l2, apply, logger)
            elif cat == "AMBIGUOUS":
                logger.info(f"  FLAG(ambiguous) {l1['icao24']} {l1['cs']} "
                            f"{l1['dep']}→[{shared}]→{l2['arr']} @ {l1['first_seen']:%m-%d %H:%M}")
                _flag(conn, l1, l2, apply)
            # REAL_STOP / NO_DATA -> leave

    if apply:
        conn.commit()
    else:
        conn.rollback()

    logger.info(f"{'Applied' if apply else 'Would apply'}: {dict(counts)}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
