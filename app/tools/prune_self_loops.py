"""
Prune ground-only self-loop legs (departure == arrival) from the review queue.

The detector flags every dep==arr leg needs_review. Most are ground artifacts —
an aircraft taxiing / parked / a sparse blip that opened and "landed" at the same
field without ever flying. Those are noise: delete them. But a dep==arr leg can
also be REAL data we must not touch:
  * a genuine return-to-field (took off, had a problem, came back), and
  * a whole rotation the detector collapsed into one leg with the arrival
    mis-snapped back to the departure airport (reaches cruise altitude!).
Both of those have an actual airborne track, so the safe discriminator is
physics, not string equality or duration:

  airborne sample := on_ground is not TRUE AND (altitude_m > alt_floor
                                                 OR velocity_ms > vel_floor)

  GROUND  (0 airborne samples in the leg window)  -> delete (or --clear the flag)
  KEEP    (>=1 airborne sample)                   -> leave flagged for a human

With --handover, one more class is deleted: the C6 phantom (tools/EDGE_CASES.md).
A leftover fix after a close re-opens a leg at the same airport; the mid-air
callsign change (C6) then closes it at the NEXT flight's first fix — so the
phantom's last_seen EQUALS its successor leg's first_seen, and that shared
boundary fix is its ONLY airborne sample (it belongs to the successor's
departure, not to this leg). Signature:

  HANDOVER (successor.first_seen == last_seen AND 0 airborne samples
            strictly before last_seen)          -> delete

Validated 2026-07-02 against all recent self-loops: of ~235, 103 were pure
ground (0 airborne) and 132 had a real airborne track (72 of them reached cruise
— mis-snapped real flights). See [[review-queue-triage]].

Dry-run by default, like lhlogging/route_enrichment.py and tools/repair_gap_splits.py.
Only looks back POSITIONS_RETENTION_DAYS (older legs have no positions to judge).

Usage:
    python -m tools.prune_self_loops                 # dry-run (recent)
    python -m tools.prune_self_loops --apply         # delete ground-only self-loops
    python -m tools.prune_self_loops --clear --apply  # just clear the flag, keep the row
    python -m tools.prune_self_loops --handover       # dry-run incl. C6 phantoms
"""
import argparse
import sys
from datetime import datetime, timedelta, timezone

from lhlogging import config, db
from lhlogging.utils import setup_logging

_ALIAS = {"EDFE": "EDDF"}


def _norm(code):
    code = (code or "").strip().upper()
    if not code or code == "UNKN":
        return None
    return _ALIAS.get(code, code)


def _candidates(conn, cutoff, alt_floor, vel_floor):
    """Recent needs_review self-loops with their in-window airborne-sample count.

    One aggregate pass: LEFT JOIN positions bounded to each leg's [first,last]
    window and count samples that are unambiguously airborne.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.icao24, btrim(f.callsign) AS cs,
                   btrim(f.departure_airport_icao) AS dep,
                   btrim(f.arrival_airport_icao) AS arr,
                   f.first_seen, f.last_seen, f.duration_minutes,
                   count(p.*) FILTER (
                       WHERE p.on_ground IS DISTINCT FROM TRUE
                         AND (p.altitude_m > %s OR p.velocity_ms > %s)
                   ) AS airborne,
                   count(p.*) FILTER (
                       WHERE p.on_ground IS DISTINCT FROM TRUE
                         AND (p.altitude_m > %s OR p.velocity_ms > %s)
                         AND p.captured_at < f.last_seen
                   ) AS airborne_excl,
                   coalesce(round(max(p.altitude_m)), 0) AS max_alt,
                   count(p.*) AS n_pos,
                   EXISTS (SELECT 1 FROM flights g
                           WHERE g.icao24 = f.icao24
                             AND g.first_seen = f.last_seen
                             AND g.first_seen <> f.first_seen) AS handover
            FROM flights f
            LEFT JOIN positions p
              ON p.icao24 = f.icao24
             AND p.captured_at BETWEEN f.first_seen AND f.last_seen
            WHERE f.needs_review = TRUE
              AND f.first_seen >= %s
              AND f.departure_airport_icao IS NOT NULL
              AND f.arrival_airport_icao IS NOT NULL
              AND btrim(f.departure_airport_icao) = btrim(f.arrival_airport_icao)
            GROUP BY f.icao24, f.callsign, f.departure_airport_icao,
                     f.arrival_airport_icao, f.first_seen, f.last_seen, f.duration_minutes
            ORDER BY f.first_seen
            """,
            (alt_floor, vel_floor, alt_floor, vel_floor, cutoff),
        )
        rows = cur.fetchall()
    out = []
    for (icao, cs, dep, arr, first_seen, last_seen, dur,
         airborne, airborne_excl, max_alt, n_pos, handover) in rows:
        if _norm(dep) is None or _norm(dep) != _norm(arr):
            continue  # not a self-loop
        out.append(dict(icao24=icao, cs=cs, ap=dep, first_seen=first_seen,
                        last_seen=last_seen, dur=dur, airborne=airborne,
                        airborne_excl=airborne_excl, max_alt=int(max_alt),
                        n_pos=n_pos, handover=handover))
    return out


def _remove(conn, leg, clear, apply):
    """Delete the ground self-loop, or just clear its needs_review flag."""
    if not apply:
        return
    with conn.cursor() as cur:
        if clear:
            cur.execute(
                "UPDATE flights SET needs_review = FALSE WHERE icao24 = %s AND first_seen = %s",
                (leg["icao24"], leg["first_seen"]),
            )
        else:
            cur.execute(
                "DELETE FROM flights WHERE icao24 = %s AND first_seen = %s",
                (leg["icao24"], leg["first_seen"]),
            )


def main() -> int:
    ap = argparse.ArgumentParser(description="Prune ground-only self-loop legs")
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry-run)")
    ap.add_argument("--clear", action="store_true",
                    help="Clear needs_review instead of deleting the row")
    ap.add_argument("--handover", action="store_true",
                    help="Also remove C6 phantoms: self-loops whose only airborne "
                         "sample is the boundary fix shared with the successor leg")
    ap.add_argument("--window-days", type=int, default=config.POSITIONS_RETENTION_DAYS,
                    help="Only consider self-loops this recent (positions retained)")
    ap.add_argument("--alt-floor", type=float, default=config.LANDING_ALTITUDE_THRESHOLD_M,
                    help="Altitude (m) above which a not-on-ground sample counts as airborne")
    ap.add_argument("--vel-floor", type=float, default=100.0,
                    help="Velocity (m/s) above which a not-on-ground sample counts as airborne")
    ap.add_argument("--cruise-alt", type=float, default=config.ONGROUND_MAX_ALTITUDE_M,
                    help="Kept self-loops above this max_alt are flagged as likely mis-snapped flights")
    args = ap.parse_args()

    logger = setup_logging("prune_self_loops")
    action = "clear-flag" if args.clear else "delete"
    logger.info(f"prune_self_loops ({'APPLY' if args.apply else 'DRY RUN'}, action={action}, "
                f"window={args.window_days}d, airborne=alt>{args.alt_floor:g}m|vel>{args.vel_floor:g})")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.window_days)
    legs = _candidates(conn, cutoff, args.alt_floor, args.vel_floor)

    ground = [l for l in legs if l["airborne"] == 0]
    phantom = [l for l in legs if args.handover and l["airborne"] > 0
               and l["handover"] and l["airborne_excl"] == 0]
    kept = [l for l in legs if l["airborne"] > 0 and l not in phantom]
    cruise = [l for l in kept if l["max_alt"] >= args.cruise_alt]

    for l in ground:
        logger.info(f"  {action.upper()} {l['icao24']} {l['cs'] or '-':8} {l['ap']}→{l['ap']} "
                    f"@ {l['first_seen']:%m-%d %H:%M} dur={l['dur']} n_pos={l['n_pos']} (ground)")
        _remove(conn, l, args.clear, args.apply)
    for l in phantom:
        logger.info(f"  {action.upper()} {l['icao24']} {l['cs'] or '-':8} {l['ap']}→{l['ap']} "
                    f"@ {l['first_seen']:%m-%d %H:%M} dur={l['dur']} n_pos={l['n_pos']} "
                    f"max_alt={l['max_alt']} (handover phantom)")
        _remove(conn, l, args.clear, args.apply)

    if args.apply:
        conn.commit()
    else:
        conn.rollback()

    logger.info(f"self-loops examined: {len(legs)}  |  "
                f"{'removed' if args.apply else 'would remove'} (ground-only): {len(ground)}"
                + (f" + handover phantoms: {len(phantom)}" if args.handover else "")
                + f"  |  kept (has airborne track): {len(kept)}")
    if cruise:
        logger.warning(f"{len(cruise)} kept self-loops reached >= {args.cruise_alt:g} m — "
                       f"likely REAL flights with a mis-snapped arrival (NOT pruned; "
                       f"separate detector issue). e.g. "
                       + ", ".join(f"{l['icao24']}/{l['cs']} {l['ap']}({l['max_alt']}m)"
                                   for l in sorted(cruise, key=lambda x: -x['max_alt'])[:5]))
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
