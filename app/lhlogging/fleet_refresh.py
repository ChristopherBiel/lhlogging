"""
Weekly entrypoint: refreshes the Lufthansa fleet from the OpenSky aircraft database CSV.

Usage:
    python -m lhlogging.fleet_refresh
"""
import sys

from lhlogging import config, db
from lhlogging.opensky_fleet import OpenSkyFleetClient, OpenSkyFleetError
from lhlogging.planespotters import PlanespottersClient, PlanespottersError, PlanespottersRateLimitError
from lhlogging.utils import setup_logging

# Minimum plausible fleet size — guards against retiring everything on a bad download
_MIN_FLEET_SANITY = 50


def main() -> int:
    logger = setup_logging("fleet_refresh")
    logger.info("Fleet refresh starting")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    run_id = db.log_batch_start(conn, "fleet_refresh")
    stats = {"ok": 0, "error": 0, "flights_upserted": 0, "status": "ok", "error_detail": None}

    try:
        client = OpenSkyFleetClient(logger)
        api_fleet = client.get_airline_fleet(
            config.PLANESPOTTERS_AIRLINE_ICAO,
            registration_prefixes=config.AIRLINE_REGISTRATION_PREFIXES,
        )
    except OpenSkyFleetError as e:
        logger.critical(f"Failed to fetch fleet from OpenSky aircraft DB: {e}")
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    if len(api_fleet) < _MIN_FLEET_SANITY:
        msg = (
            f"OpenSky aircraft DB returned only {len(api_fleet)} aircraft for DLH "
            f"(minimum expected: {_MIN_FLEET_SANITY}). "
            "Aborting to avoid incorrectly retiring active aircraft."
        )
        logger.critical(msg)
        stats["status"] = "error"
        stats["error_detail"] = msg
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    # --- Enrich type data from Planespotters (OpenSky type codes are often missing/wrong) ---
    ps_client = PlanespottersClient(logger)
    logger.info(f"Enriching {len(api_fleet)} aircraft with Planespotters type data...")
    ps_enriched = 0
    for aircraft in api_fleet:
        try:
            ps_data = ps_client.get_aircraft(aircraft["icao24"])
            if ps_data:
                if ps_data.get("aircraft_type"):
                    aircraft["aircraft_type"] = ps_data["aircraft_type"]
                    ps_enriched += 1
                if ps_data.get("aircraft_subtype"):
                    aircraft["aircraft_subtype"] = ps_data["aircraft_subtype"]
        except PlanespottersRateLimitError:
            logger.warning("Planespotters rate limit hit — stopping enrichment early")
            break
        except PlanespottersError as e:
            logger.warning(f"Planespotters lookup failed for {aircraft['icao24']}: {e}")
    logger.info(f"Planespotters enriched {ps_enriched}/{len(api_fleet)} aircraft types")

    # --- Only update aircraft already in the DB (don't add new ones) ---
    # New aircraft are added exclusively via fleet_discovery (callsign-based),
    # which ensures we only track confirmed LH mainline aircraft.
    db_fleet = db.get_active_aircraft(conn)
    db_icao24_set = {a["icao24"].strip() for a in db_fleet}

    api_by_icao24 = {ac["icao24"]: ac for ac in api_fleet}
    api_icao24_set = set(api_by_icao24.keys())

    # Update existing DB aircraft with enriched CSV + Planespotters data
    to_update = db_icao24_set & api_icao24_set
    for icao24 in to_update:
        ac = api_by_icao24[icao24]
        ac["needs_review"] = not ac.get("aircraft_type")
        try:
            db.upsert_aircraft(conn, ac)
            stats["ok"] += 1
        except Exception as e:
            logger.error(f"Failed to update {icao24}: {e}")
            stats["error"] += 1
            conn.rollback()
    conn.commit()

    skipped_new = api_icao24_set - db_icao24_set
    logger.info(
        f"Skipped {len(skipped_new)} CSV aircraft not in DB "
        f"(fleet_discovery handles additions)"
    )

    # --- Retire DB aircraft no longer in the CSV ---
    retired = db_icao24_set - api_icao24_set
    for icao24 in retired:
        try:
            db.mark_aircraft_retired(conn, icao24)
            logger.warning(f"Retired aircraft no longer in OpenSky DB: {icao24}")
        except Exception as e:
            logger.error(f"Failed to retire {icao24}: {e}")
    conn.commit()

    stats["aircraft_total"] = len(to_update)
    logger.info(
        f"Fleet refresh done — "
        f"updated: {stats['ok']}, errors: {stats['error']}, "
        f"retired: {len(retired)}, skipped new: {len(skipped_new)}"
    )
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
