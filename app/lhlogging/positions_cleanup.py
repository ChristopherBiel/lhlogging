"""
Positions cleanup — runs daily via cron.
Deletes position snapshots older than POSITIONS_RETENTION_DAYS (default 30).

Usage:
    python -m lhlogging.positions_cleanup
"""
import sys
from datetime import datetime, timedelta, timezone

from lhlogging import config, db
from lhlogging.utils import setup_logging


def main() -> int:
    logger = setup_logging("positions_cleanup")
    logger.info("Positions cleanup starting")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    run_id = db.log_batch_start(conn, "pos_cleanup")
    stats = {
        "ok": 0,
        "error": 0,
        "flights_upserted": 0,
        "status": "ok",
        "error_detail": None,
        "aircraft_total": 0,
    }

    before = datetime.now(timezone.utc) - timedelta(days=config.POSITIONS_RETENTION_DAYS)
    logger.info(
        f"Deleting positions older than {before.strftime('%Y-%m-%d %H:%M')} "
        f"({config.POSITIONS_RETENTION_DAYS} days)"
    )

    try:
        deleted = db.delete_positions_before(conn, before)
        conn.commit()
    except Exception as e:
        logger.critical(f"Delete failed: {e}")
        conn.rollback()
        stats["status"] = "error"
        stats["error_detail"] = str(e)
        db.log_batch_finish(conn, run_id, stats)
        conn.close()
        return 1

    stats["ok"] = deleted
    stats["flights_upserted"] = deleted
    logger.info(f"Positions cleanup done — {deleted} rows deleted")
    db.log_batch_finish(conn, run_id, stats)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
