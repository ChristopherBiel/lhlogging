"""
Review tool — export needs_review items to a markdown file, edit via SSH,
then apply changes back to the database.

Usage:
    # Export items needing review to a markdown file
    python -m tools.review export

    # Apply edits from the review file back to the database
    python -m tools.review apply

    # Export to a custom path
    python -m tools.review export --file /tmp/review.md

The default file path is /var/log/lhlogging/review.md (inside the container).

Workflow:
    1. SSH into the VPS
    2. docker exec -it <app-container> python -m tools.review export
    3. Edit /var/log/lhlogging/review.md  (nano is installed in the container)
    4. docker exec -it <app-container> python -m tools.review apply
"""
import argparse
import re
import sys
from datetime import datetime, timezone

from lhlogging import db
from lhlogging.utils import setup_logging

_DEFAULT_FILE = "/var/log/lhlogging/review.md"


def _export(conn, path: str, logger) -> int:
    """Export all needs_review aircraft and flights to a markdown file."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT icao24, registration, aircraft_type, aircraft_subtype
            FROM aircraft
            WHERE needs_review = TRUE
            ORDER BY icao24
            """
        )
        aircraft = cur.fetchall()

        cur.execute(
            """
            SELECT f.id, f.icao24, a.registration, f.callsign,
                   f.departure_airport_icao, f.arrival_airport_icao,
                   f.first_seen, f.last_seen, f.duration_minutes
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE f.needs_review = TRUE
            ORDER BY f.first_seen DESC
            """
        )
        flights = cur.fetchall()

    lines = [
        "# Review Queue",
        "",
        f"Exported: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "Edit the values below then run `python -m tools.review apply` to save.",
        "",
        "Actions:",
        "  PENDING  — skip, keep flagged (default)",
        "  UPDATE   — apply your edits to the database and clear the flag",
        "  DISMISS  — clear the flag without changing any data",
        "",
    ]

    # --- Aircraft ---
    lines.append("## Aircraft")
    lines.append("")
    if not aircraft:
        lines.append("No aircraft to review.")
    else:
        lines.append(f"{len(aircraft)} aircraft flagged for review.")
        lines.append("")
        lines.append("<!-- Edit registration, type, subtype. Set action to DISMISS or PENDING. -->")
        lines.append("")
        for r in aircraft:
            icao24 = r[0].strip()
            reg = (r[1] or "").strip()
            atype = (r[2] or "").strip()
            subtype = (r[3] or "").strip()
            lines.append(f"### {icao24}")
            lines.append("")
            lines.append(f"- action: PENDING")
            lines.append(f"- registration: {reg}")
            lines.append(f"- type: {atype}")
            lines.append(f"- subtype: {subtype}")
            lines.append("")

    # --- Flights ---
    lines.append("## Flights")
    lines.append("")
    if not flights:
        lines.append("No flights to review.")
    else:
        lines.append(f"{len(flights)} flights flagged for review.")
        lines.append("")
        lines.append("<!-- Edit dep/arr airports. Set action to DISMISS or PENDING. -->")
        lines.append("")
        for r in flights:
            fid = r[0]
            icao24 = r[1].strip()
            reg = (r[2] or "").strip()
            cs = (r[3] or "").strip()
            dep = (r[4] or "").strip()
            arr = (r[5] or "").strip()
            first = r[6].strftime("%Y-%m-%d %H:%M") if r[6] else ""
            last = r[7].strftime("%Y-%m-%d %H:%M") if r[7] else ""
            dur = r[8] or 0
            lines.append(f"### flight-{fid}")
            lines.append("")
            lines.append(f"- action: PENDING")
            lines.append(f"- icao24: {icao24}")
            lines.append(f"- registration: {reg}")
            lines.append(f"- callsign: {cs}")
            lines.append(f"- dep: {dep}")
            lines.append(f"- arr: {arr}")
            lines.append(f"- time: {first} — {last} ({dur}min)")
            lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")

    logger.info(
        f"Exported {len(aircraft)} aircraft and {len(flights)} flights to {path}"
    )
    return 0


def _parse_blocks(path: str) -> tuple[list[dict], list[dict]]:
    """Parse the review markdown file into aircraft and flight blocks."""
    with open(path) as f:
        text = f.read()

    aircraft_blocks = []
    flight_blocks = []

    current = None
    section = None

    for line in text.splitlines():
        line_stripped = line.strip()

        if line_stripped == "## Aircraft":
            section = "aircraft"
            continue
        elif line_stripped == "## Flights":
            section = "flights"
            continue

        # New block header
        m_aircraft = re.match(r"^###\s+([0-9a-f]{6})\s*$", line_stripped, re.I)
        m_flight = re.match(r"^###\s+flight-(\d+)\s*$", line_stripped, re.I)

        if m_aircraft and section == "aircraft":
            current = {"icao24": m_aircraft.group(1).lower()}
            aircraft_blocks.append(current)
            continue
        elif m_flight and section == "flights":
            current = {"id": int(m_flight.group(1))}
            flight_blocks.append(current)
            continue

        # Parse key-value lines within a block
        if current is not None:
            m_kv = re.match(r"^-\s+(\w+):\s*(.*?)\s*$", line_stripped)
            if m_kv:
                current[m_kv.group(1)] = m_kv.group(2)

    return aircraft_blocks, flight_blocks


def _apply(conn, path: str, logger) -> int:
    """Read the review file and apply edits to the database."""
    try:
        aircraft_blocks, flight_blocks = _parse_blocks(path)
    except FileNotFoundError:
        logger.critical(f"Review file not found: {path}")
        return 1

    ac_updated = 0
    ac_dismissed = 0
    fl_updated = 0
    fl_dismissed = 0

    with conn.cursor() as cur:
        for block in aircraft_blocks:
            action = block.get("action", "PENDING").upper()
            icao24 = block["icao24"]

            if action == "PENDING":
                continue

            if action == "DISMISS":
                cur.execute(
                    "UPDATE aircraft SET needs_review = FALSE, updated_at = NOW() "
                    "WHERE icao24 = %s",
                    (icao24,),
                )
                ac_dismissed += 1
                logger.info(f"Aircraft {icao24}: dismissed")
                continue

            if action != "UPDATE":
                logger.warning(f"Aircraft {icao24}: unknown action '{action}', skipping")
                continue

            reg = block.get("registration", "").strip()
            atype = block.get("type", "").strip() or None
            subtype = block.get("subtype", "").strip() or None

            cur.execute(
                """
                UPDATE aircraft SET
                    registration = COALESCE(NULLIF(%s, ''), registration),
                    aircraft_type = COALESCE(%s, aircraft_type),
                    aircraft_subtype = COALESCE(%s, aircraft_subtype),
                    needs_review = FALSE,
                    updated_at = NOW()
                WHERE icao24 = %s
                """,
                (reg, atype, subtype, icao24),
            )
            ac_updated += 1
            logger.info(
                f"Aircraft {icao24}: updated → reg={reg}, type={atype}, subtype={subtype}"
            )

        for block in flight_blocks:
            action = block.get("action", "PENDING").upper()
            fid = block["id"]

            if action == "PENDING":
                continue

            if action == "DISMISS":
                cur.execute(
                    "UPDATE flights SET needs_review = FALSE WHERE id = %s",
                    (fid,),
                )
                fl_dismissed += 1
                logger.info(f"Flight {fid}: dismissed")
                continue

            if action != "UPDATE":
                logger.warning(f"Flight {fid}: unknown action '{action}', skipping")
                continue

            dep = block.get("dep", "").strip() or None
            arr = block.get("arr", "").strip() or None

            cur.execute(
                """
                UPDATE flights SET
                    departure_airport_icao = COALESCE(%s, departure_airport_icao),
                    arrival_airport_icao = COALESCE(%s, arrival_airport_icao),
                    needs_review = FALSE
                WHERE id = %s
                """,
                (dep, arr, fid),
            )
            fl_updated += 1
            logger.info(f"Flight {fid}: updated → dep={dep}, arr={arr}")

    conn.commit()
    logger.info(
        f"Applied: {ac_updated} aircraft updated, {ac_dismissed} dismissed, "
        f"{fl_updated} flights updated, {fl_dismissed} dismissed"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Review needs_review items")
    parser.add_argument("command", choices=["export", "apply"])
    parser.add_argument("--file", default=_DEFAULT_FILE, help="Path to review markdown file")
    args = parser.parse_args()

    logger = setup_logging("review")

    try:
        conn = db.get_connection()
    except Exception as e:
        logger.critical(f"Cannot connect to database: {e}")
        return 1

    try:
        if args.command == "export":
            return _export(conn, args.file, logger)
        else:
            return _apply(conn, args.file, logger)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
