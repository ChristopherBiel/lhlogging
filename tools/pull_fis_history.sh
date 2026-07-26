#!/usr/bin/env bash
#
# Pull the *per-pass* flight-status history into tmp/fis_history.csv — the
# reassignment-prediction substrate (tools/build_leg_outcomes.py reads it).
#
# Sibling of pull_fis.sh, which exports one row per (observed_date, flight) for
# the older stability report. Since migration 009 the table appends one row per
# *pass* (up to 8/day), so the interesting signal — at which lead time and time
# of day a tail assignment actually changed — lives in `observed_at`, not
# `observed_date`. This export therefore keeps observed_at, the run it came
# from, and the scheduled departure time (the lead-time reference), and resolves
# each tail to its fleet type via the `aircraft` table (FIS's own
# `aircraft_type` string is free text: "Airbus A350" vs "Airbus A350-900").
#
# Read-only; never modifies the server.
#
# Configure via environment or the gitignored .env.local (see pull_fis.sh):
#   LHLOGGING_SSH, LHLOGGING_REMOTE_DIR, [LHLOGGING_DB_USER], [LHLOGGING_DB_NAME]
#
# Usage:
#   ./tools/pull_fis_history.sh          # whole table
#   FIS_HISTORY_DAYS=30 ./tools/pull_fis_history.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
[ -f "$ROOT/.env.local" ] && source "$ROOT/.env.local"

: "${LHLOGGING_SSH:?set LHLOGGING_SSH (e.g. user@server) — see header of tools/pull_data.sh}"
: "${LHLOGGING_REMOTE_DIR:?set LHLOGGING_REMOTE_DIR (the server project dir)}"
DB_USER="${LHLOGGING_DB_USER:-lhlogging}"
DB_NAME="${LHLOGGING_DB_NAME:-lhlogging}"
DAYS="${FIS_HISTORY_DAYS:-3650}"

OUT="$ROOT/tmp/fis_history.csv"
mkdir -p "$ROOT/tmp"

# No string literals in this query, so it survives the nested ssh/-c quoting
# (same constraint as pull_fis.sh). Interval arithmetic uses make_interval.
SQL="COPY (
  SELECT o.run_id, r.started_at AS run_started_at, o.observed_at, o.observed_date,
         o.flight_date, o.airline, o.flight_number, o.seed_type, o.found,
         btrim(o.registration) AS registration, fl.aircraft_type AS fleet_type,
         o.aircraft_type AS fis_type, o.dep_airport_iata, o.arr_airport_iata,
         o.dep_scheduled, o.arr_scheduled, o.overall_status,
         o.prev_airline, o.prev_flight_number, o.prev_flight_date
  FROM flight_status_observations o
  LEFT JOIN batch_runs r ON r.id = o.run_id
  LEFT JOIN (
      SELECT DISTINCT ON (registration) registration, aircraft_type
      FROM aircraft
      ORDER BY registration, is_active DESC, last_seen_date DESC NULLS LAST
  ) fl ON fl.registration = btrim(o.registration)
  WHERE o.observed_at >= NOW() - make_interval(days => $DAYS)
  ORDER BY o.flight_date, o.airline, o.flight_number, o.observed_at
) TO STDOUT WITH CSV HEADER"

echo "Pulling FIS per-pass history (last $DAYS days) from ${LHLOGGING_SSH}:${LHLOGGING_REMOTE_DIR} ..."
ssh "$LHLOGGING_SSH" \
  "cd $LHLOGGING_REMOTE_DIR && docker compose exec -T db psql -U $DB_USER -d $DB_NAME -c \"$SQL\"" \
  > "$OUT"

echo "Wrote $OUT ($(($(wc -l < "$OUT") - 1)) observations)"
