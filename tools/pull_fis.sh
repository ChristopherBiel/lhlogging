#!/usr/bin/env bash
#
# Pull a fresh flight-status (FIS) export from the production DB into
# tmp/fis_export.csv for local analysis of the nightly Lufthansa schedule
# snapshots (tools/reassignment_stability.py reads it).
#
# Sibling of pull_data.sh — same SSH mechanism, never modifies the server.
#
# Configure via environment, or a gitignored .env.local in the repo root:
#   LHLOGGING_SSH         ssh host/alias of the server      (e.g. user@host)
#   LHLOGGING_REMOTE_DIR  project dir on the server         (where docker-compose.yml lives)
#   LHLOGGING_DB_USER     DB user   (optional, default: lhlogging)
#   LHLOGGING_DB_NAME     DB name   (optional, default: lhlogging)
#
# Usage:
#   ./tools/pull_fis.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
[ -f "$ROOT/.env.local" ] && source "$ROOT/.env.local"

: "${LHLOGGING_SSH:?set LHLOGGING_SSH (e.g. user@server) — see header of tools/pull_data.sh}"
: "${LHLOGGING_REMOTE_DIR:?set LHLOGGING_REMOTE_DIR (the server project dir)}"
DB_USER="${LHLOGGING_DB_USER:-lhlogging}"
DB_NAME="${LHLOGGING_DB_NAME:-lhlogging}"

OUT="$ROOT/tmp/fis_export.csv"
mkdir -p "$ROOT/tmp"

# No string literals in this query, so it survives the nested ssh/-c quoting.
SQL="COPY (SELECT observed_date, flight_date, airline, flight_number, seed_type, found, btrim(registration) AS registration, aircraft_type, dep_airport_iata, arr_airport_iata, overall_status FROM flight_status_observations ORDER BY flight_date, airline, flight_number, observed_date) TO STDOUT WITH CSV HEADER"

echo "Pulling FIS export from ${LHLOGGING_SSH}:${LHLOGGING_REMOTE_DIR} ..."
ssh "$LHLOGGING_SSH" \
  "cd $LHLOGGING_REMOTE_DIR && docker compose exec -T db psql -U $DB_USER -d $DB_NAME -c \"$SQL\"" \
  > "$OUT"

echo "Wrote $OUT ($(($(wc -l < "$OUT") - 1)) observations)"
