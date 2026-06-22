#!/usr/bin/env bash
#
# Pull a fresh flights export from the production DB into tmp/flights_export.csv
# for local analysis (the tools/analyze_*.py / explore_fleet.py scripts read it).
#
# Codifies the one-off COPY we used to debug the rotation model. Run it whenever
# you want a current snapshot to look at; it never modifies anything on the server.
#
# Configure via environment, or a gitignored .env.local in the repo root:
#   LHLOGGING_SSH         ssh host/alias of the server      (e.g. user@host)
#   LHLOGGING_REMOTE_DIR  project dir on the server         (where docker-compose.yml lives)
#   LHLOGGING_DB_USER     DB user   (optional, default: lhlogging)
#   LHLOGGING_DB_NAME     DB name   (optional, default: lhlogging)
#
# Usage:
#   ./tools/pull_data.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
[ -f "$ROOT/.env.local" ] && source "$ROOT/.env.local"

: "${LHLOGGING_SSH:?set LHLOGGING_SSH (e.g. user@server) — see header of tools/pull_data.sh}"
: "${LHLOGGING_REMOTE_DIR:?set LHLOGGING_REMOTE_DIR (the server project dir)}"
DB_USER="${LHLOGGING_DB_USER:-lhlogging}"
DB_NAME="${LHLOGGING_DB_NAME:-lhlogging}"

OUT="$ROOT/tmp/flights_export.csv"
mkdir -p "$ROOT/tmp"

# No string literals in this query, so it survives the nested ssh/-c quoting.
SQL="COPY (SELECT a.icao24, btrim(a.registration) AS registration, a.aircraft_type, a.is_active, f.callsign, f.departure_airport_icao, f.arrival_airport_icao, f.first_seen, f.last_seen, f.flight_date, f.duration_minutes, f.needs_review FROM flights f JOIN aircraft a ON a.icao24 = f.icao24) TO STDOUT WITH CSV HEADER"

echo "Pulling flights export from ${LHLOGGING_SSH}:${LHLOGGING_REMOTE_DIR} ..."
ssh "$LHLOGGING_SSH" \
  "cd $LHLOGGING_REMOTE_DIR && docker compose exec -T db psql -U $DB_USER -d $DB_NAME -c \"$SQL\"" \
  > "$OUT"

echo "Wrote $OUT ($(($(wc -l < "$OUT") - 1)) flights)"
