#!/usr/bin/env bash
#
# Pull recorded ADS-B positions (and the airports table) from the production DB
# for offline replay of the flight detector. The positions table is what the
# detector actually consumes; the airports table is what lookup_nearest_airport
# resolves against — both are needed to reproduce detection locally.
#
# Read-only; never modifies the server. Companion to tools/pull_data.sh.
#
# Configure via .env.local in the repo root (same vars as pull_data.sh):
#   LHLOGGING_SSH         ssh host/alias of the server
#   LHLOGGING_REMOTE_DIR  project dir on the server
#   LHLOGGING_DB_USER     (optional, default: lhlogging)
#   LHLOGGING_DB_NAME     (optional, default: lhlogging)
#
# Usage:
#   ./tools/pull_positions.sh                 # ALL positions + airports (big: ~180MB)
#   ./tools/pull_positions.sh 3c65a8 3c65a1   # only these icao24s + airports
#   ./tools/pull_positions.sh --airports-only # just refresh the airports table
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
[ -f "$ROOT/.env.local" ] && source "$ROOT/.env.local"

: "${LHLOGGING_SSH:?set LHLOGGING_SSH (e.g. user@server) — see header}"
: "${LHLOGGING_REMOTE_DIR:?set LHLOGGING_REMOTE_DIR (the server project dir)}"
DB_USER="${LHLOGGING_DB_USER:-lhlogging}"
DB_NAME="${LHLOGGING_DB_NAME:-lhlogging}"
mkdir -p "$ROOT/tmp"

# Feed SQL to psql over stdin via a heredoc so quoted string literals (the
# icao24 IN-list) survive the nested ssh quoting. COPY ... TO STDOUT writes the
# CSV to psql's stdout, which ssh forwards to our local redirect; psql's status
# lines go to stderr. -T disables TTY allocation so stdin pipes straight through.
run_copy() {  # $1 = SQL (single COPY statement), reads from stdin redirect by caller
  ssh -C "$LHLOGGING_SSH" \
    "cd $LHLOGGING_REMOTE_DIR && docker compose exec -T db psql -q -U $DB_USER -d $DB_NAME" <<SQL
$1
SQL
}

# --- airports (always; small + needed for offline nearest-airport) ---
AIRPORTS_OUT="$ROOT/tmp/airports_export.csv"
echo "Pulling airports → $AIRPORTS_OUT ..."
run_copy "COPY (SELECT icao_code, type, latitude, longitude FROM airports) TO STDOUT WITH CSV HEADER" \
  > "$AIRPORTS_OUT"
echo "  airports: $(($(wc -l < "$AIRPORTS_OUT") - 1)) rows"

if [ "${1:-}" = "--airports-only" ]; then
  exit 0
fi

# --- positions (filtered by icao24 args, or all) ---
POS_OUT="$ROOT/tmp/positions_export.csv"
SELECT="SELECT icao24, callsign, captured_at, latitude, longitude, altitude_m, velocity_ms, heading, on_ground FROM positions"
if [ "$#" -gt 0 ]; then
  # Build a quoted IN-list from the icao24 args: 3c65a8 3c65a1 -> '3c65a8','3c65a1'
  inlist=""
  for code in "$@"; do
    inlist="${inlist:+$inlist,}'${code}'"
  done
  WHERE=" WHERE icao24 IN ($inlist)"
  echo "Pulling positions for $# aircraft → $POS_OUT ..."
else
  WHERE=""
  echo "Pulling ALL positions → $POS_OUT (this is large; using ssh -C) ..."
fi
run_copy "COPY ($SELECT$WHERE ORDER BY icao24, captured_at) TO STDOUT WITH CSV HEADER" \
  > "$POS_OUT"
echo "  positions: $(($(wc -l < "$POS_OUT") - 1)) rows → $POS_OUT"
