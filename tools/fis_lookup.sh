#!/usr/bin/env bash
#
# Ad-hoc Lufthansa FIS lookup — the debugging counterpart to the nightly cron.
#
# Runs the *exact same* fetcher (flightstatus/fetch_flightstatus.py) in its
# single-lookup mode, inside the production `flightstatus` container on the VPS,
# under the same headed-Chromium-on-Xvfb setup that clears Imperva/Distil. So a
# lookup here sees what the cron job would see — no separate code path, no local
# browser (local Mac / curl / headless are all blocked by Distil).
#
# Read-only: hits the public lufthansa.com FIS endpoint, writes nothing to the DB.
#
# Configure via the same gitignored .env.local as the pull_*.sh scripts:
#   LHLOGGING_SSH         ssh host/alias of the server   (e.g. deploy@host)
#   LHLOGGING_REMOTE_DIR  project dir on the server      (where docker-compose.yml lives)
#
# Usage:
#   ./tools/fis_lookup.sh LH763 2026-07-03        # summary line
#   ./tools/fis_lookup.sh LH763 2026-07-03 --raw  # + full FIS JSON payload
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
[ -f "$ROOT/.env.local" ] && source "$ROOT/.env.local"

: "${LHLOGGING_SSH:?set LHLOGGING_SSH (e.g. user@server) — see tools/pull_data.sh}"
: "${LHLOGGING_REMOTE_DIR:?set LHLOGGING_REMOTE_DIR (the server project dir)}"

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <FLIGHT> <YYYY-MM-DD> [--raw]" >&2
  echo "  e.g. $0 LH763 2026-07-03" >&2
  exit 2
fi
FLIGHT="$1"; DATE="$2"; RAW="${3:-}"

# Run the single-lookup mode of the fetcher through the nightly entrypoint (which
# sets up Xvfb). NIGHTLY_JITTER=0 skips the random pre-run sleep.
OUT="$(ssh "$LHLOGGING_SSH" \
  "cd $LHLOGGING_REMOTE_DIR && docker compose exec -T -e NIGHTLY_JITTER=0 flightstatus \
   /app/run_nightly.sh --flight $FLIGHT --date $DATE" 2>&1)"

# The fetcher emits a few log lines before the JSON blob; parse from the first '{'.
printf '%s' "$OUT" | RAW="$RAW" FLIGHT="$FLIGHT" DATE="$DATE" python3 -c '
import json, os, sys
flight, date = os.environ["FLIGHT"], os.environ["DATE"]
raw = sys.stdin.read()
i = raw.find("{")
if i < 0:
    print("no JSON in response — likely blocked or container error:", file=sys.stderr)
    print(raw[-1000:], file=sys.stderr)
    sys.exit(1)
try:
    d = json.loads(raw[i:])
except json.JSONDecodeError as e:
    print("could not parse JSON:", e, file=sys.stderr)
    print(raw[-1000:], file=sys.stderr)
    sys.exit(1)
o = d.get("observation", {})
if not o.get("found"):
    print("{} {}: NOT FOUND (feed returned no flight)".format(flight, date))
    sys.exit(0)
prev = "n/a"
if o.get("prev_flight_number"):
    prev = "{}{}@{}".format(o["prev_airline"], o["prev_flight_number"], o["prev_flight_date"])
print("{} {}".format(flight, date))
print("  tail      : {}  ({})".format(o.get("registration"), o.get("aircraft_type") or "?type"))
print("  route     : {} -> {}".format(o.get("dep_airport_iata"), o.get("arr_airport_iata")))
print("  scheduled : dep {}  arr {}".format(o.get("dep_scheduled"), o.get("arr_scheduled")))
print("  status    : {}".format(o.get("overall_status")))
print("  prevFlight: {}".format(prev))
if os.environ.get("RAW") == "--raw":
    print("  --- raw FIS payload ---")
    print(json.dumps(d.get("raw"), indent=2, ensure_ascii=False))
'
