#!/usr/bin/env bash
#
# Cron entrypoint for the Lufthansa FIS fetcher (sweeps and watch passes — see
# crontab for the daily slot plan).
#
# Each slot first sleeps a random jitter (NIGHTLY_JITTER_MAX_S, default 2h;
# the crontab uses 30 min for sweeps and 15 min for watches) so the actual hit
# on lufthansa.com never lands at a fixed instant. Set NIGHTLY_JITTER=0 to skip
# the jitter (manual / test runs).
#
# Chromium must run headed to clear Distil, so everything runs under Xvfb.
set -euo pipefail

if [ "${NIGHTLY_JITTER:-1}" = "1" ]; then
  MAX_JITTER="${NIGHTLY_JITTER_MAX_S:-7200}"
  SLEEP=$(( RANDOM % MAX_JITTER ))
  echo "$(date -u +%FT%TZ) jitter: sleeping ${SLEEP}s before run"
  sleep "$SLEEP"
fi

# One fetcher at a time: an overrunning sweep colliding with the next watch
# slot would mean two browsers fetching in parallel — double the request rate
# we've tuned to stay under Distil's threshold. Skip the slot instead. Ad-hoc
# --flight lookups (tools/fis_lookup.sh) are exempt: a single debug request is
# fine alongside a running sweep.
if [[ " $* " != *" --flight "* ]]; then
  LOCK=/var/lock/lhlogging-flightstatus.lock
  exec 9>"$LOCK"
  if ! flock -n 9; then
    echo "$(date -u +%FT%TZ) another fetcher run holds $LOCK — skipping this slot"
    exit 0
  fi
fi

cd /app

# Headed Chromium needs an X display. `xvfb-run -a` proved flaky in this base
# image (it could leave Xvfb running while the wrapped command never started),
# so start Xvfb directly on a fixed display instead — deterministic and easy to
# reason about. If the Python process exits, so do we (no orphaned hang).
export DISPLAY="${DISPLAY:-:99}"
Xvfb "$DISPLAY" -screen 0 1280x1024x24 -nolisten tcp >/var/log/lhlogging/xvfb.log 2>&1 &
XVFB_PID=$!
trap 'kill "$XVFB_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 40); do
  [ -S "/tmp/.X11-unix/X${DISPLAY#:}" ] && break
  sleep 0.25
done

python fetch_flightstatus.py "$@"
