#!/usr/bin/env bash
#
# Nightly entrypoint for the Lufthansa FIS fetcher.
#
# Cron fires this at 22:00 (container TZ = Europe/Berlin). We then sleep a random
# 0..59m59s so the actual hit on lufthansa.com lands somewhere in 22:00–23:00
# rather than at the same instant every night. Set NIGHTLY_JITTER=0 to skip the
# jitter (manual / test runs).
#
# Chromium must run headed to clear Distil, so everything runs under Xvfb.
set -euo pipefail

if [ "${NIGHTLY_JITTER:-1}" = "1" ]; then
  # Random start delay (default up to 2h) so the two daily runs land at
  # unpredictable, spread-out times rather than a fixed instant.
  MAX_JITTER="${NIGHTLY_JITTER_MAX_S:-7200}"
  SLEEP=$(( RANDOM % MAX_JITTER ))
  echo "$(date -u +%FT%TZ) jitter: sleeping ${SLEEP}s before run"
  sleep "$SLEEP"
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
