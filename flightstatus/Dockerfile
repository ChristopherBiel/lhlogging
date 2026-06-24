# Playwright's Python image ships Chromium + matching Playwright + all browser
# system deps + Xvfb. We add cron + tzdata for the nightly schedule.
FROM mcr.microsoft.com/playwright/python:v1.49.0-jammy

# Preseed the timezone so tzdata's postinst doesn't block on an interactive
# debconf prompt (the classic tzdata-in-Docker hang). DEBIAN_FRONTEND is
# belt-and-suspenders. Runtime TZ is still set via the compose env.
ENV TZ=Europe/Berlin
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone \
    && DEBIAN_FRONTEND=noninteractive apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        cron \
        tzdata \
        xvfb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fetch_flightstatus.py run_nightly.sh ./
RUN chmod +x run_nightly.sh

COPY crontab /etc/cron.d/flightstatus
RUN chmod 0644 /etc/cron.d/flightstatus && crontab /etc/cron.d/flightstatus

RUN mkdir -p /var/log/lhlogging

# Same pattern as the app container: snapshot the runtime env into /etc/environment
# so cron jobs (which run with a bare environment) can source the DB credentials.
CMD printenv | sed 's/^\(.*\)$/export \1/g' > /etc/environment && cron -f
