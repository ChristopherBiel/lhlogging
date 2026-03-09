FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install --no-cache-dir -e .

# Install crontab
COPY crontab /etc/cron.d/lhlogging
RUN chmod 0644 /etc/cron.d/lhlogging && crontab /etc/cron.d/lhlogging

RUN mkdir -p /var/log/lhlogging

CMD ["cron", "-f"]
