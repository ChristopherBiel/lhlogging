"""
LHLogging monitoring dashboard.
Serves a single-page HTML dashboard and a /api/stats JSON endpoint.
"""
import os
from collections import defaultdict
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string

load_dotenv()

app = Flask(__name__)

DB_CONNECT = dict(
    host=os.environ.get("DB_HOST", "db"),
    port=int(os.environ.get("DB_PORT", "5432")),
    dbname=os.environ.get("DB_NAME", "lhlogging"),
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)


def _db():
    return psycopg.connect(**DB_CONNECT, autocommit=True)


def _q(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _q1(conn, sql, params=None):
    rows = _q(conn, sql, params)
    return rows[0][0] if rows else None


@app.route("/api/stats")
def api_stats():
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503

    try:
        stats = {}

        # --- Fleet counts ---
        stats["aircraft_active"] = _q1(conn, "SELECT COUNT(*) FROM aircraft WHERE is_active")
        stats["aircraft_total"] = _q1(conn, "SELECT COUNT(*) FROM aircraft")
        stats["aircraft_retired"] = stats["aircraft_total"] - stats["aircraft_active"]

        # --- Flight counts (only completed flights with known departure) ---
        stats["flights_today"] = _q1(
            conn,
            "SELECT COUNT(*) FROM flights WHERE flight_date = CURRENT_DATE"
            " AND departure_airport_icao IS NOT NULL AND NOT needs_review",
        )
        stats["flights_7d"] = _q1(
            conn,
            "SELECT COUNT(*) FROM flights WHERE flight_date >= CURRENT_DATE - 7"
            " AND departure_airport_icao IS NOT NULL AND NOT needs_review",
        )
        stats["flights_total"] = _q1(
            conn,
            "SELECT COUNT(*) FROM flights WHERE departure_airport_icao IS NOT NULL"
            " AND NOT needs_review",
        )

        # --- DB size ---
        stats["db_size"] = _q1(
            conn, "SELECT pg_size_pretty(pg_database_size(current_database()))"
        )

        # --- Recent errors (last 48h) ---
        rows = _q(
            conn,
            """
            SELECT run_type, started_at, finished_at,
                   aircraft_total, aircraft_ok, aircraft_error,
                   flights_upserted, status, error_detail
            FROM batch_runs
            WHERE status != 'ok'
              AND started_at > NOW() - INTERVAL '48 hours'
            ORDER BY started_at DESC
            """,
        )
        stats["recent_errors"] = [
            {
                "run_type": r[0],
                "started_at": r[1].isoformat() if r[1] else None,
                "finished_at": r[2].isoformat() if r[2] else None,
                "aircraft_total": r[3],
                "aircraft_ok": r[4],
                "aircraft_error": r[5],
                "flights_upserted": r[6],
                "status": r[7],
                "error_detail": r[8],
            }
            for r in rows
        ]

        # --- Currently airborne ---
        stats["aircraft_airborne"] = _q1(
            conn,
            """
            SELECT COUNT(DISTINCT icao24)
            FROM positions
            WHERE on_ground = FALSE
              AND captured_at = (SELECT MAX(captured_at) FROM positions)
            """,
        ) or 0

        # --- Pending flights (departed, not yet landed) ---
        stats["flights_pending"] = _q1(
            conn, "SELECT COUNT(*) FROM flights WHERE arrival_airport_icao IS NULL"
        ) or 0

        # --- Minutes since last position poll ---
        stats["last_poll_age_minutes"] = _q1(
            conn,
            "SELECT ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(captured_at))) / 60) FROM positions",
        )

        # --- Last run per type ---
        for run_type in ("state_poller", "flight_detector", "fleet_discovery", "fleet_refresh"):
            row = _q(
                conn,
                """
                SELECT started_at, finished_at, aircraft_total, aircraft_ok,
                       aircraft_error, flights_upserted, status, error_detail
                FROM batch_runs
                WHERE run_type = %s
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (run_type,),
            )
            if row:
                r = row[0]
                stats[f"last_{run_type}"] = {
                    "started_at": r[0].isoformat() if r[0] else None,
                    "finished_at": r[1].isoformat() if r[1] else None,
                    "aircraft_total": r[2],
                    "aircraft_ok": r[3],
                    "aircraft_error": r[4],
                    "flights_upserted": r[5],
                    "status": r[6],
                    "error_detail": r[7],
                }
            else:
                stats[f"last_{run_type}"] = None

        # --- Aircraft type breakdown (active fleet) ---
        rows = _q(
            conn,
            """
            SELECT COALESCE(aircraft_type, 'unknown'), COUNT(*)
            FROM aircraft
            WHERE is_active
            GROUP BY aircraft_type
            ORDER BY COUNT(*) DESC
            """,
        )
        stats["aircraft_types"] = [{"type": r[0], "count": r[1]} for r in rows]

        # --- Aircraft that flew in last 7 days by type ---
        rows = _q(
            conn,
            """
            SELECT COALESCE(a.aircraft_type, 'unknown'), COUNT(DISTINCT a.icao24)
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE f.flight_date >= CURRENT_DATE - 7
              AND a.is_active
              AND NOT f.needs_review
            GROUP BY a.aircraft_type
            ORDER BY COUNT(DISTINCT a.icao24) DESC
            """,
        )
        stats["aircraft_flew_7d"] = [{"type": r[0], "count": r[1]} for r in rows]

        # --- Top 20 routes last 30 days ---
        rows = _q(
            conn,
            """
            SELECT COALESCE(departure_airport_icao, '?'),
                   COALESCE(arrival_airport_icao, '?'),
                   COUNT(*) AS cnt
            FROM flights
            WHERE flight_date >= CURRENT_DATE - 30
              AND departure_airport_icao IS NOT NULL
              AND arrival_airport_icao IS NOT NULL
              AND NOT needs_review
            GROUP BY 1, 2
            ORDER BY cnt DESC
            LIMIT 20
            """,
        )
        stats["top_routes"] = [
            {"dep": r[0], "arr": r[1], "count": r[2]} for r in rows
        ]

        # --- Flights per day last 14 days ---
        rows = _q(
            conn,
            """
            SELECT flight_date::text, COUNT(*)
            FROM flights
            WHERE flight_date >= CURRENT_DATE - 13
              AND NOT needs_review
            GROUP BY flight_date
            ORDER BY flight_date
            """,
        )
        stats["flights_per_day"] = [{"date": r[0], "count": r[1]} for r in rows]

        # --- Unique callsigns (flight numbers) per day last 14 days ---
        rows = _q(
            conn,
            """
            SELECT flight_date::text, COUNT(DISTINCT callsign)
            FROM flights
            WHERE flight_date >= CURRENT_DATE - 13
              AND callsign IS NOT NULL
              AND callsign != ''
              AND NOT needs_review
            GROUP BY flight_date
            ORDER BY flight_date
            """,
        )
        stats["callsigns_per_day"] = [{"date": r[0], "count": r[1]} for r in rows]

        stats["generated_at"] = datetime.now(tz=timezone.utc).isoformat()

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    return jsonify(stats)


_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LH Fleet Monitor</title>
<style>
:root {
  --bg: #101114;
  --surface: #191b20;
  --surface2: #1f2128;
  --border: #2a2c35;
  --text: #c9cdd6;
  --text-bright: #e4e7ed;
  --muted: #6b7280;
  --accent: #5b8def;
  --accent-dim: rgba(91,141,239,0.12);
  --green: #4ade80;
  --green-dim: rgba(74,222,128,0.12);
  --red: #f87171;
  --red-dim: rgba(248,113,113,0.12);
  --amber: #fbbf24;
  --amber-dim: rgba(251,191,36,0.12);
  --radius: 10px;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: var(--bg);
  color: var(--text);
  font-family: 'Inter', -apple-system, 'Segoe UI', system-ui, sans-serif;
  font-size: 14px;
  line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}

.container {
  max-width: 480px;
  margin: 0 auto;
  padding: 0 16px 32px;
}

/* Header */
.header {
  padding: 16px 0 12px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid var(--border);
  margin-bottom: 20px;
}
.header h1 {
  font-size: 17px;
  font-weight: 600;
  color: var(--text-bright);
  letter-spacing: -0.3px;
}
.header h1 span { color: var(--accent); font-weight: 700; }
.header .updated {
  font-size: 11px;
  color: var(--muted);
}

/* Health strip */
.health-strip {
  display: flex;
  gap: 8px;
  margin-bottom: 20px;
}
.health-item {
  flex: 1;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 12px;
  text-align: center;
}
.health-item .dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-right: 4px;
  vertical-align: middle;
}
.health-item .label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  color: var(--muted);
  margin-bottom: 6px;
}
.health-item .info {
  font-size: 12px;
  color: var(--text);
}

/* Section */
.section {
  margin-bottom: 20px;
}
.section-label {
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--muted);
  margin-bottom: 10px;
}

/* Metric row */
.metrics {
  display: grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: 8px;
  margin-bottom: 20px;
}
.metric {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 12px;
}
.metric .label {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--muted);
  margin-bottom: 4px;
}
.metric .value {
  font-size: 22px;
  font-weight: 700;
  color: var(--text-bright);
  letter-spacing: -0.5px;
}
.metric .sub {
  font-size: 10px;
  color: var(--muted);
  margin-top: 2px;
}

/* Cards / boxes */
.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 14px;
  margin-bottom: 12px;
}

/* Chart area */
.chart-bars {
  display: flex;
  align-items: flex-end;
  gap: 3px;
  height: 64px;
}
.chart-bars .bar {
  flex: 1;
  border-radius: 2px 2px 0 0;
  min-height: 2px;
  transition: height 0.3s ease;
  position: relative;
}
.chart-bars .bar:hover { opacity: 1 !important; }
.chart-labels {
  display: flex;
  justify-content: space-between;
  font-size: 10px;
  color: var(--muted);
  margin-top: 4px;
}

/* Horizontal bar chart */
.hbar-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 5px;
}
.hbar-row:last-child { margin-bottom: 0; }
.hbar-label {
  width: 44px;
  text-align: right;
  font-size: 12px;
  font-weight: 500;
  color: var(--text);
  flex-shrink: 0;
  font-variant-numeric: tabular-nums;
}
.hbar-track {
  flex: 1;
  background: var(--surface2);
  border-radius: 3px;
  height: 20px;
  overflow: hidden;
}
.hbar-fill {
  height: 100%;
  border-radius: 3px;
  transition: width 0.4s ease;
}
.hbar-count {
  width: 28px;
  font-size: 11px;
  color: var(--muted);
  font-variant-numeric: tabular-nums;
}
.hbar-flew {
  width: 28px;
  font-size: 11px;
  color: var(--accent);
  font-variant-numeric: tabular-nums;
  text-align: right;
}

/* Batch run rows */
.batch-row {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 8px 0;
  border-bottom: 1px solid rgba(42,44,53,0.5);
  font-size: 12px;
}
.batch-row:last-child { border-bottom: none; }
.batch-type {
  font-weight: 500;
  color: var(--text);
  width: 90px;
  flex-shrink: 0;
  font-size: 11px;
}
.batch-time {
  color: var(--muted);
  font-size: 11px;
  width: 56px;
  flex-shrink: 0;
}
.batch-detail {
  flex: 1;
  font-size: 11px;
  color: var(--muted);
}
.badge {
  display: inline-block;
  padding: 1px 7px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.3px;
}
.badge-ok { background: var(--green-dim); color: var(--green); }
.badge-error { background: var(--red-dim); color: var(--red); }
.badge-running { background: var(--accent-dim); color: var(--accent); }

/* Route table */
.route-row {
  display: flex;
  align-items: center;
  padding: 5px 0;
  border-bottom: 1px solid rgba(42,44,53,0.3);
  font-size: 12px;
}
.route-row:last-child { border-bottom: none; }
.route-pair {
  flex: 1;
  color: var(--text);
}
.route-pair .arrow { color: var(--muted); margin: 0 4px; }
.route-count {
  color: var(--muted);
  font-variant-numeric: tabular-nums;
  font-size: 11px;
}

/* Error banner */
#error-banner {
  display: none;
  background: var(--red-dim);
  border: 1px solid rgba(248,113,113,0.25);
  border-radius: var(--radius);
  padding: 10px 14px;
  margin-bottom: 16px;
  color: var(--red);
  font-size: 12px;
}

/* Tooltip */
.tooltip {
  position: fixed;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 4px 8px;
  font-size: 11px;
  color: var(--text);
  pointer-events: none;
  z-index: 100;
  white-space: nowrap;
  display: none;
}

/* Dual chart legend */
.chart-legend {
  display: flex;
  gap: 14px;
  margin-bottom: 8px;
  font-size: 10px;
  color: var(--muted);
}
.chart-legend .swatch {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 2px;
  margin-right: 4px;
  vertical-align: middle;
}

/* Error text */
.err-text {
  font-size: 10px;
  color: var(--red);
  margin-top: 2px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

@media (min-width: 600px) {
  .container { max-width: 540px; }
}
@media (min-width: 900px) {
  .container { max-width: 720px; padding: 0 24px 40px; }
  .metrics { grid-template-columns: repeat(3, 1fr); }
  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .two-col > .card { margin-bottom: 0; }
}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>LH Fleet <span>Monitor</span></h1>
    <div style="display:flex;align-items:center;gap:12px">
      <a href="/fleet" style="font-size:12px;color:var(--accent);text-decoration:none;padding:4px 10px;border:1px solid var(--accent);border-radius:6px">Fleet DB</a>
      <a href="/analysis" style="font-size:12px;color:var(--accent);text-decoration:none;padding:4px 10px;border:1px solid var(--accent);border-radius:6px">A380 Analysis &rarr;</a>
      <span class="updated" id="last-updated"></span>
    </div>
  </div>

  <div id="error-banner"></div>

  <!-- Health strip -->
  <div class="health-strip" id="health-strip"></div>

  <!-- Key metrics -->
  <div class="section">
    <div class="section-label">Fleet</div>
    <div class="metrics" id="fleet-metrics"></div>
  </div>

  <div class="section">
    <div class="section-label">Flights</div>
    <div class="metrics" id="flight-metrics"></div>
  </div>

  <!-- Flight trend chart -->
  <div class="section">
    <div class="card">
      <div class="section-label">Daily flights &amp; unique routes (14d)</div>
      <div class="chart-legend">
        <span><span class="swatch" style="background:var(--accent)"></span>Flights</span>
        <span><span class="swatch" style="background:var(--amber)"></span>Unique callsigns</span>
      </div>
      <div class="chart-bars" id="flight-chart" style="height:80px"></div>
      <div class="chart-labels" id="flight-chart-labels"></div>
    </div>
  </div>

  <!-- Fleet by type -->
  <div class="section">
    <div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
        <div class="section-label" style="margin-bottom:0">Aircraft by type</div>
        <div style="font-size:10px;color:var(--muted)">
          <span style="color:var(--accent)">In DB</span>
          <span style="margin:0 4px">/</span>
          <span style="color:var(--green)">Flew 7d</span>
        </div>
      </div>
      <div id="type-chart"></div>
    </div>
  </div>

  <!-- Recent errors (48h) -->
  <div class="section" id="errors-section" style="display:none">
    <div class="card">
      <div class="section-label">Errors (last 48h)</div>
      <div id="error-list"></div>
    </div>
  </div>

  <!-- Top routes -->
  <div class="section">
    <div class="card">
      <div class="section-label">Top routes (30d)</div>
      <div id="route-list"></div>
    </div>
  </div>

  <!-- System -->
  <div class="section">
    <div class="card" id="system-info" style="font-size:12px;color:var(--muted)"></div>
  </div>

</div>

<div class="tooltip" id="tooltip"></div>

<script>
const $ = id => document.getElementById(id);
const fmt = n => n == null ? '\u2014' : n.toLocaleString();

function ago(iso) {
  if (!iso) return '\u2014';
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  if (s < 86400) return Math.floor(s/3600) + 'h ago';
  return Math.floor(s/86400) + 'd ago';
}

function badge(status) {
  const c = status === 'ok' ? 'badge-ok' : status === 'running' ? 'badge-running' : 'badge-error';
  return '<span class="badge ' + c + '">' + status + '</span>';
}

// Tooltip
const tip = $('tooltip');
document.addEventListener('mousemove', e => {
  if (tip.style.display === 'block') {
    tip.style.left = (e.clientX + 10) + 'px';
    tip.style.top = (e.clientY - 28) + 'px';
  }
});

function showTip(text, e) {
  tip.textContent = text;
  tip.style.display = 'block';
  tip.style.left = (e.clientX + 10) + 'px';
  tip.style.top = (e.clientY - 28) + 'px';
}
function hideTip() { tip.style.display = 'none'; }

async function refresh() {
  let data;
  try {
    const r = await fetch('/api/stats');
    data = await r.json();
  } catch(e) {
    $('error-banner').style.display = 'block';
    $('error-banner').textContent = 'Connection error: ' + e;
    return;
  }
  if (data.error) {
    $('error-banner').style.display = 'block';
    $('error-banner').textContent = data.error;
    return;
  }
  $('error-banner').style.display = 'none';

  // Health strip
  const sp = data.last_state_poller;
  const fd = data.last_flight_detector;
  const fdisc = data.last_fleet_discovery;
  const fr = data.last_fleet_refresh;
  const pollAge = data.last_poll_age_minutes;
  const spOk = sp && sp.status === 'ok' && (pollAge === null || pollAge <= 10);
  const fdOk = fd && fd.status === 'ok';
  const frOk = fr && fr.status === 'ok';
  const frRecent = fr && fr.started_at &&
    (Date.now() - new Date(fr.started_at).getTime()) < 8 * 86400 * 1000;
  const fdiscOk = fdisc && fdisc.status === 'ok';
  const fdiscRecent = fdisc && fdisc.started_at &&
    (Date.now() - new Date(fdisc.started_at).getTime()) < 7 * 3600 * 1000;

  function healthDetail(run, type) {
    if (!run) return '';
    let detail = '';
    if (type === 'state_poller') detail = fmt(run.aircraft_ok) + ' seen, ' + fmt(run.flights_upserted) + ' stored';
    else if (type === 'flight_detector') detail = fmt(run.flights_upserted) + ' flights';
    else if (type === 'fleet_discovery') detail = fmt(run.aircraft_ok) + ' discovered';
    else detail = fmt(run.aircraft_ok) + '/' + fmt(run.aircraft_total) + ' updated';
    return '<div style="font-size:10px;color:var(--muted);margin-top:3px">' + detail + '</div>';
  }

  $('health-strip').innerHTML =
    '<div class="health-item">' +
      '<div class="label">State Poller</div>' +
      '<div class="info"><span class="dot" style="background:' + (spOk ? 'var(--green)' : sp ? 'var(--amber)' : 'var(--red)') + '"></span>' +
      (pollAge !== null && pollAge !== undefined ? pollAge + 'm ago' : (sp ? ago(sp.started_at) : 'never')) + '</div>' +
      healthDetail(sp, 'state_poller') +
    '</div>' +
    '<div class="health-item">' +
      '<div class="label">Flight Detector</div>' +
      '<div class="info"><span class="dot" style="background:' + (fdOk ? 'var(--green)' : fd ? 'var(--red)' : 'var(--red)') + '"></span>' +
      (fd ? ago(fd.started_at) : 'never') + '</div>' +
      healthDetail(fd, 'flight_detector') +
    '</div>' +
    '<div class="health-item">' +
      '<div class="label">Discovery</div>' +
      '<div class="info"><span class="dot" style="background:' + (fdiscOk && fdiscRecent ? 'var(--green)' : !fdiscRecent ? 'var(--amber)' : 'var(--red)') + '"></span>' +
      (fdisc ? ago(fdisc.started_at) : 'never') + '</div>' +
      healthDetail(fdisc, 'fleet_discovery') +
    '</div>' +
    '<div class="health-item">' +
      '<div class="label">Fleet Refresh</div>' +
      '<div class="info"><span class="dot" style="background:' + (frOk && frRecent ? 'var(--green)' : !frRecent ? 'var(--amber)' : 'var(--red)') + '"></span>' +
      (fr ? ago(fr.started_at) : 'never') + '</div>' +
      healthDetail(fr, 'fleet_refresh') +
    '</div>' +
    '<div class="health-item">' +
      '<div class="label">Airborne Now</div>' +
      '<div class="info" style="font-weight:600;font-size:15px">' + fmt(data.aircraft_airborne) + ' <span style="font-weight:400;font-size:10px;color:var(--muted)">aircraft</span></div>' +
    '</div>';

  // Fleet metrics
  $('fleet-metrics').innerHTML =
    '<div class="metric"><div class="label">Active</div><div class="value">' + fmt(data.aircraft_active) + '</div></div>' +
    '<div class="metric"><div class="label">Retired</div><div class="value">' + fmt(data.aircraft_retired) + '</div></div>' +
    '<div class="metric"><div class="label">Total</div><div class="value">' + fmt(data.aircraft_total) + '</div></div>';

  // Flight metrics
  $('flight-metrics').innerHTML =
    '<div class="metric"><div class="label">Today</div><div class="value">' + fmt(data.flights_today) + '</div></div>' +
    '<div class="metric"><div class="label">7 Days</div><div class="value">' + fmt(data.flights_7d) + '</div></div>' +
    '<div class="metric"><div class="label">All Time</div><div class="value">' + fmt(data.flights_total) + '</div></div>' +
    '<div class="metric"><div class="label">Pending</div><div class="value" style="color:var(--amber)">' + fmt(data.flights_pending) + '</div></div>';

  // Flight trend chart (dual: flights + callsigns)
  const days = data.flights_per_day || [];
  const csdays = data.callsigns_per_day || [];
  const csMap = {};
  csdays.forEach(d => csMap[d.date] = d.count);
  const maxF = days.length ? Math.max(...days.map(d => d.count), 1) : 1;

  $('flight-chart').innerHTML = days.map(d => {
    const h = Math.max(3, Math.round(d.count / maxF * 76));
    const csCount = csMap[d.date] || 0;
    const csH = Math.max(0, Math.round(csCount / maxF * 76));
    return '<div style="flex:1;display:flex;flex-direction:column;align-items:stretch;justify-content:flex-end;height:80px" ' +
      'onmouseenter="showTip(\\'' + d.date + ': ' + d.count + ' flights, ' + csCount + ' callsigns\\', event)" onmouseleave="hideTip()">' +
      '<div style="height:' + h + 'px;background:var(--accent);border-radius:2px 2px 0 0;opacity:0.7;position:relative">' +
      (csH > 0 ? '<div style="position:absolute;bottom:0;left:0;right:0;height:' + Math.min(csH, h) + 'px;background:var(--amber);border-radius:0 0 0 0;opacity:0.6"></div>' : '') +
      '</div></div>';
  }).join('');

  if (days.length >= 2) {
    $('flight-chart-labels').innerHTML =
      '<span>' + days[0].date.slice(5) + '</span><span>' + days[days.length-1].date.slice(5) + '</span>';
  }

  // Aircraft type chart with flew-in-7d overlay
  const types = data.aircraft_types || [];
  const flew = data.aircraft_flew_7d || [];
  const flewMap = {};
  flew.forEach(f => flewMap[f.type] = f.count);
  const maxT = types.length ? types[0].count : 1;

  $('type-chart').innerHTML = types.map(t => {
    const pct = Math.round(t.count / maxT * 100);
    const flewCount = flewMap[t.type] || 0;
    const flewPct = Math.round(flewCount / maxT * 100);
    return '<div class="hbar-row">' +
      '<div class="hbar-label">' + t.type + '</div>' +
      '<div class="hbar-track">' +
        '<div class="hbar-fill" style="width:' + pct + '%;background:var(--accent);opacity:0.5;position:relative">' +
          '<div style="position:absolute;top:0;left:0;height:100%;width:' + (t.count > 0 ? Math.round(flewCount / t.count * 100) : 0) + '%;background:var(--green);border-radius:3px;opacity:0.8"></div>' +
        '</div>' +
      '</div>' +
      '<div class="hbar-count">' + t.count + '</div>' +
      '<div class="hbar-flew">' + (flewCount || '\u2014') + '</div>' +
    '</div>';
  }).join('');

  // Recent errors (48h)
  const errors = data.recent_errors || [];
  if (errors.length) {
    $('errors-section').style.display = '';
    $('error-list').innerHTML = errors.map(r =>
      '<div class="batch-row">' +
        '<div class="batch-type">' + r.run_type.replace('_', ' ') + '</div>' +
        '<div class="batch-time">' + ago(r.started_at) + '</div>' +
        '<div class="batch-detail">' +
          (r.error_detail || r.status) +
        '</div>' +
        badge(r.status) +
      '</div>'
    ).join('');
  } else {
    $('errors-section').style.display = 'none';
  }

  // Top routes
  const routes = data.top_routes || [];
  $('route-list').innerHTML = routes.slice(0, 15).map(r =>
    '<div class="route-row">' +
      '<div class="route-pair">' + r.dep + '<span class="arrow">\u2192</span>' + r.arr + '</div>' +
      '<div class="route-count">' + r.count + '</div>' +
    '</div>'
  ).join('');

  // System info
  $('system-info').innerHTML =
    'DB size: <span style="color:var(--text)">' + (data.db_size || '\u2014') + '</span>' +
    '<span style="margin:0 10px;color:var(--border)">\u00b7</span>' +
    'Aircraft: <span style="color:var(--text)">' + fmt(data.aircraft_total) + '</span>' +
    '<span style="margin:0 10px;color:var(--border)">\u00b7</span>' +
    'Flights: <span style="color:var(--text)">' + fmt(data.flights_total) + '</span>';

  $('last-updated').textContent = new Date().toLocaleTimeString();
}

refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(_HTML)


# ── A380 Rotation Analysis ──────────────────────────────────────────


@app.route("/api/a380-analysis")
def api_a380_analysis():
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503

    try:
        data = {}

        # 1. Timeline: all A380 flights last 90 days
        rows = _q(
            conn,
            """
            SELECT a.registration,
                   f.departure_airport_icao, f.arrival_airport_icao,
                   f.first_seen, f.last_seen, f.callsign
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = 'A388'
              AND f.flight_date >= CURRENT_DATE - 90
              AND f.departure_airport_icao IS NOT NULL
              AND f.arrival_airport_icao IS NOT NULL
              AND NOT f.needs_review
            ORDER BY a.registration, f.first_seen
            """,
        )
        data["timeline"] = [
            {
                "reg": r[0].strip(),
                "dep": r[1].strip(),
                "arr": r[2].strip(),
                "t0": r[3].isoformat(),
                "t1": r[4].isoformat(),
                "cs": (r[5] or "").strip(),
            }
            for r in rows
        ]

        # 2. MUC-BKK day-of-week heatmap
        rows = _q(
            conn,
            """
            SELECT a.registration,
                   EXTRACT(DOW FROM f.first_seen AT TIME ZONE 'UTC')::int AS dow,
                   COUNT(*)
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = 'A388'
              AND f.departure_airport_icao = 'EDDM'
              AND f.arrival_airport_icao = 'VTBS'
              AND NOT f.needs_review
            GROUP BY a.registration, dow
            ORDER BY a.registration, dow
            """,
        )
        data["dow_heatmap"] = [
            {"reg": r[0].strip(), "dow": r[1], "count": r[2]} for r in rows
        ]

        # 3. MUC-BKK cycle lengths (computed in Python)
        rows = _q(
            conn,
            """
            SELECT a.registration, f.flight_date
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = 'A388'
              AND f.departure_airport_icao = 'EDDM'
              AND f.arrival_airport_icao = 'VTBS'
              AND NOT f.needs_review
            ORDER BY a.registration, f.flight_date
            """,
        )
        reg_dates = defaultdict(list)
        for r in rows:
            reg_dates[r[0].strip()].append(r[1])

        cycle_data = []
        all_gaps = []
        for reg, dates in sorted(reg_dates.items()):
            gaps = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
            cycle_data.append({"reg": reg, "gaps": gaps})
            all_gaps.extend(gaps)
        data["cycle_lengths"] = cycle_data
        data["cycle_histogram"] = sorted(all_gaps)

        # 4. Markov chain: route transitions for A380s
        rows = _q(
            conn,
            """
            WITH ordered AS (
                SELECT f.departure_airport_icao || '-' || f.arrival_airport_icao AS route,
                       LEAD(f.departure_airport_icao || '-' || f.arrival_airport_icao)
                           OVER (PARTITION BY a.registration ORDER BY f.first_seen) AS next_route
                FROM flights f
                JOIN aircraft a ON a.icao24 = f.icao24
                WHERE a.aircraft_type = 'A388'
                  AND f.departure_airport_icao IS NOT NULL
                  AND f.arrival_airport_icao IS NOT NULL
                  AND NOT f.needs_review
            )
            SELECT route, next_route, COUNT(*) AS cnt
            FROM ordered
            WHERE next_route IS NOT NULL
            GROUP BY route, next_route
            ORDER BY cnt DESC
            LIMIT 80
            """,
        )
        data["markov"] = [
            {"from": r[0].strip(), "to": r[1].strip(), "count": r[2]} for r in rows
        ]

        # 5. Registration × Route affinity (top routes only)
        rows = _q(
            conn,
            """
            WITH top_routes AS (
                SELECT f.departure_airport_icao || '-' || f.arrival_airport_icao AS route
                FROM flights f
                JOIN aircraft a ON a.icao24 = f.icao24
                WHERE a.aircraft_type = 'A388'
                  AND f.departure_airport_icao IS NOT NULL
                  AND f.arrival_airport_icao IS NOT NULL
                  AND NOT f.needs_review
                GROUP BY route
                ORDER BY COUNT(*) DESC
                LIMIT 20
            )
            SELECT a.registration,
                   f.departure_airport_icao || '-' || f.arrival_airport_icao AS route,
                   COUNT(*) AS cnt
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = 'A388'
              AND f.departure_airport_icao IS NOT NULL
              AND f.arrival_airport_icao IS NOT NULL
              AND NOT f.needs_review
              AND (f.departure_airport_icao || '-' || f.arrival_airport_icao)
                  IN (SELECT route FROM top_routes)
            GROUP BY a.registration, route
            ORDER BY a.registration, cnt DESC
            """,
        )
        data["affinity"] = [
            {"reg": r[0].strip(), "route": r[1].strip(), "count": r[2]} for r in rows
        ]

        # 6. Preceding flights before MUC-BKK
        rows = _q(
            conn,
            """
            WITH muc_bkk AS (
                SELECT f.icao24, f.first_seen
                FROM flights f
                JOIN aircraft a ON a.icao24 = f.icao24
                WHERE a.aircraft_type = 'A388'
                  AND f.departure_airport_icao = 'EDDM'
                  AND f.arrival_airport_icao = 'VTBS'
                  AND NOT f.needs_review
            ),
            prev AS (
                SELECT mb.icao24, mb.first_seen AS target,
                       f.departure_airport_icao || '-' || f.arrival_airport_icao AS route,
                       ROW_NUMBER() OVER (
                           PARTITION BY mb.icao24, mb.first_seen
                           ORDER BY f.first_seen DESC
                       ) AS rn
                FROM muc_bkk mb
                JOIN flights f ON f.icao24 = mb.icao24
                                AND f.first_seen < mb.first_seen
                WHERE f.departure_airport_icao IS NOT NULL
                  AND f.arrival_airport_icao IS NOT NULL
                  AND NOT f.needs_review
            )
            SELECT rn AS steps_before, route, COUNT(*) AS cnt
            FROM prev
            WHERE rn <= 3
            GROUP BY rn, route
            ORDER BY rn, cnt DESC
            """,
        )
        data["preceding"] = [
            {"step": r[0], "route": r[1].strip(), "count": r[2]} for r in rows
        ]

        # 7. Fleet positions (last known location per A380)
        rows = _q(
            conn,
            """
            SELECT DISTINCT ON (a.registration)
                   a.registration, f.arrival_airport_icao, f.last_seen,
                   f.departure_airport_icao, f.callsign
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = 'A388'
              AND a.is_active
              AND f.arrival_airport_icao IS NOT NULL
              AND NOT f.needs_review
            ORDER BY a.registration, f.last_seen DESC
            """,
        )
        data["fleet_positions"] = [
            {
                "reg": r[0].strip(),
                "airport": r[1].strip(),
                "last_seen": r[2].isoformat(),
                "from": r[3].strip() if r[3] else "",
                "cs": (r[4] or "").strip(),
            }
            for r in rows
        ]

        # 8. MUC-BKK flight history with registration
        rows = _q(
            conn,
            """
            SELECT a.registration, f.flight_date, f.callsign,
                   f.first_seen, f.last_seen, f.duration_minutes
            FROM flights f
            JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = 'A388'
              AND f.departure_airport_icao = 'EDDM'
              AND f.arrival_airport_icao = 'VTBS'
            ORDER BY f.flight_date DESC
            """,
        )
        data["muc_bkk_history"] = [
            {
                "reg": r[0].strip(),
                "date": r[1].isoformat(),
                "cs": (r[2] or "").strip(),
                "t0": r[3].isoformat(),
                "t1": r[4].isoformat(),
                "dur": r[5],
            }
            for r in rows
        ]

        data["generated_at"] = datetime.now(tz=timezone.utc).isoformat()

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    return jsonify(data)


_ANALYSIS_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>A380 Rotation Analysis</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
:root {
  --bg: #101114;
  --surface: #191b20;
  --surface2: #1f2128;
  --border: #2a2c35;
  --text: #c9cdd6;
  --text-bright: #e4e7ed;
  --muted: #6b7280;
  --accent: #5b8def;
  --accent-dim: rgba(91,141,239,0.12);
  --green: #4ade80;
  --green-dim: rgba(74,222,128,0.12);
  --red: #f87171;
  --red-dim: rgba(248,113,113,0.12);
  --amber: #fbbf24;
  --amber-dim: rgba(251,191,36,0.12);
  --cyan: #22d3ee;
  --purple: #a78bfa;
  --pink: #f472b6;
  --radius: 10px;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: 'Inter', -apple-system, 'Segoe UI', system-ui, sans-serif;
  font-size: 14px; line-height: 1.5;
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 1100px; margin: 0 auto; padding: 0 16px 40px; }

/* Header */
.header {
  padding: 16px 0 12px;
  display: flex; justify-content: space-between; align-items: center;
  border-bottom: 1px solid var(--border); margin-bottom: 20px;
}
.header h1 { font-size: 17px; font-weight: 600; color: var(--text-bright); letter-spacing: -0.3px; }
.header h1 span { color: var(--accent); font-weight: 700; }
.nav-link {
  font-size: 12px; color: var(--accent); text-decoration: none;
  padding: 4px 10px; border: 1px solid var(--accent); border-radius: 6px;
}
.nav-link:hover { background: var(--accent-dim); }

/* Sections */
.section { margin-bottom: 24px; }
.section-label {
  font-size: 10px; font-weight: 600; text-transform: uppercase;
  letter-spacing: 1px; color: var(--muted); margin-bottom: 10px;
}
.card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 14px; margin-bottom: 12px;
}
.card-title {
  font-size: 13px; font-weight: 600; color: var(--text-bright);
  margin-bottom: 10px;
}
.card-subtitle {
  font-size: 11px; color: var(--muted); margin-bottom: 12px;
}

/* Fleet position cards */
.fleet-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
  gap: 8px;
}
.fleet-card {
  background: var(--surface2); border: 1px solid var(--border);
  border-radius: 8px; padding: 10px; text-align: center;
}
.fleet-card .reg {
  font-size: 13px; font-weight: 700; color: var(--text-bright);
  margin-bottom: 4px;
}
.fleet-card .airport {
  font-size: 18px; font-weight: 700; letter-spacing: 1px;
}
.fleet-card .meta {
  font-size: 10px; color: var(--muted); margin-top: 4px;
}
.fleet-card.at-muc .airport { color: var(--green); }
.fleet-card.at-bkk .airport { color: var(--amber); }
.fleet-card.at-other .airport { color: var(--text); }

/* Gantt timeline */
.gantt { overflow-x: auto; }
.gantt-row {
  display: flex; align-items: center; margin-bottom: 2px; height: 22px;
}
.gantt-label {
  width: 70px; flex-shrink: 0; font-size: 11px; font-weight: 600;
  color: var(--text-bright); text-align: right; padding-right: 8px;
}
.gantt-track {
  flex: 1; position: relative; height: 18px; background: var(--surface2);
  border-radius: 3px; overflow: hidden; min-width: 800px;
}
.gantt-flight {
  position: absolute; height: 100%; border-radius: 2px;
  min-width: 2px; cursor: pointer; opacity: 0.85;
  transition: opacity 0.15s;
}
.gantt-flight:hover { opacity: 1; z-index: 2; }
.gantt-flight.muc-bkk { background: var(--accent); }
.gantt-flight.bkk-muc { background: var(--green); }
.gantt-flight.other { background: var(--muted); opacity: 0.4; }
.gantt-axis {
  display: flex; justify-content: space-between;
  margin-left: 70px; min-width: 800px;
  font-size: 10px; color: var(--muted); padding-top: 4px;
}

/* Heatmap */
.heatmap-grid {
  display: grid; gap: 2px;
  grid-template-columns: 80px repeat(7, 1fr);
}
.heatmap-cell {
  height: 28px; border-radius: 4px; display: flex;
  align-items: center; justify-content: center;
  font-size: 11px; font-weight: 600;
}
.heatmap-header {
  font-size: 10px; color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.5px;
}
.heatmap-label {
  font-size: 11px; font-weight: 600; color: var(--text-bright);
  text-align: right; padding-right: 8px;
}

/* Markov table */
.markov-row {
  display: flex; align-items: center; gap: 8px; padding: 4px 0;
  border-bottom: 1px solid rgba(42,44,53,0.3); font-size: 12px;
}
.markov-row:last-child { border-bottom: none; }
.markov-from { width: 100px; color: var(--text); font-weight: 500; text-align: right; }
.markov-arrow { color: var(--muted); font-size: 10px; }
.markov-to { width: 100px; color: var(--text-bright); font-weight: 500; }
.markov-bar { flex: 1; height: 16px; background: var(--surface2); border-radius: 3px; overflow: hidden; }
.markov-fill { height: 100%; border-radius: 3px; background: var(--accent); opacity: 0.7; }
.markov-count { width: 40px; font-size: 11px; color: var(--muted); text-align: right; }
.markov-pct { width: 40px; font-size: 11px; color: var(--accent); text-align: right; }

/* Affinity matrix */
.affinity-wrap { overflow-x: auto; }
.affinity-table { border-collapse: collapse; font-size: 11px; }
.affinity-table th {
  padding: 4px 6px; font-weight: 600; color: var(--muted);
  text-align: center; position: sticky; top: 0; background: var(--surface);
}
.affinity-table th.route-header {
  writing-mode: vertical-lr; transform: rotate(180deg);
  height: 80px; font-size: 10px; letter-spacing: 0.5px;
}
.affinity-table td {
  padding: 4px 6px; text-align: center; border-radius: 3px;
}
.affinity-table td.reg-label {
  font-weight: 600; color: var(--text-bright); text-align: right;
  position: sticky; left: 0; background: var(--surface);
}

/* Preceding flow */
.preceding-group { margin-bottom: 12px; }
.preceding-label {
  font-size: 11px; color: var(--accent); font-weight: 600;
  margin-bottom: 6px;
}
.preceding-bar-row {
  display: flex; align-items: center; gap: 6px; margin-bottom: 3px;
}
.preceding-route { width: 90px; font-size: 11px; color: var(--text); text-align: right; }
.preceding-track {
  flex: 1; height: 14px; background: var(--surface2);
  border-radius: 3px; overflow: hidden;
}
.preceding-fill { height: 100%; border-radius: 3px; }
.preceding-count { width: 30px; font-size: 10px; color: var(--muted); }

/* History table */
.history-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.history-table th {
  text-align: left; padding: 6px 8px; font-size: 10px; font-weight: 600;
  color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px;
  border-bottom: 1px solid var(--border);
}
.history-table td {
  padding: 5px 8px; border-bottom: 1px solid rgba(42,44,53,0.3);
  color: var(--text);
}
.history-table .reg-cell { font-weight: 700; color: var(--text-bright); }

/* Chart containers */
.chart-container { position: relative; height: 220px; }

/* Two-col layout */
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }

/* Tooltip */
.tooltip {
  position: fixed; background: var(--surface2); border: 1px solid var(--border);
  border-radius: 6px; padding: 6px 10px; font-size: 11px; color: var(--text);
  pointer-events: none; z-index: 100; white-space: nowrap; display: none;
  max-width: 300px;
}

/* Loading */
.loading {
  text-align: center; padding: 40px; color: var(--muted); font-size: 13px;
}

/* Error */
.error-banner {
  display: none; background: var(--red-dim); border: 1px solid rgba(248,113,113,0.25);
  border-radius: var(--radius); padding: 10px 14px; margin-bottom: 16px;
  color: var(--red); font-size: 12px;
}

@media (max-width: 800px) {
  .two-col { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>A380 Rotation <span>Analysis</span></h1>
    <a class="nav-link" href="/fleet">Fleet DB</a>
    <a class="nav-link" href="/">&larr; Monitor</a>
  </div>

  <div class="error-banner" id="error-banner"></div>
  <div class="loading" id="loading">Loading A380 analysis data&hellip;</div>
  <div id="content" style="display:none">

  <!-- 1. Fleet positions -->
  <div class="section">
    <div class="card">
      <div class="card-title">A380 Fleet Positions</div>
      <div class="card-subtitle">Last known location of each active A380</div>
      <div class="fleet-grid" id="fleet-grid"></div>
    </div>
  </div>

  <!-- 2. MUC-BKK history -->
  <div class="section">
    <div class="card">
      <div class="card-title">MUC &rarr; BKK Flight History</div>
      <div class="card-subtitle">All recorded EDDM &rarr; VTBS flights by A380 aircraft</div>
      <div id="history-table"></div>
    </div>
  </div>

  <!-- 3. Rotation timeline -->
  <div class="section">
    <div class="card">
      <div class="card-title">A380 Rotation Timeline (90 days)</div>
      <div class="card-subtitle">
        <span style="color:var(--accent)">&block;</span> MUC&rarr;BKK&ensp;
        <span style="color:var(--green)">&block;</span> BKK&rarr;MUC&ensp;
        <span style="color:var(--muted)">&block;</span> Other
      </div>
      <div class="gantt" id="gantt"></div>
    </div>
  </div>

  <!-- 4. DoW heatmap + Cycle histogram -->
  <div class="section two-col">
    <div class="card">
      <div class="card-title">MUC&rarr;BKK by Day of Week</div>
      <div class="card-subtitle">Frequency of each registration per weekday</div>
      <div id="dow-heatmap"></div>
    </div>
    <div class="card">
      <div class="card-title">Rotation Cycle Length</div>
      <div class="card-subtitle">Days between consecutive MUC&rarr;BKK flights (same aircraft)</div>
      <div class="chart-container"><canvas id="cycle-chart"></canvas></div>
    </div>
  </div>

  <!-- 5. Markov transitions -->
  <div class="section">
    <div class="card">
      <div class="card-title">Route Transition Probabilities</div>
      <div class="card-subtitle">After an A380 flies route X, what is the most likely next route? Top transitions shown.</div>
      <div id="markov-focus"></div>
    </div>
  </div>

  <!-- 6. Registration × Route affinity -->
  <div class="section">
    <div class="card">
      <div class="card-title">Registration &times; Route Affinity</div>
      <div class="card-subtitle">How often each A380 registration flies the top 20 routes (darker = more flights)</div>
      <div class="affinity-wrap" id="affinity"></div>
    </div>
  </div>

  <!-- 7. Preceding flights -->
  <div class="section two-col">
    <div class="card">
      <div class="card-title">Flights Before MUC&rarr;BKK</div>
      <div class="card-subtitle">What routes does an A380 typically fly in the 1-3 flights before a MUC&rarr;BKK departure?</div>
      <div id="preceding"></div>
    </div>
    <div class="card">
      <div class="card-title">Per-Aircraft Cycle Gaps</div>
      <div class="card-subtitle">Days between MUC&rarr;BKK appearances per registration</div>
      <div id="per-reg-cycles"></div>
    </div>
  </div>

  </div><!-- /content -->
</div>

<div class="tooltip" id="tooltip"></div>

<script>
const $ = id => document.getElementById(id);
const tip = $('tooltip');

document.addEventListener('mousemove', e => {
  if (tip.style.display === 'block') {
    tip.style.left = (e.clientX + 12) + 'px';
    tip.style.top = (e.clientY - 32) + 'px';
  }
});
function showTip(html, e) {
  tip.innerHTML = html;
  tip.style.display = 'block';
  tip.style.left = (e.clientX + 12) + 'px';
  tip.style.top = (e.clientY - 32) + 'px';
}
function hideTip() { tip.style.display = 'none'; }

function ago(iso) {
  if (!iso) return '\\u2014';
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  if (s < 86400) return Math.floor(s/3600) + 'h ago';
  return Math.floor(s/86400) + 'd ago';
}

const ICAO_NAMES = {
  EDDM:'MUC', VTBS:'BKK', EDDF:'FRA', KJFK:'JFK', KLAX:'LAX',
  KORD:'ORD', OMDB:'DXB', VHHH:'HKG', RJTT:'NRT', RKSI:'ICN',
  WSSS:'SIN', FAOR:'JNB', SBGR:'GRU', LEMD:'MAD', EGLL:'LHR',
  LFPG:'CDG', EDDB:'BER', EDDL:'DUS', EDDS:'STR', EDDH:'HAM',
  RPLL:'MNL', ZSPD:'PVG', ZBAA:'PEK', WMKK:'KUL', VIDP:'DEL',
  VABB:'BOM', LEBL:'BCN', LIRF:'FCO', YSSY:'SYD', OEJN:'JED',
  OERK:'RUH', OTHH:'DOH', OMAA:'AUH', CYYZ:'YYZ', KIAH:'IAH',
  KMIA:'MIA', KSFO:'SFO', CYVR:'YVR', RJAA:'NRT', LOWW:'VIE',
  EHAM:'AMS', LSZH:'ZRH',
};
function icaoToCity(code) { return ICAO_NAMES[code] || code; }
function routeName(r) {
  const p = r.split('-');
  return icaoToCity(p[0]) + '\\u2192' + icaoToCity(p[1]);
}

async function init() {
  let data;
  try {
    const r = await fetch('/api/a380-analysis');
    data = await r.json();
  } catch(e) {
    $('error-banner').style.display = 'block';
    $('error-banner').textContent = 'Connection error: ' + e;
    $('loading').style.display = 'none';
    return;
  }
  if (data.error) {
    $('error-banner').style.display = 'block';
    $('error-banner').textContent = data.error;
    $('loading').style.display = 'none';
    return;
  }
  $('loading').style.display = 'none';
  $('content').style.display = 'block';

  renderFleetPositions(data.fleet_positions);
  renderHistory(data.muc_bkk_history);
  renderGantt(data.timeline);
  renderDowHeatmap(data.dow_heatmap);
  renderCycleChart(data.cycle_histogram);
  renderMarkov(data.markov);
  renderAffinity(data.affinity);
  renderPreceding(data.preceding);
  renderPerRegCycles(data.cycle_lengths);
}

/* ── 1. Fleet Positions ───────────────────────────────── */
function renderFleetPositions(positions) {
  const el = $('fleet-grid');
  el.innerHTML = positions.map(p => {
    const cls = p.airport === 'EDDM' ? 'at-muc' : p.airport === 'VTBS' ? 'at-bkk' : 'at-other';
    return '<div class="fleet-card ' + cls + '">' +
      '<div class="reg">' + p.reg + '</div>' +
      '<div class="airport">' + icaoToCity(p.airport) + '</div>' +
      '<div class="meta">' + (p.cs || '') + ' &middot; ' + ago(p.last_seen) + '</div>' +
      '<div class="meta">' + icaoToCity(p.from) + '&rarr;' + icaoToCity(p.airport) + '</div>' +
    '</div>';
  }).join('');
}

/* ── 2. MUC-BKK History ──────────────────────────────── */
function renderHistory(history) {
  if (!history.length) { $('history-table').innerHTML = '<div style="color:var(--muted)">No MUC&rarr;BKK flights recorded yet</div>'; return; }
  const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  let html = '<table class="history-table"><thead><tr>' +
    '<th>Date</th><th>Day</th><th>Registration</th><th>Callsign</th><th>Duration</th>' +
    '</tr></thead><tbody>';
  history.forEach(h => {
    const d = new Date(h.date + 'T00:00:00');
    const day = days[d.getUTCDay()];
    const dur = h.dur ? Math.floor(h.dur/60) + 'h ' + (h.dur%60) + 'm' : '\\u2014';
    html += '<tr><td>' + h.date + '</td><td>' + day + '</td>' +
      '<td class="reg-cell">' + h.reg + '</td>' +
      '<td>' + (h.cs || '\\u2014') + '</td><td>' + dur + '</td></tr>';
  });
  html += '</tbody></table>';
  $('history-table').innerHTML = html;
}

/* ── 3. Gantt Timeline ───────────────────────────────── */
function renderGantt(timeline) {
  if (!timeline.length) { $('gantt').innerHTML = '<div style="color:var(--muted)">No A380 flight data</div>'; return; }
  const now = Date.now();
  const t0 = now - 90*86400000;
  const range = now - t0;

  // Group by registration
  const regs = {};
  timeline.forEach(f => {
    if (!regs[f.reg]) regs[f.reg] = [];
    regs[f.reg].push(f);
  });

  let html = '';
  Object.keys(regs).sort().forEach(reg => {
    html += '<div class="gantt-row"><div class="gantt-label">' + reg + '</div><div class="gantt-track">';
    regs[reg].forEach(f => {
      const fs = new Date(f.t0).getTime();
      const fe = new Date(f.t1).getTime();
      const left = Math.max(0, (fs - t0) / range * 100);
      const width = Math.max(0.15, (fe - fs) / range * 100);
      const isMucBkk = f.dep === 'EDDM' && f.arr === 'VTBS';
      const isBkkMuc = f.dep === 'VTBS' && f.arr === 'EDDM';
      const cls = isMucBkk ? 'muc-bkk' : isBkkMuc ? 'bkk-muc' : 'other';
      const tipText = f.reg + ' ' + icaoToCity(f.dep) + '&rarr;' + icaoToCity(f.arr) +
        '<br>' + (f.cs || '') + ' &middot; ' + f.t0.slice(0,10);
      html += '<div class="gantt-flight ' + cls + '" style="left:' + left + '%;width:' + width + '%"' +
        ' onmouseenter="showTip(\\'' + tipText.replace(/'/g, "\\\\'") + '\\', event)" onmouseleave="hideTip()"></div>';
    });
    html += '</div></div>';
  });

  // Axis labels
  html += '<div class="gantt-axis">';
  for (let i = 0; i <= 6; i++) {
    const d = new Date(t0 + (range * i / 6));
    html += '<span>' + d.toISOString().slice(5,10) + '</span>';
  }
  html += '</div>';

  $('gantt').innerHTML = html;
}

/* ── 4. Day-of-Week Heatmap ──────────────────────────── */
function renderDowHeatmap(heatData) {
  if (!heatData.length) { $('dow-heatmap').innerHTML = '<div style="color:var(--muted)">No MUC&rarr;BKK data yet</div>'; return; }
  const days = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const regSet = [...new Set(heatData.map(h => h.reg))].sort();
  const maxCount = Math.max(...heatData.map(h => h.count), 1);

  // Build lookup
  const lookup = {};
  heatData.forEach(h => { lookup[h.reg + '_' + h.dow] = h.count; });

  let html = '<div class="heatmap-grid" style="grid-template-columns: 80px repeat(7,1fr)">';
  // Header row
  html += '<div></div>';
  days.forEach(d => { html += '<div class="heatmap-cell heatmap-header">' + d + '</div>'; });
  // Data rows
  regSet.forEach(reg => {
    html += '<div class="heatmap-cell heatmap-label">' + reg + '</div>';
    for (let dow = 0; dow < 7; dow++) {
      const cnt = lookup[reg + '_' + dow] || 0;
      const intensity = cnt / maxCount;
      const bg = cnt > 0
        ? 'rgba(91,141,239,' + (0.15 + intensity * 0.75) + ')'
        : 'var(--surface2)';
      html += '<div class="heatmap-cell" style="background:' + bg + ';color:' +
        (cnt > 0 ? 'var(--text-bright)' : 'var(--muted)') + '">' + (cnt || '&middot;') + '</div>';
    }
  });
  html += '</div>';
  $('dow-heatmap').innerHTML = html;
}

/* ── 5. Cycle Length Histogram ────────────────────────── */
function renderCycleChart(gaps) {
  if (!gaps.length) { $('cycle-chart').parentElement.innerHTML = '<div style="color:var(--muted);padding:20px">Not enough data for cycle analysis</div>'; return; }
  // Bucket into bins
  const maxGap = Math.max(...gaps);
  const binSize = maxGap <= 30 ? 1 : maxGap <= 60 ? 2 : 5;
  const bins = {};
  gaps.forEach(g => {
    const b = Math.floor(g / binSize) * binSize;
    bins[b] = (bins[b] || 0) + 1;
  });
  const labels = Object.keys(bins).sort((a,b) => a-b).map(b => binSize === 1 ? b + 'd' : b + '-' + (parseInt(b)+binSize-1) + 'd');
  const values = Object.keys(bins).sort((a,b) => a-b).map(b => bins[b]);

  new Chart($('cycle-chart'), {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        data: values,
        backgroundColor: 'rgba(91,141,239,0.6)',
        borderColor: 'rgba(91,141,239,0.9)',
        borderWidth: 1, borderRadius: 3,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#6b7280', font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { color: '#6b7280', font: { size: 10 }, stepSize: 1 }, grid: { color: 'rgba(42,44,53,0.5)' } }
      }
    }
  });
}

/* ── 6. Markov Transitions ────────────────────────────── */
function renderMarkov(markov) {
  if (!markov.length) { $('markov-focus').innerHTML = '<div style="color:var(--muted)">Not enough data</div>'; return; }

  // Group by "from" route, compute probabilities
  const fromTotals = {};
  markov.forEach(m => { fromTotals[m.from] = (fromTotals[m.from] || 0) + m.count; });

  // Focus on routes relevant to MUC-BKK prediction
  const focusRoutes = ['EDDM-VTBS', 'VTBS-EDDM'];
  // Also find top routes by total transitions
  const allFromRoutes = [...new Set(markov.map(m => m.from))];

  // Show focused view first, then general top transitions
  let html = '';

  focusRoutes.forEach(fr => {
    const transitions = markov.filter(m => m.from === fr);
    if (!transitions.length) return;
    const total = fromTotals[fr];
    html += '<div style="margin-bottom:16px"><div style="font-size:12px;font-weight:600;color:var(--accent);margin-bottom:6px">After ' + routeName(fr) + ' (' + total + ' observed)</div>';
    transitions.sort((a,b) => b.count - a.count).slice(0, 8).forEach(t => {
      const pct = (t.count / total * 100).toFixed(0);
      html += '<div class="markov-row">' +
        '<div class="markov-from" style="width:auto">' + routeName(t.from) + '</div>' +
        '<div class="markov-arrow">&rarr;</div>' +
        '<div class="markov-to" style="width:auto">' + routeName(t.to) + '</div>' +
        '<div class="markov-bar"><div class="markov-fill" style="width:' + pct + '%"></div></div>' +
        '<div class="markov-count">' + t.count + '</div>' +
        '<div class="markov-pct">' + pct + '%</div>' +
      '</div>';
    });
    html += '</div>';
  });

  // General top 20
  html += '<div style="margin-top:16px"><div style="font-size:12px;font-weight:600;color:var(--muted);margin-bottom:6px">Top transitions overall</div>';
  const maxCount = markov[0].count;
  markov.slice(0, 25).forEach(t => {
    const pct = (t.count / fromTotals[t.from] * 100).toFixed(0);
    html += '<div class="markov-row">' +
      '<div class="markov-from">' + routeName(t.from) + '</div>' +
      '<div class="markov-arrow">&rarr;</div>' +
      '<div class="markov-to">' + routeName(t.to) + '</div>' +
      '<div class="markov-bar"><div class="markov-fill" style="width:' + (t.count/maxCount*100) + '%"></div></div>' +
      '<div class="markov-count">' + t.count + '</div>' +
      '<div class="markov-pct">' + pct + '%</div>' +
    '</div>';
  });
  html += '</div>';

  $('markov-focus').innerHTML = html;
}

/* ── 7. Affinity Matrix ───────────────────────────────── */
function renderAffinity(affinity) {
  if (!affinity.length) { $('affinity').innerHTML = '<div style="color:var(--muted)">Not enough data</div>'; return; }

  // Collect unique routes and registrations
  const routeCount = {};
  affinity.forEach(a => { routeCount[a.route] = (routeCount[a.route] || 0) + a.count; });
  const routes = Object.entries(routeCount).sort((a,b) => b[1]-a[1]).map(e => e[0]);
  const regs = [...new Set(affinity.map(a => a.reg))].sort();

  // Build lookup
  const lookup = {};
  let maxVal = 0;
  affinity.forEach(a => {
    lookup[a.reg + '|' + a.route] = a.count;
    if (a.count > maxVal) maxVal = a.count;
  });

  let html = '<table class="affinity-table"><thead><tr><th></th>';
  routes.forEach(r => {
    html += '<th class="route-header">' + routeName(r) + '</th>';
  });
  html += '</tr></thead><tbody>';

  regs.forEach(reg => {
    html += '<tr><td class="reg-label">' + reg + '</td>';
    routes.forEach(route => {
      const cnt = lookup[reg + '|' + route] || 0;
      const intensity = cnt / maxVal;
      const isMucBkk = route === 'EDDM-VTBS';
      const baseColor = isMucBkk ? '91,141,239' : '201,205,214';
      const bg = cnt > 0
        ? 'rgba(' + baseColor + ',' + (0.1 + intensity * 0.8) + ')'
        : 'transparent';
      html += '<td style="background:' + bg + ';color:' +
        (cnt > 0 ? 'var(--text-bright)' : '') + '">' + (cnt || '') + '</td>';
    });
    html += '</tr>';
  });

  html += '</tbody></table>';
  $('affinity').innerHTML = html;
}

/* ── 8. Preceding Flights ─────────────────────────────── */
function renderPreceding(preceding) {
  if (!preceding.length) { $('preceding').innerHTML = '<div style="color:var(--muted)">Not enough data</div>'; return; }

  const steps = [1, 2, 3];
  const stepLabels = ['Flight N-1 (immediately before)', 'Flight N-2', 'Flight N-3'];
  const colors = ['var(--accent)', 'var(--green)', 'var(--amber)'];
  let html = '';

  steps.forEach((step, idx) => {
    const items = preceding.filter(p => p.step === step).slice(0, 8);
    if (!items.length) return;
    const maxC = items[0].count;
    html += '<div class="preceding-group"><div class="preceding-label">' + stepLabels[idx] + '</div>';
    items.forEach(item => {
      const pct = (item.count / maxC * 100).toFixed(0);
      html += '<div class="preceding-bar-row">' +
        '<div class="preceding-route">' + routeName(item.route) + '</div>' +
        '<div class="preceding-track"><div class="preceding-fill" style="width:' + pct + '%;background:' + colors[idx] + ';opacity:0.7"></div></div>' +
        '<div class="preceding-count">' + item.count + '</div>' +
      '</div>';
    });
    html += '</div>';
  });

  $('preceding').innerHTML = html;
}

/* ── 9. Per-Registration Cycles ───────────────────────── */
function renderPerRegCycles(cycleLengths) {
  const withGaps = cycleLengths.filter(c => c.gaps.length > 0);
  if (!withGaps.length) { $('per-reg-cycles').innerHTML = '<div style="color:var(--muted)">Not enough data</div>'; return; }

  let html = '';
  withGaps.forEach(c => {
    const avg = (c.gaps.reduce((a,b) => a+b, 0) / c.gaps.length).toFixed(1);
    html += '<div style="margin-bottom:10px">' +
      '<div style="font-size:12px;font-weight:600;color:var(--text-bright)">' + c.reg +
      ' <span style="font-weight:400;color:var(--muted);font-size:11px">avg ' + avg + 'd</span></div>' +
      '<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:4px">';
    c.gaps.forEach(g => {
      const color = g <= 7 ? 'var(--green)' : g <= 14 ? 'var(--accent)' : g <= 21 ? 'var(--amber)' : 'var(--red)';
      html += '<span style="background:var(--surface2);border:1px solid ' + color + ';color:' + color +
        ';border-radius:4px;padding:2px 6px;font-size:10px;font-weight:600">' + g + 'd</span>';
    });
    html += '</div></div>';
  });
  $('per-reg-cycles').innerHTML = html;
}

init();
</script>
</body>
</html>
"""


@app.route("/analysis")
def analysis():
    return render_template_string(_ANALYSIS_HTML)


# ── Fleet Database ─────────────────────────────────────────────────


@app.route("/api/fleet")
def api_fleet():
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503

    try:
        rows = _q(
            conn,
            """
            SELECT a.icao24, a.registration, a.aircraft_type, a.aircraft_subtype,
                   a.is_active, a.first_seen_date::text, a.last_seen_date::text,
                   COUNT(f.id)::int AS total_flights,
                   COUNT(f.id) FILTER (WHERE f.flight_date >= CURRENT_DATE - 7)::int AS flights_7d,
                   MAX(f.flight_date)::text AS last_flight,
                   a.needs_review
            FROM aircraft a
            LEFT JOIN flights f ON f.icao24 = a.icao24
            GROUP BY a.id
            ORDER BY a.registration
            """,
        )
        aircraft = [
            {
                "icao24": r[0].strip(),
                "registration": r[1].strip() if r[1] else "",
                "aircraft_type": (r[2] or "").strip(),
                "aircraft_subtype": (r[3] or "").strip(),
                "is_active": r[4],
                "first_seen": r[5],
                "last_seen": r[6],
                "total_flights": r[7],
                "flights_7d": r[8],
                "last_flight": r[9],
                "needs_review": r[10],
            }
            for r in rows
        ]
        # Collect distinct types for filter dropdown
        types = sorted({a["aircraft_type"] for a in aircraft if a["aircraft_type"]})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    return jsonify({"aircraft": aircraft, "types": types})


@app.route("/api/fleet/<icao24>")
def api_fleet_detail(icao24):
    icao24 = icao24.strip().lower()
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503

    try:
        # Aircraft info
        rows = _q(
            conn,
            """
            SELECT icao24, registration, aircraft_type, aircraft_subtype,
                   is_active, first_seen_date::text, last_seen_date::text,
                   airline_iata, needs_review
            FROM aircraft WHERE icao24 = %s
            """,
            (icao24,),
        )
        if not rows:
            conn.close()
            return jsonify({"error": "Aircraft not found"}), 404
        r = rows[0]
        info = {
            "icao24": r[0].strip(),
            "registration": (r[1] or "").strip(),
            "aircraft_type": (r[2] or "").strip(),
            "aircraft_subtype": (r[3] or "").strip(),
            "is_active": r[4],
            "first_seen": r[5],
            "last_seen": r[6],
            "airline_iata": (r[7] or "").strip(),
            "needs_review": r[8],
        }

        # Flight stats (only count flights with known departure)
        info["total_flights"] = _q1(
            conn,
            "SELECT COUNT(*) FROM flights WHERE icao24 = %s"
            " AND departure_airport_icao IS NOT NULL AND NOT needs_review",
            (icao24,),
        )
        info["flights_7d"] = _q1(
            conn,
            "SELECT COUNT(*) FROM flights WHERE icao24 = %s AND flight_date >= CURRENT_DATE - 7"
            " AND departure_airport_icao IS NOT NULL AND NOT needs_review",
            (icao24,),
        )
        info["flights_30d"] = _q1(
            conn,
            "SELECT COUNT(*) FROM flights WHERE icao24 = %s AND flight_date >= CURRENT_DATE - 30"
            " AND departure_airport_icao IS NOT NULL AND NOT needs_review",
            (icao24,),
        )

        # Recent flights (last 100, only with known departure)
        rows = _q(
            conn,
            """
            SELECT callsign, departure_airport_icao, arrival_airport_icao,
                   first_seen, last_seen, duration_minutes, flight_date::text
            FROM flights
            WHERE icao24 = %s
              AND departure_airport_icao IS NOT NULL
            ORDER BY first_seen DESC
            LIMIT 100
            """,
            (icao24,),
        )
        flights = [
            {
                "callsign": (r[0] or "").strip(),
                "dep": (r[1] or "").strip(),
                "arr": (r[2] or "").strip() if r[2] else "",
                "first_seen": r[3].isoformat() if r[3] else None,
                "last_seen": r[4].isoformat() if r[4] else None,
                "duration": r[5],
                "date": r[6],
                "pending": r[2] is None,
            }
            for r in rows
        ]

        # Top routes
        rows = _q(
            conn,
            """
            SELECT COALESCE(departure_airport_icao, '?') || '-' || COALESCE(arrival_airport_icao, '?') AS route,
                   COUNT(*) AS cnt
            FROM flights
            WHERE icao24 = %s
              AND departure_airport_icao IS NOT NULL
              AND arrival_airport_icao IS NOT NULL
              AND NOT needs_review
            GROUP BY route
            ORDER BY cnt DESC
            LIMIT 20
            """,
            (icao24,),
        )
        routes = [{"route": r[0].strip(), "count": r[1]} for r in rows]

        # Flights per day last 30 days
        rows = _q(
            conn,
            """
            SELECT flight_date::text, COUNT(*)
            FROM flights
            WHERE icao24 = %s AND flight_date >= CURRENT_DATE - 29
              AND NOT needs_review
            GROUP BY flight_date ORDER BY flight_date
            """,
            (icao24,),
        )
        daily = [{"date": r[0], "count": r[1]} for r in rows]

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    return jsonify({"info": info, "flights": flights, "routes": routes, "daily": daily})


_FLEET_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LH Fleet Database</title>
<style>
:root {
  --bg: #101114; --surface: #191b20; --surface2: #1f2128;
  --border: #2a2c35; --text: #c9cdd6; --text-bright: #e4e7ed;
  --muted: #6b7280; --accent: #5b8def; --accent-dim: rgba(91,141,239,0.12);
  --green: #4ade80; --green-dim: rgba(74,222,128,0.12);
  --red: #f87171; --red-dim: rgba(248,113,113,0.12);
  --amber: #fbbf24; --radius: 10px;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: 'Inter', -apple-system, 'Segoe UI', system-ui, sans-serif;
  font-size: 14px; line-height: 1.5; -webkit-font-smoothing: antialiased;
}
.container { max-width: 1200px; margin: 0 auto; padding: 0 16px 40px; }
.header {
  padding: 16px 0 12px; display: flex; justify-content: space-between;
  align-items: center; border-bottom: 1px solid var(--border); margin-bottom: 20px;
}
.header h1 { font-size: 17px; font-weight: 600; color: var(--text-bright); letter-spacing: -0.3px; }
.header h1 span { color: var(--accent); font-weight: 700; }
.nav-link {
  font-size: 12px; color: var(--accent); text-decoration: none;
  padding: 4px 10px; border: 1px solid var(--accent); border-radius: 6px;
}
.nav-link:hover { background: var(--accent-dim); }

/* Toolbar */
.toolbar {
  display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; align-items: center;
}
.toolbar input[type="text"] {
  background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
  color: var(--text-bright); padding: 7px 12px; font-size: 13px; flex: 1; min-width: 200px;
  outline: none;
}
.toolbar input[type="text"]:focus { border-color: var(--accent); }
.toolbar select {
  background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
  color: var(--text); padding: 7px 10px; font-size: 12px; outline: none; cursor: pointer;
}
.toolbar .count { font-size: 12px; color: var(--muted); margin-left: auto; }

/* Toggle buttons */
.toggle-group { display: flex; border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
.toggle-btn {
  background: var(--surface); border: none; color: var(--muted); padding: 6px 12px;
  font-size: 11px; font-weight: 600; cursor: pointer; border-right: 1px solid var(--border);
  text-transform: uppercase; letter-spacing: 0.5px;
}
.toggle-btn:last-child { border-right: none; }
.toggle-btn.active { background: var(--accent-dim); color: var(--accent); }
.toggle-btn:hover:not(.active) { color: var(--text); }

/* Table */
.fleet-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.fleet-table th {
  text-align: left; padding: 8px 10px; font-size: 10px; font-weight: 700;
  color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px;
  border-bottom: 2px solid var(--border); cursor: pointer; user-select: none;
  white-space: nowrap;
}
.fleet-table th:hover { color: var(--accent); }
.fleet-table th .sort-arrow { font-size: 9px; margin-left: 3px; opacity: 0.5; }
.fleet-table th.sorted .sort-arrow { opacity: 1; color: var(--accent); }
.fleet-table td {
  padding: 7px 10px; border-bottom: 1px solid rgba(42,44,53,0.4);
  color: var(--text); white-space: nowrap;
}
.fleet-table tr { cursor: pointer; transition: background 0.1s; }
.fleet-table tbody tr:hover { background: var(--surface); }
.fleet-table .reg { font-weight: 700; color: var(--text-bright); font-size: 13px; }
.fleet-table .hex { font-family: monospace; font-size: 11px; color: var(--muted); }
.fleet-table .type { color: var(--accent); font-weight: 600; }
.fleet-table .num { text-align: right; font-variant-numeric: tabular-nums; }
.review-toggle {
  font-size: 12px; color: var(--muted); display: flex; align-items: center; gap: 5px; cursor: pointer;
}
.review-toggle input { cursor: pointer; }
.fleet-table tr.review-row { background: rgba(255, 180, 50, 0.08); }
.fleet-table tr.review-row:hover { background: rgba(255, 180, 50, 0.15); }
.badge-review {
  display: inline-block; padding: 1px 7px; border-radius: 999px;
  font-size: 10px; font-weight: 600; background: rgba(255, 180, 50, 0.15); color: #ffb432;
}
.badge-active {
  display: inline-block; padding: 1px 7px; border-radius: 999px;
  font-size: 10px; font-weight: 600; background: var(--green-dim); color: var(--green);
}
.badge-retired {
  display: inline-block; padding: 1px 7px; border-radius: 999px;
  font-size: 10px; font-weight: 600; background: var(--red-dim); color: var(--red);
}

.loading { text-align: center; padding: 40px; color: var(--muted); font-size: 13px; }
.error-banner {
  display: none; background: var(--red-dim); border: 1px solid rgba(248,113,113,0.25);
  border-radius: var(--radius); padding: 10px 14px; margin-bottom: 16px;
  color: var(--red); font-size: 12px;
}

@media (max-width: 800px) {
  .fleet-table { font-size: 11px; }
  .fleet-table th, .fleet-table td { padding: 5px 6px; }
}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>LH Fleet <span>Database</span></h1>
    <div style="display:flex;gap:10px;align-items:center">
      <a class="nav-link" href="/">&larr; Monitor</a>
      <a class="nav-link" href="/analysis">A380 Analysis</a>
    </div>
  </div>

  <div class="error-banner" id="error-banner"></div>
  <div class="loading" id="loading">Loading fleet data&hellip;</div>

  <div id="content" style="display:none">
    <div class="toolbar">
      <input type="text" id="search" placeholder="Search registration, ICAO24, type, model...">
      <select id="type-filter"><option value="">All types</option></select>
      <div class="toggle-group">
        <button class="toggle-btn active" data-status="all">All</button>
        <button class="toggle-btn" data-status="active">Active</button>
        <button class="toggle-btn" data-status="retired">Retired</button>
      </div>
      <label class="review-toggle"><input type="checkbox" id="review-filter"> Needs Review</label>
      <div class="count" id="count"></div>
    </div>

    <table class="fleet-table">
      <thead>
        <tr id="table-head">
          <th data-key="registration">Reg <span class="sort-arrow">&#9650;</span></th>
          <th data-key="icao24">ICAO24 <span class="sort-arrow">&#9650;</span></th>
          <th data-key="aircraft_type">Type <span class="sort-arrow">&#9650;</span></th>
          <th data-key="aircraft_subtype">Model <span class="sort-arrow">&#9650;</span></th>
          <th data-key="is_active">Status <span class="sort-arrow">&#9650;</span></th>
          <th data-key="total_flights" class="num">Flights <span class="sort-arrow">&#9650;</span></th>
          <th data-key="flights_7d" class="num">7d <span class="sort-arrow">&#9650;</span></th>
          <th data-key="last_flight">Last Flight <span class="sort-arrow">&#9650;</span></th>
          <th data-key="first_seen">First Seen <span class="sort-arrow">&#9650;</span></th>
        </tr>
      </thead>
      <tbody id="table-body"></tbody>
    </table>
  </div>

</div>

<script>
const $ = id => document.getElementById(id);

let allAircraft = [];
let sortKey = 'registration';
let sortAsc = true;
let statusFilter = 'all';

async function init() {
  let data;
  try {
    const r = await fetch('/api/fleet');
    data = await r.json();
  } catch(e) {
    $('error-banner').style.display = 'block';
    $('error-banner').textContent = 'Connection error: ' + e;
    $('loading').style.display = 'none';
    return;
  }
  if (data.error) {
    $('error-banner').style.display = 'block';
    $('error-banner').textContent = data.error;
    $('loading').style.display = 'none';
    return;
  }
  $('loading').style.display = 'none';
  $('content').style.display = 'block';

  allAircraft = data.aircraft;

  // Populate type filter
  const sel = $('type-filter');
  data.types.forEach(t => {
    const o = document.createElement('option');
    o.value = t; o.textContent = t;
    sel.appendChild(o);
  });

  render();
}

function getFiltered() {
  const q = $('search').value.toLowerCase().trim();
  const typeVal = $('type-filter').value;
  const reviewOnly = $('review-filter').checked;
  return allAircraft.filter(a => {
    if (reviewOnly && !a.needs_review) return false;
    if (statusFilter === 'active' && !a.is_active) return false;
    if (statusFilter === 'retired' && a.is_active) return false;
    if (typeVal && a.aircraft_type !== typeVal) return false;
    if (q) {
      const hay = (a.registration + ' ' + a.icao24 + ' ' + a.aircraft_type + ' ' + a.aircraft_subtype).toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
}

function getSorted(list) {
  return [...list].sort((a, b) => {
    let va = a[sortKey], vb = b[sortKey];
    if (va == null) va = '';
    if (vb == null) vb = '';
    if (typeof va === 'boolean') { va = va ? 1 : 0; vb = vb ? 1 : 0; }
    if (typeof va === 'number') return sortAsc ? va - vb : vb - va;
    va = String(va); vb = String(vb);
    return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
  });
}

function render() {
  const filtered = getFiltered();
  const sorted = getSorted(filtered);

  $('count').textContent = filtered.length + ' / ' + allAircraft.length + ' aircraft';

  // Update sort indicators
  document.querySelectorAll('#table-head th').forEach(th => {
    th.classList.toggle('sorted', th.dataset.key === sortKey);
    const arrow = th.querySelector('.sort-arrow');
    if (th.dataset.key === sortKey) {
      arrow.innerHTML = sortAsc ? '&#9650;' : '&#9660;';
    } else {
      arrow.innerHTML = '&#9650;';
    }
  });

  const tbody = $('table-body');
  tbody.innerHTML = sorted.map(a => {
    const statusBadge = a.is_active
      ? '<span class="badge-active">active</span>'
      : '<span class="badge-retired">retired</span>';
    const reviewBadge = a.needs_review ? ' <span class="badge-review">review</span>' : '';
    const rowClass = a.needs_review ? ' class="review-row"' : '';
    return '<tr' + rowClass + ' onclick="location.href=\\'/fleet/' + a.icao24 + '\\'">' +
      '<td class="reg">' + esc(a.registration) + '</td>' +
      '<td class="hex">' + esc(a.icao24) + '</td>' +
      '<td class="type">' + esc(a.aircraft_type || '\\u2014') + '</td>' +
      '<td>' + esc(a.aircraft_subtype || '\\u2014') + '</td>' +
      '<td>' + statusBadge + reviewBadge + '</td>' +
      '<td class="num">' + a.total_flights + '</td>' +
      '<td class="num">' + a.flights_7d + '</td>' +
      '<td>' + (a.last_flight || '\\u2014') + '</td>' +
      '<td>' + (a.first_seen || '\\u2014') + '</td>' +
    '</tr>';
  }).join('');
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// Sort on header click
document.querySelectorAll('#table-head th').forEach(th => {
  th.addEventListener('click', () => {
    const key = th.dataset.key;
    if (sortKey === key) { sortAsc = !sortAsc; }
    else { sortKey = key; sortAsc = true; }
    render();
  });
});

// Search
$('search').addEventListener('input', render);

// Type filter
$('type-filter').addEventListener('change', render);

// Needs review filter
$('review-filter').addEventListener('change', render);

// Status toggle
document.querySelectorAll('.toggle-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    statusFilter = btn.dataset.status;
    render();
  });
});

init();
</script>
</body>
</html>
"""


_FLEET_DETAIL_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Aircraft Detail</title>
<style>
:root {
  --bg: #101114; --surface: #191b20; --surface2: #1f2128;
  --border: #2a2c35; --text: #c9cdd6; --text-bright: #e4e7ed;
  --muted: #6b7280; --accent: #5b8def; --accent-dim: rgba(91,141,239,0.12);
  --green: #4ade80; --green-dim: rgba(74,222,128,0.12);
  --red: #f87171; --red-dim: rgba(248,113,113,0.12);
  --amber: #fbbf24; --radius: 10px;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: 'Inter', -apple-system, 'Segoe UI', system-ui, sans-serif;
  font-size: 14px; line-height: 1.5; -webkit-font-smoothing: antialiased;
}
.container { max-width: 1100px; margin: 0 auto; padding: 0 16px 40px; }
.header {
  padding: 16px 0 12px; display: flex; justify-content: space-between;
  align-items: center; border-bottom: 1px solid var(--border); margin-bottom: 20px;
}
.header h1 { font-size: 17px; font-weight: 600; color: var(--text-bright); }
.header h1 span { color: var(--accent); font-weight: 700; }
.nav-link {
  font-size: 12px; color: var(--accent); text-decoration: none;
  padding: 4px 10px; border: 1px solid var(--accent); border-radius: 6px;
}
.nav-link:hover { background: var(--accent-dim); }

.card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 16px; margin-bottom: 16px;
}
.card-title {
  font-size: 13px; font-weight: 600; color: var(--text-bright); margin-bottom: 12px;
}

/* Info grid */
.info-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: 12px;
}
.info-item .label {
  font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px;
  color: var(--muted); margin-bottom: 2px;
}
.info-item .value { font-size: 16px; font-weight: 700; color: var(--text-bright); }
.info-item .value.small { font-size: 14px; }

/* Metrics */
.metrics { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 16px; }
.metric {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 12px; text-align: center;
}
.metric .label {
  font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px;
  color: var(--muted); margin-bottom: 4px;
}
.metric .value { font-size: 22px; font-weight: 700; color: var(--text-bright); }

.badge-active {
  display: inline-block; padding: 2px 10px; border-radius: 999px;
  font-size: 11px; font-weight: 600; background: var(--green-dim); color: var(--green);
}
.badge-retired {
  display: inline-block; padding: 2px 10px; border-radius: 999px;
  font-size: 11px; font-weight: 600; background: var(--red-dim); color: var(--red);
}

/* Route bars */
.route-bar-row {
  display: flex; align-items: center; gap: 8px; margin-bottom: 4px;
}
.route-label { width: 90px; text-align: right; font-size: 12px; color: var(--text); font-weight: 500; }
.route-track { flex: 1; height: 18px; background: var(--surface2); border-radius: 3px; overflow: hidden; }
.route-fill { height: 100%; border-radius: 3px; background: var(--accent); opacity: 0.7; }
.route-count { width: 30px; font-size: 11px; color: var(--muted); text-align: right; font-variant-numeric: tabular-nums; }

/* Activity chart */
.chart-bars { display: flex; align-items: flex-end; gap: 2px; height: 60px; }
.chart-bar {
  flex: 1; border-radius: 2px 2px 0 0; min-height: 0; background: var(--accent); opacity: 0.7;
}
.chart-labels {
  display: flex; justify-content: space-between; font-size: 10px;
  color: var(--muted); margin-top: 4px;
}

/* Flight table */
.flight-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.flight-table th {
  text-align: left; padding: 6px 8px; font-size: 10px; font-weight: 700;
  color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px;
  border-bottom: 2px solid var(--border);
}
.flight-table td {
  padding: 5px 8px; border-bottom: 1px solid rgba(42,44,53,0.3); color: var(--text);
}
.flight-table .cs { font-weight: 600; color: var(--text-bright); }

.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.loading { text-align: center; padding: 40px; color: var(--muted); font-size: 13px; }
.error-banner {
  display: none; background: var(--red-dim); border: 1px solid rgba(248,113,113,0.25);
  border-radius: var(--radius); padding: 10px 14px; margin-bottom: 16px;
  color: var(--red); font-size: 12px;
}

@media (max-width: 800px) { .two-col { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1 id="page-title">Aircraft <span>Detail</span></h1>
    <div style="display:flex;gap:10px">
      <a class="nav-link" href="/fleet">&larr; Fleet DB</a>
      <a class="nav-link" href="/">Monitor</a>
    </div>
  </div>

  <div class="error-banner" id="error-banner"></div>
  <div class="loading" id="loading">Loading aircraft data&hellip;</div>

  <div id="content" style="display:none">
    <!-- Info card -->
    <div class="card" id="info-card"></div>

    <!-- Stats -->
    <div class="metrics" id="stats"></div>

    <!-- Activity + Routes -->
    <div class="two-col">
      <div class="card">
        <div class="card-title">Activity (last 30 days)</div>
        <div class="chart-bars" id="activity-chart"></div>
        <div class="chart-labels" id="activity-labels"></div>
      </div>
      <div class="card">
        <div class="card-title">Top Routes</div>
        <div id="routes"></div>
      </div>
    </div>

    <!-- Flight history -->
    <div class="card" style="margin-top:16px">
      <div class="card-title">Recent Flights (last 100)</div>
      <div style="overflow-x:auto">
        <table class="flight-table">
          <thead>
            <tr>
              <th>Date</th><th>Callsign</th><th>From</th><th>To</th>
              <th>Departure</th><th>Arrival</th><th>Duration</th>
            </tr>
          </thead>
          <tbody id="flight-body"></tbody>
        </table>
      </div>
    </div>
  </div>

</div>

<script>
const $ = id => document.getElementById(id);
const icao24 = location.pathname.split('/').pop();

async function init() {
  let data;
  try {
    const r = await fetch('/api/fleet/' + icao24);
    data = await r.json();
  } catch(e) {
    $('error-banner').style.display = 'block';
    $('error-banner').textContent = 'Connection error: ' + e;
    $('loading').style.display = 'none';
    return;
  }
  if (data.error) {
    $('error-banner').style.display = 'block';
    $('error-banner').textContent = data.error;
    $('loading').style.display = 'none';
    return;
  }
  $('loading').style.display = 'none';
  $('content').style.display = 'block';

  const info = data.info;
  document.title = info.registration + ' - LH Fleet';
  $('page-title').innerHTML = '<span>' + esc(info.registration) + '</span> ' + esc(info.aircraft_subtype || info.aircraft_type || '');

  const statusBadge = info.is_active
    ? '<span class="badge-active">active</span>'
    : '<span class="badge-retired">retired</span>';

  $('info-card').innerHTML = '<div class="info-grid">' +
    item('Registration', info.registration) +
    item('ICAO24', '<span style="font-family:monospace">' + info.icao24 + '</span>') +
    item('Type', info.aircraft_type || '\\u2014') +
    item('Model', info.aircraft_subtype || '\\u2014') +
    item('Status', statusBadge) +
    item('Airline', info.airline_iata || '\\u2014') +
    item('First Seen', info.first_seen || '\\u2014') +
    item('Last Seen', info.last_seen || '\\u2014') +
  '</div>';

  $('stats').innerHTML =
    '<div class="metric"><div class="label">Total Flights</div><div class="value">' + (info.total_flights || 0) + '</div></div>' +
    '<div class="metric"><div class="label">Last 30 Days</div><div class="value">' + (info.flights_30d || 0) + '</div></div>' +
    '<div class="metric"><div class="label">Last 7 Days</div><div class="value">' + (info.flights_7d || 0) + '</div></div>';

  // Activity chart
  const daily = data.daily || [];
  if (daily.length) {
    const maxD = Math.max(...daily.map(d => d.count), 1);
    $('activity-chart').innerHTML = daily.map(d =>
      '<div class="chart-bar" style="height:' + Math.max(2, d.count / maxD * 56) + 'px" title="' + d.date + ': ' + d.count + '"></div>'
    ).join('');
    $('activity-labels').innerHTML = '<span>' + daily[0].date.slice(5) + '</span><span>' + daily[daily.length-1].date.slice(5) + '</span>';
  }

  // Routes
  const routes = data.routes || [];
  if (routes.length) {
    const maxR = routes[0].count;
    $('routes').innerHTML = routes.slice(0, 12).map(r => {
      const parts = r.route.split('-');
      const label = parts[0] + '\\u2192' + parts[1];
      return '<div class="route-bar-row">' +
        '<div class="route-label">' + label + '</div>' +
        '<div class="route-track"><div class="route-fill" style="width:' + (r.count/maxR*100) + '%"></div></div>' +
        '<div class="route-count">' + r.count + '</div></div>';
    }).join('');
  } else {
    $('routes').innerHTML = '<div style="color:var(--muted)">No routes recorded</div>';
  }

  // Flights table
  const flights = data.flights || [];
  $('flight-body').innerHTML = flights.map(f => {
    const dur = f.duration ? Math.floor(f.duration/60) + 'h ' + (f.duration%60) + 'm' : '\\u2014';
    const dep = f.first_seen ? f.first_seen.slice(11,16) : '';
    const arr = f.last_seen ? f.last_seen.slice(11,16) : '';
    return '<tr>' +
      '<td>' + (f.date || '\\u2014') + '</td>' +
      '<td class="cs">' + (f.callsign || '\\u2014') + '</td>' +
      '<td>' + (f.dep || '\\u2014') + '</td>' +
      '<td>' + (f.arr || '\\u2014') + '</td>' +
      '<td>' + dep + '</td>' +
      '<td>' + arr + '</td>' +
      '<td>' + dur + '</td>' +
    '</tr>';
  }).join('') || '<tr><td colspan="7" style="color:var(--muted);text-align:center">No flights recorded</td></tr>';
}

function item(label, value) {
  return '<div class="info-item"><div class="label">' + label + '</div><div class="value small">' + value + '</div></div>';
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

init();
</script>
</body>
</html>
"""


@app.route("/fleet")
def fleet():
    return render_template_string(_FLEET_HTML)


@app.route("/fleet/<icao24>")
def fleet_detail(icao24):
    return render_template_string(_FLEET_DETAIL_HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
