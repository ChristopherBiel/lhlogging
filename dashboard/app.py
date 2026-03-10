"""
LHLogging monitoring dashboard.
Serves a single-page HTML dashboard and a /api/stats JSON endpoint.
"""
import os
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

        # --- Flight counts ---
        stats["flights_today"] = _q1(
            conn, "SELECT COUNT(*) FROM flights WHERE flight_date = CURRENT_DATE"
        )
        stats["flights_7d"] = _q1(
            conn, "SELECT COUNT(*) FROM flights WHERE flight_date >= CURRENT_DATE - 7"
        )
        stats["flights_total"] = _q1(conn, "SELECT COUNT(*) FROM flights")

        # --- DB size ---
        stats["db_size"] = _q1(
            conn, "SELECT pg_size_pretty(pg_database_size(current_database()))"
        )

        # --- Latest batch runs (last 10) ---
        rows = _q(
            conn,
            """
            SELECT run_type, started_at, finished_at,
                   aircraft_total, aircraft_ok, aircraft_error,
                   flights_upserted, status, error_detail
            FROM batch_runs
            ORDER BY started_at DESC
            LIMIT 10
            """,
        )
        stats["batch_runs"] = [
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

        # --- Last run per type ---
        for run_type in ("route_logger", "fleet_refresh"):
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
    <span class="updated" id="last-updated"></span>
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

  <!-- Batch runs + Top routes -->
  <div class="section two-col">
    <div class="card">
      <div class="section-label">Recent batch runs</div>
      <div id="batch-list"></div>
    </div>
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
  const lr = data.last_route_logger;
  const fr = data.last_fleet_refresh;
  const lrOk = lr && lr.status === 'ok';
  const frOk = fr && fr.status === 'ok';
  // Check fleet_refresh is recent (within 8 days)
  const frRecent = fr && fr.started_at &&
    (Date.now() - new Date(fr.started_at).getTime()) < 8 * 86400 * 1000;

  $('health-strip').innerHTML =
    '<div class="health-item">' +
      '<div class="label">Route Logger</div>' +
      '<div class="info"><span class="dot" style="background:' + (lrOk ? 'var(--green)' : 'var(--red)') + '"></span>' +
      (lr ? ago(lr.started_at) : 'never') + '</div>' +
    '</div>' +
    '<div class="health-item">' +
      '<div class="label">Fleet Refresh</div>' +
      '<div class="info"><span class="dot" style="background:' + (frOk && frRecent ? 'var(--green)' : !frRecent ? 'var(--amber)' : 'var(--red)') + '"></span>' +
      (fr ? ago(fr.started_at) : 'never') + '</div>' +
    '</div>' +
    '<div class="health-item">' +
      '<div class="label">Today</div>' +
      '<div class="info" style="font-weight:600;font-size:15px">' + fmt(data.flights_today) + ' <span style="font-weight:400;font-size:10px;color:var(--muted)">flights</span></div>' +
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
    '<div class="metric"><div class="label">All Time</div><div class="value">' + fmt(data.flights_total) + '</div></div>';

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

  // Batch runs
  const runs = data.batch_runs || [];
  $('batch-list').innerHTML = runs.length ? runs.map(r =>
    '<div class="batch-row">' +
      '<div class="batch-type">' + r.run_type.replace('_', ' ') + '</div>' +
      '<div class="batch-time">' + ago(r.started_at) + '</div>' +
      '<div class="batch-detail">' +
        (r.run_type === 'route_logger'
          ? fmt(r.aircraft_ok) + '/' + fmt(r.aircraft_total) + ' ac, ' + fmt(r.flights_upserted) + ' fl'
          : fmt(r.aircraft_ok) + '/' + fmt(r.aircraft_total) + ' ac') +
      '</div>' +
      badge(r.status) +
    '</div>' +
    (r.error_detail ? '<div class="err-text" title="' + r.error_detail.replace(/"/g, '&quot;') + '">' + r.error_detail + '</div>' : '')
  ).join('') : '<div style="color:var(--muted);font-size:12px">No runs recorded</div>';

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
