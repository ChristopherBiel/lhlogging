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

        # --- Aircraft type breakdown (active) ---
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

        stats["generated_at"] = datetime.now(tz=timezone.utc).isoformat()

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    return jsonify(stats)


_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LH Fleet Monitor</title>
<style>
  :root {
    --bg: #0f1117; --card: #1a1d26; --border: #2a2d3a;
    --text: #e2e8f0; --muted: #94a3b8; --accent: #3b82f6;
    --green: #22c55e; --red: #ef4444; --yellow: #f59e0b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Inter', 'Segoe UI', system-ui, sans-serif; font-size: 14px; }
  header { padding: 20px 32px; border-bottom: 1px solid var(--border); display: flex; justify-content: space-between; align-items: center; }
  header h1 { font-size: 20px; font-weight: 600; letter-spacing: -0.3px; }
  header h1 span { color: var(--accent); }
  #last-updated { color: var(--muted); font-size: 12px; }
  main { padding: 24px 32px; max-width: 1400px; margin: 0 auto; }
  .section-title { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; color: var(--muted); margin: 0 0 12px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px; margin-bottom: 28px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px 18px; }
  .card .label { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
  .card .value { font-size: 28px; font-weight: 700; letter-spacing: -1px; }
  .card .sub { font-size: 11px; color: var(--muted); margin-top: 4px; }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 28px; }
  .grid3 { display: grid; grid-template-columns: 2fr 1fr 1fr; gap: 20px; margin-bottom: 28px; }
  .box { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 18px 20px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 6px 8px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: var(--muted); border-bottom: 1px solid var(--border); }
  td { padding: 7px 8px; border-bottom: 1px solid rgba(42,45,58,0.5); }
  tr:last-child td { border-bottom: none; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 600; }
  .badge-ok { background: rgba(34,197,94,0.15); color: var(--green); }
  .badge-error { background: rgba(239,68,68,0.15); color: var(--red); }
  .badge-running { background: rgba(59,130,246,0.15); color: var(--accent); }
  .bar-chart { display: flex; flex-direction: column; gap: 6px; }
  .bar-row { display: flex; align-items: center; gap: 8px; }
  .bar-label { width: 80px; text-align: right; font-size: 12px; color: var(--muted); flex-shrink: 0; }
  .bar-track { flex: 1; background: var(--border); border-radius: 3px; height: 18px; overflow: hidden; }
  .bar-fill { height: 100%; background: var(--accent); border-radius: 3px; transition: width 0.4s; }
  .bar-count { width: 36px; font-size: 12px; color: var(--muted); }
  .sparkline { display: flex; align-items: flex-end; gap: 3px; height: 48px; }
  .spark-col { flex: 1; background: var(--accent); border-radius: 2px 2px 0 0; opacity: 0.8; }
  .error-detail { font-size: 11px; color: var(--red); max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  #error-banner { display: none; background: rgba(239,68,68,0.1); border: 1px solid rgba(239,68,68,0.3); border-radius: 8px; padding: 12px 18px; margin-bottom: 20px; color: var(--red); }
  @media (max-width: 800px) { .grid2, .grid3 { grid-template-columns: 1fr; } main { padding: 16px; } }
</style>
</head>
<body>
<header>
  <h1>LH Fleet <span>Monitor</span></h1>
  <span id="last-updated">Loading…</span>
</header>
<main>
  <div id="error-banner"></div>

  <p class="section-title">Fleet</p>
  <div class="cards" id="fleet-cards"></div>

  <p class="section-title">Flights</p>
  <div class="cards" id="flight-cards"></div>

  <div class="grid3">
    <div class="box">
      <p class="section-title">Batch Run History</p>
      <table id="batch-table">
        <thead><tr><th>Type</th><th>Started</th><th>Aircraft</th><th>Flights</th><th>Status</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="box">
      <p class="section-title">Aircraft Types (active)</p>
      <div class="bar-chart" id="type-bars"></div>
    </div>
    <div class="box">
      <p class="section-title">Top Routes (30d)</p>
      <table id="route-table">
        <thead><tr><th>Dep</th><th>Arr</th><th>#</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <div class="grid2">
    <div class="box">
      <p class="section-title" id="sparkline-title">Flights per day (14d)</p>
      <div style="display:flex;align-items:flex-end;gap:4px;height:80px;margin-top:8px" id="sparkline"></div>
      <div style="display:flex;justify-content:space-between;margin-top:4px;font-size:11px;color:var(--muted)" id="sparkline-labels"></div>
    </div>
    <div class="box">
      <p class="section-title">System</p>
      <table id="system-table">
        <tbody></tbody>
      </table>
    </div>
  </div>
</main>

<script>
function fmt(n) { return n == null ? '—' : n.toLocaleString(); }
function ago(iso) {
  if (!iso) return '—';
  const d = new Date(iso), now = new Date();
  const s = Math.floor((now - d) / 1000);
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  if (s < 86400) return Math.floor(s/3600) + 'h ago';
  return Math.floor(s/86400) + 'd ago';
}
function badge(status) {
  const cls = status === 'ok' ? 'badge-ok' : status === 'running' ? 'badge-running' : 'badge-error';
  return `<span class="badge ${cls}">${status}</span>`;
}

async function refresh() {
  let data;
  try {
    const r = await fetch('/api/stats');
    data = await r.json();
  } catch(e) {
    document.getElementById('error-banner').style.display = 'block';
    document.getElementById('error-banner').textContent = 'Failed to fetch stats: ' + e;
    return;
  }
  if (data.error) {
    document.getElementById('error-banner').style.display = 'block';
    document.getElementById('error-banner').textContent = 'DB error: ' + data.error;
    return;
  }
  document.getElementById('error-banner').style.display = 'none';

  // Fleet cards
  document.getElementById('fleet-cards').innerHTML = `
    <div class="card"><div class="label">Active Aircraft</div><div class="value">${fmt(data.aircraft_active)}</div></div>
    <div class="card"><div class="label">Retired</div><div class="value">${fmt(data.aircraft_retired)}</div></div>
    <div class="card"><div class="label">Total in DB</div><div class="value">${fmt(data.aircraft_total)}</div></div>
  `;

  // Flight cards
  document.getElementById('flight-cards').innerHTML = `
    <div class="card"><div class="label">Flights Today</div><div class="value">${fmt(data.flights_today)}</div></div>
    <div class="card"><div class="label">Last 7 Days</div><div class="value">${fmt(data.flights_7d)}</div></div>
    <div class="card"><div class="label">All Time</div><div class="value">${fmt(data.flights_total)}</div></div>
  `;

  // Batch run table
  const btbody = document.querySelector('#batch-table tbody');
  btbody.innerHTML = (data.batch_runs || []).map(r => `
    <tr>
      <td>${r.run_type}</td>
      <td>${ago(r.started_at)}</td>
      <td>${fmt(r.aircraft_ok)}/${fmt(r.aircraft_total)}</td>
      <td>${fmt(r.flights_upserted)}</td>
      <td>${badge(r.status)}${r.error_detail ? '<br><span class="error-detail" title="'+r.error_detail+'">'+r.error_detail+'</span>' : ''}</td>
    </tr>
  `).join('');

  // Aircraft type bars
  const types = data.aircraft_types || [];
  const maxCount = types.length ? types[0].count : 1;
  document.getElementById('type-bars').innerHTML = types.slice(0, 12).map(t => `
    <div class="bar-row">
      <div class="bar-label">${t.type}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${Math.round(t.count/maxCount*100)}%"></div></div>
      <div class="bar-count">${t.count}</div>
    </div>
  `).join('');

  // Top routes
  const rtbody = document.querySelector('#route-table tbody');
  rtbody.innerHTML = (data.top_routes || []).map(r => `
    <tr><td>${r.dep}</td><td>${r.arr}</td><td>${r.count}</td></tr>
  `).join('');

  // Sparkline
  const days = data.flights_per_day || [];
  const maxF = days.length ? Math.max(...days.map(d => d.count)) : 1;
  document.getElementById('sparkline').innerHTML = days.map(d =>
    `<div title="${d.date}: ${d.count}" style="flex:1;background:var(--accent);border-radius:2px 2px 0 0;height:${Math.max(4, Math.round(d.count/maxF*76))}px;opacity:0.8"></div>`
  ).join('');
  if (days.length >= 2) {
    const lblDiv = document.getElementById('sparkline-labels');
    lblDiv.innerHTML = `<span>${days[0].date.slice(5)}</span><span>${days[days.length-1].date.slice(5)}</span>`;
  }

  // System table
  const lastRun = (data.batch_runs || [])[0];
  document.querySelector('#system-table tbody').innerHTML = `
    <tr><td style="color:var(--muted)">DB size</td><td>${data.db_size || '—'}</td></tr>
    <tr><td style="color:var(--muted)">Last run</td><td>${lastRun ? ago(lastRun.started_at) + ' (' + lastRun.run_type + ')' : '—'}</td></tr>
    <tr><td style="color:var(--muted)">Last status</td><td>${lastRun ? badge(lastRun.status) : '—'}</td></tr>
  `;

  document.getElementById('last-updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
}

refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(_HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
