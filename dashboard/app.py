"""
LHLogging monitoring dashboard.
Serves a single-page HTML dashboard and a /api/stats JSON endpoint.
"""
import os
import random
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request

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
        for run_type in ("state_poller", "flight_detector", "fleet_discovery", "fleet_refresh", "flightstatus"):
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>
:root {
  /* chassis — light cream Teenage-Engineering faceplate */
  --bg:#f3efe6;
  --surface:#ffffff;
  --surface2:#f1ebdd;
  --border:#e6dfce;
  --line:#d3c9b4;
  --text:#36342c;
  --text-bright:#1a1812;
  --muted:#9d9482;
  /* pastel "ink" accents — readable on cream and used as solid key fills */
  --accent:#6aa0d8;   /* sky */
  --green:#5cb487;    /* mint */
  --amber:#d3a23c;    /* butter */
  --red:#e07b6b;      /* coral */
  --purple:#a487d6;   /* lavender */
  --cyan:#46b2a8;     /* teal */
  /* pale pastel fills */
  --accent-dim:#dcebf9;
  --green-dim:#d7f0e2;
  --amber-dim:#f5ead0;
  --red-dim:#f8ddd6;
  --purple-dim:#e8defa;
  --radius:0;
  --radius-sm:0;
  --mono:'Space Mono','SFMono-Regular',ui-monospace,Menlo,monospace;
  --sans:'Space Grotesk',-apple-system,'Segoe UI',system-ui,sans-serif;
}

* { box-sizing:border-box; margin:0; padding:0; }

body {
  background:var(--bg);
  color:var(--text);
  font-family:var(--sans);
  font-size:14px;
  line-height:1.5;
  -webkit-font-smoothing:antialiased;
  counter-reset:sec;
}

.container { max-width:480px; margin:0 auto; padding:0 16px 40px; }

/* ── Header / device label ──────────────────────────────── */
.header { display:flex; align-items:center; gap:14px 16px; flex-wrap:wrap; padding:22px 0 14px; }
.brand { display:flex; align-items:center; gap:11px; }
.led { width:11px; height:11px; border-radius:0; background:var(--green);
  box-shadow:0 0 0 4px var(--green-dim); animation:pulse 2.6s ease-in-out infinite; flex-shrink:0; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
.header h1 { font-size:18px; font-weight:700; letter-spacing:-0.3px; color:var(--text-bright);
  text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--accent); }
.model { font-family:var(--mono); font-size:9.5px; letter-spacing:1.5px; color:var(--muted);
  text-transform:uppercase; margin-top:3px; }
.nav { display:flex; gap:7px; flex-wrap:wrap; }
.nav a { font-family:var(--mono); font-size:11px; letter-spacing:.4px; text-transform:uppercase;
  color:var(--text-bright); text-decoration:none; background:var(--surface); border:1.5px solid var(--line);
  border-radius:0; padding:6px 13px; transition:transform .08s ease, background .15s, border-color .15s; }
.nav a:hover { background:var(--accent-dim); border-color:var(--accent); transform:translateY(-1px); }
.updated { font-family:var(--mono); font-size:11px; color:var(--muted); margin-left:auto; }

/* speaker grille / perforation strip */
.grille { height:14px; margin:6px 0 24px; border-radius:0;
  background-image:repeating-linear-gradient(90deg, var(--line) 0 2px, transparent 2px 12px); }

/* ── Health strip ───────────────────────────────────────── */
.health-strip { display:flex; gap:9px; margin-bottom:26px; flex-wrap:wrap; counter-reset:hk; }
.health-item { flex:1; min-width:86px; background:var(--surface); border:1.5px solid var(--border);
  border-radius:0; padding:12px 12px 11px; text-align:left; }
.health-item .label { font-family:var(--mono); font-size:9px; text-transform:uppercase; letter-spacing:.7px;
  color:var(--muted); margin-bottom:8px; }
.health-item .label::before { counter-increment:hk; content:counter(hk,decimal-leading-zero)" "; color:var(--accent); }
.health-item .dot { display:inline-block; width:8px; height:8px; border-radius:0; margin-right:6px; vertical-align:middle; }
.health-item .info { font-family:var(--mono); font-size:12px; color:var(--text); }

/* ── Section ────────────────────────────────────────────── */
.section { margin-bottom:24px; }
.section-label { font-family:var(--mono); font-size:10px; font-weight:700; text-transform:uppercase;
  letter-spacing:1px; color:var(--muted); margin-bottom:12px; display:flex; align-items:center; gap:9px;
  counter-increment:sec; }
.section-label::before { content:counter(sec,decimal-leading-zero); font-size:9px; color:var(--text-bright);
  background:var(--accent-dim); border-radius:0; padding:3px 6px; letter-spacing:.5px; }

/* ── Metric keys ────────────────────────────────────────── */
.metrics { display:grid; grid-template-columns:1fr 1fr 1fr; gap:9px; margin-bottom:24px; counter-reset:key; }
.metric { background:var(--surface); border:1.5px solid var(--border); border-radius:0;
  padding:14px 13px 13px; position:relative; }
.metric::after { content:''; position:absolute; top:13px; right:13px; width:7px; height:7px; border-radius:0; background:var(--accent); }
.metric:nth-child(3n+1)::after { background:var(--green); }
.metric:nth-child(3n+2)::after { background:var(--accent); }
.metric:nth-child(3n+3)::after { background:var(--purple); }
.metric .label { font-family:var(--mono); font-size:9px; text-transform:uppercase; letter-spacing:.6px;
  color:var(--muted); margin-bottom:7px; }
.metric .label::before { counter-increment:key; content:counter(key,decimal-leading-zero)" "; color:var(--accent); }
.metric .value { font-family:var(--mono); font-size:26px; font-weight:700; color:var(--text-bright); letter-spacing:-1px; line-height:1; }
.metric .sub { font-size:10px; color:var(--muted); margin-top:4px; }

/* ── Cards ──────────────────────────────────────────────── */
.card { background:var(--surface); border:1.5px solid var(--border); border-radius:0; padding:16px; margin-bottom:14px; }

/* ── Chart bars ─────────────────────────────────────────── */
.chart-bars { display:flex; align-items:flex-end; gap:3px; height:64px; }
.chart-bars .bar { flex:1; border-radius:0; min-height:2px; transition:height .3s ease; position:relative; }
.chart-bars .bar:hover { opacity:1 !important; }
.chart-labels { display:flex; justify-content:space-between; font-family:var(--mono); font-size:10px; color:var(--muted); margin-top:6px; }
.chart-legend { display:flex; gap:14px; margin-bottom:10px; font-family:var(--mono); font-size:9.5px;
  color:var(--muted); text-transform:uppercase; letter-spacing:.4px; }
.chart-legend .swatch { display:inline-block; width:9px; height:9px; border-radius:0; margin-right:5px; vertical-align:middle; }

/* ── Horizontal bars ────────────────────────────────────── */
.hbar-row { display:flex; align-items:center; gap:9px; margin-bottom:6px; }
.hbar-row:last-child { margin-bottom:0; }
.hbar-label { width:46px; text-align:right; font-family:var(--mono); font-size:12px; font-weight:700;
  color:var(--text); flex-shrink:0; font-variant-numeric:tabular-nums; }
.hbar-track { flex:1; background:var(--surface2); border-radius:0; height:20px; overflow:hidden; }
.hbar-fill { height:100%; border-radius:0; transition:width .4s ease; }
.hbar-count { width:28px; font-family:var(--mono); font-size:11px; color:var(--muted); font-variant-numeric:tabular-nums; }
.hbar-flew { width:28px; font-family:var(--mono); font-size:11px; color:var(--green); text-align:right; font-variant-numeric:tabular-nums; }

/* ── Batch rows ─────────────────────────────────────────── */
.batch-row { display:flex; align-items:center; gap:10px; padding:9px 0; border-bottom:1.5px solid var(--border); font-size:12px; }
.batch-row:last-child { border-bottom:none; }
.batch-type { font-weight:600; color:var(--text); width:90px; flex-shrink:0; font-size:11px; text-transform:capitalize; }
.batch-time { font-family:var(--mono); color:var(--muted); font-size:11px; width:56px; flex-shrink:0; }
.batch-detail { flex:1; font-size:11px; color:var(--muted); }
.badge { display:inline-block; font-family:var(--mono); padding:3px 9px; border-radius:0; font-size:9.5px;
  font-weight:700; letter-spacing:.4px; text-transform:uppercase; }
.badge-ok { background:var(--green-dim); color:#2f8159; }
.badge-error { background:var(--red-dim); color:#c0533f; }
.badge-running { background:var(--accent-dim); color:#3a6ea5; }

/* ── Route table ────────────────────────────────────────── */
.route-row { display:flex; align-items:center; padding:7px 0; border-bottom:1.5px solid var(--border); font-size:12px; }
.route-row:last-child { border-bottom:none; }
.route-pair { flex:1; color:var(--text); font-weight:500; }
.route-pair .arrow { color:var(--accent); margin:0 6px; font-family:var(--mono); }
.route-count { color:var(--muted); font-family:var(--mono); font-variant-numeric:tabular-nums; font-size:11px; }

/* ── Misc ───────────────────────────────────────────────── */
#error-banner { display:none; background:var(--red-dim); border:1.5px solid var(--red); border-radius:0;
  padding:11px 14px; margin-bottom:16px; color:#bb4f3c; font-size:12px; }
.tooltip { position:fixed; background:var(--text-bright); border:none; border-radius:0; padding:5px 9px;
  font-family:var(--mono); font-size:11px; color:#fff; pointer-events:none; z-index:100; white-space:nowrap; display:none; }
.err-text { font-size:10px; color:var(--red); margin-top:2px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }

@media (min-width:600px) { .container { max-width:560px; } }
@media (min-width:900px) {
  .container { max-width:760px; padding:0 24px 48px; }
  .metrics { grid-template-columns:repeat(3,1fr); }
  .two-col { display:grid; grid-template-columns:1fr 1fr; gap:14px; }
  .two-col > .card { margin-bottom:0; }
}
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <div class="brand">
      <span class="led"></span>
      <div>
        <h1>LH&middot;Fleet <span>Monitor</span></h1>
        <div class="model">FLT-MON &middot; Fleet Telemetry</div>
      </div>
    </div>
    <nav class="nav">
      <a href="/book">Book</a>
      <a href="/schedule">Schedule</a>
      <a href="/fleet">Fleet DB</a>
      <a href="/analysis">A380</a>
      <a href="/analysis-747">747-8</a>
    </nav>
    <span class="updated" id="last-updated"></span>
  </div>

  <div class="grille"></div>

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
  // Flight-status (FIS) runs nightly; classify blocked vs broken vs stale so a
  // silent outage (the kind a retry can't fix) is visible at a glance.
  const fs = data.last_flightstatus;
  const fsAgeH = fs && fs.started_at ? (Date.now() - new Date(fs.started_at).getTime()) / 3600000 : null;
  const fsFresh = fsAgeH !== null && fsAgeH <= 26;            // one healthy run within ~a day
  const fsBlocked = fs && fs.status === 'ok' && (fs.aircraft_ok || 0) === 0 && (fs.aircraft_total || 0) > 0;
  const fsOk = fs && fs.status === 'ok' && (fs.aircraft_ok || 0) > 0 && fsFresh;
  const fsColor = !fs ? 'var(--red)'
    : fs.status === 'error' ? 'var(--red)'
    : fsBlocked ? 'var(--amber)'
    : !fsFresh ? 'var(--amber)'
    : fsOk ? 'var(--green)' : 'var(--amber)';
  const fsNote = !fs ? '' : fs.status === 'error' ? ' \\u00b7 error'
    : fsBlocked ? ' \\u00b7 blocked' : !fsFresh ? ' \\u00b7 stale' : '';

  function healthDetail(run, type) {
    if (!run) return '';
    let detail = '';
    if (type === 'state_poller') detail = fmt(run.aircraft_ok) + ' seen, ' + fmt(run.flights_upserted) + ' stored';
    else if (type === 'flight_detector') detail = fmt(run.flights_upserted) + ' flights';
    else if (type === 'fleet_discovery') detail = fmt(run.aircraft_ok) + ' discovered';
    else if (type === 'flightstatus') detail = fmt(run.aircraft_ok) + ' found, ' + fmt(run.aircraft_error) + ' blocked';
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
      '<div class="label">Schedule (FIS)</div>' +
      '<div class="info"><span class="dot" style="background:' + fsColor + '"></span>' +
      (fs ? ago(fs.started_at) + fsNote : 'never') + '</div>' +
      healthDetail(fs, 'flightstatus') +
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
      '<div style="height:' + h + 'px;background:var(--accent);border-radius:0;opacity:0.7;position:relative">' +
      (csH > 0 ? '<div style="position:absolute;bottom:0;left:0;right:0;height:' + Math.min(csH, h) + 'px;background:var(--amber);border-radius:0;opacity:0.6"></div>' : '') +
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
          '<div style="position:absolute;top:0;left:0;height:100%;width:' + (t.count > 0 ? Math.round(flewCount / t.count * 100) : 0) + '%;background:var(--green);border-radius:0;opacity:0.8"></div>' +
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
<footer style="text-align:center;padding:24px 0 8px;font-size:11px;color:var(--muted)">
  <a href="/impressum" style="color:var(--muted);text-decoration:none">Impressum</a>
  <span style="margin:0 6px">&middot;</span>
  <a href="/datenschutz" style="color:var(--muted);text-decoration:none">Datenschutz</a>
</footer>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(_HTML)


# ── Legal Pages ─────────────────────────────────────────────────────

_LEGAL_CSS = """
<style>
:root {
  --bg: #101114; --surface: #191b20; --border: #2a2c35;
  --text: #c9cdd6; --text-bright: #e4e7ed; --muted: #6b7280; --accent: #5b8def;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: 'Inter', -apple-system, 'Segoe UI', system-ui, sans-serif;
  font-size: 14px; line-height: 1.6; -webkit-font-smoothing: antialiased;
}
.container { max-width: 480px; margin: 0 auto; padding: 24px 16px 32px; }
h1 { color: var(--text-bright); font-size: 20px; margin-bottom: 16px; }
h2 { color: var(--text-bright); font-size: 15px; margin: 20px 0 8px; }
p, li { margin-bottom: 8px; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
.back { font-size: 12px; margin-bottom: 16px; display: inline-block; }
</style>
"""

_IMPRESSUM_HTML = (
    '<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8">'
    '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
    "<title>Impressum</title>"
    + _LEGAL_CSS
    + """</head><body><div class="container">
<a class="back" href="/">&larr; Back</a>
<h1>Impressum</h1>
<h2>Angaben gem&auml;&szlig; &sect; 5 TMG</h2>
<p>Christopher Biel<br>Leopoldstr. 48<br>80802 M&uuml;nchen</p>
<h2>Kontakt</h2>
<p>E-Mail: info@biels.net</p>
</div></body></html>"""
)

_DATENSCHUTZ_HTML = (
    '<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8">'
    '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
    "<title>Datenschutzerkl&auml;rung</title>"
    + _LEGAL_CSS
    + """</head><body><div class="container">
<a class="back" href="/">&larr; Back</a>
<h1>Datenschutzerkl&auml;rung</h1>
<h2>1. Verantwortlicher</h2>
<p>Christopher Biel, Leopoldstr. 48, 80802 M&uuml;nchen</p>
<h2>2. Erhebung und Verarbeitung personenbezogener Daten</h2>
<p>Diese Website erhebt, speichert und verarbeitet keine personenbezogenen Daten
ihrer Besucher. Es werden keine Cookies gesetzt, keine Analyse- oder Tracking-Tools
eingesetzt und keine Daten an Dritte weitergegeben.</p>
<h2>3. Server-Logfiles</h2>
<p>Beim Zugriff auf diese Website werden m&ouml;glicherweise durch den
Hosting-Provider technische Daten (z.&nbsp;B. IP-Adresse, Zeitpunkt des Zugriffs)
in Server-Logfiles gespeichert. Diese Daten werden nicht mit anderen Datenquellen
zusammengef&uuml;hrt und nach kurzer Zeit gel&ouml;scht.</p>
<h2>4. Ihre Rechte</h2>
<p>Sie haben das Recht auf Auskunft, Berichtigung, L&ouml;schung und
Einschr&auml;nkung der Verarbeitung Ihrer personenbezogenen Daten gem&auml;&szlig;
der DSGVO. Da wir keine personenbezogenen Daten erheben, fallen in der Regel keine
solchen Daten an.</p>
</div></body></html>"""
)


@app.route("/impressum")
def impressum():
    return _IMPRESSUM_HTML


@app.route("/datenschutz")
def datenschutz():
    return _DATENSCHUTZ_HTML


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


# ── 747-8 (B748) Analysis — D-ABYN route prediction ─────────────────


def _median(vals):
    s = sorted(vals)
    n = len(s)
    if n == 0:
        return None
    m = n // 2
    return float(s[m]) if n % 2 else (s[m - 1] + s[m]) / 2.0


# Canonical-route helpers for the rotation model.
_NORM_ALIAS = {"EDFE": "EDDF"}  # Egelsbach GA strip → Frankfurt hub
_TURN_MAP = {
    "RJTT": "HND", "SAEZ": "EZE", "FAOR": "JNB",
    "KLAX": "USW", "KSFO": "USW",
    "KORD": "USE", "KBOS": "USE", "KIAD": "USE", "KEWR": "USE",
    "KMIA": "USE", "KIAH": "USE",
    "MMMX": "MEX", "SBGR": "GRU", "SBGL": "GRU",
}
_TARGET_TURNS = ["HND", "EZE", "JNB"]
_TARGET_ROUTE = {"HND": "EDDF-RJTT", "EZE": "EDDF-SAEZ", "JNB": "EDDF-FAOR"}
_TARGET_ARR = {"HND": "RJTT", "EZE": "SAEZ", "JNB": "FAOR"}


def _norm_ap(code):
    """Normalize an airport code; None for unresolved (UNKN/empty)."""
    code = (code or "").strip().upper()
    if not code or code == "UNKN":
        return None
    return _NORM_ALIAS.get(code, code)


def _turn_type(arr):
    return _TURN_MAP.get(arr, "OTHER")


def _outbound_turns(flights):
    """Chronological [(date, turn_type)] of FRA departures, deduped per (day, type).

    `flights` must be ordered by first_seen and carry normalized dep/arr.
    """
    seq, seen = [], set()
    for f in flights:
        if f["dep"] == "EDDF" and f["arr"] and f["arr"] != "EDDF":
            key = (f["date"], _turn_type(f["arr"]))
            if key in seen:
                continue
            seen.add(key)
            seq.append(key)
    return seq


def _trans_counts(sequences):
    """First-order transition counts over turn-types across many sequences."""
    m = defaultdict(Counter)
    for seq in sequences:
        types = [t for _, t in seq]
        for a, b in zip(types, types[1:]):
            m[a][b] += 1
    return m


def _utc(dt):
    return dt.astimezone(timezone.utc)


def _tod_minutes(dt):
    dt = _utc(dt)
    return dt.hour * 60 + dt.minute


def _schedule_params(fleet_lists):
    """Per turn-type schedule, learned from the (reliable) FRA clock events:
    dep_tod  — median FRA departure time-of-day (minutes UTC),
    span_h   — median round-trip span, FRA departure → FRA return (hours),
    block_h  — median return-leg block time (hours, from clean X→FRA legs).
    `fleet_lists` is an iterable of per-reg flight lists ordered by first_seen.
    """
    dep_tod, spans, blocks = defaultdict(list), defaultdict(list), defaultdict(list)
    for flights in fleet_lists:
        for i, f in enumerate(flights):
            if f["dep"] == "EDDF" and f["arr"] and f["arr"] != "EDDF":
                t = _turn_type(f["arr"])
                dep_tod[t].append(_tod_minutes(f["first_seen"]))
                for g in flights[i + 1:]:
                    if g["arr"] == "EDDF":
                        h = (g["last_seen"] - f["first_seen"]).total_seconds() / 3600.0
                        if 0 < h < 120:
                            spans[t].append(h)
                        break
            if f["arr"] == "EDDF" and f["dep"] and f["dep"] != "EDDF":
                h = (f["last_seen"] - f["first_seen"]).total_seconds() / 3600.0
                if 0 < h < 24:
                    blocks[_turn_type(f["dep"])].append(h)
    out = {}
    for t in set(dep_tod) & set(spans):
        out[t] = {
            "dep_tod": int(_median(dep_tod[t])),
            "span_h": _median(spans[t]),
            "block_h": _median(blocks[t]) if blocks.get(t) else None,
        }
    return out


def _next_departure_dt(clock, dep_tod, turnaround_h=2.0):
    """The next datetime at minute-of-day `dep_tod` that is >= clock + turnaround.
    Encodes "you can't catch a flight that already left": a morning slot rolls to
    the next day when the aircraft only becomes available in the afternoon.
    """
    clock = _utc(clock)
    earliest = clock + timedelta(hours=turnaround_h)
    cand = earliest.replace(hour=dep_tod // 60, minute=dep_tod % 60, second=0, microsecond=0)
    if cand < earliest:
        cand += timedelta(days=1)
    return cand


def _trans_p_factory(tail_seq, fleet_counts, states, alpha=5.0):
    tail_counts = _trans_counts([tail_seq])
    state_list = list(states)

    def fleet_p(cur):
        c = fleet_counts.get(cur, {})
        tot = sum(c.values())
        if tot == 0:
            return {s: 1.0 / len(state_list) for s in state_list}
        return {s: c.get(s, 0) / tot for s in state_list}

    def trans_p(cur):
        fp = fleet_p(cur)
        c = tail_counts.get(cur, {})
        tot = sum(c.values())
        return {s: (c.get(s, 0) + alpha * fp[s]) / (tot + alpha) for s in state_list}

    return trans_p, state_list


def _rotation_forecast(tail_seq, fleet_counts, states, sched, start_clock,
                       start_state, targets, horizon_days=7, n_sims=4000,
                       max_legs=6, seed=1234):
    """Schedule-aware Monte-Carlo: from `start_clock` (when the aircraft is next
    available at FRA) sample turn types and place each at its scheduled departure
    slot, advancing a real clock by the turn's round-trip span. Yields the next
    departure distribution (with times), per-target P within the horizon (with
    times), and a modal rotation timeline — all to the hour.
    """
    if len(tail_seq) < 3 or not sched:
        return None
    trans_p, state_list = _trans_p_factory(tail_seq, fleet_counts, states)
    weights = {s: [trans_p(s)[t] for t in state_list] for s in state_list}

    def turn_sched(t):
        s = sched.get(t)
        if s:
            return s["dep_tod"], s["span_h"]
        return 12 * 60, 24.0  # default for turn types without learned times (e.g. OTHER)

    rng = random.Random(seed)
    next_first, next_first_dt = Counter(), defaultdict(list)
    occ = {t: 0 for t in targets}
    occ_dt = {t: [] for t in targets}

    for _ in range(n_sims):
        clock, cur, seen = start_clock, start_state, set()
        for step in range(max_legs):
            cur = rng.choices(state_list, weights=weights[cur])[0]
            tod, span = turn_sched(cur)
            dep = _next_departure_dt(clock, tod)
            ret = dep + timedelta(hours=span)
            if step == 0:
                next_first[cur] += 1
                next_first_dt[cur].append(dep)
            days = (dep - start_clock).total_seconds() / 86400.0
            if cur in targets and cur not in seen and days <= horizon_days:
                seen.add(cur)
                occ[cur] += 1
                occ_dt[cur].append(dep)
            clock = ret

    def med_dt(lst):
        s = sorted(lst)
        return s[len(s) // 2] if s else None

    tot = sum(next_first.values()) or 1
    next_departure = [
        {"turn": k, "route": _TARGET_ROUTE.get(k, k), "p": round(v / tot, 3),
         "when": med_dt(next_first_dt[k]).isoformat()}
        for k, v in next_first.most_common()
    ]

    per_route = []
    for t in targets:
        dts = sorted(occ_dt[t])
        if dts:
            per_route.append({
                "turn": t, "route": _TARGET_ROUTE[t], "p": round(occ[t] / n_sims, 3),
                "when": dts[len(dts) // 2].isoformat(),
                "q1": dts[len(dts) // 4].isoformat(),
                "q3": dts[min(len(dts) - 1, 3 * len(dts) // 4)].isoformat(),
            })
        else:
            per_route.append({"turn": t, "route": _TARGET_ROUTE[t], "p": 0.0,
                              "when": None, "q1": None, "q3": None})

    # Modal rotation timeline: a single coherent greedy-argmax trajectory with an
    # exact, contiguous clock (the "most likely sequence"), rather than per-ordinal
    # averages which collapse onto the dominant turn.
    timeline = []
    clock, cur = start_clock, start_state
    for _ in range(max_legs):
        p = trans_p(cur)
        nxt = max(state_list, key=lambda s: p[s])
        tod, span = turn_sched(nxt)
        dep = _next_departure_dt(clock, tod)
        ret = dep + timedelta(hours=span)
        timeline.append({
            "turn": nxt, "route": _TARGET_ROUTE.get(nxt, nxt), "is_target": nxt in targets,
            "p": round(p[nxt], 3), "dep": dep.isoformat(), "ret": ret.isoformat(),
        })
        clock, cur = ret, nxt
        if (dep - start_clock).total_seconds() / 86400.0 > horizon_days and len(timeline) >= 3:
            break

    return {
        "as_of": _utc(start_clock).isoformat(),
        "start_turn": start_state,
        "next_departure": next_departure,
        "per_route": per_route,
        "timeline": timeline,
        "horizon_days": horizon_days,
        "n_turns": len(tail_seq),
    }


def _dabyn_status(dab_flights, sched, now):
    """Infer the current rotation phase from D-ABYN's reliable FRA clock events
    (FRA departures and FRA arrivals; foreign-airport detection is unreliable).
    Returns (status_dict, start_clock, start_state) or None.
    """
    last_out = None   # (first_seen, turn, dest)
    last_in = None    # FRA arrival last_seen
    for f in dab_flights:
        if f["dep"] == "EDDF" and f["arr"] and f["arr"] != "EDDF":
            last_out = (f["first_seen"], _turn_type(f["arr"]), f["arr"])
        if f["arr"] == "EDDF":
            last_in = f["last_seen"]
    if last_out is None and last_in is None:
        return None

    at_fra = last_in is not None and (last_out is None or last_in >= last_out[0])
    if at_fra:
        start_clock = max(_utc(last_in), now)
        status = {
            "phase": "at_fra", "airborne": False, "location": "EDDF",
            "since": _utc(last_in).isoformat(),
            "last_turn": last_out[1] if last_out else None,
        }
        return status, start_clock, (last_out[1] if last_out else "HND")

    dep_dt, turn, dest = last_out
    s = sched.get(turn, {})
    span = s.get("span_h", 30.0)
    block = s.get("block_h") or max(8.0, span / 2.0 - 1.0)
    dwell = max(0.0, span - 2 * block)
    elapsed = (now - _utc(dep_dt)).total_seconds() / 3600.0
    expected_return = _utc(dep_dt) + timedelta(hours=span)
    if elapsed < block:
        phase, airborne, loc = "outbound", True, dest
    elif elapsed < block + dwell:
        phase, airborne, loc = "at_dest", False, dest
    elif elapsed < span:
        phase, airborne, loc = "returning", True, "EDDF"
    else:
        phase, airborne, loc = "overdue", False, "EDDF"
    status = {
        "phase": phase, "airborne": airborne, "location": loc, "dest": dest,
        "turn": turn, "departed": _utc(dep_dt).isoformat(),
        "due_back": expected_return.isoformat(), "last_turn": turn,
    }
    return status, max(expected_return, now), turn


def _rotation_backtest(tail_types, fleet_counts, states, alpha=5.0, k=20):
    """Walk-forward skill check: predict each of the last k turn-types from its
    prefix and compare top-1/top-2 hit rate against the always-modal baseline.
    """
    if len(tail_types) < 5:
        return None
    state_list = list(states)
    base_top = Counter(tail_types).most_common(1)[0][0]

    def fleet_p(cur):
        c = fleet_counts.get(cur, {})
        tot = sum(c.values())
        if tot == 0:
            return {s: 1.0 / len(state_list) for s in state_list}
        return {s: c.get(s, 0) / tot for s in state_list}

    hits = top2 = base = n = 0
    start = max(2, len(tail_types) - k)
    for i in range(start, len(tail_types)):
        prefix = tail_types[:i]
        cur = prefix[-1]
        tc = Counter(b for a, b in zip(prefix, prefix[1:]) if a == cur)
        fp = fleet_p(cur)
        tot = sum(tc.values())
        p = {s: (tc.get(s, 0) + alpha * fp[s]) / (tot + alpha) for s in state_list}
        ranked = sorted(p, key=lambda s: -p[s])
        actual = tail_types[i]
        n += 1
        hits += ranked[0] == actual
        top2 += actual in ranked[:2]
        base += actual == base_top
    if not n:
        return None
    return {"n": n, "top1": round(hits / n, 3), "top2": round(top2 / n, 3),
            "base": round(base / n, 3)}


_B748_FETCH_SQL = """
    SELECT TRIM(a.registration), a.is_active, TRIM(f.callsign),
           {dep_expr}, {arr_expr},
           f.flight_date, f.first_seen, f.last_seen, f.duration_minutes,
           f.arrival_airport_icao
    FROM flights f
    JOIN aircraft a ON a.icao24 = f.icao24
    {join}
    WHERE a.aircraft_type = 'B748'
    ORDER BY a.registration, f.first_seen
"""


@app.route("/api/b748-analysis")
def api_b748_analysis():
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503

    TARGET_REG = "D-ABYN"

    try:
        data = {"target_reg": TARGET_REG}
        today = datetime.now(tz=timezone.utc).date()

        # Pull all B748 flights with the canonical route resolved from the
        # callsign reference (falls back to the detector's dep/arr). The
        # callsign route is robust even when arrival detection failed (UNKN).
        enriched = _B748_FETCH_SQL.format(
            dep_expr="COALESCE(fr.departure_airport_icao, f.departure_airport_icao)",
            arr_expr="COALESCE(fr.arrival_airport_icao, f.arrival_airport_icao)",
            join="LEFT JOIN flight_routes fr ON fr.callsign = TRIM(f.callsign)",
        )
        try:
            rows = _q(conn, enriched)
        except Exception:
            # flight_routes not present yet — degrade to raw dep/arr.
            conn.rollback()
            rows = _q(conn, _B748_FETCH_SQL.format(
                dep_expr="f.departure_airport_icao",
                arr_expr="f.arrival_airport_icao",
                join="",
            ))

        by_reg = defaultdict(list)
        for r in rows:
            by_reg[r[0]].append({
                "reg": r[0], "active": r[1], "cs": (r[2] or "").strip(),
                "dep": _norm_ap(r[3]), "arr": _norm_ap(r[4]),
                "date": r[5], "first_seen": r[6], "last_seen": r[7], "dur": r[8],
                "open": r[9] is None,  # raw arrival NULL → flight still in progress
            })

        def route_known(f):
            return bool(f["arr"] and f["arr"] != "EDDF" and f["dep"])

        # ── Rotation model (schedule-aware) ───────────────────────────
        now = datetime.now(tz=timezone.utc)
        fleet_lists = list(by_reg.values())
        fleet_seqs = [_outbound_turns(fl) for fl in fleet_lists]
        fleet_counts = _trans_counts(fleet_seqs)
        states = sorted(
            {t for seq in fleet_seqs for _, t in seq} | set(_TARGET_TURNS) | {"OTHER"}
        )
        sched = _schedule_params(fleet_lists)
        data["schedule"] = {
            t: {"dep_tod": sched[t]["dep_tod"], "span_h": round(sched[t]["span_h"], 1)}
            for t in _TARGET_TURNS if t in sched
        }
        dab = by_reg.get(TARGET_REG, [])
        dab_seq = _outbound_turns(dab)

        st = _dabyn_status(dab, sched, now)
        if st:
            data["status"], start_clock, start_state = st
            data["prediction"] = _rotation_forecast(
                dab_seq, fleet_counts, states, sched,
                start_clock, start_state, _TARGET_TURNS,
            )
        else:
            data["status"] = None
            data["prediction"] = None
        data["backtest"] = _rotation_backtest(
            [t for _, t in dab_seq], fleet_counts, states
        )

        # ── D-ABYN recent flights ─────────────────────────────────────
        dab_done = sorted(
            (f for f in dab if route_known(f)),
            key=lambda f: f["last_seen"], reverse=True,
        )
        data["dabyn_recent"] = [
            {"dep": f["dep"], "arr": f["arr"], "date": f["date"].isoformat(),
             "cs": f["cs"], "dur": f["dur"]}
            for f in dab_done[:20]
        ]

        # ── Registration affinity (which tails fly the target routes) ──
        target_arrs = set(_TARGET_ARR.values())
        affinity = defaultdict(Counter)
        for fl in by_reg.values():
            for f in fl:
                if f["dep"] == "EDDF" and f["arr"] in target_arrs:
                    affinity[f["reg"]]["EDDF-" + f["arr"]] += 1
        data["affinity"] = [
            {"reg": reg, "route": route, "count": c}
            for reg, rc in affinity.items() for route, c in rc.items()
        ]

        # ── Fleet turn-type transition matrix (full B748 fleet) ───────
        _order = ["HND", "EZE", "JNB", "USW", "USE", "MEX", "GRU", "OTHER"]
        tstates = ([s for s in _order if s in states]
                   + [s for s in states if s not in _order])
        data["fleet_transitions"] = {
            "states": tstates,
            "matrix": [[fleet_counts.get(a, {}).get(b, 0) for b in tstates]
                       for a in tstates],
        }

        # ── D-ABYN rotation timeline (180 days) ───────────────────────
        cutoff = today - timedelta(days=180)
        data["timeline"] = [
            {"dep": f["dep"] or "?", "arr": f["arr"] or "?",
             "t0": f["first_seen"].isoformat(), "t1": f["last_seen"].isoformat(),
             "cs": f["cs"]}
            for f in dab if f["date"] >= cutoff
        ]

        # ── B748 fleet positions (airborne vs last known per active tail) ──
        fleet_positions = []
        for reg, fl in by_reg.items():
            if not fl or not fl[0]["active"]:
                continue
            latest = max(fl, key=lambda f: f["first_seen"])
            # In progress: most recent leg is still open and seen recently.
            airborne = (
                latest["open"]
                and (now - latest["last_seen"]).total_seconds() < 24 * 3600
            )
            grounded = [f for f in fl if route_known(f)]
            last_known = max(grounded, key=lambda f: f["last_seen"]) if grounded else None
            if airborne:
                fleet_positions.append({
                    "reg": reg, "airborne": True,
                    "dest": latest["arr"], "from": latest["dep"],
                    "cs": latest["cs"], "last_seen": latest["last_seen"].isoformat(),
                    "is_dabyn": reg == TARGET_REG,
                })
            elif last_known:
                fleet_positions.append({
                    "reg": reg, "airborne": False, "airport": last_known["arr"],
                    "from": last_known["dep"], "cs": last_known["cs"],
                    "last_seen": last_known["last_seen"].isoformat(),
                    "is_dabyn": reg == TARGET_REG,
                })
        fleet_positions.sort(key=lambda p: p["reg"])
        data["fleet_positions"] = fleet_positions

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
    <a class="nav-link" href="/schedule">Schedule</a>
    <a class="nav-link" href="/analysis-747">747-8 Analysis</a>
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
<footer style="text-align:center;padding:24px 0 8px;font-size:11px;color:var(--muted)">
  <a href="/impressum" style="color:var(--muted);text-decoration:none">Impressum</a>
  <span style="margin:0 6px">&middot;</span>
  <a href="/datenschutz" style="color:var(--muted);text-decoration:none">Datenschutz</a>
</footer>
</body>
</html>
"""


@app.route("/analysis")
def analysis():
    return render_template_string(_ANALYSIS_HTML)


_ANALYSIS_747_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>747-8 Analysis &middot; D-ABYN</title>
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

.section { margin-bottom: 24px; }
.card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 14px; margin-bottom: 12px;
}
.card-title { font-size: 13px; font-weight: 600; color: var(--text-bright); margin-bottom: 10px; }
.card-subtitle { font-size: 11px; color: var(--muted); margin-bottom: 12px; }

/* Prediction cards */
.pred-grid {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px;
}
.pred-card {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 16px; position: relative;
}
.pred-card .pred-route {
  font-size: 15px; font-weight: 700; color: var(--text-bright); margin-bottom: 2px;
}
.pred-card .pred-label {
  font-size: 10px; text-transform: uppercase; letter-spacing: 1px;
  color: var(--muted); margin: 10px 0 2px;
}
.pred-card .pred-date { font-size: 22px; font-weight: 700; color: var(--accent); letter-spacing: -0.5px; }
.pred-card .pred-when { font-size: 11px; color: var(--muted); margin-top: 2px; }
.pred-card .pred-meta { font-size: 11px; color: var(--text); margin-top: 12px; line-height: 1.7; }
.pred-card .pred-meta b { color: var(--text-bright); font-weight: 600; }
.pred-card .pred-note { font-size: 10px; color: var(--amber); margin-top: 8px; }
.conf-chip {
  position: absolute; top: 14px; right: 14px;
  font-size: 9px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px;
  padding: 2px 7px; border-radius: 10px;
}
.conf-high { background: var(--green-dim); color: var(--green); }
.conf-medium { background: var(--accent-dim); color: var(--accent); }
.conf-low { background: var(--amber-dim); color: var(--amber); }
.conf-none { background: var(--red-dim); color: var(--red); }
.pred-card .prob-bar { height: 8px; background: var(--surface2); border-radius: 4px; overflow: hidden; margin-top: 10px; }
.pred-card .prob-fill { height: 100%; border-radius: 4px; }

/* Next-departure distribution */
.dist-row { display: flex; align-items: center; gap: 8px; margin-bottom: 5px; font-size: 12px; }
.dist-label { width: 96px; text-align: right; color: var(--text); font-weight: 500; }
.dist-bar { flex: 1; height: 16px; background: var(--surface2); border-radius: 3px; overflow: hidden; }
.dist-fill { height: 100%; border-radius: 3px; background: var(--muted); opacity: 0.85; }
.dist-fill.t-hnd { background: var(--accent); }
.dist-fill.t-jnb { background: var(--green); }
.dist-fill.t-eze { background: var(--purple); }
.dist-pct { width: 42px; text-align: right; color: var(--muted); }
.dist-when { width: 130px; text-align: right; color: var(--text); font-size: 11px; }
@media (max-width: 560px) { .dist-when { display: none; } }

/* Status card */
.status-card { display: block; }
.status-card.air { border-left: 3px solid var(--cyan); }
.status-card.ground { border-left: 3px solid var(--green); }
.st-row { display: flex; align-items: center; gap: 14px; }
.st-icon { font-size: 22px; line-height: 1; }
.st-icon.air { color: var(--cyan); }
.st-icon.ground { color: var(--green); }
.st-head { font-size: 17px; color: var(--text); }
.st-head b { color: var(--text-bright); font-weight: 700; }
.st-sub { font-size: 12px; color: var(--muted); margin-top: 2px; }

/* Forecast rotation timeline */
.leg {
  background: var(--surface2); border: 1px solid var(--border); border-left: 3px solid var(--muted);
  border-radius: 8px; padding: 9px 12px; margin-bottom: 7px;
}
.leg-route { font-size: 13px; font-weight: 700; }
.leg-p { font-size: 11px; font-weight: 500; color: var(--muted); }
.leg-times { font-size: 12px; color: var(--text); margin-top: 2px; }
.leg-times b { color: var(--text-bright); }
.leg-dur { color: var(--muted); }

/* Backtest banner */
.backtest-banner {
  font-size: 11px; color: var(--muted); background: var(--surface2);
  border: 1px solid var(--border); border-radius: 8px; padding: 8px 12px; margin-bottom: 12px;
}
.backtest-banner b { color: var(--text-bright); }
.backtest-banner .skill { color: var(--green); font-weight: 600; }

/* Status banner */
.status-banner {
  display: flex; align-items: center; gap: 16px; flex-wrap: wrap;
}
.status-banner .big {
  font-size: 26px; font-weight: 700; letter-spacing: 1px; color: var(--text-bright);
}
.status-banner .sub { font-size: 12px; color: var(--muted); }

/* Fleet position cards */
.fleet-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 8px; }
.fleet-card {
  background: var(--surface2); border: 1px solid var(--border);
  border-radius: 8px; padding: 10px; text-align: center;
}
.fleet-card.dabyn { border-color: var(--accent); box-shadow: 0 0 0 1px var(--accent); }
.fleet-card.air { border-color: var(--cyan); }
.fleet-card .reg { font-size: 13px; font-weight: 700; color: var(--text-bright); margin-bottom: 4px; }
.fleet-card .airport { font-size: 18px; font-weight: 700; letter-spacing: 1px; color: var(--text); }
.fleet-card .airport.air { color: var(--cyan); font-size: 16px; letter-spacing: 0.5px; }
.fleet-card .meta { font-size: 10px; color: var(--muted); margin-top: 4px; }

/* Gantt timeline */
.gantt { overflow-x: auto; }
.gantt-row { display: flex; align-items: center; margin-bottom: 2px; height: 22px; }
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
  min-width: 2px; cursor: pointer; opacity: 0.85; transition: opacity 0.15s;
}
.gantt-flight:hover { opacity: 1; z-index: 2; }
.gantt-flight.t-hnd { background: var(--accent); }
.gantt-flight.t-jnb { background: var(--green); }
.gantt-flight.t-eze { background: var(--purple); }
.gantt-flight.other { background: var(--muted); opacity: 0.4; }
.gantt-axis {
  display: flex; justify-content: space-between;
  margin-left: 70px; min-width: 800px;
  font-size: 10px; color: var(--muted); padding-top: 4px;
}

/* Heatmap */
.heatmap-grid { display: grid; gap: 2px; grid-template-columns: 110px repeat(7, 1fr); }
.heatmap-cell {
  height: 28px; border-radius: 4px; display: flex;
  align-items: center; justify-content: center; font-size: 11px; font-weight: 600;
}
.heatmap-header { font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }
.heatmap-label { font-size: 11px; font-weight: 600; color: var(--text-bright); text-align: right; padding-right: 8px; }

/* Affinity matrix */
.affinity-wrap { overflow-x: auto; }
.affinity-table { border-collapse: collapse; font-size: 11px; }
.affinity-table th {
  padding: 4px 10px; font-weight: 600; color: var(--muted);
  text-align: center; background: var(--surface);
}
.affinity-table td { padding: 4px 10px; text-align: center; border-radius: 3px; }
.affinity-table td.reg-label {
  font-weight: 600; color: var(--text-bright); text-align: right;
  position: sticky; left: 0; background: var(--surface);
}
.affinity-table tr.dabyn td.reg-label { color: var(--accent); }

/* History table */
.history-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.history-table th {
  text-align: left; padding: 6px 8px; font-size: 10px; font-weight: 600;
  color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px;
  border-bottom: 1px solid var(--border);
}
.history-table td { padding: 5px 8px; border-bottom: 1px solid rgba(42,44,53,0.3); color: var(--text); }
.history-table .reg-cell { font-weight: 700; color: var(--text-bright); }
.history-table tr.dabyn { background: var(--accent-dim); }
.history-table tr.dabyn .reg-cell { color: var(--accent); }

/* Tooltip */
.tooltip {
  position: fixed; background: var(--surface2); border: 1px solid var(--border);
  border-radius: 6px; padding: 6px 10px; font-size: 11px; color: var(--text);
  pointer-events: none; z-index: 100; white-space: nowrap; display: none; max-width: 300px;
}
.loading { text-align: center; padding: 40px; color: var(--muted); font-size: 13px; }
.error-banner {
  display: none; background: var(--red-dim); border: 1px solid rgba(248,113,113,0.25);
  border-radius: var(--radius); padding: 10px 14px; margin-bottom: 16px;
  color: var(--red); font-size: 12px;
}
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 800px) { .two-col { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="container">

  <div class="header">
    <h1>747-8 Analysis &middot; <span>D-ABYN</span></h1>
    <div style="display:flex;gap:8px">
      <a class="nav-link" href="/schedule">Schedule</a>
      <a class="nav-link" href="/analysis">A380 Analysis</a>
      <a class="nav-link" href="/fleet">Fleet DB</a>
      <a class="nav-link" href="/">&larr; Monitor</a>
    </div>
  </div>

  <div class="error-banner" id="error-banner"></div>
  <div class="loading" id="loading">Loading 747-8 analysis data&hellip;</div>
  <div id="content" style="display:none">

  <!-- 1. Current status -->
  <div class="section">
    <div id="dabyn-status" class="status-card"></div>
  </div>

  <!-- 2. Predictions -->
  <div class="section">
    <div class="card-title">When will D-ABYN next fly each route?</div>
    <div class="card-subtitle">
      A schedule-aware rotation model: it learns D-ABYN's sequence of long-haul
      &ldquo;turns&rdquo; and each flight's scheduled departure time, then simulates the
      rotation forward on a real clock. Routes <em>compete</em> &mdash; one FRA departure
      per slot, and a slot that has already left rolls to the next day.
    </div>
    <div class="backtest-banner" id="backtest"></div>
    <div class="card" style="margin-bottom:12px">
      <div class="card-title">Most likely next FRA departure</div>
      <div class="card-subtitle">Where &amp; when D-ABYN heads on its very next departure (times UTC)</div>
      <div id="next-dep"></div>
    </div>
    <div class="pred-grid" id="pred-grid"></div>
  </div>

  <!-- 3. Forecast rotation timeline -->
  <div class="section">
    <div class="card">
      <div class="card-title">Most likely upcoming rotations</div>
      <div class="card-subtitle">The single most-probable sequence of turns, to the hour (UTC) &mdash; each leg must wait for the previous one to get back</div>
      <div id="forecast"></div>
    </div>
  </div>

  <!-- 3. D-ABYN recent flights -->
  <div class="section">
    <div class="card">
      <div class="card-title">D-ABYN Recent Flights</div>
      <div class="card-subtitle">Last 20 completed flights &mdash; route resolved by callsign</div>
      <div id="dabyn-recent"></div>
    </div>
  </div>

  <!-- 5. Fleet transition matrix -->
  <div class="section">
    <div class="card">
      <div class="card-title">Fleet Rotation Transitions (747-8)</div>
      <div class="card-subtitle">After a turn (row), where the whole 747-8 fleet goes next (column). Row-normalised; darker = more likely.</div>
      <div class="affinity-wrap" id="transitions"></div>
    </div>
  </div>

  <!-- 6. Affinity -->
  <div class="section">
    <div class="card">
      <div class="card-title">Which 747-8 Tails Fly These Routes</div>
      <div class="card-subtitle">Flights per registration on each target route (darker = more)</div>
      <div class="affinity-wrap" id="affinity"></div>
    </div>
  </div>

  <!-- 7. Gantt -->
  <div class="section">
    <div class="card">
      <div class="card-title">D-ABYN Rotation Timeline (180 days)</div>
      <div class="card-subtitle">
        <span style="color:var(--accent)">&block;</span> FRA&rarr;HND&ensp;
        <span style="color:var(--green)">&block;</span> FRA&rarr;JNB&ensp;
        <span style="color:var(--purple)">&block;</span> FRA&rarr;EZE&ensp;
        <span style="color:var(--muted)">&block;</span> Other
      </div>
      <div class="gantt" id="gantt"></div>
    </div>
  </div>

  <!-- 8. Fleet positions -->
  <div class="section">
    <div class="card">
      <div class="card-title">747-8 Fleet Positions</div>
      <div class="card-subtitle">Where each active 747-8 is now &mdash; airborne tails show their destination; D-ABYN outlined</div>
      <div class="fleet-grid" id="fleet-grid"></div>
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
  tip.innerHTML = html; tip.style.display = 'block';
  tip.style.left = (e.clientX + 12) + 'px'; tip.style.top = (e.clientY - 32) + 'px';
}
function hideTip() { tip.style.display = 'none'; }

const DAYS = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function ago(iso) {
  if (!iso) return '\\u2014';
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  if (s < 86400) return Math.floor(s/3600) + 'h ago';
  return Math.floor(s/86400) + 'd ago';
}
function fmtDate(iso) {
  if (!iso) return '\\u2014';
  const d = new Date(iso + 'T00:00:00Z');
  return DAYS[d.getUTCDay()] + ', ' + d.getUTCDate() + ' ' + MON[d.getUTCMonth()] + ' ' + d.getUTCFullYear();
}
function daysUntil(iso) {
  const d = new Date(iso + 'T00:00:00Z').getTime();
  const n = new Date();
  const t = Date.UTC(n.getUTCFullYear(), n.getUTCMonth(), n.getUTCDate());
  return Math.round((d - t) / 86400000);
}

const ICAO_NAMES = {
  EDDF:'FRA', RJTT:'HND', FAOR:'JNB', SAEZ:'EZE',
  EDDM:'MUC', VTBS:'BKK', KJFK:'JFK', KLAX:'LAX', KORD:'ORD', OMDB:'DXB',
  VHHH:'HKG', RKSI:'ICN', WSSS:'SIN', SBGR:'GRU', LEMD:'MAD', EGLL:'LHR',
  LFPG:'CDG', EDDB:'BER', EDDL:'DUS', EDDS:'STR', EDDH:'HAM', RPLL:'MNL',
  ZSPD:'PVG', ZBAA:'PEK', WMKK:'KUL', VIDP:'DEL', VABB:'BOM', LEBL:'BCN',
  LIRF:'FCO', YSSY:'SYD', OEJN:'JED', OERK:'RUH', OTHH:'DOH', OMAA:'AUH',
  CYYZ:'YYZ', KIAH:'IAH', KMIA:'MIA', KSFO:'SFO', CYVR:'YVR', RJAA:'NRT',
  LOWW:'VIE', EHAM:'AMS', LSZH:'ZRH', KEWR:'EWR', SCEL:'SCL', SKBO:'BOG',
  MMMX:'MEX', FACT:'CPT', HECA:'CAI', VTBD:'BKK', ZGGG:'CAN', RCTP:'TPE',
};
function icaoToCity(code) { return ICAO_NAMES[code] || code; }
function routeName(r) {
  const p = r.split('-');
  return icaoToCity(p[0]) + '\\u2192' + icaoToCity(p[1]);
}
const TURN_NAME = {
  HND:'FRA\\u2192HND', EZE:'FRA\\u2192EZE', JNB:'FRA\\u2192JNB',
  USW:'US West', USE:'US East', MEX:'Mexico City', GRU:'S\\u00e3o Paulo', OTHER:'Other',
};
const TURN_CLS = { HND:'t-hnd', JNB:'t-jnb', EZE:'t-eze' };
function turnColor(t) {
  return t==='HND'?'var(--accent)':t==='JNB'?'var(--green)':t==='EZE'?'var(--purple)':'var(--muted)';
}

async function init() {
  let data;
  try {
    const r = await fetch('/api/b748-analysis');
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

  renderStatus(data.status);
  renderBacktest(data.backtest, data.prediction);
  renderNextDeparture(data.prediction);
  renderPredictions(data.prediction);
  renderForecast(data.prediction);
  renderRecent(data.dabyn_recent);
  renderTransitions(data.fleet_transitions);
  renderAffinity(data.affinity);
  renderGantt(data.timeline);
  renderFleetPositions(data.fleet_positions);
}

/* date-time helpers (all UTC) */
function fmtDT(iso) {
  if (!iso) return '\\u2014';
  const d = new Date(iso);
  const hh = String(d.getUTCHours()).padStart(2,'0'), mm = String(d.getUTCMinutes()).padStart(2,'0');
  return DAYS[d.getUTCDay()] + ' ' + d.getUTCDate() + ' ' + MON[d.getUTCMonth()] + ', ' + hh + ':' + mm;
}
function untilStr(iso) {
  const ms = new Date(iso).getTime() - Date.now();
  if (ms < 0) return 'now';
  const h = Math.round(ms / 3600000);
  return h < 48 ? 'in ~' + h + 'h' : 'in ~' + Math.round(h/24) + 'd';
}

/* ── 1. Current status ────────────────────────────────── */
function renderStatus(st) {
  const el = $('dabyn-status');
  if (!st) { el.innerHTML = '<div class="card"><div style="color:var(--muted)">No recent activity for D-ABYN</div></div>'; return; }
  const dot = st.airborne
    ? '<span class="st-icon air">&#9992;</span>'
    : '<span class="st-icon ground">&#9679;</span>';
  let head, sub;
  if (st.phase === 'at_fra') {
    head = 'On the ground at <b>FRA</b>';
    sub = (st.last_turn ? 'Last turn ' + (TURN_NAME[st.last_turn] || st.last_turn) + ' &middot; ' : '') +
          'idle since ' + fmtDT(st.since) + ' &middot; ready for next departure';
  } else if (st.phase === 'outbound') {
    head = 'Airborne &mdash; <b>FRA &rarr; ' + icaoToCity(st.dest) + '</b>';
    sub = 'Departed ' + fmtDT(st.departed) + ' &middot; due back FRA ~' + fmtDT(st.due_back);
  } else if (st.phase === 'at_dest') {
    head = 'On the ground at <b>' + icaoToCity(st.dest) + '</b>';
    sub = 'Mid-rotation (' + (TURN_NAME[st.turn] || st.turn) + ') &middot; due back FRA ~' + fmtDT(st.due_back);
  } else if (st.phase === 'returning') {
    head = 'Airborne &mdash; <b>' + icaoToCity(st.dest) + ' &rarr; FRA</b>';
    sub = 'Returning &middot; due FRA ~' + fmtDT(st.due_back) + ' (' + untilStr(st.due_back) + ')';
  } else { /* overdue */
    head = 'Likely back at <b>FRA</b>';
    sub = (TURN_NAME[st.turn] || st.turn) + ' turn was due ~' + fmtDT(st.due_back) + ' (detection gap)';
  }
  el.className = 'status-card card ' + (st.airborne ? 'air' : 'ground');
  el.innerHTML =
    '<div class="st-row">' + dot +
      '<div><div class="st-head">' + head + '</div>' +
      '<div class="st-sub">' + sub + '</div></div>' +
    '</div>';
}

/* ── 2. Predictions (schedule-aware rotation model) ───── */
function renderBacktest(bt, pred) {
  const el = $('backtest');
  if (!pred) { el.innerHTML = 'Not enough rotation history for D-ABYN to model yet.'; return; }
  let s = 'Based on <b>' + pred.n_turns + '</b> past FRA departures. Forecast clock starts ' +
    '<b>' + fmtDT(pred.as_of) + '</b> UTC (when D-ABYN is next free at FRA).';
  if (bt) {
    s += ' Backtest skill: <span class="skill">' + Math.round(bt.top1*100) + '%</span> top-1 / ' +
      Math.round(bt.top2*100) + '% top-2 next-turn accuracy (vs ' + Math.round(bt.base*100) +
      '% for always-guess-most-common), over the last ' + bt.n + ' departures.';
  }
  el.innerHTML = s;
}
function renderNextDeparture(pred) {
  if (!pred || !pred.next_departure.length) {
    $('next-dep').innerHTML = '<div style="color:var(--muted)">\\u2014</div>'; return;
  }
  const max = pred.next_departure[0].p || 1;
  $('next-dep').innerHTML = pred.next_departure.map(d =>
    '<div class="dist-row">' +
      '<div class="dist-label">' + (TURN_NAME[d.turn] || routeName(d.route)) + '</div>' +
      '<div class="dist-bar"><div class="dist-fill ' + (TURN_CLS[d.turn] || '') +
        '" style="width:' + (d.p/max*100) + '%"></div></div>' +
      '<div class="dist-pct">' + Math.round(d.p*100) + '%</div>' +
      '<div class="dist-when">' + fmtDT(d.when) + '</div>' +
    '</div>'
  ).join('');
}
function renderPredictions(pred) {
  if (!pred) { $('pred-grid').innerHTML = '<div style="color:var(--muted)">Not enough rotation data to predict.</div>'; return; }
  const H = pred.horizon_days;
  $('pred-grid').innerHTML = pred.per_route.map(p => {
    const color = turnColor(p.turn);
    let dateBlock;
    if (p.when) {
      dateBlock = '<div class="pred-date" style="color:' + color + '">' + fmtDT(p.when) + '</div>' +
        '<div class="pred-when">most likely &middot; ' + untilStr(p.when) + '</div>' +
        '<div class="pred-meta">Window: <b>' + fmtDT(p.q1) + '</b> &ndash; <b>' + fmtDT(p.q3) + '</b></div>';
    } else {
      dateBlock = '<div class="pred-date" style="color:var(--muted)">\\u2014</div>' +
        '<div class="pred-when">not expected within ' + H + ' days</div>';
    }
    return '<div class="pred-card">' +
      '<div class="pred-route">' + routeName(p.route) + '</div>' +
      '<div class="pred-label">Chance within ' + H + ' days</div>' +
      '<div class="pred-date" style="color:' + color + '">' + Math.round(p.p*100) + '%</div>' +
      '<div class="prob-bar"><div class="prob-fill" style="width:' + (p.p*100) + '%;background:' + color + '"></div></div>' +
      '<div class="pred-label" style="margin-top:14px">Most likely departure</div>' +
      dateBlock +
    '</div>';
  }).join('');
}

/* ── 3. Forecast rotation timeline ────────────────────── */
function renderForecast(pred) {
  const el = $('forecast');
  if (!pred || !pred.timeline.length) { el.innerHTML = '<div style="color:var(--muted)">No forecast available</div>'; return; }
  el.innerHTML = pred.timeline.map(L => {
    const color = turnColor(L.turn);
    const hrs = Math.round((new Date(L.ret) - new Date(L.dep)) / 3600000);
    const label = L.is_target ? routeName(L.route) : (TURN_NAME[L.turn] || L.turn);
    return '<div class="leg" style="border-left-color:' + color + '">' +
      '<div class="leg-route" style="color:' + (L.is_target ? color : 'var(--text-bright)') + '">' +
        label + '&#8202;&#8644; <span class="leg-p">' + Math.round(L.p*100) + '%</span></div>' +
      '<div class="leg-times">depart <b>' + fmtDT(L.dep) + '</b> &rarr; back <b>' + fmtDT(L.ret) +
        '</b> <span class="leg-dur">(' + hrs + 'h round trip)</span></div>' +
    '</div>';
  }).join('');
}

/* ── 3. Recent flights ────────────────────────────────── */
function renderRecent(recent) {
  if (!recent || !recent.length) { $('dabyn-recent').innerHTML = '<div style="color:var(--muted)">No flights recorded</div>'; return; }
  let html = '<table class="history-table"><thead><tr>' +
    '<th>Date</th><th>Route</th><th>Callsign</th><th>Duration</th></tr></thead><tbody>';
  recent.forEach(h => {
    const dur = h.dur ? Math.floor(h.dur/60) + 'h ' + (h.dur%60) + 'm' : '\\u2014';
    html += '<tr><td>' + h.date + '</td>' +
      '<td>' + icaoToCity(h.dep) + '\\u2192' + icaoToCity(h.arr) + '</td>' +
      '<td>' + (h.cs || '\\u2014') + '</td><td>' + dur + '</td></tr>';
  });
  html += '</tbody></table>';
  $('dabyn-recent').innerHTML = html;
}

/* ── 5. Fleet transition matrix ───────────────────────── */
function renderTransitions(tr) {
  if (!tr || !tr.states.length) { $('transitions').innerHTML = '<div style="color:var(--muted)">Not enough data</div>'; return; }
  const st = tr.states, M = tr.matrix;
  let html = '<table class="affinity-table"><thead><tr><th></th>';
  st.forEach(s => { html += '<th>' + (TURN_NAME[s] || s) + '</th>'; });
  html += '</tr></thead><tbody>';
  st.forEach((from, i) => {
    const rowTotal = M[i].reduce((a, b) => a + b, 0);
    html += '<tr><td class="reg-label">' + (TURN_NAME[from] || from) + '</td>';
    st.forEach((to, j) => {
      const cnt = M[i][j];
      const frac = rowTotal ? cnt / rowTotal : 0;
      const bg = cnt > 0 ? 'rgba(91,141,239,' + (0.1 + frac * 0.85) + ')' : 'transparent';
      const title = cnt + ' (' + Math.round(frac * 100) + '%)';
      html += '<td title="' + title + '" style="background:' + bg + ';color:' +
        (frac > 0.08 ? 'var(--text-bright)' : 'var(--muted)') + '">' +
        (cnt ? Math.round(frac * 100) + '%' : '') + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  $('transitions').innerHTML = html;
}

/* ── 6. Affinity ──────────────────────────────────────── */
function renderAffinity(affinity) {
  if (!affinity || !affinity.length) { $('affinity').innerHTML = '<div style="color:var(--muted)">Not enough data</div>'; return; }
  const routes = [...new Set(affinity.map(a => a.route))].sort();
  const regs = [...new Set(affinity.map(a => a.reg))].sort();
  const lookup = {};
  let maxVal = 0;
  affinity.forEach(a => { lookup[a.reg + '|' + a.route] = a.count; if (a.count > maxVal) maxVal = a.count; });

  let html = '<table class="affinity-table"><thead><tr><th></th>';
  routes.forEach(r => { html += '<th>' + routeName(r) + '</th>'; });
  html += '</tr></thead><tbody>';
  regs.forEach(reg => {
    html += '<tr class="' + (reg === 'D-ABYN' ? 'dabyn' : '') + '"><td class="reg-label">' + reg + '</td>';
    routes.forEach(route => {
      const cnt = lookup[reg + '|' + route] || 0;
      const intensity = cnt / maxVal;
      const bg = cnt > 0 ? 'rgba(91,141,239,' + (0.1 + intensity * 0.8) + ')' : 'transparent';
      html += '<td style="background:' + bg + ';color:' + (cnt > 0 ? 'var(--text-bright)' : '') + '">' + (cnt || '') + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  $('affinity').innerHTML = html;
}

/* ── 7. Gantt ─────────────────────────────────────────── */
const TARGET_CLS = { 'EDDF-RJTT':'t-hnd', 'EDDF-FAOR':'t-jnb', 'EDDF-SAEZ':'t-eze' };
function renderGantt(timeline) {
  if (!timeline || !timeline.length) { $('gantt').innerHTML = '<div style="color:var(--muted)">No flight data in the last 180 days</div>'; return; }
  const now = Date.now();
  const t0 = now - 180*86400000;
  const range = now - t0;

  let html = '<div class="gantt-row"><div class="gantt-label">D-ABYN</div><div class="gantt-track">';
  timeline.forEach(f => {
    const fs = new Date(f.t0).getTime();
    const fe = new Date(f.t1).getTime();
    const left = Math.max(0, (fs - t0) / range * 100);
    const width = Math.max(0.15, (fe - fs) / range * 100);
    const cls = TARGET_CLS[f.dep + '-' + f.arr] || 'other';
    const tipText = icaoToCity(f.dep) + '&rarr;' + icaoToCity(f.arr) +
      '<br>' + (f.cs || '') + ' &middot; ' + f.t0.slice(0,10);
    html += '<div class="gantt-flight ' + cls + '" style="left:' + left + '%;width:' + width + '%"' +
      ' onmouseenter="showTip(\\'' + tipText.replace(/'/g, "\\\\'") + '\\', event)" onmouseleave="hideTip()"></div>';
  });
  html += '</div></div>';
  html += '<div class="gantt-axis">';
  for (let i = 0; i <= 6; i++) {
    const d = new Date(t0 + (range * i / 6));
    html += '<span>' + d.toISOString().slice(5,10) + '</span>';
  }
  html += '</div>';
  $('gantt').innerHTML = html;
}

/* ── 8. Fleet positions ───────────────────────────────── */
function renderFleetPositions(positions) {
  if (!positions || !positions.length) { $('fleet-grid').innerHTML = '<div style="color:var(--muted)">No fleet positions</div>'; return; }
  $('fleet-grid').innerHTML = positions.map(p => {
    const cls = 'fleet-card ' + (p.is_dabyn ? 'dabyn ' : '') + (p.airborne ? 'air' : '');
    if (p.airborne) {
      const dest = (p.dest && p.dest !== 'EDDF') ? icaoToCity(p.dest) : (p.dest ? icaoToCity(p.dest) : null);
      const big = dest ? '&#9992; ' + dest : '&#9992; airborne';
      const sub = dest
        ? 'airborne &middot; ' + icaoToCity(p.from) + '&rarr;' + dest
        : 'airborne &middot; from ' + icaoToCity(p.from);
      return '<div class="' + cls + '">' +
        '<div class="reg">' + p.reg + '</div>' +
        '<div class="airport air">' + big + '</div>' +
        '<div class="meta">' + (p.cs || '') + ' &middot; ' + ago(p.last_seen) + '</div>' +
        '<div class="meta">' + sub + '</div>' +
      '</div>';
    }
    return '<div class="' + cls + '">' +
      '<div class="reg">' + p.reg + '</div>' +
      '<div class="airport">' + icaoToCity(p.airport) + '</div>' +
      '<div class="meta">' + (p.cs || '') + ' &middot; ' + ago(p.last_seen) + '</div>' +
      '<div class="meta">' + icaoToCity(p.from) + '&rarr;' + icaoToCity(p.airport) + '</div>' +
    '</div>';
  }).join('');
}

init();
</script>
<footer style="text-align:center;padding:24px 0 8px;font-size:11px;color:var(--muted)">
  <a href="/impressum" style="color:var(--muted);text-decoration:none">Impressum</a>
  <span style="margin:0 6px">&middot;</span>
  <a href="/datenschutz" style="color:var(--muted);text-decoration:none">Datenschutz</a>
</footer>
</body>
</html>
"""


@app.route("/analysis-747")
def analysis_747():
    return render_template_string(_ANALYSIS_747_HTML)


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
                   a.needs_review,
                   EXISTS (
                       SELECT 1 FROM positions p
                       WHERE p.icao24 = a.icao24
                         AND p.captured_at >= NOW() - INTERVAL '5 minutes'
                   ) AS currently_tracking
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
                "currently_tracking": r[11],
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
.badge-tracking {
  display: inline-block; padding: 1px 7px; border-radius: 999px;
  font-size: 10px; font-weight: 600; background: rgba(56, 189, 248, 0.15); color: #38bdf8;
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
        <button class="toggle-btn" data-status="tracking">Tracking</button>
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
    if (statusFilter === 'tracking' && !a.currently_tracking) return false;
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
    const trackingBadge = a.currently_tracking ? ' <span class="badge-tracking">tracking</span>' : '';
    const reviewBadge = a.needs_review ? ' <span class="badge-review">review</span>' : '';
    const rowClass = a.needs_review ? ' class="review-row"' : '';
    return '<tr' + rowClass + ' onclick="location.href=\\'/fleet/' + a.icao24 + '\\'">' +
      '<td class="reg">' + esc(a.registration) + '</td>' +
      '<td class="hex">' + esc(a.icao24) + '</td>' +
      '<td class="type">' + esc(a.aircraft_type || '\\u2014') + '</td>' +
      '<td>' + esc(a.aircraft_subtype || '\\u2014') + '</td>' +
      '<td>' + statusBadge + trackingBadge + reviewBadge + '</td>' +
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
<footer style="text-align:center;padding:24px 0 8px;font-size:11px;color:var(--muted)">
  <a href="/impressum" style="color:var(--muted);text-decoration:none">Impressum</a>
  <span style="margin:0 6px">&middot;</span>
  <a href="/datenschutz" style="color:var(--muted);text-decoration:none">Datenschutz</a>
</footer>
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
<footer style="text-align:center;padding:24px 0 8px;font-size:11px;color:var(--muted)">
  <a href="/impressum" style="color:var(--muted);text-decoration:none">Impressum</a>
  <span style="margin:0 6px">&middot;</span>
  <a href="/datenschutz" style="color:var(--muted);text-decoration:none">Datenschutz</a>
</footer>
</body>
</html>
"""


@app.route("/fleet")
def fleet():
    return render_template_string(_FLEET_HTML)


@app.route("/fleet/<icao24>")
def fleet_detail(icao24):
    return render_template_string(_FLEET_DETAIL_HTML)


# ── Admin Interface ────────────────────────────────────────────────

ADMIN_PATH_PREFIX = os.environ.get("ADMIN_PATH_PREFIX", "").strip().strip("/")


def _db_dict():
    """Connection returning dict rows for JSON-friendly results."""
    return psycopg.connect(**DB_CONNECT, autocommit=True, row_factory=psycopg.rows.dict_row)


def _serialize_row(row):
    """Convert a dict row to JSON-safe types."""
    from datetime import date as _date
    out = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif isinstance(v, _date):
            out[k] = v.isoformat()
        elif isinstance(v, str):
            out[k] = v.strip()
        else:
            out[k] = v
    return out


if ADMIN_PATH_PREFIX:
    _pfx = f"/{ADMIN_PATH_PREFIX}"

    # ── Aircraft API ──────────────────────────────────────────────

    @app.route(f"{_pfx}/api/aircraft")
    def admin_aircraft_list():
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            search = request.args.get("search", "").strip()
            type_filter = request.args.get("type", "").strip()
            status = request.args.get("status", "").strip()
            needs_review = request.args.get("needs_review", "").strip()

            sql = """
                SELECT icao24, registration, aircraft_type, aircraft_subtype,
                       airline_iata, is_active, needs_review,
                       first_seen_date, last_seen_date,
                       created_at, updated_at
                FROM aircraft WHERE 1=1
            """
            params = []

            if search:
                sql += " AND (icao24 ILIKE %s OR registration ILIKE %s OR aircraft_subtype ILIKE %s)"
                params += [f"%{search}%", f"%{search}%", f"%{search}%"]
            if type_filter:
                sql += " AND aircraft_type = %s"
                params.append(type_filter)
            if status == "active":
                sql += " AND is_active = TRUE"
            elif status == "retired":
                sql += " AND is_active = FALSE"
            if needs_review == "true":
                sql += " AND needs_review = TRUE"

            sql += " ORDER BY registration"

            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        return jsonify({"aircraft": [_serialize_row(r) for r in rows]})

    @app.route(f"{_pfx}/api/aircraft/<icao24>")
    def admin_aircraft_get(icao24):
        icao24 = icao24.strip().lower()
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM aircraft WHERE icao24 = %s", (icao24,)
                )
                row = cur.fetchone()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify(_serialize_row(row))

    @app.route(f"{_pfx}/api/aircraft/<icao24>", methods=["PUT"])
    def admin_aircraft_update(icao24):
        icao24 = icao24.strip().lower()
        data = request.get_json(force=True)
        allowed = {"registration", "aircraft_type", "aircraft_subtype", "airline_iata", "needs_review"}
        fields = {k: v for k, v in data.items() if k in allowed}
        if not fields:
            return jsonify({"error": "no valid fields to update"}), 400

        sets = ", ".join(f"{k} = %s" for k in fields)
        vals = list(fields.values())
        vals.append(icao24)

        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE aircraft SET {sets}, updated_at = NOW() WHERE icao24 = %s RETURNING *",
                    vals,
                )
                row = cur.fetchone()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify(_serialize_row(row))

    @app.route(f"{_pfx}/api/aircraft", methods=["POST"])
    def admin_aircraft_create():
        data = request.get_json(force=True)
        icao24 = (data.get("icao24") or "").strip().lower()
        registration = (data.get("registration") or "").strip().upper()
        if not icao24 or not registration:
            return jsonify({"error": "icao24 and registration are required"}), 400

        aircraft_type = (data.get("aircraft_type") or "").strip().upper() or None
        aircraft_subtype = (data.get("aircraft_subtype") or "").strip() or None
        airline_iata = (data.get("airline_iata") or "LH").strip().upper()

        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO aircraft (icao24, registration, aircraft_type, aircraft_subtype, airline_iata)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (icao24, registration, aircraft_type, aircraft_subtype, airline_iata),
                )
                row = cur.fetchone()
        except psycopg.errors.UniqueViolation:
            conn.close()
            return jsonify({"error": f"aircraft {icao24} already exists"}), 409
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        return jsonify(_serialize_row(row)), 201

    @app.route(f"{_pfx}/api/aircraft/<icao24>", methods=["DELETE"])
    def admin_aircraft_delete(icao24):
        icao24 = icao24.strip().lower()
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                # Check for flights
                cur.execute("SELECT COUNT(*) AS cnt FROM flights WHERE icao24 = %s", (icao24,))
                cnt = cur.fetchone()["cnt"]
                if cnt > 0:
                    conn.close()
                    return jsonify({"error": f"cannot delete: {cnt} flights reference this aircraft"}), 409
                cur.execute("DELETE FROM aircraft WHERE icao24 = %s RETURNING icao24", (icao24,))
                row = cur.fetchone()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify({"deleted": icao24})

    @app.route(f"{_pfx}/api/aircraft/<icao24>/retire", methods=["POST"])
    def admin_aircraft_retire(icao24):
        icao24 = icao24.strip().lower()
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE aircraft SET is_active = FALSE, last_seen_date = CURRENT_DATE, updated_at = NOW()
                    WHERE icao24 = %s RETURNING *
                    """,
                    (icao24,),
                )
                row = cur.fetchone()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify(_serialize_row(row))

    @app.route(f"{_pfx}/api/aircraft/<icao24>/reactivate", methods=["POST"])
    def admin_aircraft_reactivate(icao24):
        icao24 = icao24.strip().lower()
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE aircraft SET is_active = TRUE, last_seen_date = NULL, updated_at = NOW()
                    WHERE icao24 = %s RETURNING *
                    """,
                    (icao24,),
                )
                row = cur.fetchone()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify(_serialize_row(row))

    # ── Flight API ────────────────────────────────────────────────

    @app.route(f"{_pfx}/api/flights")
    def admin_flights_list():
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            icao24 = request.args.get("icao24", "").strip()
            callsign = request.args.get("callsign", "").strip()
            dep = request.args.get("dep", "").strip()
            arr = request.args.get("arr", "").strip()
            date_from = request.args.get("date_from", "").strip()
            date_to = request.args.get("date_to", "").strip()
            needs_review = request.args.get("needs_review", "").strip()
            page = int(request.args.get("page", "1"))
            per_page = min(int(request.args.get("per_page", "50")), 200)

            sql = """
                SELECT f.id, f.icao24, f.callsign,
                       f.departure_airport_icao, f.arrival_airport_icao,
                       f.first_seen, f.last_seen,
                       f.flight_date, f.duration_minutes, f.needs_review,
                       a.registration, a.aircraft_type
                FROM flights f
                JOIN aircraft a ON a.icao24 = f.icao24
                WHERE 1=1
            """
            count_sql = "SELECT COUNT(*) AS cnt FROM flights f WHERE 1=1"
            params = []
            count_params = []

            if icao24:
                clause = " AND f.icao24 = %s"
                sql += clause
                count_sql += clause
                params.append(icao24.lower())
                count_params.append(icao24.lower())
            if callsign:
                clause = " AND f.callsign ILIKE %s"
                sql += clause
                count_sql += clause
                params.append(f"%{callsign}%")
                count_params.append(f"%{callsign}%")
            if dep:
                clause = " AND f.departure_airport_icao = %s"
                sql += clause
                count_sql += clause
                params.append(dep.upper())
                count_params.append(dep.upper())
            if arr:
                clause = " AND f.arrival_airport_icao = %s"
                sql += clause
                count_sql += clause
                params.append(arr.upper())
                count_params.append(arr.upper())
            if date_from:
                clause = " AND f.flight_date >= %s"
                sql += clause
                count_sql += clause
                params.append(date_from)
                count_params.append(date_from)
            if date_to:
                clause = " AND f.flight_date <= %s"
                sql += clause
                count_sql += clause
                params.append(date_to)
                count_params.append(date_to)
            if needs_review == "true":
                clause = " AND f.needs_review = TRUE"
                sql += clause
                count_sql += clause

            sql += " ORDER BY f.first_seen DESC LIMIT %s OFFSET %s"
            params += [per_page, (page - 1) * per_page]

            with conn.cursor() as cur:
                cur.execute(count_sql, count_params)
                total = cur.fetchone()["cnt"]
                cur.execute(sql, params)
                rows = cur.fetchall()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        return jsonify({
            "flights": [_serialize_row(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page if per_page else 1,
        })

    @app.route(f"{_pfx}/api/flights/<int:flight_id>")
    def admin_flight_get(flight_id):
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT f.*, a.registration, a.aircraft_type, a.aircraft_subtype
                    FROM flights f
                    JOIN aircraft a ON a.icao24 = f.icao24
                    WHERE f.id = %s
                    """,
                    (flight_id,),
                )
                row = cur.fetchone()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify(_serialize_row(row))

    @app.route(f"{_pfx}/api/flights/<int:flight_id>", methods=["PUT"])
    def admin_flight_update(flight_id):
        data = request.get_json(force=True)
        allowed = {"callsign", "departure_airport_icao", "arrival_airport_icao", "needs_review"}
        fields = {k: v for k, v in data.items() if k in allowed}
        if not fields:
            return jsonify({"error": "no valid fields to update"}), 400

        sets = ", ".join(f"{k} = %s" for k in fields)
        vals = list(fields.values())
        vals.append(flight_id)

        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE flights SET {sets} WHERE id = %s RETURNING *",
                    vals,
                )
                row = cur.fetchone()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify(_serialize_row(row))

    @app.route(f"{_pfx}/api/flights", methods=["POST"])
    def admin_flight_create():
        data = request.get_json(force=True)
        icao24 = (data.get("icao24") or "").strip().lower()
        first_seen = data.get("first_seen")
        last_seen = data.get("last_seen")
        if not icao24 or not first_seen or not last_seen:
            return jsonify({"error": "icao24, first_seen, and last_seen are required"}), 400

        callsign = (data.get("callsign") or "").strip().upper() or None
        dep = (data.get("departure_airport_icao") or "").strip().upper() or None
        arr = (data.get("arrival_airport_icao") or "").strip().upper() or None

        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                # Verify aircraft exists
                cur.execute("SELECT 1 FROM aircraft WHERE icao24 = %s", (icao24,))
                if not cur.fetchone():
                    conn.close()
                    return jsonify({"error": f"aircraft {icao24} not found"}), 404
                cur.execute(
                    """
                    INSERT INTO flights (icao24, callsign, departure_airport_icao, arrival_airport_icao, first_seen, last_seen)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (icao24, callsign, dep, arr, first_seen, last_seen),
                )
                row = cur.fetchone()
        except psycopg.errors.UniqueViolation:
            conn.close()
            return jsonify({"error": "flight with this icao24 + first_seen already exists"}), 409
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        return jsonify(_serialize_row(row)), 201

    @app.route(f"{_pfx}/api/flights/<int:flight_id>", methods=["DELETE"])
    def admin_flight_delete(flight_id):
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM flights WHERE id = %s RETURNING id", (flight_id,))
                row = cur.fetchone()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        if not row:
            return jsonify({"error": "not found"}), 404
        return jsonify({"deleted": flight_id})

    # ── Autocomplete helpers ──────────────────────────────────────

    @app.route(f"{_pfx}/api/airports")
    def admin_airports_search():
        q = request.args.get("q", "").strip().upper()
        if len(q) < 2:
            return jsonify({"airports": []})
        try:
            conn = _db_dict()
        except Exception as e:
            return jsonify({"error": str(e)}), 503

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT icao_code, name FROM airports WHERE icao_code LIKE %s OR name ILIKE %s LIMIT 20",
                    (f"{q}%", f"%{q}%"),
                )
                rows = cur.fetchall()
        except Exception as e:
            conn.close()
            return jsonify({"error": str(e)}), 500

        conn.close()
        return jsonify({"airports": [_serialize_row(r) for r in rows]})

    # ── Admin HTML ────────────────────────────────────────────────

    _ADMIN_HTML = (
        """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LH Admin</title>
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
  background: var(--bg); color: var(--text);
  font-family: 'Inter', -apple-system, 'Segoe UI', system-ui, sans-serif;
  font-size: 14px; line-height: 1.5; -webkit-font-smoothing: antialiased;
}
.container { max-width: 1100px; margin: 0 auto; padding: 0 16px 32px; }
.header {
  padding: 16px 0 12px; display: flex; justify-content: space-between;
  align-items: center; border-bottom: 1px solid var(--border); margin-bottom: 20px;
}
.header h1 { font-size: 17px; font-weight: 600; color: var(--text-bright); }
.header h1 span { color: var(--accent); }

/* Tabs */
.tabs { display: flex; gap: 0; margin-bottom: 20px; border-bottom: 1px solid var(--border); }
.tab {
  padding: 10px 20px; cursor: pointer; color: var(--muted); font-size: 13px;
  font-weight: 500; border-bottom: 2px solid transparent; transition: all 0.2s;
}
.tab:hover { color: var(--text); }
.tab.active { color: var(--accent); border-bottom-color: var(--accent); }
.tab-content { display: none; }
.tab-content.active { display: block; }

/* Controls */
.controls {
  display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; align-items: center;
}
input, select {
  background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
  padding: 7px 10px; color: var(--text); font-size: 13px; outline: none;
}
input:focus, select:focus { border-color: var(--accent); }
input::placeholder { color: var(--muted); }
select { cursor: pointer; }

/* Buttons */
.btn {
  padding: 7px 14px; border: none; border-radius: 6px; font-size: 12px;
  font-weight: 600; cursor: pointer; transition: opacity 0.2s; white-space: nowrap;
}
.btn:hover { opacity: 0.85; }
.btn-primary { background: var(--accent); color: #fff; }
.btn-danger { background: var(--red); color: #fff; }
.btn-success { background: var(--green); color: #111; }
.btn-muted { background: var(--surface2); color: var(--text); border: 1px solid var(--border); }
.btn-sm { padding: 4px 10px; font-size: 11px; }

/* Table */
table {
  width: 100%; border-collapse: collapse; font-size: 12px;
}
th {
  text-align: left; padding: 8px 6px; color: var(--muted); font-size: 10px;
  text-transform: uppercase; letter-spacing: 0.8px; font-weight: 600;
  border-bottom: 1px solid var(--border); white-space: nowrap; cursor: pointer;
  user-select: none;
}
th:hover { color: var(--text); }
td {
  padding: 6px; border-bottom: 1px solid rgba(42,44,53,0.5);
  vertical-align: middle;
}
tr:hover td { background: var(--surface); }
tr.review td { background: var(--amber-dim); }
.editable {
  cursor: text; padding: 2px 4px; border-radius: 3px; min-width: 30px;
  display: inline-block;
}
.editable:hover { background: var(--surface2); }
.editable:focus {
  outline: 1px solid var(--accent); background: var(--surface2);
}
.actions { white-space: nowrap; display: flex; gap: 4px; }

/* Modal */
.modal-bg {
  display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.7); z-index: 100; justify-content: center; align-items: center;
}
.modal-bg.show { display: flex; }
.modal {
  background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
  padding: 24px; width: 90%; max-width: 440px;
}
.modal h3 { font-size: 15px; color: var(--text-bright); margin-bottom: 16px; }
.modal label {
  display: block; font-size: 11px; color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.5px; margin-bottom: 4px; margin-top: 10px;
}
.modal input { width: 100%; }
.modal-actions { display: flex; gap: 8px; justify-content: flex-end; margin-top: 20px; }

/* Pagination */
.pagination {
  display: flex; gap: 8px; align-items: center; justify-content: center;
  margin-top: 16px; font-size: 12px; color: var(--muted);
}

/* Status badges */
.badge {
  display: inline-block; padding: 1px 7px; border-radius: 999px;
  font-size: 10px; font-weight: 600;
}
.badge-active { background: var(--green-dim); color: var(--green); }
.badge-retired { background: var(--red-dim); color: var(--red); }
.badge-review { background: var(--amber-dim); color: var(--amber); }

/* Toast */
.toast {
  position: fixed; bottom: 24px; right: 24px; padding: 10px 18px;
  border-radius: 8px; font-size: 13px; font-weight: 500; z-index: 200;
  transition: opacity 0.3s; pointer-events: none;
}
.toast-ok { background: var(--green); color: #111; }
.toast-err { background: var(--red); color: #fff; }
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1><span>LH</span> Admin</h1>
    <a href="/" style="color:var(--muted);font-size:12px;text-decoration:none">&larr; Dashboard</a>
  </div>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('aircraft')">Aircraft</div>
    <div class="tab" onclick="switchTab('flights')">Flights</div>
  </div>

  <!-- ── Aircraft Tab ── -->
  <div id="tab-aircraft" class="tab-content active">
    <div class="controls">
      <input id="ac-search" type="text" placeholder="Search icao24 / reg / subtype..." style="width:220px" oninput="debounce(loadAircraft,300)()">
      <select id="ac-type" onchange="loadAircraft()"><option value="">All types</option></select>
      <select id="ac-status" onchange="loadAircraft()">
        <option value="">All status</option>
        <option value="active">Active</option>
        <option value="retired">Retired</option>
      </select>
      <label style="font-size:12px;display:flex;align-items:center;gap:4px;cursor:pointer">
        <input type="checkbox" id="ac-review" onchange="loadAircraft()"> Needs review
      </label>
      <div style="flex:1"></div>
      <button class="btn btn-primary" onclick="showAddAircraft()">+ Aircraft</button>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr>
          <th>ICAO24</th><th>Reg</th><th>Type</th><th>Subtype</th><th>Airline</th>
          <th>Status</th><th>Review</th><th>Actions</th>
        </tr></thead>
        <tbody id="ac-body"></tbody>
      </table>
    </div>
    <div id="ac-count" style="margin-top:8px;font-size:11px;color:var(--muted)"></div>
  </div>

  <!-- ── Flights Tab ── -->
  <div id="tab-flights" class="tab-content">
    <div class="controls">
      <input id="fl-icao24" type="text" placeholder="ICAO24" style="width:90px" oninput="debounce(loadFlights,300)()">
      <input id="fl-callsign" type="text" placeholder="Callsign" style="width:100px" oninput="debounce(loadFlights,300)()">
      <input id="fl-dep" type="text" placeholder="Dep" style="width:70px" oninput="debounce(loadFlights,300)()">
      <input id="fl-arr" type="text" placeholder="Arr" style="width:70px" oninput="debounce(loadFlights,300)()">
      <input id="fl-from" type="date" onchange="loadFlights()" style="width:130px">
      <input id="fl-to" type="date" onchange="loadFlights()" style="width:130px">
      <label style="font-size:12px;display:flex;align-items:center;gap:4px;cursor:pointer">
        <input type="checkbox" id="fl-review" onchange="loadFlights()"> Review
      </label>
      <div style="flex:1"></div>
      <button class="btn btn-primary" onclick="showAddFlight()">+ Flight</button>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr>
          <th>Date</th><th>ICAO24</th><th>Reg</th><th>Callsign</th>
          <th>Dep</th><th>Arr</th><th>Dep Time</th><th>Arr Time</th>
          <th>Dur</th><th>Review</th><th>Actions</th>
        </tr></thead>
        <tbody id="fl-body"></tbody>
      </table>
    </div>
    <div class="pagination" id="fl-pagination"></div>
  </div>
</div>

<!-- Add Aircraft Modal -->
<div class="modal-bg" id="modal-ac">
  <div class="modal">
    <h3 id="modal-ac-title">Add Aircraft</h3>
    <label>ICAO24 (hex)</label>
    <input id="m-ac-icao24" maxlength="6" placeholder="3c6752">
    <label>Registration</label>
    <input id="m-ac-reg" maxlength="10" placeholder="D-AIXX">
    <label>Type (ICAO code)</label>
    <input id="m-ac-type" maxlength="10" placeholder="A359">
    <label>Subtype</label>
    <input id="m-ac-subtype" maxlength="50" placeholder="Airbus A350-941">
    <label>Airline IATA</label>
    <input id="m-ac-airline" maxlength="3" placeholder="LH" value="LH">
    <div class="modal-actions">
      <button class="btn btn-muted" onclick="closeModal('modal-ac')">Cancel</button>
      <button class="btn btn-primary" id="modal-ac-save" onclick="saveAircraft()">Add</button>
    </div>
  </div>
</div>

<!-- Add Flight Modal -->
<div class="modal-bg" id="modal-fl">
  <div class="modal">
    <h3>Add Flight</h3>
    <label>ICAO24</label>
    <input id="m-fl-icao24" maxlength="6" placeholder="3c6752">
    <label>Callsign</label>
    <input id="m-fl-callsign" maxlength="10" placeholder="DLH400">
    <label>Departure Airport (ICAO)</label>
    <input id="m-fl-dep" maxlength="4" placeholder="EDDF">
    <label>Arrival Airport (ICAO)</label>
    <input id="m-fl-arr" maxlength="4" placeholder="KJFK">
    <label>First Seen (UTC)</label>
    <input id="m-fl-first" type="datetime-local">
    <label>Last Seen (UTC)</label>
    <input id="m-fl-last" type="datetime-local">
    <div class="modal-actions">
      <button class="btn btn-muted" onclick="closeModal('modal-fl')">Cancel</button>
      <button class="btn btn-primary" onclick="saveFlight()">Add</button>
    </div>
  </div>
</div>

<div id="toast" class="toast" style="opacity:0"></div>

<script>
const PFX = '""" + ADMIN_PATH_PREFIX + """';
const API = '/' + PFX + '/api';
let flPage = 1;

function $(id) { return document.getElementById(id); }

function switchTab(tab) {
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelector('.tab[onclick*="' + tab + '"]').classList.add('active');
  $('tab-' + tab).classList.add('active');
  if (tab === 'flights') loadFlights();
}

function toast(msg, ok) {
  const t = $('toast');
  t.textContent = msg;
  t.className = 'toast ' + (ok ? 'toast-ok' : 'toast-err');
  t.style.opacity = '1';
  setTimeout(() => t.style.opacity = '0', 2500);
}

function debounce(fn, ms) {
  let t;
  return function() { clearTimeout(t); t = setTimeout(fn, ms); };
}

async function api(path, opts) {
  const r = await fetch(API + path, opts);
  const d = await r.json();
  if (!r.ok) { toast(d.error || 'Error', false); return null; }
  return d;
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s || '';
  return d.innerHTML;
}

// ── Aircraft ─────────────────────────────────────────────────

async function loadAircraft() {
  const p = new URLSearchParams();
  const s = $('ac-search').value.trim();
  if (s) p.set('search', s);
  const t = $('ac-type').value;
  if (t) p.set('type', t);
  const st = $('ac-status').value;
  if (st) p.set('status', st);
  if ($('ac-review').checked) p.set('needs_review', 'true');

  const d = await api('/aircraft?' + p);
  if (!d) return;

  // Populate type filter (only once)
  const sel = $('ac-type');
  if (sel.options.length <= 1) {
    const types = [...new Set(d.aircraft.map(a => a.aircraft_type).filter(Boolean))].sort();
    types.forEach(t => { const o = document.createElement('option'); o.value = t; o.textContent = t; sel.appendChild(o); });
  }

  $('ac-count').textContent = d.aircraft.length + ' aircraft';
  $('ac-body').innerHTML = d.aircraft.map(a => {
    const cls = a.needs_review ? ' class="review"' : '';
    const statusBadge = a.is_active
      ? '<span class="badge badge-active">Active</span>'
      : '<span class="badge badge-retired">Retired</span>';
    const reviewBadge = a.needs_review
      ? '<span class="badge badge-review">Review</span>' : '';
    return '<tr' + cls + '>' +
      '<td>' + esc(a.icao24) + '</td>' +
      '<td><span class="editable" contenteditable data-icao="' + esc(a.icao24) + '" data-field="registration" onblur="inlineEditAc(this)">' + esc(a.registration) + '</span></td>' +
      '<td><span class="editable" contenteditable data-icao="' + esc(a.icao24) + '" data-field="aircraft_type" onblur="inlineEditAc(this)">' + esc(a.aircraft_type) + '</span></td>' +
      '<td><span class="editable" contenteditable data-icao="' + esc(a.icao24) + '" data-field="aircraft_subtype" onblur="inlineEditAc(this)">' + esc(a.aircraft_subtype) + '</span></td>' +
      '<td><span class="editable" contenteditable data-icao="' + esc(a.icao24) + '" data-field="airline_iata" onblur="inlineEditAc(this)">' + esc(a.airline_iata) + '</span></td>' +
      '<td>' + statusBadge + '</td>' +
      '<td>' + reviewBadge + '</td>' +
      '<td class="actions">' +
        (a.is_active
          ? '<button class="btn btn-muted btn-sm" onclick="retireAc(\\'' + esc(a.icao24) + '\\')">Retire</button>'
          : '<button class="btn btn-success btn-sm" onclick="reactivateAc(\\'' + esc(a.icao24) + '\\')">Reactivate</button>') +
        '<button class="btn btn-danger btn-sm" onclick="deleteAc(\\'' + esc(a.icao24) + '\\')">Del</button>' +
        (a.needs_review
          ? '<button class="btn btn-muted btn-sm" onclick="clearReviewAc(\\'' + esc(a.icao24) + '\\')">OK</button>'
          : '') +
      '</td></tr>';
  }).join('') || '<tr><td colspan="8" style="text-align:center;color:var(--muted)">No aircraft found</td></tr>';
}

async function inlineEditAc(el) {
  const icao = el.dataset.icao;
  const field = el.dataset.field;
  const val = el.textContent.trim();
  const d = await api('/aircraft/' + icao, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({[field]: val})
  });
  if (d) toast('Updated ' + icao, true);
}

async function retireAc(icao) {
  if (!confirm('Retire ' + icao + '?')) return;
  const d = await api('/aircraft/' + icao + '/retire', { method: 'POST' });
  if (d) { toast('Retired ' + icao, true); loadAircraft(); }
}

async function reactivateAc(icao) {
  const d = await api('/aircraft/' + icao + '/reactivate', { method: 'POST' });
  if (d) { toast('Reactivated ' + icao, true); loadAircraft(); }
}

async function deleteAc(icao) {
  if (!confirm('Delete ' + icao + '? This cannot be undone.')) return;
  const d = await api('/aircraft/' + icao, { method: 'DELETE' });
  if (d) { toast('Deleted ' + icao, true); loadAircraft(); }
}

async function clearReviewAc(icao) {
  const d = await api('/aircraft/' + icao, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({needs_review: false})
  });
  if (d) { toast('Cleared review for ' + icao, true); loadAircraft(); }
}

function showAddAircraft() {
  $('m-ac-icao24').value = '';
  $('m-ac-reg').value = '';
  $('m-ac-type').value = '';
  $('m-ac-subtype').value = '';
  $('m-ac-airline').value = 'LH';
  $('modal-ac').classList.add('show');
}

async function saveAircraft() {
  const body = {
    icao24: $('m-ac-icao24').value,
    registration: $('m-ac-reg').value,
    aircraft_type: $('m-ac-type').value,
    aircraft_subtype: $('m-ac-subtype').value,
    airline_iata: $('m-ac-airline').value
  };
  const d = await api('/aircraft', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  if (d) { toast('Added aircraft', true); closeModal('modal-ac'); loadAircraft(); }
}

// ── Flights ──────────────────────────────────────────────────

async function loadFlights() {
  const p = new URLSearchParams();
  const v = (id) => $(id).value.trim();
  if (v('fl-icao24')) p.set('icao24', v('fl-icao24'));
  if (v('fl-callsign')) p.set('callsign', v('fl-callsign'));
  if (v('fl-dep')) p.set('dep', v('fl-dep'));
  if (v('fl-arr')) p.set('arr', v('fl-arr'));
  if (v('fl-from')) p.set('date_from', v('fl-from'));
  if (v('fl-to')) p.set('date_to', v('fl-to'));
  if ($('fl-review').checked) p.set('needs_review', 'true');
  p.set('page', flPage);

  const d = await api('/flights?' + p);
  if (!d) return;

  $('fl-body').innerHTML = d.flights.map(f => {
    const cls = f.needs_review ? ' class="review"' : '';
    const dur = f.duration_minutes != null ? Math.floor(f.duration_minutes/60) + 'h ' + (f.duration_minutes%60) + 'm' : '\\u2014';
    const depTime = f.first_seen ? f.first_seen.slice(11,16) : '';
    const arrTime = f.last_seen ? f.last_seen.slice(11,16) : '';
    const reviewBadge = f.needs_review ? '<span class="badge badge-review">Review</span>' : '';
    return '<tr' + cls + '>' +
      '<td>' + esc(f.flight_date) + '</td>' +
      '<td>' + esc(f.icao24) + '</td>' +
      '<td>' + esc(f.registration || '') + '</td>' +
      '<td><span class="editable" contenteditable data-id="' + f.id + '" data-field="callsign" onblur="inlineEditFl(this)">' + esc(f.callsign) + '</span></td>' +
      '<td><span class="editable" contenteditable data-id="' + f.id + '" data-field="departure_airport_icao" onblur="inlineEditFl(this)">' + esc(f.departure_airport_icao) + '</span></td>' +
      '<td><span class="editable" contenteditable data-id="' + f.id + '" data-field="arrival_airport_icao" onblur="inlineEditFl(this)">' + esc(f.arrival_airport_icao) + '</span></td>' +
      '<td>' + depTime + '</td>' +
      '<td>' + arrTime + '</td>' +
      '<td>' + dur + '</td>' +
      '<td>' + reviewBadge + '</td>' +
      '<td class="actions">' +
        (f.needs_review ? '<button class="btn btn-muted btn-sm" onclick="clearReviewFl(' + f.id + ')">OK</button>' : '') +
        '<button class="btn btn-danger btn-sm" onclick="deleteFl(' + f.id + ')">Del</button>' +
      '</td></tr>';
  }).join('') || '<tr><td colspan="11" style="text-align:center;color:var(--muted)">No flights found</td></tr>';

  // Pagination
  const pg = $('fl-pagination');
  if (d.pages > 1) {
    let html = '<button class="btn btn-muted btn-sm" onclick="flGo(' + (d.page-1) + ')" ' + (d.page<=1?'disabled':'') + '>&laquo;</button>';
    html += '<span>Page ' + d.page + ' / ' + d.pages + ' (' + d.total + ' flights)</span>';
    html += '<button class="btn btn-muted btn-sm" onclick="flGo(' + (d.page+1) + ')" ' + (d.page>=d.pages?'disabled':'') + '>&raquo;</button>';
    pg.innerHTML = html;
  } else {
    pg.innerHTML = d.total ? '<span>' + d.total + ' flights</span>' : '';
  }
}

function flGo(p) { flPage = Math.max(1, p); loadFlights(); }

async function inlineEditFl(el) {
  const id = el.dataset.id;
  const field = el.dataset.field;
  const val = el.textContent.trim();
  const d = await api('/flights/' + id, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({[field]: val || null})
  });
  if (d) toast('Updated flight #' + id, true);
}

async function clearReviewFl(id) {
  const d = await api('/flights/' + id, {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({needs_review: false})
  });
  if (d) { toast('Cleared review', true); loadFlights(); }
}

async function deleteFl(id) {
  if (!confirm('Delete flight #' + id + '?')) return;
  const d = await api('/flights/' + id, { method: 'DELETE' });
  if (d) { toast('Deleted', true); loadFlights(); }
}

function showAddFlight() {
  $('m-fl-icao24').value = '';
  $('m-fl-callsign').value = '';
  $('m-fl-dep').value = '';
  $('m-fl-arr').value = '';
  $('m-fl-first').value = '';
  $('m-fl-last').value = '';
  $('modal-fl').classList.add('show');
}

async function saveFlight() {
  const first = $('m-fl-first').value;
  const last = $('m-fl-last').value;
  const body = {
    icao24: $('m-fl-icao24').value,
    callsign: $('m-fl-callsign').value,
    departure_airport_icao: $('m-fl-dep').value,
    arrival_airport_icao: $('m-fl-arr').value,
    first_seen: first ? first + ':00Z' : '',
    last_seen: last ? last + ':00Z' : ''
  };
  const d = await api('/flights', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  if (d) { toast('Added flight', true); closeModal('modal-fl'); loadFlights(); }
}

function closeModal(id) { $(id).classList.remove('show'); }

// Close modals on backdrop click
document.querySelectorAll('.modal-bg').forEach(bg => {
  bg.addEventListener('click', e => { if (e.target === bg) bg.classList.remove('show'); });
});

// Init
loadAircraft();
</script>
<footer style="text-align:center;padding:24px 0 8px;font-size:11px;color:var(--muted)">
  <a href="/impressum" style="color:var(--muted);text-decoration:none">Impressum</a>
  <span style="margin:0 6px">&middot;</span>
  <a href="/datenschutz" style="color:var(--muted);text-decoration:none">Datenschutz</a>
</footer>
</body>
</html>"""
    )

    @app.route(f"{_pfx}/")
    def admin_page():
        return render_template_string(_ADMIN_HTML)


# ── Upcoming schedule (Lufthansa FIS observations) ─────────────────

# Canonical fleet types (from the aircraft table) → short Gantt labels.
_CANON_SHORT = {"B748": "748", "A388": "388"}
# Only these airframe types are shown on the schedule.
_SCHEDULE_TYPES = ("B748", "A388")
# Tails the user is most interested in — pinned to the top and highlighted.
_WATCH_TAILS = ("D-ABYN", "D-AIMH")
# German hub airports (all share the Frankfurt timezone) used to anchor each
# leg onto a single Frankfurt-local clock.
_DE_HUBS = {"FRA", "MUC", "DUS", "BER", "HAM", "STR", "CGN", "NUE", "LEJ", "TXL"}


def _iso_dur_min(s):
    """Parse an ISO-8601 duration like 'PT12H40M' / 'PT2H' / 'PT55M' to minutes."""
    if not s or not s.startswith("PT"):
        return None
    h = m = 0
    num = ""
    for ch in s[2:]:
        if ch.isdigit():
            num += ch
        elif ch == "H":
            h, num = int(num or 0), ""
        elif ch == "M":
            m, num = int(num or 0), ""
    return h * 60 + m


_BERLIN = ZoneInfo("Europe/Berlin")

# ICAO (ADS-B) → IATA (FIS), for the LH widebody network, so actual legs match
# the planned ones. Unmapped codes fall back to the ICAO string.
_ICAO_IATA = {
    "EDDF": "FRA", "EDDM": "MUC", "KEWR": "EWR", "KJFK": "JFK", "KLAX": "LAX",
    "KSFO": "SFO", "KIAD": "IAD", "KBOS": "BOS", "KORD": "ORD", "KMIA": "MIA",
    "KIAH": "IAH", "KDEN": "DEN", "KSEA": "SEA", "KDFW": "DFW", "KATL": "ATL",
    "KCLT": "CLT", "CYYZ": "YYZ", "CYVR": "YVR", "RJTT": "HND", "RJAA": "NRT",
    "VIDP": "DEL", "VABB": "BOM", "ZBAA": "PEK", "ZBAD": "PKX", "ZSPD": "PVG",
    "VHHH": "HKG", "WSSS": "SIN", "RKSI": "ICN", "RPLL": "MNL", "VTBS": "BKK",
    "SAEZ": "EZE", "SBGR": "GRU", "SBGL": "GIG", "FAOR": "JNB", "OMDB": "DXB",
    "OTHH": "DOH", "MMMX": "MEX", "SKBO": "BOG", "SEQM": "UIO", "SPJC": "LIM",
    "SCEL": "SCL", "HECA": "CAI", "LTFM": "IST", "OERK": "RUH",
}


def _icao_to_iata(code):
    code = (code or "").strip().upper()
    return _ICAO_IATA.get(code, code)


def _digits(s):
    return "".join(c for c in (s or "") if c.isdigit())


def _berlin_fake_utc(dt):
    """Convert a true-UTC datetime to Frankfurt wall-clock, then re-stamp it as
    UTC — so ADS-B actuals plot on the same fake-UTC axis as the FIS bars."""
    if dt is None:
        return None
    return dt.astimezone(_BERLIN).replace(tzinfo=timezone.utc)


# ── Reassignment stability + booking confidence ───────────────────────
# How often the tail published L days before departure still holds as the
# flight nears, mined from the nightly FIS snapshots. Powers the /book
# confidence chip and the schedule-reliability insights. Small data → always
# carry n and fall back route → type → overall. Validated offline by
# tools/reassignment_stability.py (identical scoring logic).
_STAB_TTL_S = 600
_STAB_MIN_N = 10  # below this a slice is too thin to trust on its own → fall back
_stab_cache = {"ts": 0.0, "data": None}


def _stab_rates1(bucket):
    return {lead: {"p": sum(h) / len(h), "n": len(h)} for lead, h in bucket.items() if h}


def _stab_rates2(bucket):
    out = defaultdict(dict)
    for (key, lead), h in bucket.items():
        if h:
            out[key][lead] = {"p": sum(h) / len(h), "n": len(h)}
    return dict(out)


def _reassignment_stability(conn):
    """{'overall': {lead:{p,n}}, 'type'|'route'|'tail': {key:{lead:{p,n}}}}.

    'final' assignment = the closest-to-departure snapshot per flight; for each
    earlier lead we score whether that lead's tail equals the final tail. Type
    is resolved via the aircraft table (ICAO code), not the raw FIS string.
    Cached for _STAB_TTL_S so the per-request cost stays flat.
    """
    nowts = datetime.now(timezone.utc).timestamp()
    if _stab_cache["data"] is not None and nowts - _stab_cache["ts"] < _STAB_TTL_S:
        return _stab_cache["data"]

    rows = _q(conn, """
        SELECT o.flight_date, o.airline, o.flight_number, o.observed_date,
               btrim(o.registration), a.aircraft_type,
               o.dep_airport_iata, o.arr_airport_iata
        FROM flight_status_observations o
        LEFT JOIN aircraft a ON a.registration = o.registration
        WHERE o.found AND o.registration IS NOT NULL
    """)
    groups = defaultdict(dict)  # (fdate, airline, fnum) -> {lead: snap}
    for fdate, airline, fnum, obs, reg, atype, dep, arr in rows:
        lead = (fdate - obs).days
        if lead < 0:
            continue
        groups[(fdate, airline, fnum)][lead] = {
            "reg": reg, "type": _CANON_SHORT.get(atype, atype) or "?",
            "route": f"{dep or '?'}-{arr or '?'}",
        }

    overall, by_type, by_route, by_tail = (defaultdict(list) for _ in range(4))
    for snaps in groups.values():
        if len(snaps) < 2:
            continue
        fl = min(snaps)
        final = snaps[fl]
        for lead, s in snaps.items():
            if lead <= fl:
                continue
            held = s["reg"] == final["reg"]
            overall[lead].append(held)
            by_type[(final["type"], lead)].append(held)
            by_route[(final["route"], lead)].append(held)
            by_tail[(final["reg"], lead)].append(held)

    data = {
        "overall": _stab_rates1(overall),
        "type": _stab_rates2(by_type),
        "route": _stab_rates2(by_route),
        "tail": _stab_rates2(by_tail),
    }
    _stab_cache.update(ts=nowts, data=data)
    return data


def _hold_probability(stab, lead, route, short_type, min_n=_STAB_MIN_N):
    """Most specific stability cell with enough support: route → type → overall.
    Returns {'p','n','basis','lead'} or None when no data covers this lead."""
    cell = (stab["route"].get(route) or {}).get(lead)
    if cell and cell["n"] >= min_n:
        return {**cell, "basis": "route", "lead": lead}
    cell = (stab["type"].get(short_type) or {}).get(lead)
    if cell and cell["n"] >= min_n:
        return {**cell, "basis": "type", "lead": lead}
    cell = stab["overall"].get(lead)
    if cell:
        return {**cell, "basis": "overall", "lead": lead}
    return None


def _latest_assignments(conn, *, reg=None, dep=None, arr=None,
                        date_from=None, date_to=None, types=_SCHEDULE_TYPES):
    """Latest nightly snapshot per upcoming flight, optionally filtered to a tail
    (its *current* assignment) or a route. The DISTINCT ON collapses to the newest
    snapshot first, then reg/route filter on that — so a flight reassigned away
    from a tail no longer shows under it. Ordered by scheduled departure."""
    inner = ["o.found", "o.registration IS NOT NULL", "o.dep_scheduled IS NOT NULL",
             "o.flight_date >= CURRENT_DATE"]
    params = []
    if date_from:
        inner.append("o.flight_date >= %s"); params.append(date_from)
    if date_to:
        inner.append("o.flight_date <= %s"); params.append(date_to)
    if types:
        inner.append("a.aircraft_type = ANY(%s)"); params.append(list(types))
    outer = []
    if reg:
        outer.append("reg = %s"); params.append(reg)
    if dep:
        outer.append("dep = %s"); params.append(dep)
    if arr:
        outer.append("arr = %s"); params.append(arr)
    outer_sql = (" WHERE " + " AND ".join(outer)) if outer else ""
    sql = f"""
        WITH latest AS (
            SELECT DISTINCT ON (o.flight_date, o.flight_number)
                o.flight_date AS fdate, o.airline, o.flight_number AS fnum,
                btrim(o.registration) AS reg, a.aircraft_type AS atype,
                o.dep_airport_iata AS dep, o.arr_airport_iata AS arr,
                o.dep_scheduled, o.arr_scheduled, o.overall_status
            FROM flight_status_observations o
            LEFT JOIN aircraft a ON a.registration = o.registration
            WHERE {" AND ".join(inner)}
            ORDER BY o.flight_date, o.flight_number, o.observed_date DESC
        )
        SELECT fdate, airline, fnum, reg, atype, dep, arr,
               dep_scheduled, arr_scheduled, overall_status
        FROM latest{outer_sql}
        ORDER BY dep_scheduled
    """
    return _q(conn, sql, params)


@app.route("/api/schedule")
def api_schedule():
    """Per-airframe upcoming schedule from the latest FIS snapshot of each
    (flight_date, flight_number), grouped by tail for a Gantt timeline.

    Only B748/A388 airframes (canonical type from the aircraft table) are
    returned. Bars are laid out on a single Frankfurt-local clock: each leg is
    anchored to its German-hub endpoint and sized by true flight duration, so
    both east- and westbound legs show their real length (no tz compression).
    """
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503

    today = date.today()
    now_fake = datetime.now(_BERLIN).replace(tzinfo=timezone.utc)  # Berlin wall as fake-UTC
    win_start = now_fake - timedelta(hours=24)  # rolling last 24h of history
    try:
        rows = _q(conn, """
            SELECT DISTINCT ON (o.flight_date, o.flight_number)
                o.flight_date, o.airline, o.flight_number, o.registration,
                a.aircraft_type, o.seed_type, o.dep_airport_iata, o.arr_airport_iata,
                o.dep_scheduled, o.arr_scheduled, o.overall_status,
                o.prev_airline, o.prev_flight_number,
                o.raw->'legs'->0->>'flightDuration'
            FROM flight_status_observations o
            JOIN aircraft a ON a.registration = o.registration
            WHERE o.found AND o.registration IS NOT NULL AND o.dep_scheduled IS NOT NULL
              AND o.flight_date >= CURRENT_DATE - 1
              AND a.aircraft_type = ANY(%s)
            ORDER BY o.flight_date, o.flight_number, o.observed_date DESC
        """, (list(_SCHEDULE_TYPES),))
        swap_rows = _q(conn, """
            SELECT flight_date, flight_number
            FROM flight_status_observations
            WHERE found AND registration IS NOT NULL AND flight_date >= CURRENT_DATE - 1
            GROUP BY flight_date, flight_number
            HAVING COUNT(DISTINCT registration) > 1
        """)
        # Actual flights (ADS-B) for the shown fleet over the recent past.
        act_rows = _q(conn, """
            SELECT a.registration, f.callsign, f.departure_airport_icao,
                   f.arrival_airport_icao, f.first_seen, f.last_seen
            FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = ANY(%s)
              AND f.first_seen >= NOW() - INTERVAL '40 hours'
              AND f.departure_airport_icao IS NOT NULL
            ORDER BY a.registration, f.first_seen
        """, (list(_SCHEDULE_TYPES),))
    finally:
        conn.close()

    swapped = {(r[0], r[1]) for r in swap_rows}

    # Actual legs per registration, on the same fake-UTC (Berlin) axis.
    actuals = defaultdict(list)
    for reg, cs, dep_icao, arr_icao, fs, ls in act_rows:
        actuals[reg].append({
            "cs": (cs or "").strip(),
            "dep": _icao_to_iata(dep_icao), "arr": _icao_to_iata(arr_icao),
            "start": _berlin_fake_utc(fs), "end": _berlin_fake_utc(ls), "used": False,
        })

    by_reg = defaultdict(list)
    types = {}
    starts, ends = [], []
    for (fdate, airline, fnum, reg, atype, seed, dep, arr,
         dep_t, arr_t, fis_status, pa, pn, dur_iso) in rows:
        dur = _iso_dur_min(dur_iso)
        # Frankfurt-local clock: anchor to the German endpoint, size by duration.
        if dur and dep in _DE_HUBS:
            start, end = dep_t, dep_t + timedelta(minutes=dur)
        elif dur and arr in _DE_HUBS and arr_t:
            start, end = arr_t - timedelta(minutes=dur), arr_t
        else:
            start, end = dep_t, (arr_t or dep_t)
        if end < win_start:
            continue  # ended before the rolling 24h window
        starts.append(start)
        ends.append(end)
        types[reg] = _CANON_SHORT.get(atype, atype)

        leg = {
            "fl": f"{airline}{fnum}", "num": fnum, "fdate": fdate.isoformat(),
            "dep": dep, "arr": arr,
            "start": start.isoformat(), "end": end.isoformat(),
            "dep_t": dep_t.isoformat(), "arr_t": arr_t.isoformat() if arr_t else None,
            "dur": dur, "seed": seed, "swap": (fdate, fnum) in swapped,
            "lead": (fdate - today).days, "prev": f"{pa}{pn}" if pn else None,
            "status": "planned", "act": None,
        }
        # Plan-vs-actual overlay for legs whose planned departure is in the past.
        if start < now_fake:
            window = timedelta(hours=8)
            cands = [act for act in actuals.get(reg, [])
                     if not act["used"] and abs(act["start"] - start) <= window]
            # prefer an actual whose callsign matches the planned flight number
            # (robust to tactical callsigns falling through to nearest-in-time)
            exact = [act for act in cands if _digits(act["cs"]) == fnum]
            best = (exact[0] if exact
                    else min(cands, key=lambda act: abs(act["start"] - start)) if cands else None)
            if best is None:
                leg["status"] = "missing"
            else:
                best["used"] = True
                delta = round((best["start"] - start).total_seconds() / 60)
                leg["act"] = {
                    "start": best["start"].isoformat(), "end": best["end"].isoformat(),
                    "dep": best["dep"], "arr": best["arr"], "cs": best["cs"], "delta": delta,
                }
                same_num = _digits(best["cs"]) == fnum
                if not best["arr"]:
                    # still airborne (no arrival yet): trust the flight number
                    leg["status"] = "tracked" if same_num else "deviation"
                else:
                    leg["status"] = ("tracked" if (best["dep"] == dep and best["arr"] == arr)
                                     else "deviation")
        by_reg[reg].append(leg)

    # Actual legs with no matching plan (positioning, tactical, or unseeded).
    for reg, acts in actuals.items():
        if reg not in types:
            continue
        for act in acts:
            if act["used"] or act["start"] >= now_fake or act["end"] < win_start:
                continue
            starts.append(act["start"])
            ends.append(act["end"])
            by_reg[reg].append({
                "fl": act["cs"] or "—", "num": None, "fdate": None,
                "dep": act["dep"], "arr": act["arr"],
                "start": act["start"].isoformat(), "end": act["end"].isoformat(),
                "dep_t": act["start"].isoformat(), "arr_t": act["end"].isoformat(),
                "dur": None, "seed": None, "swap": False, "lead": None, "prev": None,
                "status": "extra", "act": None,
            })

    if not starts:
        return jsonify({"airframes": [], "window": None, "now": now_fake.isoformat(),
                        "generated": datetime.now(timezone.utc).isoformat()})

    # win_start is now-24h (set above); window runs through the last planned arrival.
    win_end = max(ends).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    type_order = {"748": 0, "388": 1}
    airframes = [
        {"reg": reg, "type": types[reg], "watch": reg in _WATCH_TAILS,
         "legs": sorted(by_reg[reg], key=lambda x: x["start"])}
        for reg in by_reg
    ]
    airframes.sort(key=lambda a: (not a["watch"], type_order.get(a["type"], 9), a["reg"]))
    return jsonify({
        "airframes": airframes,
        "window": {"start": win_start.isoformat(), "end": win_end.isoformat()},
        "now": now_fake.isoformat(),
        "generated": datetime.now(timezone.utc).isoformat(),
        "swaps": len(swapped),
    })


@app.route("/api/schedule/flight/<airline>/<number>/<fdate>")
def api_schedule_flight(airline, number, fdate):
    """Detail for one planned flight: the latest snapshot's full info plus the
    assignment history (which tail was planned at each nightly snapshot), so a
    reassignment shows what was *originally* planned vs. what's planned now."""
    try:
        fdate_d = date.fromisoformat(fdate)
    except ValueError:
        return jsonify({"error": "bad date"}), 400
    airline = airline.upper()
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503
    try:
        latest = _q(conn, """
            SELECT o.registration, a.aircraft_type, o.dep_airport_iata, o.arr_airport_iata,
                   o.dep_scheduled, o.arr_scheduled, o.overall_status,
                   o.prev_airline, o.prev_flight_number, o.prev_flight_date, o.raw, o.observed_date
            FROM flight_status_observations o
            LEFT JOIN aircraft a ON a.registration = o.registration
            WHERE o.flight_date=%s AND o.airline=%s AND o.flight_number=%s AND o.found
            ORDER BY o.observed_date DESC LIMIT 1
        """, (fdate_d, airline, number))
        hist = _q(conn, """
            SELECT o.observed_date, o.registration, a.aircraft_type, o.overall_status, o.found
            FROM flight_status_observations o
            LEFT JOIN aircraft a ON a.registration = o.registration
            WHERE o.flight_date=%s AND o.airline=%s AND o.flight_number=%s
            ORDER BY o.observed_date
        """, (fdate_d, airline, number))
        stab = _reassignment_stability(conn)
    finally:
        conn.close()

    history = [
        {"observed": d.isoformat(), "reg": reg,
         "type": _CANON_SHORT.get(at, at) if at else None,
         "status": st, "found": found}
        for (d, reg, at, st, found) in hist
    ]
    regs_seq = [h["reg"] for h in history if h["reg"]]
    out = {
        "flight": f"{airline}{number}", "flight_date": fdate,
        "found": bool(latest), "history": history,
        "original_reg": regs_seq[0] if regs_seq else None,
        "reassigned": len(set(regs_seq)) > 1,
    }
    if latest:
        (reg, at, dep, arr, dep_t, arr_t, st, pa, pn, pd, raw, obs_d) = latest[0]
        raw = raw or {}
        _lead = (fdate_d - date.today()).days
        _hold = (_hold_probability(stab, _lead, f"{dep or '?'}-{arr or '?'}",
                                   _CANON_SHORT.get(at, at)) if _lead >= 0 else None)
        leg = (raw.get("legs") or [{}])[0]
        depj, arrj = leg.get("departure") or {}, leg.get("arrival") or {}
        ac = raw.get("aircraftInfo") or {}
        cs = [(m.get("marketingFlightAirlineIndicator") or "") + (m.get("marketingFlightNumber") or "")
              for m in (leg.get("marketingFlightNumbers") or [])]
        out.update({
            "current_reg": reg, "current_type": _CANON_SHORT.get(at, at) if at else None,
            "dep_iata": dep, "arr_iata": arr,
            "dep_name": depj.get("departureAirportName"), "arr_name": arrj.get("arrivalAirportName"),
            "dep_sched": dep_t.isoformat() if dep_t else None,
            "arr_sched": arr_t.isoformat() if arr_t else None,
            "dep_term": depj.get("departureTerminal"), "dep_gate": depj.get("departureGate"),
            "arr_term": arrj.get("arrivalTerminal"), "arr_gate": arrj.get("arrivalGate"),
            "duration": leg.get("flightDuration"), "status": st,
            "codeshares": [c for c in cs if c],
            "prev": f"{pa}{pn}" if pn else None,
            "prev_date": pd.isoformat() if pd else None,
            "observed": obs_d.isoformat(),
            "lead": _lead, "hold": _hold,
        })
    return jsonify(out)


_BOOK_SWAP_SQL = """
    SELECT flight_date, flight_number
    FROM flight_status_observations
    WHERE found AND registration IS NOT NULL AND flight_date >= CURRENT_DATE
    GROUP BY flight_date, flight_number
    HAVING COUNT(DISTINCT registration) > 1
"""


@app.route("/api/book")
def api_book():
    """Upcoming flights for booking — tail-first (?reg=) or route-first
    (?dep=&arr=) — each with the current published assignment and a measured
    hold-probability (how often that tail holds to departure)."""
    reg = (request.args.get("reg") or "").strip().upper() or None
    dep = (request.args.get("dep") or "").strip().upper() or None
    arr = (request.args.get("arr") or "").strip().upper() or None
    if not reg and not dep and not arr:
        return jsonify({"error": "provide ?reg= (tail) or ?dep=&arr= (route)"}), 400
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503
    try:
        rows = _latest_assignments(conn, reg=reg, dep=dep, arr=arr)
        swapped = {(r[0], r[1]) for r in _q(conn, _BOOK_SWAP_SQL)}
        stab = _reassignment_stability(conn)
    finally:
        conn.close()

    today = date.today()
    flights = []
    for (fdate, airline, fnum, r_reg, atype, d, a, dep_t, arr_t, status) in rows:
        short = _CANON_SHORT.get(atype, atype)
        lead = (fdate - today).days
        flights.append({
            "flight": f"{airline}{fnum}", "number": fnum, "flight_date": fdate.isoformat(),
            "dep": d, "arr": a,
            "dep_sched": dep_t.isoformat() if dep_t else None,
            "arr_sched": arr_t.isoformat() if arr_t else None,
            "reg": r_reg, "type": short, "watch": r_reg in _WATCH_TAILS,
            "lead": lead, "status": status,
            "reassigned": (fdate, fnum) in swapped,
            "hold": _hold_probability(stab, lead, f"{d or '?'}-{a or '?'}", short),
        })
    return jsonify({
        "mode": "tail" if reg else "route",
        "query": {"reg": reg, "dep": dep, "arr": arr},
        "flights": flights,
        "horizon": flights[-1]["flight_date"] if flights else None,
        "generated": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/insights")
def api_insights():
    """Descriptive fleet analytics for one aircraft type (optionally one tail):
    route frequency, rotation transitions, per-airframe profiles, and — for types
    we collect schedule data on — reliability. Purely backward-looking; no
    prediction. Drives the parameterised /insights page."""
    atype = (request.args.get("type") or "B748").strip().upper()
    reg = (request.args.get("reg") or "").strip().upper() or None
    short = _CANON_SHORT.get(atype, atype)

    scope = "a.aircraft_type = %s AND NOT f.needs_review"
    sp = [atype]
    if reg:
        scope += " AND btrim(a.registration) = %s"
        sp.append(reg)
    # exclude unresolved/loop legs from the route-shaped queries
    clean = (" AND f.departure_airport_icao IS NOT NULL AND f.arrival_airport_icao IS NOT NULL"
             " AND f.departure_airport_icao <> f.arrival_airport_icao"
             " AND f.departure_airport_icao <> 'UNKN' AND f.arrival_airport_icao <> 'UNKN'")

    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503
    try:
        meta = _q(conn, f"""
            SELECT COUNT(*), COUNT(DISTINCT a.registration),
                   MIN(f.flight_date), MAX(f.flight_date)
            FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
            WHERE {scope}
        """, sp)[0]

        routes = _q(conn, f"""
            SELECT f.departure_airport_icao || '-' || f.arrival_airport_icao AS route,
                   COUNT(*) AS n,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY f.duration_minutes) AS med
            FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
            WHERE {scope}{clean}
            GROUP BY route ORDER BY n DESC LIMIT 25
        """, sp)

        airframes = _q(conn, f"""
            SELECT btrim(a.registration), COUNT(*),
                   ROUND(SUM(f.duration_minutes) / 60.0, 1),
                   MIN(f.flight_date), MAX(f.flight_date)
            FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
            WHERE {scope}
            GROUP BY 1 ORDER BY 2 DESC
        """, sp)

        grounding = _q(conn, f"""
            WITH g AS (
              SELECT btrim(a.registration) AS reg,
                     f.flight_date - LAG(f.flight_date)
                       OVER (PARTITION BY a.registration ORDER BY f.flight_date) AS gap
              FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
              WHERE {scope}
            )
            SELECT reg, MAX(gap) FROM g GROUP BY reg
        """, sp)

        rotation = _q(conn, f"""
            WITH ordered AS (
              SELECT f.departure_airport_icao || '-' || f.arrival_airport_icao AS route,
                     LEAD(f.departure_airport_icao || '-' || f.arrival_airport_icao)
                       OVER (PARTITION BY a.registration ORDER BY f.first_seen) AS nxt
              FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
              WHERE {scope}{clean}
            )
            SELECT route, nxt, COUNT(*) AS n FROM ordered
            WHERE nxt IS NOT NULL GROUP BY route, nxt ORDER BY n DESC LIMIT 60
        """, sp)

        # Schedule reliability (FIS) — only meaningful for collected types.
        ontime = _q(conn, """
            WITH latest AS (
                SELECT DISTINCT ON (o.flight_date, o.flight_number) o.overall_status
                FROM flight_status_observations o
                JOIN aircraft a ON a.registration = o.registration
                WHERE o.found AND a.aircraft_type = %s
                ORDER BY o.flight_date, o.flight_number, o.observed_date DESC
            )
            SELECT COALESCE(overall_status, 'UNKNOWN'), COUNT(*)
            FROM latest GROUP BY 1 ORDER BY 2 DESC
        """, [atype])
        stab = _reassignment_stability(conn)
    finally:
        conn.close()

    ground = {r[0]: r[1] for r in grounding}
    reliability = None
    if ontime or stab["type"].get(short):
        reliability = {
            "ontime": [{"status": s, "n": n} for s, n in ontime],
            "hold_by_lead": stab["type"].get(short) or stab["overall"],
            "churn_by_route": stab["route"],
        }

    return jsonify({
        "type": atype, "short": short, "reg": reg,
        "meta": {"flights": meta[0], "tails": meta[1],
                 "first": meta[2].isoformat() if meta[2] else None,
                 "last": meta[3].isoformat() if meta[3] else None},
        "routes": [{"route": r[0], "n": r[1], "median_min": int(r[2]) if r[2] is not None else None}
                   for r in routes],
        "airframes": [{"reg": r[0], "legs": r[1], "hours": float(r[2]) if r[2] is not None else 0.0,
                       "first": r[3].isoformat() if r[3] else None,
                       "last": r[4].isoformat() if r[4] else None,
                       "watch": r[0] in _WATCH_TAILS, "max_ground_days": ground.get(r[0])}
                      for r in airframes],
        "rotation": [{"from": r[0], "to": r[1], "n": r[2]} for r in rotation],
        "reliability": reliability,
        "generated": datetime.now(timezone.utc).isoformat(),
    })


_SCHEDULE_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Upcoming Schedule | LH Fleet</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>
:root {
  /* chassis — light cream Teenage-Engineering faceplate */
  --bg:#f3efe6; --surface:#ffffff; --surface2:#f1ebdd; --border:#e6dfce; --line:#d3c9b4;
  --text:#36342c; --text-bright:#1a1812; --muted:#9d9482;
  /* pastel "ink" accents */
  --accent:#6aa0d8; --green:#5cb487; --amber:#d3a23c; --red:#e07b6b; --purple:#a487d6; --cyan:#46b2a8;
  /* pale pastel fills */
  --accent-dim:#dcebf9; --green-dim:#d7f0e2; --amber-dim:#f5ead0; --red-dim:#f8ddd6; --purple-dim:#e8defa;
  --radius:0; --radius-sm:0;
  --mono:'Space Mono','SFMono-Regular',ui-monospace,Menlo,monospace;
  --sans:'Space Grotesk',-apple-system,'Segoe UI',system-ui,sans-serif;
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-size:14px; line-height:1.5;
  font-family:var(--sans); -webkit-font-smoothing:antialiased; }
.container { width:96vw; max-width:2000px; margin:0 auto; padding:0 18px 48px; }

/* header / device label */
.header { padding:22px 0 12px; display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }
.brand { display:flex; align-items:center; gap:11px; }
.led { width:11px; height:11px; border-radius:0; background:var(--green);
  box-shadow:0 0 0 4px var(--green-dim); animation:pulse 2.6s ease-in-out infinite; flex-shrink:0; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
.header h1 { font-size:18px; font-weight:700; color:var(--text-bright); letter-spacing:-0.3px; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--accent); }
.model { font-family:var(--mono); font-size:9.5px; letter-spacing:1.5px; color:var(--muted); text-transform:uppercase; margin-top:3px; }
.nav { display:flex; gap:7px; flex-wrap:wrap; }
.nav-link { font-family:var(--mono); font-size:11px; letter-spacing:.4px; text-transform:uppercase;
  color:var(--text-bright); text-decoration:none; background:var(--surface); border:1.5px solid var(--line);
  border-radius:0; padding:6px 13px; transition:transform .08s ease, background .15s, border-color .15s; }
.nav-link:hover { background:var(--accent-dim); border-color:var(--accent); transform:translateY(-1px); }

.grille { height:14px; margin:6px 0 18px; border-radius:0;
  background-image:repeating-linear-gradient(90deg, var(--line) 0 2px, transparent 2px 12px); }

.meta { font-family:var(--mono); font-size:11px; color:var(--muted); margin-bottom:14px; line-height:1.6; }
.meta b { color:var(--text); font-weight:700; }

.controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin-bottom:16px; }
.controls input { background:var(--surface); border:1.5px solid var(--border); border-radius:0;
  color:var(--text-bright); padding:8px 14px; font-size:13px; width:300px; font-family:var(--mono); }
.controls input:focus { outline:none; border-color:var(--accent); }
.controls input::placeholder { color:var(--muted); }
.legend { display:flex; gap:13px; flex-wrap:wrap; font-family:var(--mono); font-size:10px; color:var(--muted);
  text-transform:uppercase; letter-spacing:.3px; align-items:center; }
.legend .sw { display:inline-block; width:12px; height:12px; border-radius:0; vertical-align:middle; margin-right:5px; }

/* gantt */
.gantt { overflow-x:auto; border:1.5px solid var(--border); border-radius:0; background:var(--surface); padding:14px 14px 6px; }
.gantt-inner { min-width:820px; }
.gantt-axis-row { display:flex; align-items:center; margin-bottom:10px; }
.gantt-axis { flex:1; position:relative; height:13px; }
.gantt-axis .gantt-day { position:absolute; font-family:var(--mono); font-size:10px; color:var(--muted);
  text-transform:uppercase; letter-spacing:.4px; padding-left:7px; border-left:1.5px solid var(--line); white-space:nowrap; }
.gantt-rows { position:relative; }
.gantt-grid { position:absolute; inset:0; pointer-events:none; z-index:4; }
.grid-line { position:absolute; top:0; bottom:0; width:1px; background:var(--line); opacity:.7; }
.gantt-row { display:flex; align-items:center; margin-bottom:4px; height:24px; }
.gantt-row.dim { opacity:.22; }
.gantt-row.watch { background:var(--amber-dim); border-radius:0; }
.gantt-row.watch .gantt-label { color:var(--text-bright); }
.gantt-label .star { color:var(--amber); font-size:10px; margin-right:1px; }
.gantt-label { width:104px; flex-shrink:0; font-family:var(--mono); font-size:11px; font-weight:700;
  color:var(--text-bright); padding-right:8px; display:flex; align-items:center; gap:5px; }
.tbadge { font-family:var(--mono); font-size:9px; font-weight:700; padding:1px 5px; border-radius:0; color:var(--text-bright); }
.t748 .tbadge, .tbadge.t748 { background:var(--accent); }
.t388 .tbadge, .tbadge.t388 { background:var(--green); }
.t359 .tbadge, .tbadge.t359 { background:var(--purple); }
.tbadge.tother { background:var(--surface2); color:var(--muted); }
.gantt-track { flex:1; position:relative; height:22px; background:var(--surface2); border-radius:0; overflow:hidden; }
.gantt-flight { position:absolute; top:1px; height:20px; border-radius:0; overflow:hidden; display:flex;
  align-items:center; cursor:pointer; opacity:.95; transition:opacity .15s, outline .15s; }
.gantt-flight:hover { opacity:1; z-index:3; }
.gantt-flight.dim { opacity:.12; }
.gantt-flight .lbl { font-family:var(--mono); font-size:10px; font-weight:700; white-space:nowrap; padding:0 6px; color:var(--text-bright); }
.gantt-flight.t748 { background:var(--accent); }
.gantt-flight.t388 { background:var(--green); }
.gantt-flight.t359 { background:var(--purple); }
.gantt-flight.tother { background:var(--surface2); }
.gantt-flight.tother .lbl { color:var(--muted); }
.gantt-flight.swap { outline:2px solid var(--amber); outline-offset:-2px; }
/* plan-vs-actual status borders (left of "now") */
.gantt-flight.ob-tracked   { outline:2px solid var(--text-bright); outline-offset:-2px; }
.gantt-flight.ob-deviation { outline:2px dashed var(--red); outline-offset:-2px; }
.gantt-flight.ob-missing   { outline:2px dashed var(--muted); outline-offset:-2px; }
.gantt-flight.ob-extra     { outline:2px solid var(--cyan); outline-offset:-2px; }
.gantt-flight.ghost { opacity:.34; }
.gantt-flight.ghost .lbl { opacity:.7; }
.now-line { position:absolute; top:0; bottom:0; width:2px; background:var(--amber); opacity:.95; z-index:6; pointer-events:none; }
.now-line::after { content:''; position:absolute; top:-3px; left:-3px; width:8px; height:8px; border-radius:0; background:var(--amber); }
.empty { color:var(--muted); padding:30px; text-align:center; font-family:var(--mono); font-size:12px; }
footer { text-align:center; padding:26px 0 10px; font-family:var(--mono); font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }
footer a { color:var(--muted); text-decoration:none; }
footer a:hover { color:var(--text); }

/* flight detail modal */
.modal-bg { display:none; position:fixed; inset:0; background:rgba(36,33,27,0.5); z-index:100; justify-content:center; align-items:center; padding:16px; }
.modal-bg.show { display:flex; }
.modal { background:var(--surface); border:1.5px solid var(--border); border-radius:0; padding:22px; width:100%; max-width:460px; max-height:88vh; overflow-y:auto; position:relative; }
.modal h3 { font-size:15px; color:var(--text-bright); margin-bottom:3px; padding-right:24px; }
.modal .sub { font-size:12px; color:var(--muted); margin-bottom:16px; }
.modal .close { position:absolute; top:14px; right:18px; cursor:pointer; color:var(--muted); font-size:22px; line-height:1; border:none; background:none; }
.modal .close:hover { color:var(--text-bright); }
.reassign-banner { background:var(--amber-dim); border:1.5px solid var(--amber); color:#9a6f1e; border-radius:0; padding:9px 11px; font-size:12px; margin-bottom:16px; }
.conf-chip { display:flex; align-items:center; gap:12px; border:1.5px solid var(--border); border-radius:0; padding:10px 12px; margin-bottom:16px; }
.conf-chip .cp { font-family:var(--mono); font-size:22px; font-weight:700; line-height:1; flex-shrink:0; }
.conf-chip .ct { font-size:12px; color:var(--text-bright); line-height:1.45; }
.conf-chip .cn { color:var(--muted); font-size:11px; font-family:var(--mono); }
.conf-chip.cg { background:var(--green-dim); border-color:var(--green); } .conf-chip.cg .cp { color:#3d7d5c; }
.conf-chip.ca { background:var(--amber-dim); border-color:var(--amber); } .conf-chip.ca .cp { color:#9a6f1e; }
.conf-chip.cr { background:var(--red-dim); border-color:var(--red); } .conf-chip.cr .cp { color:#b4503f; }
.det-grid { display:grid; grid-template-columns:auto 1fr; gap:7px 16px; font-size:12px; margin-bottom:18px; }
.det-grid .k { color:var(--muted); white-space:nowrap; font-family:var(--mono); font-size:11px; text-transform:uppercase; letter-spacing:.3px; }
.det-grid .v { color:var(--text-bright); }
.hist-title { font-family:var(--mono); font-size:10px; text-transform:uppercase; letter-spacing:1px; color:var(--muted); margin:0 0 10px; }
.hist-row { display:flex; align-items:center; gap:10px; font-size:12px; padding:6px 0; border-bottom:1.5px solid var(--border); }
.hist-row:last-child { border-bottom:none; }
.hist-row .obs { width:96px; color:var(--muted); flex-shrink:0; font-family:var(--mono); font-size:11px; }
.hist-row .reg { font-family:var(--mono); font-weight:700; color:var(--text-bright); min-width:64px; }
.hist-row.changed .reg { color:var(--amber); }
.hist-row .tag { font-size:10px; color:var(--muted); }
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <div class="brand">
      <span class="led"></span>
      <div>
        <h1>Upcoming <span>Schedule</span></h1>
        <div class="model">SCHED &middot; Airframe Rotation</div>
      </div>
    </div>
    <nav class="nav">
      <a class="nav-link" href="/book">Book</a>
      <a class="nav-link" href="/analysis-747">747-8</a>
      <a class="nav-link" href="/analysis">A380</a>
      <a class="nav-link" href="/fleet">Fleet DB</a>
      <a class="nav-link" href="/">&larr; Monitor</a>
    </nav>
  </div>
  <div class="grille"></div>
  <div class="meta" id="meta">Loading planned airframe rotations…</div>
  <div class="controls">
    <input id="filter" type="text" placeholder="Filter: tail, airport or flight (e.g. HND, D-ABYN, 716)">
    <div class="legend">
      <span><span class="sw" style="background:var(--accent)"></span>747-8</span>
      <span><span class="sw" style="background:var(--green)"></span>A380</span>
      <span><span class="star" style="color:var(--amber)">&#9733;</span>watched</span>
      <span style="opacity:0.4">|</span>
      <span><span class="sw" style="background:var(--surface2);outline:2px solid var(--text-bright);outline-offset:-2px"></span>tracked</span>
      <span><span class="sw" style="background:var(--surface2);outline:2px solid var(--cyan);outline-offset:-2px"></span>actual (unplanned)</span>
      <span><span class="sw" style="background:var(--surface2);outline:2px dashed var(--red);outline-offset:-2px"></span>deviation</span>
      <span><span class="sw" style="background:var(--surface2);outline:2px dashed var(--muted);outline-offset:-2px"></span>not tracked</span>
      <span><span class="sw" style="background:var(--surface2);outline:2px solid var(--amber);outline-offset:-2px"></span>reassigned</span>
    </div>
  </div>
  <div class="gantt"><div class="gantt-inner" id="gantt"><div class="empty">Loading…</div></div></div>
  <div class="meta" style="margin-top:10px">Times in Frankfurt local; bar length = real flight duration. The amber <b>now</b> line splits the chart: <b>left</b> = what each tail is actually doing (ADS-B) overlaid on the plan &mdash; <span style="color:var(--green)">green</span> flew the planned route, <span style="color:var(--red)">red</span> deviation, grey not-tracked-yet, <span style="color:var(--cyan)">cyan</span> flew something unplanned; <b>right</b> = the plan. Click a leg for details &amp; history.</div>
</div>
<div class="modal-bg" id="fl-modal"><div class="modal" id="fl-modal-body"></div></div>
<footer>
  <a href="/impressum">Impressum</a> <span style="margin:0 6px">&middot;</span> <a href="/datenschutz">Datenschutz</a>
</footer>
<script>
const $ = id => document.getElementById(id);
function fmt(iso){ if(!iso) return '?'; const d=new Date(iso);
  return d.toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
function tcls(t){ return ['748','388','359'].includes(t) ? 't'+t : 'tother'; }

async function init(){
  let d;
  try { d = await (await fetch('/api/schedule')).json(); }
  catch(e){ $('gantt').innerHTML='<div class="empty">Failed to load schedule.</div>'; return; }
  if(d.error){ $('gantt').innerHTML='<div class="empty">'+d.error+'</div>'; return; }
  if(!d.airframes || !d.airframes.length){ $('gantt').innerHTML='<div class="empty">No upcoming schedule data yet — the nightly collector has not populated future flights.</div>'; $('meta').textContent=''; return; }

  const t0=new Date(d.window.start).getTime(), t1=new Date(d.window.end).getTime();
  const range=t1-t0, dayMs=86400000;
  const frac = ms => (ms - t0)/range;                        // 0..1 across the time area
  const oLeft = f => 'calc(104px + (100% - 104px) * '+f+')'; // overlay coord (rows incl. label)
  const dayName = ms => new Date(ms).toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'});

  // midnight ticks (fake-UTC midnights are epoch multiples of a day)
  const ticks=[]; for(let m=Math.ceil(t0/dayMs)*dayMs; m<t1; m+=dayMs) ticks.push(m);

  // axis: day labels at the left edge + each midnight
  let html='<div class="gantt-axis-row"><div class="gantt-label"></div><div class="gantt-axis">';
  html+='<span class="gantt-day" style="left:0;border-left:none">'+dayName(t0)+'</span>';
  ticks.forEach(m=>{ html+='<span class="gantt-day" style="left:'+(frac(m)*100)+'%">'+dayName(m)+'</span>'; });
  html+='</div></div>';

  // one overlay across all rows: midnight gridlines + the single now line
  let overlay='<div class="gantt-grid">';
  ticks.forEach(m=>{ overlay+='<div class="grid-line" style="left:'+oLeft(frac(m))+'"></div>'; });
  if(d.now!=null){ const nf=frac(new Date(d.now).getTime());
    if(nf>=0&&nf<=1) overlay+='<div class="now-line" style="left:'+oLeft(nf)+'"></div>'; }
  overlay+='</div>';
  html+='<div class="gantt-rows">'+overlay;

  d.airframes.forEach(a=>{
    const tc=tcls(a.type);
    html+='<div class="gantt-row'+(a.watch?' watch':'')+'" data-reg="'+a.reg+'" data-dests="'+a.legs.map(l=>l.dep+' '+l.arr).join(' ')+'" data-fls="'+a.legs.map(l=>l.fl).join(' ')+'">';
    html+='<div class="gantt-label">'+(a.watch?'<span class="star">\\u2605</span>':'')+a.reg+'<span class="tbadge '+tc+'">'+a.type+'</span></div>';
    html+='<div class="gantt-track">';
    a.legs.forEach(l=>{
      const s=new Date(l.start).getTime(), e=new Date(l.end).getTime();
      // clip to the visible window: keep the true end position, cut the start at the edge
      const left=Math.max(0,(s-t0)/range*100), right=Math.min(100,(e-t0)/range*100);
      const width=Math.max(0.5,right-left);
      const dur=l.dur?(Math.floor(l.dur/60)+'h'+String(l.dur%60).padStart(2,'0')):'';
      const st=l.status||'planned';
      const obCls = st==='tracked'?' ob-tracked' : st==='deviation'?' ob-deviation'
        : st==='missing'?' ob-missing' : st==='extra'?' ob-extra' : (l.swap?' swap':'');
      const ghost = (st==='deviation'||st==='missing')?' ghost':'';
      let title;
      if(st==='extra'){
        title=(l.fl||'actual')+'  '+l.dep+' \\u2192 '+l.arr+'\\n\\u2713 tracked (actual) \\u00b7 no matching plan'
          +'\\n'+fmt(l.start)+' \\u2192 '+fmt(l.end);
      } else {
        title=l.fl+'  '+l.dep+' \\u2192 '+l.arr
          +'\\nDep '+fmt(l.dep_t)+' ('+l.dep+')'+(l.arr_t?'\\nArr '+fmt(l.arr_t)+' ('+l.arr+')':'')
          +(dur?'\\nFlight '+dur:'')+'  \\u00b7  '+a.type
          +(l.prev?'\\nprev: '+l.prev:'')+'\\nlead: +'+l.lead+'d'+(l.swap?'  \\u00b7  \\u26a0 reassigned':'');
        if((st==='tracked'||st==='deviation') && l.act){
          const dm=l.act.delta, ds=(dm>0?'+':'')+dm+'m';
          title+='\\n\\u2713 tracked: '+l.act.dep+'\\u2192'+l.act.arr+(l.act.cs?' ('+l.act.cs+')':'')+'  ['+ds+' vs plan]';
        } else if(st==='missing'){ title+='\\n\\u2014 no ADS-B track found yet'; }
      }
      const lbl = st==='extra' ? (l.dep+'\\u2192'+l.arr) : (l.fl.replace('LH','')+' '+l.dep+'\\u2192'+l.arr);
      html+='<div class="gantt-flight '+tc+obCls+ghost+'" style="left:'+left+'%;width:'+width+'%"'
        +' data-dest="'+l.dep+' '+l.arr+'" data-fl="'+(l.fl||'')+'" data-num="'+(l.num||'')+'" data-fdate="'+(l.fdate||'')+'"'
        +' title="'+title.replace(/"/g,'&quot;')+'">'
        +'<span class="lbl">'+lbl+'</span></div>';
      // deviation: also draw the actual route the tail really flew
      if(st==='deviation' && l.act){
        const as=new Date(l.act.start).getTime(), ae=new Date(l.act.end).getTime();
        const al=Math.max(0,(as-t0)/range*100), ar=Math.min(100,(ae-t0)/range*100);
        const aw=Math.max(0.5,ar-al);
        const at=a.reg+' actually flew\\n'+l.act.dep+' \\u2192 '+l.act.arr+(l.act.cs?' ('+l.act.cs+')':'')
          +'\\n'+fmt(l.act.start)+' \\u2192 '+fmt(l.act.end)+'\\n(planned '+l.fl+' '+l.dep+'\\u2192'+l.arr+')';
        html+='<div class="gantt-flight '+tc+' ob-extra" style="left:'+al+'%;width:'+aw+'%"'
          +' data-dest="'+l.act.dep+' '+l.act.arr+'" title="'+at.replace(/"/g,'&quot;')+'">'
          +'<span class="lbl">'+l.act.dep+'\\u2192'+l.act.arr+'</span></div>';
      }
    });
    html+='</div></div>';
  });
  html+='</div>';
  $('gantt').innerHTML=html;
  const sw=d.swaps?(' \\u00b7 '+d.swaps+' reassignment'+(d.swaps>1?'s':'')):'';
  $('meta').textContent=d.airframes.length+' airframes \\u00b7 last 24h + plan \\u00b7 updated '
    +new Date(d.generated).toLocaleString('en-GB',{timeZone:'UTC',hour12:false})+' UTC'+sw;
}

$('filter').addEventListener('input', e=>{
  const q=e.target.value.trim().toUpperCase();
  document.querySelectorAll('.gantt-row').forEach(row=>{
    if(!q){ row.classList.remove('dim'); row.querySelectorAll('.gantt-flight').forEach(f=>f.classList.remove('dim')); return; }
    const regMatch=(row.dataset.reg||'').toUpperCase().includes(q);
    let any=regMatch;
    row.querySelectorAll('.gantt-flight').forEach(f=>{
      const hit=regMatch || (f.dataset.dest||'').toUpperCase().includes(q) || (f.dataset.fl||'').toUpperCase().includes(q);
      f.classList.toggle('dim', !hit); if(hit) any=true;
    });
    row.classList.toggle('dim', !any);
  });
});

/* ── Flight detail modal ──────────────────────────────── */
function row(k,v){ return '<span class="k">'+k+'</span><span class="v">'+v+'</span>'; }
function confChip(hold, reg){
  if(!hold) return '';
  const p=Math.round(hold.p*100);
  const cls=p>=85?'cg':(p>=60?'ca':'cr');
  const label=p>=85?'likely to hold':(p>=60?'could still change':'often changes');
  const basis=hold.basis==='route'?'this route':(hold.basis==='type'?'this aircraft type':'all tracked flights');
  return '<div class="conf-chip '+cls+'"><span class="cp">'+p+'%</span><span class="ct">'
    +'chance it stays <b>'+(reg||'this tail')+'</b> &middot; '+label
    +'<br><span class="cn">'+hold.lead+'d before departure &middot; based on '+basis+' (n='+hold.n+')</span>'
    +'</span></div>';
}
function fmtD(iso){ if(!iso) return ''; return new Date(iso+'T00:00:00Z').toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'}); }
function isoDur(s){ if(!s) return ''; const m=s.match(/PT(?:(\\d+)H)?(?:(\\d+)M)?/); if(!m) return ''; return (m[1]?m[1]+'h':'')+(m[2]?String(m[2]).padStart(2,'0')+'m':''); }
function closeFl(){ $('fl-modal').classList.remove('show'); }
function openFlight(num,fdate){
  const b=$('fl-modal-body'); b.innerHTML='<div class="empty">Loading\\u2026</div>'; $('fl-modal').classList.add('show');
  fetch('/api/schedule/flight/LH/'+num+'/'+fdate).then(r=>r.json()).then(renderFlight)
    .catch(()=>{ b.innerHTML='<button class="close" onclick="closeFl()">\\u00d7</button><div class="empty">Failed to load.</div>'; });
}
function renderFlight(d){
  const b=$('fl-modal-body');
  let h='<button class="close" onclick="closeFl()">\\u00d7</button>';
  if(d.error){ b.innerHTML=h+'<div class="empty">'+d.error+'</div>'; return; }
  h+='<h3>'+d.flight+(d.dep_iata?'  \\u00b7  '+d.dep_iata+'\\u2192'+d.arr_iata:'')+'</h3>';
  h+='<div class="sub">'+[d.dep_name,d.arr_name].filter(Boolean).join(' \\u2192 ')+'  \\u00b7  '+fmtD(d.flight_date)+'</div>';
  if(d.reassigned){ h+='<div class="reassign-banner">\\u26a0 Reassigned \\u2014 originally <b>'+(d.original_reg||'?')+'</b>, now <b>'+(d.current_reg||'?')+'</b></div>'; }
  h+=confChip(d.hold, d.current_reg);
  if(d.found){
    const dur=isoDur(d.duration);
    h+='<div class="det-grid">';
    h+=row('Aircraft',(d.current_reg||'?')+(d.current_type?' \\u00b7 '+d.current_type:''));
    h+=row('Departure',fmt(d.dep_sched)+(d.dep_term?' \\u00b7 T'+d.dep_term:'')+(d.dep_gate?' \\u00b7 Gate '+d.dep_gate:''));
    h+=row('Arrival',fmt(d.arr_sched)+(d.arr_term?' \\u00b7 T'+d.arr_term:'')+(d.arr_gate?' \\u00b7 Gate '+d.arr_gate:''));
    if(dur) h+=row('Flight time',dur);
    if(d.status) h+=row('Status',d.status);
    if(d.codeshares&&d.codeshares.length) h+=row('Codeshare',d.codeshares.join(', '));
    if(d.prev) h+=row('Previous leg',d.prev+(d.prev_date?' ('+fmtD(d.prev_date)+')':''));
    h+='</div>';
  } else { h+='<div class="sub">No current assignment for this date.</div>'; }
  if(d.history&&d.history.length){
    h+='<div class="hist-title">Assignment history</div>';
    let pr=null;
    d.history.forEach((x,i)=>{
      const ch = pr!==null && x.reg!==pr;
      const tags=[]; if(i===0) tags.push('originally planned'); if(i===d.history.length-1) tags.push('current');
      h+='<div class="hist-row'+(ch?' changed':'')+'"><span class="obs">'+fmtD(x.observed)+'</span>'
        +'<span class="reg">'+(x.reg||'\\u2014')+'</span><span class="tag">'
        +[x.type,tags.join(' \\u00b7 ')].filter(Boolean).join('  \\u00b7  ')+'</span></div>';
      pr=x.reg;
    });
  }
  b.innerHTML=h;
}
$('gantt').addEventListener('click', e=>{ const f=e.target.closest('.gantt-flight'); if(f&&f.dataset.num) openFlight(f.dataset.num, f.dataset.fdate); });
$('fl-modal').addEventListener('click', e=>{ if(e.target.id==='fl-modal') closeFl(); });
document.addEventListener('keydown', e=>{ if(e.key==='Escape') closeFl(); });

init();
</script>
</body>
</html>"""


@app.route("/schedule")
def schedule():
    return render_template_string(_SCHEDULE_HTML)


_BOOK_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Catch a Tail | LH Fleet</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg:#f3efe6; --surface:#ffffff; --surface2:#f1ebdd; --border:#e6dfce; --line:#d3c9b4;
  --text:#36342c; --text-bright:#1a1812; --muted:#9d9482;
  --accent:#6aa0d8; --green:#5cb487; --amber:#d3a23c; --red:#e07b6b; --purple:#a487d6; --cyan:#46b2a8;
  --accent-dim:#dcebf9; --green-dim:#d7f0e2; --amber-dim:#f5ead0; --red-dim:#f8ddd6; --purple-dim:#e8defa;
  --mono:'Space Mono','SFMono-Regular',ui-monospace,Menlo,monospace;
  --sans:'Space Grotesk',-apple-system,'Segoe UI',system-ui,sans-serif;
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-size:14px; line-height:1.5; font-family:var(--sans); -webkit-font-smoothing:antialiased; }
.container { width:96vw; max-width:1100px; margin:0 auto; padding:0 18px 48px; }
.header { padding:22px 0 12px; display:flex; justify-content:space-between; align-items:center; gap:12px; flex-wrap:wrap; }
.brand { display:flex; align-items:center; gap:11px; }
.led { width:11px; height:11px; background:var(--green); box-shadow:0 0 0 4px var(--green-dim); animation:pulse 2.6s ease-in-out infinite; flex-shrink:0; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
.header h1 { font-size:18px; font-weight:700; color:var(--text-bright); letter-spacing:-0.3px; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--accent); }
.model { font-family:var(--mono); font-size:9.5px; letter-spacing:1.5px; color:var(--muted); text-transform:uppercase; margin-top:3px; }
.nav { display:flex; gap:7px; flex-wrap:wrap; }
.nav-link { font-family:var(--mono); font-size:11px; letter-spacing:.4px; text-transform:uppercase; color:var(--text-bright); text-decoration:none; background:var(--surface); border:1.5px solid var(--line); padding:6px 13px; transition:transform .08s ease, background .15s, border-color .15s; }
.nav-link:hover { background:var(--accent-dim); border-color:var(--accent); transform:translateY(-1px); }
.grille { height:14px; margin:6px 0 18px; background-image:repeating-linear-gradient(90deg, var(--line) 0 2px, transparent 2px 12px); }
.meta { font-family:var(--mono); font-size:11px; color:var(--muted); margin-bottom:14px; line-height:1.6; }
.meta b { color:var(--text); font-weight:700; }
.modeswitch { display:flex; margin-bottom:12px; }
.modeswitch button { font-family:var(--mono); font-size:12px; text-transform:uppercase; letter-spacing:.4px; padding:8px 18px; border:1.5px solid var(--line); border-right-width:0; background:var(--surface); color:var(--text-bright); cursor:pointer; }
.modeswitch button:last-child { border-right-width:1.5px; }
.modeswitch button.active { background:var(--accent); border-color:var(--accent); color:#fff; }
.searchbar { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:8px; }
.searchbar input { background:var(--surface); border:1.5px solid var(--border); padding:9px 14px; font-size:14px; font-family:var(--mono); color:var(--text-bright); width:200px; text-transform:uppercase; }
.searchbar input::placeholder { color:var(--muted); text-transform:none; }
.searchbar input:focus { outline:none; border-color:var(--accent); }
.searchbar .go { background:var(--accent); border:1.5px solid var(--accent); color:#fff; font-family:var(--mono); font-size:12px; text-transform:uppercase; letter-spacing:.4px; padding:9px 20px; cursor:pointer; }
.searchbar .go:hover { background:#5790cc; }
.hint { font-family:var(--mono); font-size:11px; color:var(--muted); margin-bottom:18px; }
.results { display:flex; flex-direction:column; gap:8px; }
.fcard { display:flex; align-items:center; gap:14px; border:1.5px solid var(--border); background:var(--surface); padding:12px 14px; cursor:pointer; transition:border-color .12s; }
.fcard:hover { border-color:var(--accent); }
.fcard .when { width:96px; font-family:var(--mono); font-size:11px; color:var(--muted); flex-shrink:0; line-height:1.5; }
.fcard .when b { display:block; color:var(--text-bright); font-size:13px; }
.fcard .route { flex:1; min-width:0; }
.fcard .route .pair { font-size:15px; font-weight:700; color:var(--text-bright); }
.fcard .route .sub { font-family:var(--mono); font-size:11px; color:var(--muted); margin-top:1px; }
.fcard .tail { font-family:var(--mono); font-weight:700; font-size:13px; color:var(--text-bright); display:flex; align-items:center; gap:5px; min-width:90px; }
.fcard .tail .star { color:var(--amber); }
.tbadge { font-family:var(--mono); font-size:9px; font-weight:700; padding:1px 5px; color:var(--text-bright); }
.tbadge.t748 { background:var(--accent); } .tbadge.t388 { background:var(--green); }
.tbadge.t359 { background:var(--purple); } .tbadge.tother { background:var(--surface2); color:var(--muted); }
.miniconf { display:flex; flex-direction:column; align-items:center; justify-content:center; min-width:62px; padding:5px 8px; border:1.5px solid var(--border); flex-shrink:0; }
.miniconf .p { font-family:var(--mono); font-weight:700; font-size:17px; line-height:1; }
.miniconf .cn { font-family:var(--mono); font-size:9px; text-transform:uppercase; letter-spacing:.4px; color:var(--muted); margin-top:3px; }
.miniconf.cg { background:var(--green-dim); border-color:var(--green); } .miniconf.cg .p { color:#3d7d5c; }
.miniconf.ca { background:var(--amber-dim); border-color:var(--amber); } .miniconf.ca .p { color:#9a6f1e; }
.miniconf.cr { background:var(--red-dim); border-color:var(--red); } .miniconf.cr .p { color:#b4503f; }
.empty { color:var(--muted); padding:30px; text-align:center; font-family:var(--mono); font-size:12px; border:1.5px dashed var(--border); }
footer { text-align:center; padding:26px 0 10px; font-family:var(--mono); font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }
footer a { color:var(--muted); text-decoration:none; } footer a:hover { color:var(--text); }
.modal-bg { display:none; position:fixed; inset:0; background:rgba(36,33,27,0.5); z-index:100; justify-content:center; align-items:center; padding:16px; }
.modal-bg.show { display:flex; }
.modal { background:var(--surface); border:1.5px solid var(--border); padding:22px; width:100%; max-width:460px; max-height:88vh; overflow-y:auto; position:relative; }
.modal h3 { font-size:15px; color:var(--text-bright); margin-bottom:3px; padding-right:24px; }
.modal .sub { font-size:12px; color:var(--muted); margin-bottom:16px; }
.modal .close { position:absolute; top:14px; right:18px; cursor:pointer; color:var(--muted); font-size:22px; line-height:1; border:none; background:none; }
.modal .close:hover { color:var(--text-bright); }
.reassign-banner { background:var(--amber-dim); border:1.5px solid var(--amber); color:#9a6f1e; padding:9px 11px; font-size:12px; margin-bottom:16px; }
.conf-chip { display:flex; align-items:center; gap:12px; border:1.5px solid var(--border); padding:10px 12px; margin-bottom:16px; }
.conf-chip .cp { font-family:var(--mono); font-size:22px; font-weight:700; line-height:1; flex-shrink:0; }
.conf-chip .ct { font-size:12px; color:var(--text-bright); line-height:1.45; }
.conf-chip .cn { color:var(--muted); font-size:11px; font-family:var(--mono); }
.conf-chip.cg { background:var(--green-dim); border-color:var(--green); } .conf-chip.cg .cp { color:#3d7d5c; }
.conf-chip.ca { background:var(--amber-dim); border-color:var(--amber); } .conf-chip.ca .cp { color:#9a6f1e; }
.conf-chip.cr { background:var(--red-dim); border-color:var(--red); } .conf-chip.cr .cp { color:#b4503f; }
.det-grid { display:grid; grid-template-columns:auto 1fr; gap:7px 16px; font-size:12px; margin-bottom:18px; }
.det-grid .k { color:var(--muted); white-space:nowrap; font-family:var(--mono); font-size:11px; text-transform:uppercase; letter-spacing:.3px; }
.det-grid .v { color:var(--text-bright); }
.hist-title { font-family:var(--mono); font-size:10px; text-transform:uppercase; letter-spacing:1px; color:var(--muted); margin:0 0 10px; }
.hist-row { display:flex; align-items:center; gap:10px; font-size:12px; padding:6px 0; border-bottom:1.5px solid var(--border); }
.hist-row:last-child { border-bottom:none; }
.hist-row .obs { width:96px; color:var(--muted); flex-shrink:0; font-family:var(--mono); font-size:11px; }
.hist-row .reg { font-family:var(--mono); font-weight:700; color:var(--text-bright); min-width:64px; }
.hist-row.changed .reg { color:var(--amber); }
.hist-row .tag { font-size:10px; color:var(--muted); }
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <div class="brand">
      <span class="led"></span>
      <div>
        <h1>Catch a <span>Tail</span></h1>
        <div class="model">BOOK &middot; Airframe Finder</div>
      </div>
    </div>
    <nav class="nav">
      <a class="nav-link" href="/schedule">Schedule</a>
      <a class="nav-link" href="/analysis-747">747-8</a>
      <a class="nav-link" href="/analysis">A380</a>
      <a class="nav-link" href="/fleet">Fleet DB</a>
      <a class="nav-link" href="/">&larr; Monitor</a>
    </nav>
  </div>
  <div class="grille"></div>
  <div class="meta">Find an upcoming flight by <b>airframe</b> or <b>route</b>, with the currently published tail and a measured chance it still holds by departure. Schedule is published ~4 days out, so check back closer to your date.</div>
  <div class="modeswitch">
    <button id="m-tail" class="active" onclick="setMode('tail')">By tail</button>
    <button id="m-route" onclick="setMode('route')">By route</button>
  </div>
  <div class="searchbar">
    <input id="tail-in" type="text" placeholder="registration, e.g. D-ABYN">
    <input id="dep-in" type="text" placeholder="from, e.g. FRA" style="display:none">
    <input id="arr-in" type="text" placeholder="to, e.g. HND" style="display:none">
    <button class="go" onclick="search()">Search</button>
  </div>
  <div class="hint" id="hint">Tip: watched airframes are starred. Confidence is grey when there isn't enough history yet.</div>
  <div class="results" id="results"><div class="empty">Search a tail (e.g. D-ABYN) or a route (e.g. FRA &rarr; HND).</div></div>
</div>
<div class="modal-bg" id="fl-modal"><div class="modal" id="fl-modal-body"></div></div>
<footer>
  <a href="/impressum">Impressum</a> <span style="margin:0 6px">&middot;</span> <a href="/datenschutz">Datenschutz</a>
</footer>
<script>
const $ = id => document.getElementById(id);
let mode = 'tail';
function setMode(m){
  mode = m;
  $('m-tail').classList.toggle('active', m==='tail');
  $('m-route').classList.toggle('active', m==='route');
  $('tail-in').style.display = m==='tail' ? '' : 'none';
  $('dep-in').style.display = m==='route' ? '' : 'none';
  $('arr-in').style.display = m==='route' ? '' : 'none';
  ($(m==='tail'?'tail-in':'dep-in')).focus();
}
function fmtDay(iso){ if(!iso) return '?'; return new Date(iso).toLocaleDateString('en-GB',{weekday:'short',day:'2-digit',month:'short',timeZone:'UTC'}); }
function fmtClock(iso){ if(!iso) return ''; return new Date(iso).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
function tcls(t){ return ['748','388','359'].includes(t) ? 't'+t : 'tother'; }

function miniChip(hold){
  if(!hold) return '<div class="miniconf"><span class="p" style="color:var(--muted)">&mdash;</span><span class="cn">no data</span></div>';
  const p = Math.round(hold.p*100);
  const cls = p>=85 ? 'cg' : (p>=60 ? 'ca' : 'cr');
  return '<div class="miniconf '+cls+'" title="chance the published tail still holds by departure ('+hold.lead+'d out, '+hold.basis+', n='+hold.n+')">'
    +'<span class="p">'+p+'%</span><span class="cn">holds</span></div>';
}

function renderResults(d){
  const R = $('results');
  if(d.error){ R.innerHTML = '<div class="empty">'+d.error+'</div>'; return; }
  const fs = d.flights || [];
  if(!fs.length){ R.innerHTML = '<div class="empty">No upcoming flights found. The schedule is only published ~4 days ahead &mdash; try again closer to your date, or check the spelling.</div>'; return; }
  let h = '';
  fs.forEach(f => {
    const lead = f.lead===0 ? 'today' : (f.lead===1 ? 'tomorrow' : 'in '+f.lead+' days');
    const reassigned = f.reassigned ? ' &middot; <span style="color:var(--amber)">&#9888; reassigned before</span>' : '';
    h += '<div class="fcard" data-num="'+f.number+'" data-fdate="'+f.flight_date+'">'
      + '<div class="when"><b>'+fmtDay(f.dep_sched)+'</b>'+lead+'</div>'
      + '<div class="route"><div class="pair">'+f.dep+' &rarr; '+f.arr+'</div>'
      + '<div class="sub">'+f.flight+' &middot; dep '+fmtClock(f.dep_sched)+(f.arr_sched?' &middot; arr '+fmtClock(f.arr_sched):'')+reassigned+'</div></div>'
      + '<div class="tail">'+(f.watch?'<span class="star">&#9733;</span>':'')+(f.reg||'?')
      + '<span class="tbadge '+tcls(f.type)+'">'+(f.type||'?')+'</span></div>'
      + miniChip(f.hold) + '</div>';
  });
  R.innerHTML = h;
}

function search(){
  let url;
  if(mode==='tail'){
    const r = $('tail-in').value.trim().toUpperCase();
    if(!r){ return; }
    url = '/api/book?reg=' + encodeURIComponent(r);
  } else {
    const dp = $('dep-in').value.trim().toUpperCase();
    const ar = $('arr-in').value.trim().toUpperCase();
    if(!dp && !ar){ return; }
    url = '/api/book?dep=' + encodeURIComponent(dp) + '&arr=' + encodeURIComponent(ar);
  }
  $('results').innerHTML = '<div class="empty">Searching&hellip;</div>';
  fetch(url).then(r=>r.json()).then(renderResults)
    .catch(()=>{ $('results').innerHTML = '<div class="empty">Search failed.</div>'; });
}

document.querySelectorAll('.searchbar input').forEach(el =>
  el.addEventListener('keydown', e => { if(e.key==='Enter') search(); }));

/* ── Flight detail modal (shared shape with /schedule) ─────────── */
function fmt(iso){ if(!iso) return '?'; const d=new Date(iso);
  return d.toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
function row(k,v){ return '<span class="k">'+k+'</span><span class="v">'+v+'</span>'; }
function confChip(hold, reg){
  if(!hold) return '';
  const p = Math.round(hold.p*100);
  const cls = p>=85 ? 'cg' : (p>=60 ? 'ca' : 'cr');
  const label = p>=85 ? 'likely to hold' : (p>=60 ? 'could still change' : 'often changes');
  const basis = hold.basis==='route' ? 'this route' : (hold.basis==='type' ? 'this aircraft type' : 'all tracked flights');
  return '<div class="conf-chip '+cls+'"><span class="cp">'+p+'%</span><span class="ct">'
    + 'chance it stays <b>'+(reg||'this tail')+'</b> &middot; '+label
    + '<br><span class="cn">'+hold.lead+'d before departure &middot; based on '+basis+' (n='+hold.n+')</span>'
    + '</span></div>';
}
function fmtD(iso){ if(!iso) return ''; return new Date(iso+'T00:00:00Z').toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'}); }
function isoDur(s){ if(!s) return ''; const m=s.match(/PT(?:(\\d+)H)?(?:(\\d+)M)?/); if(!m) return ''; return (m[1]?m[1]+'h':'')+(m[2]?String(m[2]).padStart(2,'0')+'m':''); }
function closeFl(){ $('fl-modal').classList.remove('show'); }
function openFlight(num, fdate){
  const b = $('fl-modal-body'); b.innerHTML = '<div class="empty">Loading&hellip;</div>'; $('fl-modal').classList.add('show');
  fetch('/api/schedule/flight/LH/'+num+'/'+fdate).then(r=>r.json()).then(renderFlight)
    .catch(()=>{ b.innerHTML = '<button class="close" onclick="closeFl()">&times;</button><div class="empty">Failed to load.</div>'; });
}
function renderFlight(d){
  const b = $('fl-modal-body');
  let h = '<button class="close" onclick="closeFl()">&times;</button>';
  if(d.error){ b.innerHTML = h+'<div class="empty">'+d.error+'</div>'; return; }
  h += '<h3>'+d.flight+(d.dep_iata?'  &middot;  '+d.dep_iata+'&rarr;'+d.arr_iata:'')+'</h3>';
  h += '<div class="sub">'+[d.dep_name,d.arr_name].filter(Boolean).join(' &rarr; ')+'  &middot;  '+fmtD(d.flight_date)+'</div>';
  if(d.reassigned){ h += '<div class="reassign-banner">&#9888; Reassigned &mdash; originally <b>'+(d.original_reg||'?')+'</b>, now <b>'+(d.current_reg||'?')+'</b></div>'; }
  h += confChip(d.hold, d.current_reg);
  if(d.found){
    const dur = isoDur(d.duration);
    h += '<div class="det-grid">';
    h += row('Aircraft',(d.current_reg||'?')+(d.current_type?' &middot; '+d.current_type:''));
    h += row('Departure',fmt(d.dep_sched)+(d.dep_term?' &middot; T'+d.dep_term:'')+(d.dep_gate?' &middot; Gate '+d.dep_gate:''));
    h += row('Arrival',fmt(d.arr_sched)+(d.arr_term?' &middot; T'+d.arr_term:'')+(d.arr_gate?' &middot; Gate '+d.arr_gate:''));
    if(dur) h += row('Flight time',dur);
    if(d.status) h += row('Status',d.status);
    if(d.codeshares&&d.codeshares.length) h += row('Codeshare',d.codeshares.join(', '));
    if(d.prev) h += row('Previous leg',d.prev+(d.prev_date?' ('+fmtD(d.prev_date)+')':''));
    h += '</div>';
  } else { h += '<div class="sub">No current assignment for this date.</div>'; }
  if(d.history&&d.history.length){
    h += '<div class="hist-title">Assignment history</div>';
    let pr = null;
    d.history.forEach((x,i) => {
      const ch = pr!==null && x.reg!==pr;
      const tags = []; if(i===0) tags.push('originally planned'); if(i===d.history.length-1) tags.push('current');
      h += '<div class="hist-row'+(ch?' changed':'')+'"><span class="obs">'+fmtD(x.observed)+'</span>'
        + '<span class="reg">'+(x.reg||'&mdash;')+'</span><span class="tag">'
        + [x.type,tags.join(' &middot; ')].filter(Boolean).join('  &middot;  ')+'</span></div>';
      pr = x.reg;
    });
  }
  b.innerHTML = h;
}
$('results').addEventListener('click', e => { const c=e.target.closest('.fcard'); if(c) openFlight(c.dataset.num, c.dataset.fdate); });
$('fl-modal').addEventListener('click', e => { if(e.target.id==='fl-modal') closeFl(); });
document.addEventListener('keydown', e => { if(e.key==='Escape') closeFl(); });

/* Prefill + auto-search from URL (?reg= or ?dep=&arr=) so links land on results */
(function(){
  const p = new URLSearchParams(location.search);
  if(p.get('reg')){ setMode('tail'); $('tail-in').value = p.get('reg'); search(); }
  else if(p.get('dep') || p.get('arr')){ setMode('route'); $('dep-in').value = p.get('dep')||''; $('arr-in').value = p.get('arr')||''; search(); }
})();
</script>
</body>
</html>"""


@app.route("/book")
def book():
    return render_template_string(_BOOK_HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
