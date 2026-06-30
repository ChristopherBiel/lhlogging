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
from flask import Flask, Response, jsonify, redirect, render_template_string, request

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


# ── Faceplate design system (vendored from github:ChristopherBiel/faceplate#v1.0.0) ──
# Embedded as constants so the single-file deploy model keeps working
# (the Dockerfile copies only app.py). Pages load /faceplate.css and alias
# their semantic CSS variables onto the --fp-* tokens defined here.
_FACEPLATE_CSS = r"""/*! Faceplate v1.0 — Christopher Biel's portable design system
 *  TE-inspired · hard square edges · sage + terracotta · Manrope + IBM Plex Mono
 *  Everything is prefixed `--fp-` / `.fp-` to avoid collisions in existing codebases.
 *  Styles live in @layer fp.* so a project's own (unlayered) CSS always wins / can override.
 *  Fonts are NOT imported here — see fonts.css (self-host) or add a Google Fonts <link>.
 */

@layer fp.tokens, fp.base, fp.components, fp.utilities;

/* ============================================================ TOKENS */
@layer fp.tokens {
  :root {
    /* base neutrals */
    --fp-bg:#FFFFFF;
    --fp-surface:#F7F8F8;
    --fp-border:#E4E7E9;
    --fp-muted:#8A9097;
    --fp-body:#52585E;
    --fp-ink:#1C1E21;

    /* accent — sage */
    --fp-sage:#5E7A50;
    --fp-sage-deep:#455C3A;   /* small text / links on white (AA) */
    --fp-sage-tint:#DCE5D2;
    --fp-sage-xl:#EFF3EA;

    /* complement — terracotta */
    --fp-terra:#BB6240;
    --fp-terra-deep:#9E4F33;
    --fp-terra-tint:#F0D9CC;

    /* signature neutral — #CBCBCB reads as CB·CB·CB */
    --fp-gray:#CBCBCB;

    /* data visualization (categorical) */
    --fp-dv-1:#5E7A50; --fp-dv-2:#BB6240; --fp-dv-3:#D6A23C;
    --fp-dv-4:#2C7A78; --fp-dv-5:#8A6A53; --fp-dv-6:#9BA0A4;

    /* typography */
    --fp-font-sans:"Manrope", system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    --fp-font-mono:"IBM Plex Mono", ui-monospace, SFMono-Regular, Menlo, monospace;
    --fp-text-xs:.75rem;     --fp-text-sm:.875rem;   --fp-text-base:1rem;
    --fp-text-h3:1.25rem;    --fp-text-h2:1.5625rem; --fp-text-h1:2rem;  --fp-text-display:2.5rem;
    --fp-leading:1.65;       --fp-leading-tight:1.15;
    --fp-track-tight:-.02em; --fp-track-label:.12em;

    /* shape & structure */
    --fp-radius:0;          /* hard square edges — the Faceplate signature */
    --fp-keyline:1.5px;     /* structural border (ink) */
    --fp-hairline:1px;
    --fp-rule:var(--fp-sage);

    /* spacing — 8px base */
    --fp-space-1:.25rem; --fp-space-2:.5rem; --fp-space-3:1rem;
    --fp-space-4:1.5rem; --fp-space-5:2rem; --fp-space-6:3rem; --fp-space-7:4rem;
  }
}

/* ============================================================ BASE (opt-in) */
/* Apply class `fp` to a wrapper (or <body>) to adopt the base type/colors.
   Kept opt-in so Faceplate never restyles a host project globally. */
@layer fp.base {
  .fp {
    font-family:var(--fp-font-sans);
    color:var(--fp-ink);
    background:var(--fp-bg);
    -webkit-font-smoothing:antialiased;
    text-rendering:optimizeLegibility;
  }
  .fp :where(p){ color:var(--fp-body); line-height:var(--fp-leading); }
  .fp :where(a):not([class]){ color:var(--fp-sage-deep); }
}

/* ============================================================ COMPONENTS */
@layer fp.components {
  /* --- type --- */
  .fp-display{ font-weight:800; font-size:var(--fp-text-display); letter-spacing:var(--fp-track-tight); line-height:var(--fp-leading-tight); }
  .fp-h1{ font-weight:800; font-size:var(--fp-text-h1); letter-spacing:var(--fp-track-tight); line-height:1.15; }
  .fp-h2{ font-weight:700; font-size:var(--fp-text-h2); letter-spacing:-.01em; line-height:1.2; }
  .fp-h3{ font-weight:600; font-size:var(--fp-text-h3); line-height:1.3; }
  .fp-body{ font-size:var(--fp-text-base); line-height:var(--fp-leading); color:var(--fp-body); }
  .fp-mono{ font-family:var(--fp-font-mono); }
  .fp-label{ font-family:var(--fp-font-mono); font-weight:500; font-size:var(--fp-text-xs); letter-spacing:var(--fp-track-label); text-transform:uppercase; color:var(--fp-muted); }
  .fp-kicker{ font-family:var(--fp-font-mono); font-size:var(--fp-text-xs); letter-spacing:var(--fp-track-label); text-transform:uppercase; color:var(--fp-sage); }
  .fp-wordmark{ font-family:var(--fp-font-sans); font-weight:800; letter-spacing:var(--fp-track-tight); line-height:1; }

  /* accent rule under a heading */
  .fp-rule{ display:block; height:3px; width:48px; background:var(--fp-rule); border:0; }

  /* monogram / mark plate (square, #CBCBCB by default) */
  .fp-plate{ display:inline-grid; place-items:center; aspect-ratio:1; background:var(--fp-gray); color:var(--fp-ink); border-radius:var(--fp-radius); }
  .fp-plate--sage{ background:var(--fp-sage); color:#fff; }

  /* buttons */
  .fp-btn{ display:inline-block; font-family:var(--fp-font-sans); font-weight:700; font-size:.8125rem; letter-spacing:.01em;
           padding:.62em 1.05em; border:var(--fp-keyline) solid var(--fp-ink); border-radius:var(--fp-radius);
           background:var(--fp-bg); color:var(--fp-ink); cursor:pointer; text-decoration:none; line-height:1; }
  .fp-btn--solid{ background:var(--fp-sage); border-color:var(--fp-sage); color:#fff; }
  .fp-btn--terra{ background:var(--fp-terra); border-color:var(--fp-terra); color:#fff; }
  .fp-btn--ghost{ background:transparent; }

  /* chips & topical tags (chip + icon) */
  .fp-chip{ display:inline-flex; align-items:center; gap:.45em; font-family:var(--fp-font-mono); font-weight:600;
            font-size:.65rem; letter-spacing:.06em; text-transform:uppercase;
            padding:.32em .62em; border:var(--fp-keyline) solid var(--fp-ink); border-radius:var(--fp-radius); }
  .fp-chip--sage{ background:var(--fp-sage-tint); }
  .fp-chip--terra{ background:var(--fp-terra-tint); }
  .fp-chip--gray{ background:var(--fp-gray); }
  .fp-chip svg{ width:1.05em; height:1.05em; }
  .fp-tag{ /* alias for a chip used as a category tag with an icon */ }

  /* card & panel */
  .fp-card{ background:var(--fp-bg); border:var(--fp-keyline) solid var(--fp-ink); border-radius:var(--fp-radius); padding:var(--fp-space-4); }
  .fp-panel{ background:var(--fp-surface); border:var(--fp-hairline) solid var(--fp-border); border-radius:var(--fp-radius); padding:var(--fp-space-4); }

  /* segmented control */
  .fp-seg{ display:inline-flex; border:var(--fp-keyline) solid var(--fp-ink); border-radius:var(--fp-radius); }
  .fp-seg > *{ font-family:var(--fp-font-mono); font-size:.75rem; padding:.42em .82em; border-right:var(--fp-keyline) solid var(--fp-ink); background:transparent; }
  .fp-seg > *:last-child{ border-right:0; }
  .fp-seg > .is-on, .fp-seg > [aria-selected="true"]{ background:var(--fp-ink); color:#fff; }

  /* dividers */
  .fp-hr{ border:0; border-top:2px solid var(--fp-ink); }
  .fp-hr--dashed{ border:0; border-top:2px dashed var(--fp-ink); }
  .fp-hr--gray{ border:0; border-top:var(--fp-hairline) solid var(--fp-gray); }

  /* link */
  .fp-link{ color:var(--fp-sage-deep); font-weight:600; text-decoration:none; border-bottom:2px solid var(--fp-sage-tint); }

  /* intensity bands (reversed surfaces for registers 03/04) */
  .fp-band{ background:var(--fp-sage); color:#fff; }
  .fp-band--terra{ background:var(--fp-terra); color:#fff; }
  .fp-band--ink{ background:var(--fp-ink); color:#fff; }
}

/* ============================================================ UTILITIES */
@layer fp.utilities {
  .fp-square{ border-radius:0 !important; }
  .fp-c-ink{ color:var(--fp-ink); }   .fp-c-sage{ color:var(--fp-sage); }   .fp-c-terra{ color:var(--fp-terra); }
  .fp-c-muted{ color:var(--fp-muted); } .fp-c-body{ color:var(--fp-body); }
  .fp-bg-sage{ background:var(--fp-sage); color:#fff; }
  .fp-bg-gray{ background:var(--fp-gray); color:var(--fp-ink); }
  .fp-bg-surface{ background:var(--fp-surface); }
}"""

_FAVICON_SVG = r"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" width="64" height="64" role="img" aria-label="Christopher Biel favicon">
  <!-- Favicon: sage step-response curve on transparent. Setpoint line dropped for clarity at tiny sizes. -->
  <path d="M 8.00,52.00 L 15.68,52.00 L 16.32,51.84 L 16.96,51.12 L 17.60,49.92 L 18.24,48.32 L 18.88,46.42 L 19.52,44.29 L 20.16,42.00 L 20.80,39.63 L 21.44,37.23 L 22.08,34.85 L 22.72,32.54 L 23.36,30.34 L 24.00,28.26 L 24.64,26.35 L 25.28,24.61 L 25.92,23.06 L 26.56,21.69 L 27.20,20.52 L 27.84,19.54 L 28.48,18.74 L 29.12,18.12 L 29.76,17.65 L 30.40,17.34 L 31.04,17.16 L 31.68,17.11 L 32.32,17.16 L 32.96,17.30 L 33.60,17.51 L 34.24,17.79 L 34.88,18.11 L 35.52,18.46 L 36.16,18.84 L 36.80,19.23 L 37.44,19.62 L 38.08,20.01 L 38.72,20.38 L 39.36,20.73 L 40.00,21.06 L 40.64,21.37 L 41.28,21.64 L 41.92,21.89 L 42.56,22.10 L 43.20,22.29 L 43.84,22.44 L 44.48,22.56 L 45.12,22.66 L 45.76,22.72 L 46.40,22.77 L 47.04,22.79 L 47.68,22.80 L 48.32,22.78 L 48.96,22.76 L 49.60,22.72 L 50.24,22.67 L 50.88,22.62 L 51.52,22.56 L 52.16,22.50 L 52.80,22.43 L 53.44,22.37 L 54.08,22.31 L 54.72,22.25 L 55.36,22.19 L 56.00,22.14"
        fill="none" stroke="#5E7A50" stroke-width="5" stroke-linecap="round" stroke-linejoin="round"/>
  <circle cx="56" cy="22.14" r="3.6" fill="#5E7A50"/>
</svg>"""


@app.route("/faceplate.css")
def faceplate_css():
    return Response(
        _FACEPLATE_CSS,
        mimetype="text/css",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@app.route("/favicon.svg")
def favicon_svg():
    return Response(
        _FAVICON_SVG,
        mimetype="image/svg+xml",
        headers={"Cache-Control": "public, max-age=86400"},
    )


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
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/faceplate.css">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>
:root {
  /* Faceplate v1.0 — semantic tokens aliased onto --fp-* (see /faceplate.css).
     Sage primary, terracotta complement, ochre/teal/clay for categorical data. */
  --bg:var(--fp-surface);          /* soft off-white page; cards sit on white   */
  --surface:var(--fp-bg);          /* #fff cards / panels                        */
  --surface2:var(--fp-sage-xl);    /* faint sage wells, tracks, hovers           */
  --border:var(--fp-border);
  --line:var(--fp-gray);           /* #CBCBCB structural keyline (CB·CB·CB)      */
  --text:var(--fp-body);
  --text-bright:var(--fp-ink);
  --muted:var(--fp-muted);
  /* solid key fills — Faceplate accents */
  --accent:var(--fp-sage);         /* sage — primary / info / B748              */
  --green:var(--fp-dv-4);          /* teal — ok / A388                          */
  --amber:var(--fp-dv-3);          /* ochre — warn / watch                      */
  --red:var(--fp-terra);           /* terracotta — error / deviation            */
  --purple:var(--fp-dv-5);         /* clay — A359                               */
  --cyan:var(--fp-dv-4);           /* teal — extra                              */
  /* pale fills */
  --accent-dim:var(--fp-sage-tint);
  --green-dim:color-mix(in srgb, var(--fp-dv-4) 16%, var(--fp-bg));
  --amber-dim:color-mix(in srgb, var(--fp-dv-3) 22%, var(--fp-bg));
  --red-dim:var(--fp-terra-tint);
  --purple-dim:color-mix(in srgb, var(--fp-dv-5) 18%, var(--fp-bg));
  --radius:var(--fp-radius);
  --radius-sm:var(--fp-radius);
  --mono:var(--fp-font-mono);
  --sans:var(--fp-font-sans);
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
/* ── Header · Faceplate band (intensity 03): plate + wordmark + label ─────── */
.header { display:flex; align-items:center; gap:14px 22px; flex-wrap:wrap;
  padding:16px 22px; margin:18px 0 24px; }  /* sage band + #fff come from .fp-band */
.brand { display:flex; align-items:center; gap:14px; }
.brand .fp-plate { width:46px; height:46px; flex-shrink:0; }
.brand .fp-plate svg { width:27px; height:27px; display:block; }
.header h1 { font-family:var(--fp-font-sans); font-size:22px; font-weight:800; letter-spacing:-.02em;
  color:#fff; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--fp-sage-tint); }
.model { font-family:var(--fp-font-sans); font-size:10px; letter-spacing:.12em;
  color:rgba(255,255,255,.82); text-transform:uppercase; margin-top:5px; }
.nav { display:flex; gap:18px; flex-wrap:wrap; margin-left:auto; }
.nav a, .nav-link { font-family:var(--fp-font-sans); font-size:11px; font-weight:500; letter-spacing:.1em;
  text-transform:uppercase; color:rgba(255,255,255,.82); text-decoration:none;
  padding:3px 0; border-bottom:2px solid transparent; transition:color .14s, border-color .14s; }
.nav a:hover, .nav-link:hover { color:#fff; border-bottom-color:#fff; text-decoration:none; }
.updated { font-family:var(--fp-font-mono); font-size:10.5px; color:rgba(255,255,255,.82); }

/* ── Health strip ───────────────────────────────────────── */
.health-strip { display:flex; gap:9px; margin-bottom:26px; flex-wrap:wrap; counter-reset:hk; }
.health-item { flex:1; min-width:86px; background:var(--surface); border:1.5px solid var(--fp-ink);
  border-radius:0; padding:12px 12px 11px; text-align:left; }
.health-item .label { font-family:var(--sans); font-size:9px; text-transform:uppercase; letter-spacing:.7px;
  color:var(--muted); margin-bottom:8px; }
.health-item .label::before { counter-increment:hk; content:counter(hk,decimal-leading-zero)" "; color:var(--accent); }
.health-item .dot { display:inline-block; width:8px; height:8px; border-radius:0; margin-right:6px; vertical-align:middle; }
.health-item .info { font-family:var(--sans); font-size:12px; color:var(--text); }

/* ── Section ────────────────────────────────────────────── */
.section { margin-bottom:24px; }
.section-label { font-family:var(--sans); font-size:10px; font-weight:700; text-transform:uppercase;
  letter-spacing:1px; color:var(--muted); margin-bottom:12px; display:flex; align-items:center; gap:9px;
  counter-increment:sec; }
.section-label::before { content:counter(sec,decimal-leading-zero); font-size:9px; color:var(--text-bright);
  background:var(--accent-dim); border-radius:0; padding:3px 6px; letter-spacing:.5px; }

/* ── Metric keys ────────────────────────────────────────── */
.metrics { display:grid; grid-template-columns:1fr 1fr 1fr; gap:9px; margin-bottom:24px; counter-reset:key; }
.metric { background:var(--surface); border:1.5px solid var(--fp-ink); border-radius:0;
  padding:14px 13px 13px; position:relative; }
.metric::after { content:''; position:absolute; top:13px; right:13px; width:7px; height:7px; border-radius:0; background:var(--accent); }
.metric:nth-child(3n+1)::after { background:var(--green); }
.metric:nth-child(3n+2)::after { background:var(--accent); }
.metric:nth-child(3n+3)::after { background:var(--purple); }
.metric .label { font-family:var(--sans); font-size:9px; text-transform:uppercase; letter-spacing:.6px;
  color:var(--muted); margin-bottom:7px; }
.metric .label::before { counter-increment:key; content:counter(key,decimal-leading-zero)" "; color:var(--accent); }
.metric .value { font-family:var(--mono); font-size:26px; font-weight:700; color:var(--text-bright); letter-spacing:-1px; line-height:1; }
.metric .sub { font-size:10px; color:var(--muted); margin-top:4px; }

/* ── Cards ──────────────────────────────────────────────── */
.card { background:var(--surface); border:1.5px solid var(--fp-ink); border-radius:0; padding:16px; margin-bottom:14px; }

/* ── Chart bars ─────────────────────────────────────────── */
.chart-bars { display:flex; align-items:flex-end; gap:3px; height:64px; }
.chart-bars .bar { flex:1; border-radius:0; min-height:2px; transition:height .3s ease; position:relative; }
.chart-bars .bar:hover { opacity:1 !important; }
.chart-labels { display:flex; justify-content:space-between; font-family:var(--mono); font-size:10px; color:var(--muted); margin-top:6px; }
.chart-legend { display:flex; gap:14px; margin-bottom:10px; font-family:var(--sans); font-size:9.5px;
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
/* status badges = Faceplate .fp-chip + a semantic tint */
.badge-ok { background:var(--green-dim); color:var(--green); }
.badge-error { background:var(--red-dim); color:var(--fp-terra-deep); }
.badge-running { background:var(--accent-dim); color:var(--fp-sage-deep); }

/* ── Route table ────────────────────────────────────────── */
.route-row { display:flex; align-items:center; padding:7px 0; border-bottom:1.5px solid var(--border); font-size:12px; }
.route-row:last-child { border-bottom:none; }
.route-pair { flex:1; color:var(--text); font-weight:500; }
.route-pair .arrow { color:var(--accent); margin:0 6px; font-family:var(--mono); }
.route-count { color:var(--muted); font-family:var(--mono); font-variant-numeric:tabular-nums; font-size:11px; }

/* ── Misc ───────────────────────────────────────────────── */
#error-banner { display:none; background:var(--red-dim); border:1.5px solid var(--red); border-radius:0;
  padding:11px 14px; margin-bottom:16px; color:var(--fp-terra-deep); font-size:12px; }
.tooltip { position:fixed; background:var(--text-bright); border:none; border-radius:0; padding:5px 9px;
  font-family:var(--sans); font-size:11px; color:#fff; pointer-events:none; z-index:100; white-space:nowrap; display:none; }
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
<body class="fp">
<div class="container">

  <div class="header fp-band">
    <div class="brand">
      <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
      <div>
        <h1>LH&middot;Fleet <span>Monitor</span></h1>
        <div class="fp-label model">FLT-MON &middot; Fleet Telemetry</div>
      </div>
    </div>
    <nav class="nav">
      <a href="/book">Book</a>
      <a href="/schedule">Schedule</a>
      <a href="/fleet">Fleet DB</a>
      <a href="/insights">Insights</a>
    </nav>
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
        <span><span class="swatch" style="background:var(--fp-dv-1)"></span>Flights</span>
        <span><span class="swatch" style="background:var(--fp-dv-3)"></span>Unique callsigns</span>
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
          <span style="color:var(--fp-dv-6)">In DB</span>
          <span style="margin:0 4px">/</span>
          <span style="color:var(--fp-dv-1)">Flew 7d</span>
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
  return '<span class="fp-chip ' + c + '">' + status + '</span>';
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
      '<div style="height:' + h + 'px;background:var(--fp-dv-1);border-radius:0;position:relative">' +
      (csH > 0 ? '<div style="position:absolute;bottom:0;left:0;right:0;height:' + Math.min(csH, h) + 'px;background:var(--fp-dv-3);border-radius:0"></div>' : '') +
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
        '<div class="hbar-fill" style="width:' + pct + '%;background:var(--fp-dv-6);position:relative">' +
          '<div style="position:absolute;top:0;left:0;height:100%;width:' + (t.count > 0 ? Math.round(flewCount / t.count * 100) : 0) + '%;background:var(--fp-dv-1);border-radius:0"></div>' +
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/faceplate.css">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>
:root {
  /* Faceplate v1.0 — semantic tokens aliased onto --fp-* (see /faceplate.css) */
  --bg:var(--fp-surface); --surface:var(--fp-bg); --border:var(--fp-border);
  --text:var(--fp-body); --text-bright:var(--fp-ink); --muted:var(--fp-muted); --accent:var(--fp-sage-deep);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: var(--fp-font-sans);
  font-size: 14px; line-height: 1.6; -webkit-font-smoothing: antialiased;
}
.container { max-width: 480px; margin: 0 auto; padding: 24px 16px 32px; }
h1 { color: var(--text-bright); font-size: 20px; margin-bottom: 16px; }
h2 { color: var(--text-bright); font-size: 15px; margin: 20px 0 8px; }
p, li { margin-bottom: 8px; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }

/* ── Header · Faceplate band (intensity 03): plate + wordmark + label ─────── */
.header { display:flex; align-items:center; gap:14px 22px; flex-wrap:wrap;
  padding:16px 22px; margin:18px 0 24px; }  /* sage band + #fff come from .fp-band */
.brand { display:flex; align-items:center; gap:14px; }
.brand .fp-plate { width:46px; height:46px; flex-shrink:0; }
.brand .fp-plate svg { width:27px; height:27px; display:block; }
.header h1 { font-family:var(--fp-font-sans); font-size:22px; font-weight:800; letter-spacing:-.02em;
  color:#fff; text-transform:uppercase; line-height:1; margin-bottom:0; }
.header h1 span { color:var(--fp-sage-tint); }
.model { font-family:var(--fp-font-sans); font-size:10px; letter-spacing:.12em;
  color:rgba(255,255,255,.82); text-transform:uppercase; margin-top:5px; }
.nav { display:flex; gap:18px; flex-wrap:wrap; margin-left:auto; }
.nav a, .nav-link { font-family:var(--fp-font-sans); font-size:11px; font-weight:500; letter-spacing:.1em;
  text-transform:uppercase; color:rgba(255,255,255,.82); text-decoration:none;
  padding:3px 0; border-bottom:2px solid transparent; transition:color .14s, border-color .14s; }
.nav a:hover, .nav-link:hover { color:#fff; border-bottom-color:#fff; text-decoration:none; }
</style>
"""

_IMPRESSUM_HTML = (
    '<!DOCTYPE html><html lang="de"><head><meta charset="UTF-8">'
    '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
    "<title>Impressum</title>"
    + _LEGAL_CSS
    + """</head><body class="fp"><div class="container">
<div class="header fp-band">
  <div class="brand">
    <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
    <div>
      <h1>LH&middot;Fleet <span>Monitor</span></h1>
      <div class="fp-label model">LEGAL &middot; Impressum</div>
    </div>
  </div>
  <nav class="nav"><a class="nav-link" href="/">&larr; Back</a></nav>
</div>
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
    + """</head><body class="fp"><div class="container">
<div class="header fp-band">
  <div class="brand">
    <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
    <div>
      <h1>LH&middot;Fleet <span>Monitor</span></h1>
      <div class="fp-label model">LEGAL &middot; Datenschutz</div>
    </div>
  </div>
  <nav class="nav"><a class="nav-link" href="/">&larr; Back</a></nav>
</div>
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


# ── Legacy analysis pages → /insights (predictive model retired) ────


@app.route("/analysis")
def analysis():
    return redirect("/insights?type=A388", code=302)


@app.route("/analysis-747")
def analysis_747():
    return redirect("/insights?type=B748", code=302)


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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/faceplate.css">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>
:root {
  /* Faceplate v1.0 — semantic tokens aliased onto --fp-* (see /faceplate.css) */
  --bg:var(--fp-surface); --surface:var(--fp-bg); --surface2:var(--fp-sage-xl);
  --border:var(--fp-border); --line:var(--fp-gray); --text:var(--fp-body); --text-bright:var(--fp-ink);
  --muted:var(--fp-muted); --accent:var(--fp-sage); --accent-dim:var(--fp-sage-tint);
  --green:var(--fp-dv-4); --green-dim:color-mix(in srgb,var(--fp-dv-4) 16%,var(--fp-bg));
  --red:var(--fp-terra); --red-dim:var(--fp-terra-tint);
  --amber:var(--fp-dv-3); --amber-dim:color-mix(in srgb,var(--fp-dv-3) 22%,var(--fp-bg)); --radius:var(--fp-radius);
  --mono:var(--fp-font-mono); --sans:var(--fp-font-sans);
}
/* Faceplate: hard square edges + mono instrument labels */
input, select, button, textarea,
.btn, .badge, .badge-active, .badge-retired, .badge-review, .badge-tracking,
.toggle-group, .toggle-btn, .nav-link, .card, .metric, .modal, .toast,
.toolbar input, .toolbar select, .route-track, .route-fill, .chart-bar,
.editable { border-radius: 0 !important; }
th, .label, .metric .label, .info-item .label, .modal label { font-family: var(--fp-font-sans); }
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: var(--fp-font-sans);
  font-size: 14px; line-height: 1.5; -webkit-font-smoothing: antialiased;
}
.container { max-width: 1200px; margin: 0 auto; padding: 0 16px 40px; }
/* ── Header · Faceplate band (intensity 03): plate + wordmark + label ─────── */
.header { display:flex; align-items:center; gap:14px 22px; flex-wrap:wrap;
  padding:16px 22px; margin:18px 0 24px; }  /* sage band + #fff come from .fp-band */
.brand { display:flex; align-items:center; gap:14px; }
.brand .fp-plate { width:46px; height:46px; flex-shrink:0; }
.brand .fp-plate svg { width:27px; height:27px; display:block; }
.header h1 { font-family:var(--fp-font-sans); font-size:22px; font-weight:800; letter-spacing:-.02em;
  color:#fff; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--fp-sage-tint); }
.model { font-family:var(--fp-font-sans); font-size:10px; letter-spacing:.12em;
  color:rgba(255,255,255,.82); text-transform:uppercase; margin-top:5px; }
.nav { display:flex; gap:18px; flex-wrap:wrap; margin-left:auto; }
.nav a, .nav-link { font-family:var(--fp-font-sans); font-size:11px; font-weight:500; letter-spacing:.1em;
  text-transform:uppercase; color:rgba(255,255,255,.82); text-decoration:none;
  padding:3px 0; border-bottom:2px solid transparent; transition:color .14s, border-color .14s; }
.nav a:hover, .nav-link:hover { color:#fff; border-bottom-color:#fff; text-decoration:none; }
.updated { font-family:var(--fp-font-mono); font-size:10.5px; color:rgba(255,255,255,.82); }

/* Toolbar */
.toolbar {
  display: flex; gap: 10px; margin-bottom: 16px; flex-wrap: wrap; align-items: center;
}
.toolbar input[type="text"] {
  background: var(--surface); border: 1.5px solid var(--fp-ink); border-radius: 6px;
  color: var(--text-bright); padding: 7px 12px; font-size: 13px; flex: 1; min-width: 200px;
  outline: none;
}
.toolbar input[type="text"]:focus { border-color: var(--accent); }
.toolbar select {
  background: var(--surface); border: 1.5px solid var(--fp-ink); border-radius: 6px;
  color: var(--text); padding: 7px 10px; font-size: 12px; outline: none; cursor: pointer;
}
.toolbar .count { font-size: 12px; color: var(--muted); margin-left: auto; }

/* status filter = Faceplate .fp-seg; .toggle-btn kept as JS hook, reset <button> UA */
.fp-seg > button { border-top:0; border-bottom:0; border-left:0; color:var(--fp-ink); cursor:pointer; }
.fp-seg > .active { background:var(--fp-ink); color:#fff; }

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
  padding: 7px 10px; border-bottom: 1px solid var(--border);
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
.fleet-table tr.review-row { background: color-mix(in srgb,var(--fp-dv-3) 12%,var(--fp-bg)); }
.fleet-table tr.review-row:hover { background: var(--amber-dim); }
/* status badges = Faceplate .fp-chip + a semantic tint */
.badge-review { background: var(--amber-dim); color: var(--amber); }
.badge-active { background: var(--green-dim); color: var(--green); }
.badge-retired { background: var(--red-dim); color: var(--red); }
.badge-tracking { background: var(--green-dim); color: var(--green); }

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
<body class="fp">
<div class="container">

  <div class="header fp-band">
    <div class="brand">
      <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
      <div>
        <h1>LH Fleet <span>Database</span></h1>
        <div class="fp-label model">FLEET DB &middot; Aircraft Registry</div>
      </div>
    </div>
    <nav class="nav">
      <a class="nav-link" href="/">&larr; Monitor</a>
      <a class="nav-link" href="/insights">Insights</a>
    </nav>
  </div>
  
  <div class="error-banner" id="error-banner"></div>
  <div class="loading" id="loading">Loading fleet data&hellip;</div>

  <div id="content" style="display:none">
    <div class="toolbar">
      <input type="text" id="search" placeholder="Search registration, ICAO24, type, model...">
      <select id="type-filter"><option value="">All types</option></select>
      <div class="fp-seg">
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
      ? '<span class="fp-chip badge-active">active</span>'
      : '<span class="fp-chip badge-retired">retired</span>';
    const trackingBadge = a.currently_tracking ? ' <span class="fp-chip badge-tracking">tracking</span>' : '';
    const reviewBadge = a.needs_review ? ' <span class="fp-chip badge-review">review</span>' : '';
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/faceplate.css">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>
:root {
  /* Faceplate v1.0 — semantic tokens aliased onto --fp-* (see /faceplate.css) */
  --bg:var(--fp-surface); --surface:var(--fp-bg); --surface2:var(--fp-sage-xl);
  --border:var(--fp-border); --line:var(--fp-gray); --text:var(--fp-body); --text-bright:var(--fp-ink);
  --muted:var(--fp-muted); --accent:var(--fp-sage); --accent-dim:var(--fp-sage-tint);
  --green:var(--fp-dv-4); --green-dim:color-mix(in srgb,var(--fp-dv-4) 16%,var(--fp-bg));
  --red:var(--fp-terra); --red-dim:var(--fp-terra-tint);
  --amber:var(--fp-dv-3); --amber-dim:color-mix(in srgb,var(--fp-dv-3) 22%,var(--fp-bg)); --radius:var(--fp-radius);
  --mono:var(--fp-font-mono); --sans:var(--fp-font-sans);
}
/* Faceplate: hard square edges + mono instrument labels */
input, select, button, textarea,
.btn, .badge, .badge-active, .badge-retired, .badge-review, .badge-tracking,
.toggle-group, .toggle-btn, .nav-link, .card, .metric, .modal, .toast,
.toolbar input, .toolbar select, .route-track, .route-fill, .chart-bar,
.editable { border-radius: 0 !important; }
th, .label, .metric .label, .info-item .label, .modal label { font-family: var(--fp-font-sans); }
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: var(--fp-font-sans);
  font-size: 14px; line-height: 1.5; -webkit-font-smoothing: antialiased;
}
.container { max-width: 1100px; margin: 0 auto; padding: 0 16px 40px; }
/* ── Header · Faceplate band (intensity 03): plate + wordmark + label ─────── */
.header { display:flex; align-items:center; gap:14px 22px; flex-wrap:wrap;
  padding:16px 22px; margin:18px 0 24px; }  /* sage band + #fff come from .fp-band */
.brand { display:flex; align-items:center; gap:14px; }
.brand .fp-plate { width:46px; height:46px; flex-shrink:0; }
.brand .fp-plate svg { width:27px; height:27px; display:block; }
.header h1 { font-family:var(--fp-font-sans); font-size:22px; font-weight:800; letter-spacing:-.02em;
  color:#fff; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--fp-sage-tint); }
.model { font-family:var(--fp-font-sans); font-size:10px; letter-spacing:.12em;
  color:rgba(255,255,255,.82); text-transform:uppercase; margin-top:5px; }
.nav { display:flex; gap:18px; flex-wrap:wrap; margin-left:auto; }
.nav a, .nav-link { font-family:var(--fp-font-sans); font-size:11px; font-weight:500; letter-spacing:.1em;
  text-transform:uppercase; color:rgba(255,255,255,.82); text-decoration:none;
  padding:3px 0; border-bottom:2px solid transparent; transition:color .14s, border-color .14s; }
.nav a:hover, .nav-link:hover { color:#fff; border-bottom-color:#fff; text-decoration:none; }
.updated { font-family:var(--fp-font-mono); font-size:10.5px; color:rgba(255,255,255,.82); }

.card {
  background: var(--surface); border: 1.5px solid var(--fp-ink);
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
  background: var(--surface); border: 1.5px solid var(--fp-ink);
  border-radius: var(--radius); padding: 12px; text-align: center;
}
.metric .label {
  font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px;
  color: var(--muted); margin-bottom: 4px;
}
.metric .value { font-size: 22px; font-weight: 700; color: var(--text-bright); }

/* status badges = Faceplate .fp-chip + a semantic tint */
.badge-active { background: var(--green-dim); color: var(--green); }
.badge-retired {
  background: var(--red-dim); color: var(--red);
}

/* Route bars */
.route-bar-row {
  display: flex; align-items: center; gap: 8px; margin-bottom: 4px;
}
.route-label { width: 90px; text-align: right; font-size: 12px; color: var(--text); font-weight: 500; }
.route-track { flex: 1; height: 18px; background: var(--surface2); border-radius: 3px; overflow: hidden; }
.route-fill { height: 100%; border-radius: 0; background: var(--fp-dv-1); }
.route-count { width: 30px; font-size: 11px; color: var(--muted); text-align: right; font-variant-numeric: tabular-nums; }

/* Activity chart */
.chart-bars { display: flex; align-items: flex-end; gap: 2px; height: 60px; }
.chart-bar {
  flex: 1; border-radius: 0; min-height: 0; background: var(--fp-dv-1);
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
  padding: 5px 8px; border-bottom: 1px solid var(--border); color: var(--text);
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
<body class="fp">
<div class="container">

  <div class="header fp-band">
    <div class="brand">
      <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
      <div>
        <h1 id="page-title">Aircraft <span>Detail</span></h1>
        <div class="fp-label model">AIRCRAFT &middot; Tail Detail</div>
      </div>
    </div>
    <nav class="nav">
      <a class="nav-link" href="/fleet">&larr; Fleet DB</a>
      <a class="nav-link" href="/">Monitor</a>
    </nav>
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
    ? '<span class="fp-chip badge-active">active</span>'
    : '<span class="fp-chip badge-retired">retired</span>';

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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/faceplate.css">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>
:root {
  /* Faceplate v1.0 — semantic tokens aliased onto --fp-* (see /faceplate.css) */
  --bg:var(--fp-surface); --surface:var(--fp-bg); --surface2:var(--fp-sage-xl);
  --border:var(--fp-border); --line:var(--fp-gray); --text:var(--fp-body); --text-bright:var(--fp-ink);
  --muted:var(--fp-muted); --accent:var(--fp-sage); --accent-dim:var(--fp-sage-tint);
  --green:var(--fp-dv-4); --green-dim:color-mix(in srgb,var(--fp-dv-4) 16%,var(--fp-bg));
  --red:var(--fp-terra); --red-dim:var(--fp-terra-tint);
  --amber:var(--fp-dv-3); --amber-dim:color-mix(in srgb,var(--fp-dv-3) 22%,var(--fp-bg)); --radius:var(--fp-radius);
  --mono:var(--fp-font-mono); --sans:var(--fp-font-sans);
}
/* Faceplate: hard square edges + mono instrument labels */
input, select, button, textarea,
.btn, .badge, .badge-active, .badge-retired, .badge-review, .badge-tracking,
.toggle-group, .toggle-btn, .nav-link, .card, .metric, .modal, .toast,
.toolbar input, .toolbar select, .route-track, .route-fill, .chart-bar,
.editable { border-radius: 0 !important; }
th, .label, .metric .label, .info-item .label, .modal label { font-family: var(--fp-font-sans); }
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: var(--fp-font-sans);
  font-size: 14px; line-height: 1.5; -webkit-font-smoothing: antialiased;
}
.container { max-width: 1100px; margin: 0 auto; padding: 0 16px 32px; }
/* ── Header · Faceplate band (intensity 03): plate + wordmark + label ─────── */
.header { display:flex; align-items:center; gap:14px 22px; flex-wrap:wrap;
  padding:16px 22px; margin:18px 0 24px; }  /* sage band + #fff come from .fp-band */
.brand { display:flex; align-items:center; gap:14px; }
.brand .fp-plate { width:46px; height:46px; flex-shrink:0; }
.brand .fp-plate svg { width:27px; height:27px; display:block; }
.header h1 { font-family:var(--fp-font-sans); font-size:22px; font-weight:800; letter-spacing:-.02em;
  color:#fff; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--fp-sage-tint); }
.model { font-family:var(--fp-font-sans); font-size:10px; letter-spacing:.12em;
  color:rgba(255,255,255,.82); text-transform:uppercase; margin-top:5px; }
.nav { display:flex; gap:18px; flex-wrap:wrap; margin-left:auto; }
.nav a, .nav-link { font-family:var(--fp-font-sans); font-size:11px; font-weight:500; letter-spacing:.1em;
  text-transform:uppercase; color:rgba(255,255,255,.82); text-decoration:none;
  padding:3px 0; border-bottom:2px solid transparent; transition:color .14s, border-color .14s; }
.nav a:hover, .nav-link:hover { color:#fff; border-bottom-color:#fff; text-decoration:none; }
.updated { font-family:var(--fp-font-mono); font-size:10.5px; color:rgba(255,255,255,.82); }

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
  background: var(--surface); border: 1.5px solid var(--fp-ink); border-radius: 6px;
  padding: 7px 10px; color: var(--text); font-size: 13px; outline: none;
}
input:focus, select:focus { border-color: var(--accent); }
input::placeholder { color: var(--muted); }
select { cursor: pointer; }

/* Buttons */
/* buttons use the Faceplate .fp-btn component; these add hover + a small size */
.fp-btn { transition: opacity 0.2s; white-space: nowrap; }
.fp-btn:hover { opacity: 0.85; }
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
  padding: 6px; border-bottom: 1px solid var(--border);
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
  background: var(--surface); border: 1.5px solid var(--fp-ink); border-radius: var(--radius);
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
/* status badges = Faceplate .fp-chip + a semantic tint */
.badge-active { background: var(--green-dim); color: var(--green); }
.badge-retired { background: var(--red-dim); color: var(--red); }
.badge-review { background: var(--amber-dim); color: var(--amber); }

/* Toast */
.toast {
  position: fixed; bottom: 24px; right: 24px; padding: 10px 18px;
  border-radius: 8px; font-size: 13px; font-weight: 500; z-index: 200;
  transition: opacity 0.3s; pointer-events: none;
}
.toast-ok { background: var(--green); color: #fff; }
.toast-err { background: var(--red); color: #fff; }
</style>
</head>
<body class="fp">
<div class="container">
  <div class="header fp-band">
    <div class="brand">
      <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
      <div>
        <h1><span>LH</span> Admin</h1>
        <div class="fp-label model">ADMIN &middot; Fleet Editor</div>
      </div>
    </div>
    <nav class="nav">
      <a class="nav-link" href="/">&larr; Dashboard</a>
    </nav>
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
      <button class="fp-btn fp-btn--solid" onclick="showAddAircraft()">+ Aircraft</button>
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
      <button class="fp-btn fp-btn--solid" onclick="showAddFlight()">+ Flight</button>
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
      <button class="fp-btn" onclick="closeModal('modal-ac')">Cancel</button>
      <button class="fp-btn fp-btn--solid" id="modal-ac-save" onclick="saveAircraft()">Add</button>
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
      <button class="fp-btn" onclick="closeModal('modal-fl')">Cancel</button>
      <button class="fp-btn fp-btn--solid" onclick="saveFlight()">Add</button>
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
      ? '<span class="fp-chip badge-active">Active</span>'
      : '<span class="fp-chip badge-retired">Retired</span>';
    const reviewBadge = a.needs_review
      ? '<span class="fp-chip badge-review">Review</span>' : '';
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
          ? '<button class="fp-btn btn-sm" onclick="retireAc(\\'' + esc(a.icao24) + '\\')">Retire</button>'
          : '<button class="fp-btn fp-btn--solid btn-sm" onclick="reactivateAc(\\'' + esc(a.icao24) + '\\')">Reactivate</button>') +
        '<button class="fp-btn fp-btn--terra btn-sm" onclick="deleteAc(\\'' + esc(a.icao24) + '\\')">Del</button>' +
        (a.needs_review
          ? '<button class="fp-btn btn-sm" onclick="clearReviewAc(\\'' + esc(a.icao24) + '\\')">OK</button>'
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
    const reviewBadge = f.needs_review ? '<span class="fp-chip badge-review">Review</span>' : '';
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
        (f.needs_review ? '<button class="fp-btn btn-sm" onclick="clearReviewFl(' + f.id + ')">OK</button>' : '') +
        '<button class="fp-btn fp-btn--terra btn-sm" onclick="deleteFl(' + f.id + ')">Del</button>' +
      '</td></tr>';
  }).join('') || '<tr><td colspan="11" style="text-align:center;color:var(--muted)">No flights found</td></tr>';

  // Pagination
  const pg = $('fl-pagination');
  if (d.pages > 1) {
    let html = '<button class="fp-btn btn-sm" onclick="flGo(' + (d.page-1) + ')" ' + (d.page<=1?'disabled':'') + '>&laquo;</button>';
    html += '<span>Page ' + d.page + ' / ' + d.pages + ' (' + d.total + ' flights)</span>';
    html += '<button class="fp-btn btn-sm" onclick="flGo(' + (d.page+1) + ')" ' + (d.page>=d.pages?'disabled':'') + '>&raquo;</button>';
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

# Airports where an LH widebody arrival usually signals a diversion/positioning
# (i.e. not the FRA/MUC bases). Used to mark likely incidents on the
# reschedulings timeline — a heuristic, so the hover always shows the real route.
_DIVERSION_AIRPORTS = frozenset({
    "EDDK", "EDLW", "EDDP", "EDDV", "EDDN", "EDSB", "ELLX",
    "EDDH", "EDDB", "EDDL", "EDDS", "EDDR", "EDFH",
})


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


def _route_endpoints(conn):
    """callsign -> {endpoint ICAOs} from flight_routes; {} if the table is absent
    (migration 004 not yet applied) so stitching degrades to a no-op."""
    try:
        rows = _q(conn, "SELECT btrim(callsign), departure_airport_icao, "
                        "arrival_airport_icao FROM flight_routes")
    except Exception:
        return {}
    out = {}
    for cs, dep, arr in rows:
        s = {(c or "").strip() for c in (dep, arr)}
        out[cs] = s - {"", "UNKN"}
    return out


def _stitch_phantom_legs(legs, endpoints):
    """Collapse consecutive same-callsign legs split by a phantom waypoint —
    leg1.arr == leg2.dep where that airport is NOT an endpoint of the callsign's
    reference route (the on_ground cruise-snap signature). Returns a new list;
    the merged leg keeps leg1's dep/start and takes leg2's arr/end. Defensive
    read-time mirror of the detector fix, so phantom splits never reach the
    plan-vs-actual overlay even before backfill catches them.

    Each leg is a dict with cs / dep / arr (ICAO) and start / end.
    """
    if not legs:
        return legs
    out = [dict(legs[0])]
    for leg in legs[1:]:
        prev = out[-1]
        cs = (leg.get("cs") or "").strip()
        shared = (prev.get("arr") or "").strip()
        ep = endpoints.get(cs)
        if (cs and cs == (prev.get("cs") or "").strip() and shared
                and shared == (leg.get("dep") or "").strip()
                and ep is not None and shared not in ep):
            prev["arr"] = leg.get("arr")
            prev["end"] = leg.get("end")
        else:
            out.append(dict(leg))
    return out


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
        endpoints = _route_endpoints(conn)
    finally:
        conn.close()

    swapped = {(r[0], r[1]) for r in swap_rows}

    # Actual legs per registration, on the same fake-UTC (Berlin) axis. Stitch
    # phantom cruise-snap splits (A→phantom + phantom→B) back into one leg so the
    # plan-vs-actual overlay never shows a false "extra" bar / "deviation".
    raw_actuals = defaultdict(list)
    for reg, cs, dep_icao, arr_icao, fs, ls in act_rows:
        raw_actuals[reg].append({
            "cs": (cs or "").strip(), "dep": dep_icao, "arr": arr_icao,
            "start": _berlin_fake_utc(fs), "end": _berlin_fake_utc(ls),
        })
    actuals = defaultdict(list)
    for reg, legs in raw_actuals.items():
        for leg in _stitch_phantom_legs(legs, endpoints):
            actuals[reg].append({
                "cs": leg["cs"],
                "dep": _icao_to_iata(leg["dep"]), "arr": _icao_to_iata(leg["arr"]),
                "start": leg["start"], "end": leg["end"], "used": False,
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

        # Reschedulings over time: per observed_date, how many flights had their
        # tail change vs the previous nightly snapshot (the "re-planned today" axis).
        reschedulings = _q(conn, """
            WITH snaps AS (
                SELECT o.observed_date, btrim(o.registration) AS reg,
                       LAG(btrim(o.registration)) OVER (
                           PARTITION BY o.flight_date, o.airline, o.flight_number
                           ORDER BY o.observed_date) AS prev_reg
                FROM flight_status_observations o
                JOIN aircraft a ON a.registration = o.registration
                WHERE o.found AND o.registration IS NOT NULL AND a.aircraft_type = %s
            )
            SELECT observed_date,
                   COUNT(*) FILTER (WHERE prev_reg IS NOT NULL AND reg <> prev_reg) AS changes
            FROM snaps GROUP BY observed_date ORDER BY observed_date
        """, [atype])

        # Likely incidents to overlay: widebody arrivals at non-base fields.
        diversions = _q(conn, """
            SELECT btrim(a.registration), f.flight_date, btrim(f.callsign),
                   f.departure_airport_icao, f.arrival_airport_icao
            FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
            WHERE a.aircraft_type = %s AND NOT f.needs_review
              AND f.arrival_airport_icao = ANY(%s)
              AND f.flight_date >= CURRENT_DATE - 21
              AND NOT EXISTS (
                  -- exclude interior phantoms / continued stops: the same
                  -- callsign departs this same airport again shortly after,
                  -- so it wasn't a terminal incident.
                  SELECT 1 FROM flights f2
                  WHERE f2.icao24 = f.icao24
                    AND btrim(f2.callsign) = btrim(f.callsign)
                    AND btrim(f2.departure_airport_icao) = btrim(f.arrival_airport_icao)
                    AND f2.first_seen > f.first_seen
                    AND f2.first_seen < f.last_seen + INTERVAL '6 hours'
              )
            ORDER BY f.first_seen
        """, [atype, list(_DIVERSION_AIRPORTS)])
    finally:
        conn.close()

    ground = {r[0]: r[1] for r in grounding}
    resched = [{"date": r[0].isoformat(), "n": r[1]} for r in reschedulings]
    reliability = None
    if ontime or stab["type"].get(short) or resched:
        reliability = {
            "ontime": [{"status": s, "n": n} for s, n in ontime],
            "hold_by_lead": stab["type"].get(short) or stab["overall"],
            "churn_by_route": stab["route"],
            "reschedulings": resched,
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
        "diversions": [{"reg": r[0], "date": r[1].isoformat(), "callsign": r[2],
                        "dep": r[3], "arr": r[4]} for r in diversions],
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
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/faceplate.css">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>
:root {
  /* Faceplate v1.0 — semantic tokens aliased onto --fp-* (see /faceplate.css) */
  --bg:var(--fp-surface); --surface:var(--fp-bg); --surface2:var(--fp-sage-xl); --border:var(--fp-border); --line:var(--fp-gray);
  --text:var(--fp-body); --text-bright:var(--fp-ink); --muted:var(--fp-muted);
  --accent:var(--fp-sage); --green:var(--fp-dv-4); --amber:var(--fp-dv-3); --red:var(--fp-terra); --purple:var(--fp-dv-5); --cyan:var(--fp-dv-4);
  --accent-dim:var(--fp-sage-tint); --green-dim:color-mix(in srgb,var(--fp-dv-4) 16%,var(--fp-bg)); --amber-dim:color-mix(in srgb,var(--fp-dv-3) 22%,var(--fp-bg)); --red-dim:var(--fp-terra-tint); --purple-dim:color-mix(in srgb,var(--fp-dv-5) 18%,var(--fp-bg));
  --radius:0; --radius-sm:0;
  --mono:var(--fp-font-mono); --sans:var(--fp-font-sans);
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-size:14px; line-height:1.5;
  font-family:var(--sans); -webkit-font-smoothing:antialiased; }
.container { width:96vw; max-width:2000px; margin:0 auto; padding:0 18px 48px; }

/* header / device label */
/* ── Header · Faceplate band (intensity 03): plate + wordmark + label ─────── */
.header { display:flex; align-items:center; gap:14px 22px; flex-wrap:wrap;
  padding:16px 22px; margin:18px 0 24px; }  /* sage band + #fff come from .fp-band */
.brand { display:flex; align-items:center; gap:14px; }
.brand .fp-plate { width:46px; height:46px; flex-shrink:0; }
.brand .fp-plate svg { width:27px; height:27px; display:block; }
.header h1 { font-family:var(--fp-font-sans); font-size:22px; font-weight:800; letter-spacing:-.02em;
  color:#fff; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--fp-sage-tint); }
.model { font-family:var(--fp-font-sans); font-size:10px; letter-spacing:.12em;
  color:rgba(255,255,255,.82); text-transform:uppercase; margin-top:5px; }
.nav { display:flex; gap:18px; flex-wrap:wrap; margin-left:auto; }
.nav a, .nav-link { font-family:var(--fp-font-sans); font-size:11px; font-weight:500; letter-spacing:.1em;
  text-transform:uppercase; color:rgba(255,255,255,.82); text-decoration:none;
  padding:3px 0; border-bottom:2px solid transparent; transition:color .14s, border-color .14s; }
.nav a:hover, .nav-link:hover { color:#fff; border-bottom-color:#fff; text-decoration:none; }
.updated { font-family:var(--fp-font-mono); font-size:10.5px; color:rgba(255,255,255,.82); }

.meta { font-family:var(--sans); font-size:11px; color:var(--muted); margin-bottom:14px; line-height:1.6; }
.meta b { color:var(--text); font-weight:700; }

.controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin-bottom:16px; }
.controls input { background:var(--surface); border:1.5px solid var(--fp-ink); border-radius:0;
  color:var(--text-bright); padding:8px 14px; font-size:13px; width:300px; font-family:var(--sans); }
.controls input:focus { outline:none; border-color:var(--accent); }
.controls input::placeholder { color:var(--muted); }
.legend { display:flex; gap:13px; flex-wrap:wrap; font-family:var(--sans); font-size:10px; color:var(--muted);
  text-transform:uppercase; letter-spacing:.3px; align-items:center; }
.legend .sw { display:inline-block; width:12px; height:12px; border-radius:0; vertical-align:middle; margin-right:5px; }

/* gantt */
.gantt { overflow-x:auto; border:1.5px solid var(--fp-ink); border-radius:0; background:var(--surface); padding:14px 14px 6px; }
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
.empty { color:var(--muted); padding:30px; text-align:center; font-family:var(--sans); font-size:12px; }
footer { text-align:center; padding:26px 0 10px; font-family:var(--sans); font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }
footer a { color:var(--muted); text-decoration:none; }
footer a:hover { color:var(--text); }

/* flight detail modal */
.modal-bg { display:none; position:fixed; inset:0; background:rgba(36,33,27,0.5); z-index:100; justify-content:center; align-items:center; padding:16px; }
.modal-bg.show { display:flex; }
.modal { background:var(--surface); border:1.5px solid var(--fp-ink); border-radius:0; padding:22px; width:100%; max-width:460px; max-height:88vh; overflow-y:auto; position:relative; }
.modal h3 { font-size:15px; color:var(--text-bright); margin-bottom:3px; padding-right:24px; }
.modal .sub { font-size:12px; color:var(--muted); margin-bottom:16px; }
.modal .close { position:absolute; top:14px; right:18px; cursor:pointer; color:var(--muted); font-size:22px; line-height:1; border:none; background:none; }
.modal .close:hover { color:var(--text-bright); }
.reassign-banner { background:var(--amber-dim); border:1.5px solid var(--amber); color:color-mix(in srgb,var(--fp-dv-3) 72%,var(--fp-ink)); border-radius:0; padding:9px 11px; font-size:12px; margin-bottom:16px; }
.conf-chip { display:flex; align-items:center; gap:12px; border:1.5px solid var(--fp-ink); border-radius:0; padding:10px 12px; margin-bottom:16px; }
.conf-chip .cp { font-family:var(--mono); font-size:22px; font-weight:700; line-height:1; flex-shrink:0; }
.conf-chip .ct { font-size:12px; color:var(--text-bright); line-height:1.45; }
.conf-chip .cn { color:var(--muted); font-size:11px; font-family:var(--sans); }
.conf-chip.cg { background:var(--green-dim); border-color:var(--green); } .conf-chip.cg .cp { color:var(--green); }
.conf-chip.ca { background:var(--amber-dim); border-color:var(--amber); } .conf-chip.ca .cp { color:color-mix(in srgb,var(--fp-dv-3) 72%,var(--fp-ink)); }
.conf-chip.cr { background:var(--red-dim); border-color:var(--red); } .conf-chip.cr .cp { color:var(--fp-terra-deep); }
.det-grid { display:grid; grid-template-columns:auto 1fr; gap:7px 16px; font-size:12px; margin-bottom:18px; }
.det-grid .k { color:var(--muted); white-space:nowrap; font-family:var(--sans); font-size:11px; text-transform:uppercase; letter-spacing:.3px; }
.det-grid .v { color:var(--text-bright); }
.hist-title { font-family:var(--sans); font-size:10px; text-transform:uppercase; letter-spacing:1px; color:var(--muted); margin:0 0 10px; }
.hist-row { display:flex; align-items:center; gap:10px; font-size:12px; padding:6px 0; border-bottom:1.5px solid var(--border); }
.hist-row:last-child { border-bottom:none; }
.hist-row .obs { width:96px; color:var(--muted); flex-shrink:0; font-family:var(--mono); font-size:11px; }
.hist-row .reg { font-family:var(--mono); font-weight:700; color:var(--text-bright); min-width:64px; }
.hist-row.changed .reg { color:var(--amber); }
.hist-row .tag { font-size:10px; color:var(--muted); }
</style>
</head>
<body class="fp">
<div class="container">
  <div class="header fp-band">
    <div class="brand">
      <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
      <div>
        <h1>Upcoming <span>Schedule</span></h1>
        <div class="fp-label model">SCHED &middot; Airframe Rotation</div>
      </div>
    </div>
    <nav class="nav">
      <a class="nav-link" href="/book">Book</a>
      <a class="nav-link" href="/insights">Insights</a>
      <a class="nav-link" href="/fleet">Fleet DB</a>
      <a class="nav-link" href="/">&larr; Monitor</a>
    </nav>
  </div>
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
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/faceplate.css">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>
:root {
  --bg:var(--fp-surface); --surface:var(--fp-bg); --surface2:var(--fp-sage-xl); --border:var(--fp-border); --line:var(--fp-gray);
  --text:var(--fp-body); --text-bright:var(--fp-ink); --muted:var(--fp-muted);
  --accent:var(--fp-sage); --green:var(--fp-dv-4); --amber:var(--fp-dv-3); --red:var(--fp-terra); --purple:var(--fp-dv-5); --cyan:var(--fp-dv-4);
  --accent-dim:var(--fp-sage-tint); --green-dim:color-mix(in srgb,var(--fp-dv-4) 16%,var(--fp-bg)); --amber-dim:color-mix(in srgb,var(--fp-dv-3) 22%,var(--fp-bg)); --red-dim:var(--fp-terra-tint); --purple-dim:color-mix(in srgb,var(--fp-dv-5) 18%,var(--fp-bg));
  --mono:var(--fp-font-mono); --sans:var(--fp-font-sans);
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-size:14px; line-height:1.5; font-family:var(--sans); -webkit-font-smoothing:antialiased; }
.container { width:96vw; max-width:1100px; margin:0 auto; padding:0 18px 48px; }
/* ── Header · Faceplate band (intensity 03): plate + wordmark + label ─────── */
.header { display:flex; align-items:center; gap:14px 22px; flex-wrap:wrap;
  padding:16px 22px; margin:18px 0 24px; }  /* sage band + #fff come from .fp-band */
.brand { display:flex; align-items:center; gap:14px; }
.brand .fp-plate { width:46px; height:46px; flex-shrink:0; }
.brand .fp-plate svg { width:27px; height:27px; display:block; }
.header h1 { font-family:var(--fp-font-sans); font-size:22px; font-weight:800; letter-spacing:-.02em;
  color:#fff; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--fp-sage-tint); }
.model { font-family:var(--fp-font-sans); font-size:10px; letter-spacing:.12em;
  color:rgba(255,255,255,.82); text-transform:uppercase; margin-top:5px; }
.nav { display:flex; gap:18px; flex-wrap:wrap; margin-left:auto; }
.nav a, .nav-link { font-family:var(--fp-font-sans); font-size:11px; font-weight:500; letter-spacing:.1em;
  text-transform:uppercase; color:rgba(255,255,255,.82); text-decoration:none;
  padding:3px 0; border-bottom:2px solid transparent; transition:color .14s, border-color .14s; }
.nav a:hover, .nav-link:hover { color:#fff; border-bottom-color:#fff; text-decoration:none; }
.updated { font-family:var(--fp-font-mono); font-size:10.5px; color:rgba(255,255,255,.82); }
.meta { font-family:var(--sans); font-size:11px; color:var(--muted); margin-bottom:14px; line-height:1.6; }
.meta b { color:var(--text); font-weight:700; }
/* segmented control = Faceplate .fp-seg; reset <button> UA borders + map .active */
.fp-seg { margin-bottom:12px; }
.fp-seg > button { border-top:0; border-bottom:0; border-left:0; color:var(--fp-ink); cursor:pointer; }
.fp-seg > .active { background:var(--fp-ink); color:#fff; }
.searchbar { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:8px; }
.searchbar input { background:var(--surface); border:1.5px solid var(--fp-ink); padding:9px 14px; font-size:14px; font-family:var(--sans); color:var(--text-bright); width:200px; text-transform:uppercase; }
.searchbar input::placeholder { color:var(--muted); text-transform:none; }
.searchbar input:focus { outline:none; border-color:var(--accent); }
.fp-btn:hover { opacity:.88; }  /* search button uses .fp-btn--solid */
.hint { font-family:var(--sans); font-size:11px; color:var(--muted); margin-bottom:18px; }
.results { display:flex; flex-direction:column; gap:8px; }
.fcard { display:flex; align-items:center; gap:14px; border:1.5px solid var(--fp-ink); background:var(--surface); padding:12px 14px; cursor:pointer; transition:border-color .12s; }
.fcard:hover { border-color:var(--accent); }
.fcard .when { width:96px; font-family:var(--mono); font-size:11px; color:var(--muted); flex-shrink:0; line-height:1.5; }
.fcard .when b { display:block; color:var(--text-bright); font-size:13px; }
.fcard .route { flex:1; min-width:0; }
.fcard .route .pair { font-size:15px; font-weight:700; color:var(--text-bright); }
.fcard .route .sub { font-family:var(--sans); font-size:11px; color:var(--muted); margin-top:1px; }
.fcard .tail { font-family:var(--mono); font-weight:700; font-size:13px; color:var(--text-bright); display:flex; align-items:center; gap:5px; min-width:90px; }
.fcard .tail .star { color:var(--amber); }
.tbadge { font-family:var(--mono); font-size:9px; font-weight:700; padding:1px 5px; color:var(--text-bright); }
.tbadge.t748 { background:var(--accent); } .tbadge.t388 { background:var(--green); }
.tbadge.t359 { background:var(--purple); } .tbadge.tother { background:var(--surface2); color:var(--muted); }
.miniconf { display:flex; flex-direction:column; align-items:center; justify-content:center; min-width:62px; padding:5px 8px; border:1.5px solid var(--fp-ink); flex-shrink:0; }
.miniconf .p { font-family:var(--mono); font-weight:700; font-size:17px; line-height:1; }
.miniconf .cn { font-family:var(--sans); font-size:9px; text-transform:uppercase; letter-spacing:.4px; color:var(--muted); margin-top:3px; }
.miniconf.cg { background:var(--green-dim); border-color:var(--green); } .miniconf.cg .p { color:var(--green); }
.miniconf.ca { background:var(--amber-dim); border-color:var(--amber); } .miniconf.ca .p { color:color-mix(in srgb,var(--fp-dv-3) 72%,var(--fp-ink)); }
.miniconf.cr { background:var(--red-dim); border-color:var(--red); } .miniconf.cr .p { color:var(--fp-terra-deep); }
.empty { color:var(--muted); padding:30px; text-align:center; font-family:var(--sans); font-size:12px; border:1.5px dashed var(--border); }
footer { text-align:center; padding:26px 0 10px; font-family:var(--sans); font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }
footer a { color:var(--muted); text-decoration:none; } footer a:hover { color:var(--text); }
.modal-bg { display:none; position:fixed; inset:0; background:rgba(36,33,27,0.5); z-index:100; justify-content:center; align-items:center; padding:16px; }
.modal-bg.show { display:flex; }
.modal { background:var(--surface); border:1.5px solid var(--fp-ink); padding:22px; width:100%; max-width:460px; max-height:88vh; overflow-y:auto; position:relative; }
.modal h3 { font-size:15px; color:var(--text-bright); margin-bottom:3px; padding-right:24px; }
.modal .sub { font-size:12px; color:var(--muted); margin-bottom:16px; }
.modal .close { position:absolute; top:14px; right:18px; cursor:pointer; color:var(--muted); font-size:22px; line-height:1; border:none; background:none; }
.modal .close:hover { color:var(--text-bright); }
.reassign-banner { background:var(--amber-dim); border:1.5px solid var(--amber); color:color-mix(in srgb,var(--fp-dv-3) 72%,var(--fp-ink)); padding:9px 11px; font-size:12px; margin-bottom:16px; }
.conf-chip { display:flex; align-items:center; gap:12px; border:1.5px solid var(--fp-ink); padding:10px 12px; margin-bottom:16px; }
.conf-chip .cp { font-family:var(--mono); font-size:22px; font-weight:700; line-height:1; flex-shrink:0; }
.conf-chip .ct { font-size:12px; color:var(--text-bright); line-height:1.45; }
.conf-chip .cn { color:var(--muted); font-size:11px; font-family:var(--sans); }
.conf-chip.cg { background:var(--green-dim); border-color:var(--green); } .conf-chip.cg .cp { color:var(--green); }
.conf-chip.ca { background:var(--amber-dim); border-color:var(--amber); } .conf-chip.ca .cp { color:color-mix(in srgb,var(--fp-dv-3) 72%,var(--fp-ink)); }
.conf-chip.cr { background:var(--red-dim); border-color:var(--red); } .conf-chip.cr .cp { color:var(--fp-terra-deep); }
.det-grid { display:grid; grid-template-columns:auto 1fr; gap:7px 16px; font-size:12px; margin-bottom:18px; }
.det-grid .k { color:var(--muted); white-space:nowrap; font-family:var(--sans); font-size:11px; text-transform:uppercase; letter-spacing:.3px; }
.det-grid .v { color:var(--text-bright); }
.hist-title { font-family:var(--sans); font-size:10px; text-transform:uppercase; letter-spacing:1px; color:var(--muted); margin:0 0 10px; }
.hist-row { display:flex; align-items:center; gap:10px; font-size:12px; padding:6px 0; border-bottom:1.5px solid var(--border); }
.hist-row:last-child { border-bottom:none; }
.hist-row .obs { width:96px; color:var(--muted); flex-shrink:0; font-family:var(--mono); font-size:11px; }
.hist-row .reg { font-family:var(--mono); font-weight:700; color:var(--text-bright); min-width:64px; }
.hist-row.changed .reg { color:var(--amber); }
.hist-row .tag { font-size:10px; color:var(--muted); }
</style>
</head>
<body class="fp">
<div class="container">
  <div class="header fp-band">
    <div class="brand">
      <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
      <div>
        <h1>Catch a <span>Tail</span></h1>
        <div class="fp-label model">BOOK &middot; Airframe Finder</div>
      </div>
    </div>
    <nav class="nav">
      <a class="nav-link" href="/schedule">Schedule</a>
      <a class="nav-link" href="/insights">Insights</a>
      <a class="nav-link" href="/fleet">Fleet DB</a>
      <a class="nav-link" href="/">&larr; Monitor</a>
    </nav>
  </div>
    <div class="meta">Find an upcoming flight by <b>airframe</b> or <b>route</b>, with the currently published tail and a measured chance it still holds by departure. Schedule is published ~4 days out, so check back closer to your date.</div>
  <div class="fp-seg">
    <button id="m-tail" class="active" onclick="setMode('tail')">By tail</button>
    <button id="m-route" onclick="setMode('route')">By route</button>
  </div>
  <div class="searchbar">
    <input id="tail-in" type="text" placeholder="registration, e.g. D-ABYN">
    <input id="dep-in" type="text" placeholder="from, e.g. FRA" style="display:none">
    <input id="arr-in" type="text" placeholder="to, e.g. HND" style="display:none">
    <button class="fp-btn fp-btn--solid" onclick="search()">Search</button>
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


_INSIGHTS_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Fleet Insights | LH Fleet</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
<link rel="stylesheet" href="/faceplate.css">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<style>
:root {
  --bg:var(--fp-surface); --surface:var(--fp-bg); --surface2:var(--fp-sage-xl); --border:var(--fp-border); --line:var(--fp-gray);
  --text:var(--fp-body); --text-bright:var(--fp-ink); --muted:var(--fp-muted);
  --accent:var(--fp-sage); --green:var(--fp-dv-4); --amber:var(--fp-dv-3); --red:var(--fp-terra); --purple:var(--fp-dv-5); --cyan:var(--fp-dv-4);
  --accent-dim:var(--fp-sage-tint); --green-dim:color-mix(in srgb,var(--fp-dv-4) 16%,var(--fp-bg)); --amber-dim:color-mix(in srgb,var(--fp-dv-3) 22%,var(--fp-bg)); --red-dim:var(--fp-terra-tint); --purple-dim:color-mix(in srgb,var(--fp-dv-5) 18%,var(--fp-bg));
  --mono:var(--fp-font-mono); --sans:var(--fp-font-sans);
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-size:14px; line-height:1.5; font-family:var(--sans); -webkit-font-smoothing:antialiased; }
.container { width:96vw; max-width:1180px; margin:0 auto; padding:0 18px 48px; }
/* ── Header · Faceplate band (intensity 03): plate + wordmark + label ─────── */
.header { display:flex; align-items:center; gap:14px 22px; flex-wrap:wrap;
  padding:16px 22px; margin:18px 0 24px; }  /* sage band + #fff come from .fp-band */
.brand { display:flex; align-items:center; gap:14px; }
.brand .fp-plate { width:46px; height:46px; flex-shrink:0; }
.brand .fp-plate svg { width:27px; height:27px; display:block; }
.header h1 { font-family:var(--fp-font-sans); font-size:22px; font-weight:800; letter-spacing:-.02em;
  color:#fff; text-transform:uppercase; line-height:1; }
.header h1 span { color:var(--fp-sage-tint); }
.model { font-family:var(--fp-font-sans); font-size:10px; letter-spacing:.12em;
  color:rgba(255,255,255,.82); text-transform:uppercase; margin-top:5px; }
.nav { display:flex; gap:18px; flex-wrap:wrap; margin-left:auto; }
.nav a, .nav-link { font-family:var(--fp-font-sans); font-size:11px; font-weight:500; letter-spacing:.1em;
  text-transform:uppercase; color:rgba(255,255,255,.82); text-decoration:none;
  padding:3px 0; border-bottom:2px solid transparent; transition:color .14s, border-color .14s; }
.nav a:hover, .nav-link:hover { color:#fff; border-bottom-color:#fff; text-decoration:none; }
.updated { font-family:var(--fp-font-mono); font-size:10.5px; color:rgba(255,255,255,.82); }
.controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin-bottom:8px; }
/* segmented control = Faceplate .fp-seg; <a> children just need link resets */
.fp-seg > a { color:var(--fp-ink); text-decoration:none; }
.fp-seg > .active { background:var(--fp-ink); color:#fff; }
.controls input { background:var(--surface); border:1.5px solid var(--fp-ink); padding:8px 14px; font-size:13px; font-family:var(--sans); color:var(--text-bright); width:190px; text-transform:uppercase; }
.controls input::placeholder { color:var(--muted); text-transform:none; }
.controls input:focus { outline:none; border-color:var(--accent); }
.meta { font-family:var(--sans); font-size:11px; color:var(--muted); margin-bottom:16px; line-height:1.6; }
.meta b { color:var(--text); font-weight:700; }
.module { border:1.5px solid var(--fp-ink); background:var(--surface); margin-bottom:16px; }
.modhead { font-family:var(--sans); font-size:11px; text-transform:uppercase; letter-spacing:1px; color:var(--text-bright); padding:11px 14px; border-bottom:1.5px solid var(--border); background:var(--surface2); }
.modhead .sub { color:var(--muted); text-transform:none; letter-spacing:0; font-size:10px; margin-left:8px; }
.modbody { padding:14px; }
.cols { display:grid; grid-template-columns:1fr 1fr; gap:22px; }
@media (max-width:760px){ .cols { grid-template-columns:1fr; } }
table { width:100%; border-collapse:collapse; font-size:12px; }
th { text-align:left; font-family:var(--sans); font-size:9.5px; text-transform:uppercase; letter-spacing:.5px; color:var(--muted); padding:5px 8px; border-bottom:1.5px solid var(--border); }
td { padding:5px 8px; border-bottom:1px solid var(--surface2); color:var(--text-bright); }
td.r, th.r { text-align:right; }
tr:last-child td { border-bottom:none; }
.tail-reg { font-family:var(--mono); font-weight:700; }
.star { color:var(--amber); }
.subhead { font-family:var(--sans); font-size:10px; text-transform:uppercase; letter-spacing:.6px; color:var(--muted); margin:4px 0 9px; }
/* reschedulings chart */
.rs-note { font-family:var(--sans); font-size:10px; color:var(--muted); margin-bottom:12px; }
.rs-chart { display:flex; align-items:flex-end; gap:8px; padding:4px 2px; }
.rs-col { flex:1; text-align:center; min-width:34px; }
.rs-n { font-family:var(--mono); font-size:11px; font-weight:700; color:var(--text-bright); margin-bottom:4px; }
.rs-barwrap { height:150px; display:flex; flex-direction:column; justify-content:flex-end; align-items:center; position:relative; }
.rs-bar { width:62%; max-width:46px; background:var(--fp-dv-1); min-height:2px; }
.rs-incident .rs-bar { background:var(--fp-dv-2); }
.rs-mark { color:var(--red); font-size:11px; margin-bottom:2px; }
.rs-date { font-family:var(--mono); font-size:9px; color:var(--muted); margin-top:6px; white-space:nowrap; }
.rs-base .rs-bar { background:var(--fp-dv-6); }
.legend { font-family:var(--sans); font-size:10px; color:var(--muted); margin-top:10px; display:flex; gap:14px; flex-wrap:wrap; }
.legend .sw { display:inline-block; width:10px; height:10px; vertical-align:middle; margin-right:4px; }
/* on-time stacked bar */
.ot-bar { display:flex; height:22px; border:1.5px solid var(--fp-ink); margin:4px 0 8px; }
.ot-seg { height:100%; }
.ot-list { font-family:var(--sans); font-size:11px; color:var(--muted); }
.empty { color:var(--muted); padding:22px; text-align:center; font-family:var(--sans); font-size:12px; }
footer { text-align:center; padding:26px 0 10px; font-family:var(--sans); font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; }
footer a { color:var(--muted); text-decoration:none; } footer a:hover { color:var(--text); }
</style>
</head>
<body class="fp">
<div class="container">
  <div class="header fp-band">
    <div class="brand">
      <span class="fp-plate"><svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true"><path d="M21,16V14L13,9V3.5A1.5,1.5 0 0,0 11.5,2A1.5,1.5 0 0,0 10,3.5V9L2,14V16L10,13.5V19L8,20.5V22L11.5,21L15,22V20.5L13,19V13.5L21,16Z"/></svg></span>
      <div>
        <h1>Fleet <span>Insights</span></h1>
        <div class="fp-label model">INSIGHTS &middot; Fleet Analytics</div>
      </div>
    </div>
    <nav class="nav">
      <a class="nav-link" href="/book">Book</a>
      <a class="nav-link" href="/schedule">Schedule</a>
      <a class="nav-link" href="/fleet">Fleet DB</a>
      <a class="nav-link" href="/">&larr; Monitor</a>
    </nav>
  </div>
    <div class="controls">
    <div class="fp-seg">
      <a href="/insights?type=B748" id="seg-B748">747-8</a>
      <a href="/insights?type=A388" id="seg-A388">A380</a>
    </div>
    <input id="tail-in" type="text" placeholder="filter one tail, e.g. D-ABYN">
  </div>
  <div class="meta" id="meta">Loading&hellip;</div>
  <div id="body"></div>
</div>
<footer>
  <a href="/impressum">Impressum</a> <span style="margin:0 6px">&middot;</span> <a href="/datenschutz">Datenschutz</a>
</footer>
<script>
const $ = id => document.getElementById(id);
const P = new URLSearchParams(location.search);
const TYPE = (P.get('type') || 'B748').toUpperCase();
const REG = (P.get('reg') || '').toUpperCase();

function fmtMin(m){ if(m==null) return '—'; return Math.floor(m/60)+'h'+String(m%60).padStart(2,'0'); }
function shortDate(iso){ if(!iso) return ''; return new Date(iso+'T00:00:00Z').toLocaleDateString('en-GB',{day:'2-digit',month:'short',timeZone:'UTC'}); }
function esc(s){ return (s==null?'':String(s)).replace(/"/g,'&quot;'); }

function routesTable(routes){
  if(!routes.length) return '<div class="empty">No routes yet.</div>';
  let h='<table><tr><th>Route</th><th class="r">Flights</th><th class="r">Median time</th></tr>';
  routes.slice(0,15).forEach(r=>{ h+='<tr><td>'+r.route+'</td><td class="r">'+r.n+'</td><td class="r">'+fmtMin(r.median_min)+'</td></tr>'; });
  return h+'</table>';
}
function rotationList(rot){
  if(!rot.length) return '<div class="empty">Not enough sequence data.</div>';
  let h='<table><tr><th>After</th><th>most often flies</th><th class="r">n</th></tr>';
  rot.slice(0,14).forEach(r=>{ h+='<tr><td>'+r.from+'</td><td>'+r.to+'</td><td class="r">'+r.n+'</td></tr>'; });
  return h+'</table>';
}
function airframesTable(af){
  if(!af.length) return '<div class="empty">No airframes.</div>';
  let h='<table><tr><th>Tail</th><th class="r">Legs</th><th class="r">Hours</th><th class="r">Longest gap</th><th class="r">Last seen</th></tr>';
  af.forEach(a=>{
    h+='<tr><td class="tail-reg">'+(a.watch?'<span class="star">&#9733;</span>':'')+a.reg+'</td>'
      +'<td class="r">'+a.legs+'</td><td class="r">'+a.hours+'h</td>'
      +'<td class="r">'+(a.max_ground_days!=null?a.max_ground_days+'d':'—')+'</td>'
      +'<td class="r">'+shortDate(a.last)+'</td></tr>';
  });
  return h+'</table>';
}
function reschedChart(rel, diversions){
  const data = (rel && rel.reschedulings) || [];
  if(!data.length) return '<div class="empty">No schedule data collected for this type yet.</div>';
  const byDate = {};
  (diversions||[]).forEach(d=>{ (byDate[d.date]=byDate[d.date]||[]).push(d); });
  const max = Math.max(1, ...data.map(d=>d.n));
  let bars='';
  data.forEach((d,i)=>{
    const px = Math.round(d.n/max*148);
    const divs = byDate[d.date]||[];
    const isDiv = divs.length>0;
    const isBase = i===0 && d.n===0;
    const tip = shortDate(d.date)+': '+d.n+' reschedulings'
      + (isDiv ? '  \\u00b7  '+divs.map(x=>x.reg+' '+x.dep+'\\u2192'+x.arr).join(', ') : '')
      + (isBase ? '  (collection start)' : '');
    bars += '<div class="rs-col'+(isDiv?' rs-incident':'')+(isBase?' rs-base':'')+'" title="'+esc(tip)+'">'
      + '<div class="rs-n">'+d.n+'</div>'
      + '<div class="rs-barwrap">'+(isDiv?'<div class="rs-mark">&#9670;</div>':'')
      + '<div class="rs-bar" style="height:'+Math.max(2,px)+'px"></div></div>'
      + '<div class="rs-date">'+shortDate(d.date)+'</div></div>';
  });
  return '<div class="rs-chart">'+bars+'</div>'
    + '<div class="legend"><span><span class="sw" style="background:var(--accent)"></span>reschedulings/day</span>'
    + '<span><span class="sw" style="background:var(--red)"></span>&#9670; likely incident (diversion)</span></div>';
}
function ontimeBar(rel){
  const ot = (rel && rel.ontime) || [];
  if(!ot.length) return '';
  const total = ot.reduce((s,x)=>s+x.n,0) || 1;
  const col = s => s==='ONTIME'||s==='EARLY'||s==='ARRIVED' ? 'var(--fp-dv-1)' : s==='DELAYED' ? 'var(--fp-dv-3)' : 'var(--fp-dv-6)';
  let seg='', list=[];
  ot.forEach(x=>{ seg+='<div class="ot-seg" style="width:'+(x.n/total*100)+'%;background:'+col(x.status)+'" title="'+esc(x.status+': '+x.n)+'"></div>'; list.push(x.status+' '+x.n); });
  return '<div class="subhead">On-time mix (latest snapshot per flight)</div><div class="ot-bar">'+seg+'</div><div class="ot-list">'+list.join('  \\u00b7  ')+'</div>';
}
function holdTable(rel){
  const hb = rel && rel.hold_by_lead;
  if(!hb) return '';
  const leads = Object.keys(hb).map(Number).sort((a,b)=>a-b);
  if(!leads.length) return '';
  let h='<div class="subhead" style="margin-top:14px">Tail holds by lead (how often the published tail survives to departure)</div>';
  h+='<table><tr><th>Days out</th><th class="r">Holds</th><th class="r">n</th></tr>';
  leads.forEach(l=>{ const c=hb[l]; h+='<tr><td>'+l+'d</td><td class="r">'+Math.round(c.p*100)+'%</td><td class="r">'+c.n+'</td></tr>'; });
  return h+'</table>';
}

function module(title, sub, inner){
  return '<div class="module"><div class="modhead">'+title+(sub?'<span class="sub">'+sub+'</span>':'')+'</div><div class="modbody">'+inner+'</div></div>';
}

async function init(){
  $('seg-'+TYPE) && $('seg-'+TYPE).classList.add('active');
  if(REG) $('tail-in').value = REG;
  let d;
  try { d = await (await fetch('/api/insights?type='+encodeURIComponent(TYPE)+(REG?'&reg='+encodeURIComponent(REG):''))).json(); }
  catch(e){ $('body').innerHTML='<div class="empty">Failed to load.</div>'; $('meta').textContent=''; return; }
  if(d.error){ $('body').innerHTML='<div class="empty">'+d.error+'</div>'; return; }
  const m=d.meta||{};
  $('meta').innerHTML = '<b>'+(d.type||'')+'</b>'+(REG?' &middot; '+REG:'')+' &middot; <b>'+(m.flights||0)+'</b> flights &middot; <b>'+(m.tails||0)+'</b> tails &middot; '+shortDate(m.first)+' &ndash; '+shortDate(m.last);

  let html = '';
  html += module('Schedule reliability', 'reschedulings &amp; incidents',
      '<div class="rs-note">Bars = tails reassigned each day vs the night before. Diamonds mark likely incidents (widebody at a non-base field) &mdash; watch for a spike <i>after</i> one. Correlation, not proof; early days are thin.</div>'
      + reschedChart(d.reliability, d.diversions)
      + '<div style="margin-top:18px">' + ontimeBar(d.reliability) + holdTable(d.reliability) + '</div>');
  html += module('Routes &amp; rotation', 'where this '+(REG||d.type)+' flies',
      '<div class="cols"><div><div class="subhead">Top routes</div>'+routesTable(d.routes)+'</div>'
      + '<div><div class="subhead">Typical next leg</div>'+rotationList(d.rotation)+'</div></div>');
  html += module('Per-airframe profiles', 'utilisation &amp; groundings',
      airframesTable(d.airframes));
  $('body').innerHTML = html;
}
$('tail-in').addEventListener('keydown', e=>{
  if(e.key==='Enter'){ const r=e.target.value.trim().toUpperCase();
    location.href='/insights?type='+encodeURIComponent(TYPE)+(r?'&reg='+encodeURIComponent(r):''); }
});
init();
</script>
</body>
</html>"""


@app.route("/insights")
def insights():
    return render_template_string(_INSIGHTS_HTML)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
