"""
LHLogging monitoring dashboard.
Serves a single-page HTML dashboard and a /api/stats JSON endpoint.
"""
import os
import random
import re
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
/* square native form controls — divs/spans set radius:0 in their own rules */
input, select, button, textarea { border-radius: 0 !important; }
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
  background: var(--surface); border: 1.5px solid var(--fp-ink); border-radius: 0;
  color: var(--text-bright); padding: 7px 12px; font-size: 13px; flex: 1; min-width: 200px;
  outline: none;
}
.toolbar input[type="text"]:focus { border-color: var(--accent); }
.toolbar select {
  background: var(--surface); border: 1.5px solid var(--fp-ink); border-radius: 0;
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
.fleet-table .hex { font-family: var(--fp-font-mono); font-size: 11px; color: var(--muted); }
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
  display: none; background: var(--red-dim); border: 1.5px solid var(--red);
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
/* square native form controls — divs/spans set radius:0 in their own rules */
input, select, button, textarea { border-radius: 0 !important; }
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
.route-track { flex: 1; height: 18px; background: var(--surface2); border-radius: 0; overflow: hidden; }
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
  display: none; background: var(--red-dim); border: 1.5px solid var(--red);
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
    item('ICAO24', '<span style="font-family:var(--fp-font-mono)">' + info.icao24 + '</span>') +
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
/* square native form controls — divs/spans set radius:0 in their own rules */
input, select, button, textarea { border-radius: 0 !important; }
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
  background: var(--surface); border: 1.5px solid var(--fp-ink); border-radius: 0;
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
  cursor: text; padding: 2px 4px; border-radius: 0; min-width: 30px;
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
  border-radius: 0; font-size: 13px; font-weight: 500; z-index: 200;
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
_CANON_SHORT = {"B748": "748", "A388": "388", "B788": "788", "B789": "789",
                "B78X": "78X", "A359": "359", "A35K": "35K"}
# Only these airframe types are shown on the schedule. Mirrors the fetcher's
# FIS_SEED_TYPES, incl. the not-yet-delivered variants (B788/B78X/A35K) so they
# appear the day the collector first sees one.
_SCHEDULE_TYPES = ("B748", "A388", "B788", "B789", "B78X", "A359", "A35K")
# Tails the user is most interested in — pinned to the top and highlighted.
# A watched tail stays visible even when its type is hidden via the checkboxes.
_WATCH_TAILS = ("D-ABYN", "D-AIMH", "D-AIXL", "D-ABPU")
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


# ── Allegris cabin detection ───────────────────────────────────────────
# FIS reports aircraftInfo.allegris per observation (kept in `raw`). A tail
# counts as Allegris if any recent observation says so: retrofits only ever
# add the cabin, and the rare one-off false (a glitchy short-haul leg) is
# ignored. Derived at read time from raw — no migration, and it backfills
# from the start of collection. TTL cache keeps the per-request cost flat.
_ALLEGRIS_TTL_S = 600
_allegris_cache = {"ts": 0.0, "tails": frozenset()}


def _allegris_tails(conn):
    """Registrations whose cabin is Allegris per recent FIS payloads."""
    nowts = datetime.now(timezone.utc).timestamp()
    if nowts - _allegris_cache["ts"] < _ALLEGRIS_TTL_S:
        return _allegris_cache["tails"]
    try:
        rows = _q(conn, """
            SELECT DISTINCT btrim(registration)
            FROM flight_status_observations
            WHERE found AND registration IS NOT NULL
              AND (raw->'aircraftInfo'->>'allegris')::boolean
              AND observed_at >= NOW() - INTERVAL '45 days'
        """)
    except Exception:
        return _allegris_cache["tails"]  # stale beats a 500 mid-request
    _allegris_cache.update(ts=nowts, tails=frozenset(r[0] for r in rows))
    return _allegris_cache["tails"]


# ── Cabin configuration ────────────────────────────────────────────────
# FIS also reports aircraftInfo.seatConfig per observation (kept in `raw`),
# e.g. "F8C80E32M244" = First 8 / Business (C) 80 / Premium Eco (E) 32 /
# Economy (M) 244; absent cabins are simply omitted. The cabin belongs to the
# airframe, so we keep the latest string per tail — same read-time / TTL
# pattern as the Allegris set, and retrofits show up on their own.
_SEAT_RE = re.compile(r"([FCEM])(\d+)")
_CABIN_TTL_S = 600
_cabin_cache = {"ts": 0.0, "map": {}}


def _parse_seat_config(s):
    """'F8C80E32M244' -> {'F':8,'C':80,'E':32,'M':244}; None when unknown."""
    cabins = {k: int(v) for k, v in _SEAT_RE.findall(s or "")}
    return cabins or None


def _cabin_configs(conn):
    """Registration -> latest seatConfig string per recent FIS payloads."""
    nowts = datetime.now(timezone.utc).timestamp()
    if nowts - _cabin_cache["ts"] < _CABIN_TTL_S:
        return _cabin_cache["map"]
    try:
        rows = _q(conn, """
            SELECT DISTINCT ON (btrim(registration))
                   btrim(registration), raw->'aircraftInfo'->>'seatConfig'
            FROM flight_status_observations
            WHERE found AND registration IS NOT NULL
              AND raw->'aircraftInfo'->>'seatConfig' IS NOT NULL
              AND observed_at >= NOW() - INTERVAL '45 days'
            ORDER BY btrim(registration), observed_at DESC
        """)
    except Exception:
        return _cabin_cache["map"]  # stale beats a 500 mid-request
    _cabin_cache.update(ts=nowts, map={r[0]: r[1] for r in rows})
    return _cabin_cache["map"]


def _merge_hold(cells):
    """n-weighted combination of several {lead: {p,n}} stability dicts — used
    when an insights tab spans a type family. None/empty members are skipped."""
    acc = defaultdict(lambda: [0.0, 0])
    for cell in cells:
        for lead, v in (cell or {}).items():
            acc[lead][0] += v["p"] * v["n"]
            acc[lead][1] += v["n"]
    return {lead: {"p": s / n, "n": n} for lead, (s, n) in acc.items() if n}


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
        ORDER BY o.observed_at
    """)
    groups = defaultdict(dict)  # (fdate, airline, fnum) -> {lead: snap}
    # Rows come ordered by observed_at, so with several passes per day
    # (migration 009) the last write per lead below is the day's latest view.
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
    (its *current* assignment) or a route — dep/arr take a single IATA code or a
    list of alternatives. The DISTINCT ON collapses to the newest snapshot first,
    then reg/route filter on that — so a flight reassigned away from a tail no
    longer shows under it. Ordered by scheduled departure."""
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
        outer.append("dep = ANY(%s)"); params.append([dep] if isinstance(dep, str) else list(dep))
    if arr:
        outer.append("arr = ANY(%s)"); params.append([arr] if isinstance(arr, str) else list(arr))
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
            ORDER BY o.flight_date, o.flight_number, o.observed_at DESC
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
                a.aircraft_type, a.icao24, o.seed_type, o.dep_airport_iata, o.arr_airport_iata,
                o.dep_scheduled, o.arr_scheduled, o.overall_status,
                o.prev_airline, o.prev_flight_number,
                o.raw->'legs'->0->>'flightDuration'
            FROM flight_status_observations o
            JOIN aircraft a ON a.registration = o.registration
            WHERE o.found AND o.registration IS NOT NULL AND o.dep_scheduled IS NOT NULL
              AND o.flight_date >= CURRENT_DATE - 1
              AND a.aircraft_type = ANY(%s)
            ORDER BY o.flight_date, o.flight_number, o.observed_at DESC
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
        alleg = _allegris_tails(conn)
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
    icao24_by_reg = {}
    starts, ends = [], []
    for (fdate, airline, fnum, reg, atype, icao24, seed, dep, arr,
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
        icao24_by_reg[reg] = icao24

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
    type_order = {"748": 0, "388": 1, "788": 2, "789": 3, "78X": 4, "359": 5, "35K": 6}
    airframes = [
        {"reg": reg, "type": types[reg], "watch": reg in _WATCH_TAILS,
         "icao24": icao24_by_reg.get(reg),
         "allegris": (reg or "").strip() in alleg,
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
            ORDER BY o.observed_at DESC LIMIT 1
        """, (fdate_d, airline, number))
        hist = _q(conn, """
            SELECT o.observed_date, o.registration, a.aircraft_type, o.overall_status, o.found
            FROM flight_status_observations o
            LEFT JOIN aircraft a ON a.registration = o.registration
            WHERE o.flight_date=%s AND o.airline=%s AND o.flight_number=%s
            ORDER BY o.observed_at
        """, (fdate_d, airline, number))
        stab = _reassignment_stability(conn)
        alleg = _allegris_tails(conn)
    finally:
        conn.close()

    # With per-pass rows (migration 009) a day holds several snapshots; collapse
    # runs of identical assignment state so the history reads as changes (each
    # row = the pass that first saw that state), not one row per pass.
    history = []
    for (d, reg, at, st, found) in hist:
        if history and history[-1]["reg"] == reg and history[-1]["found"] == found:
            continue
        history.append({
            "observed": d.isoformat(), "reg": reg,
            "type": _CANON_SHORT.get(at, at) if at else None,
            "allegris": (reg or "").strip() in alleg,
            "status": st, "found": found,
        })
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
            "allegris": bool(ac.get("allegris")),  # this flight's payload, not the tail-set
            "cabin": _parse_seat_config(ac.get("seatConfig")),
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


def _codes_param(s):
    """'fra, muc' -> ['FRA','MUC'] — unique IATA-shaped codes, capped, or None."""
    codes = []
    for c in re.split(r"[,\s]+", (s or "").strip().upper()):
        if re.fullmatch(r"[A-Z0-9]{3}", c) and c not in codes:
            codes.append(c)
    return codes[:8] or None


@app.route("/api/book")
def api_book():
    """Upcoming flights for booking — tail-first (?reg=) or route-first
    (?dep=&arr=, each a comma-separated list of alternative airports) — each
    with the current published assignment, its cabin configuration, and a
    measured hold-probability (how often that tail holds to departure)."""
    reg = (request.args.get("reg") or "").strip().upper() or None
    dep = _codes_param(request.args.get("dep"))
    arr = _codes_param(request.args.get("arr"))
    if not reg and not dep and not arr:
        return jsonify({"error": "provide ?reg= (tail) or ?dep=&arr= (route, "
                                 "comma-separated for several airports)"}), 400
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503
    try:
        rows = _latest_assignments(conn, reg=reg, dep=dep, arr=arr)
        swapped = {(r[0], r[1]) for r in _q(conn, _BOOK_SWAP_SQL)}
        stab = _reassignment_stability(conn)
        alleg = _allegris_tails(conn)
        cabins = _cabin_configs(conn)
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
            "allegris": (r_reg or "").strip() in alleg,
            "cabin": _parse_seat_config(cabins.get((r_reg or "").strip())),
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


# Airport pickers on /book suggest from this: every dep/arr airport seen in
# recent FIS payloads, with the city name FIS itself publishes (ICN → "Seoul").
# Derived at read time from raw, so the list tracks the collected network
# exactly — no airports-table change, no migration. `n` (observation count)
# lets the client rank busier airports first.
_BOOK_AIRPORTS_TTL_S = 3600
_book_airports_cache = {"ts": 0.0, "data": []}


def _book_airports(conn):
    nowts = datetime.now(timezone.utc).timestamp()
    if nowts - _book_airports_cache["ts"] < _BOOK_AIRPORTS_TTL_S:
        return _book_airports_cache["data"]
    try:
        rows = _q(conn, """
            WITH ends AS (
                SELECT raw->'legs'->0->'departure'->>'departureAirport' AS code,
                       raw->'legs'->0->'departure'->>'departureAirportName' AS name
                FROM flight_status_observations
                WHERE found AND raw IS NOT NULL
                  AND observed_at >= NOW() - INTERVAL '120 days'
                UNION ALL
                SELECT raw->'legs'->0->'arrival'->>'arrivalAirport',
                       raw->'legs'->0->'arrival'->>'arrivalAirportName'
                FROM flight_status_observations
                WHERE found AND raw IS NOT NULL
                  AND observed_at >= NOW() - INTERVAL '120 days'
            )
            SELECT code, name, n FROM (
                SELECT code, name,
                       SUM(COUNT(*)) OVER (PARTITION BY code) AS n,
                       ROW_NUMBER() OVER (PARTITION BY code
                                          ORDER BY COUNT(*) DESC) AS rk
                FROM ends WHERE code IS NOT NULL
                GROUP BY code, name
            ) t WHERE rk = 1 ORDER BY code
        """)
    except Exception:
        return _book_airports_cache["data"]  # stale beats a 500 mid-request
    data = [{"code": c, "name": nm, "n": int(n)} for c, nm, n in rows]
    _book_airports_cache.update(ts=nowts, data=data)
    return _book_airports_cache["data"]


@app.route("/api/book/airports")
def api_book_airports():
    """Searchable airports for the /book route pickers (see _book_airports)."""
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503
    try:
        data = _book_airports(conn)
    finally:
        conn.close()
    return jsonify({"airports": data,
                    "generated": datetime.now(timezone.utc).isoformat()})


# ── Map mode on /book ─────────────────────────────────────────────────────
# Land + country outlines, Natural Earth 1:110m "admin_0_countries" (public
# domain), Douglas-Peucker simplified to ~0.35deg and rounded to 2dp — enough
# to recognise a coastline, small enough to inline. Antarctica is dropped and
# the far south clipped at -58deg: no scheduled service down there, and the
# equirectangular smear would only waste canvas.
#
# Coordinates ARE the SVG user space: the page draws with an equirectangular
# projection, so x = longitude and y = -latitude, no client-side maths.
# Regenerate both this and _AIRPORT_LL with tools/build_book_map.py if the
# outline ever needs more detail or the network reaches a new field.
_WORLD_OUTLINE = (
    "M33.9,0.95L37.7,3.1L37.77,3.68L39.2,4.68L38.74,5.91L39.44,6.84L39.19,8.49L40.32,10.32L39.52,"
    "10.9L36.51,11.72L34.56,11.52L33.74,9.42L30.74,8.34L29.62,6.52L29.34,4.5L30.75,3.36L30.42,1.1"
    "3L33.9,0.95ZM-8.67,-27.66L-8.69,-25.88L-11.97,-25.93L-11.94,-23.37L-12.87,-23.28L-12.93,-21."
    "33L-17.06,-21L-17.02,-21.42L-14.75,-21.5L-13.89,-23.69L-12.5,-24.77L-11.39,-26.88L-8.79,-27."
    "12L-8.67,-27.66ZM-122.84,-49L-127.44,-50.83L-127.85,-52.33L-129.13,-52.76L-129.31,-53.56L-13"
    "0.51,-54.29L-130.01,-55.92L-131.71,-56.55L-135.48,-59.79L-137.45,-58.91L-139.04,-60L-141,-60"
    ".31L-140.99,-69.71L-136.5,-68.9L-129.79,-70.19L-129.11,-69.78L-128.14,-70.48L-125.76,-69.48L"
    "-124.42,-70.16L-124.29,-69.4L-121.47,-69.8L-115.25,-68.91L-113.9,-68.4L-115.3,-67.9L-113.5,-"
    "67.69L-109.95,-67.98L-108.88,-67.38L-107.79,-67.89L-108.81,-68.31L-108.17,-68.65L-106.15,-68"
    ".8L-101.45,-67.65L-98.44,-67.78L-98.56,-68.4L-97.67,-68.58L-96.12,-68.24L-96.13,-67.29L-95.4"
    "9,-68.09L-94.69,-68.06L-94.23,-69.07L-96.47,-70.09L-96.39,-71.19L-95.21,-71.92L-92.88,-71.32"
    "L-91.52,-70.19L-92.41,-69.7L-90.55,-69.5L-90.55,-68.47L-89.22,-69.26L-88.02,-68.62L-88.32,-6"
    "7.87L-87.35,-67.2L-85.58,-68.78L-85.52,-69.88L-82.62,-69.66L-81.28,-69.16L-81.96,-68.13L-81."
    "26,-67.6L-81.39,-67.11L-83.34,-66.41L-85.77,-66.56L-87.32,-64.78L-90.7,-63.61L-90.77,-62.96L"
    "-91.93,-62.84L-94.24,-60.9L-94.68,-58.95L-93.22,-58.78L-92.3,-57.09L-90.9,-57.28L-85.01,-55."
    "3L-82.27,-55.15L-82.13,-53.28L-79.91,-51.21L-78.6,-52.56L-79.83,-54.67L-78.23,-55.14L-76.54,"
    "-56.53L-77.3,-58.05L-78.52,-58.8L-77.34,-59.85L-78.11,-62.32L-73.84,-62.44L-71.37,-61.14L-69"
    ".59,-61.06L-69.29,-58.96L-67.65,-58.21L-66.2,-58.77L-64.58,-60.34L-61.4,-56.97L-61.8,-56.34L"
    "-57.33,-54.63L-56.94,-53.78L-55.76,-53.27L-55.68,-52.15L-60.03,-50.24L-66.4,-50.23L-71.1,-46"
    ".82L-68.65,-48.3L-65.06,-49.23L-64.17,-48.74L-65.12,-48.07L-64.47,-46.24L-61.52,-45.88L-60.5"
    "2,-47.01L-59.8,-45.92L-65.36,-43.55L-66.12,-43.62L-66.16,-44.47L-64.43,-45.29L-67.14,-45.14L"
    "-67.79,-45.7L-67.79,-47.07L-69.24,-47.45L-71.51,-45.01L-74.87,-45L-76.82,-43.63L-78.72,-43.6"
    "3L-79.17,-43.47L-78.94,-42.86L-82.44,-41.68L-83.14,-41.98L-82.14,-43.57L-82.55,-45.35L-88.38"
    ",-48.3L-91.64,-48.14L-94.33,-48.67L-94.82,-49.39L-95.16,-49L-122.84,-49ZM-79.78,-72.8L-80.83"
    ",-73.69L-78.06,-73.65L-76.25,-72.83L-79.78,-72.8ZM-93.61,-74.98L-94.16,-74.59L-96.82,-74.93L"
    "-94.85,-75.65L-93.61,-74.98ZM-96.75,-78.77L-95.56,-78.42L-97.31,-77.85L-98.63,-78.87L-96.75,"
    "-78.77ZM-88.15,-74.39L-92.42,-74.84L-92.89,-75.88L-93.89,-76.32L-97.12,-76.75L-96.75,-77.16L"
    "-91.61,-76.78L-90.74,-76.45L-90.97,-76.07L-89.19,-75.61L-81.13,-75.71L-79.83,-74.92L-88.15,-"
    "74.39ZM-111.26,-78.15L-109.85,-78L-112.05,-77.41L-113.53,-77.73L-111.26,-78.15ZM-55.6,-51.32"
    "L-56.8,-49.81L-56.14,-50.15L-55.47,-49.94L-55.82,-49.59L-53.48,-49.25L-53.79,-48.52L-53.09,-"
    "48.69L-52.65,-47.54L-53.07,-46.66L-54.18,-46.81L-54.24,-47.75L-55.4,-46.88L-56,-46.92L-55.29"
    ",-47.39L-56.25,-47.63L-59.27,-47.6L-58.8,-48.25L-59.23,-48.52L-57.36,-50.72L-55.87,-51.63L-5"
    "5.6,-51.32ZM-83.88,-65.11L-80.1,-63.73L-80.99,-63.41L-83.11,-64.1L-85.52,-63.05L-85.87,-63.6"
    "4L-87.22,-63.54L-86.35,-64.04L-85.88,-65.74L-83.88,-65.11ZM-78.77,-72.35L-77.82,-72.75L-74.2"
    "3,-71.77L-74.1,-71.33L-72.24,-71.56L-68.79,-70.53L-66.97,-69.19L-68.81,-68.72L-61.85,-66.86L"
    "-63.92,-65L-66.72,-66.39L-68.02,-66.26L-68.14,-65.69L-65.32,-64.38L-64.67,-63.39L-65.01,-62."
    "67L-68.78,-63.75L-66.17,-61.93L-68.88,-62.33L-74.83,-64.68L-77.71,-64.23L-78.56,-64.57L-77.9"
    ",-65.31L-73.96,-65.45L-73.94,-66.31L-72.65,-67.28L-73.31,-68.07L-76.87,-68.89L-76.23,-69.15L"
    "-78.96,-70.17L-81.31,-69.74L-88.68,-70.41L-89.51,-70.76L-88.47,-71.22L-89.89,-71.22L-90.21,-"
    "72.24L-88.41,-73.54L-85.83,-73.8L-86.56,-73.16L-85.77,-72.53L-84.85,-73.34L-82.32,-73.75L-80"
    ".6,-72.72L-80.75,-72.06L-78.77,-72.35ZM-94.5,-74.13L-90.51,-73.86L-94.27,-72.02L-95.41,-72.0"
    "6L-96.02,-73.44L-94.5,-74.13ZM-122.85,-76.12L-119.1,-77.51L-116.2,-77.65L-116.34,-76.88L-117"
    ".11,-76.53L-122.85,-76.12ZM-132.71,-54.04L-131.75,-54.12L-132.05,-52.98L-131.18,-52.18L-133."
    "05,-53.41L-133.18,-54.17L-132.71,-54.04ZM-105.49,-79.3L-100.83,-78.8L-99.67,-77.91L-105.18,-"
    "78.38L-104.21,-78.68L-105.49,-79.3ZM-123.51,-48.51L-125.66,-48.83L-128.06,-49.99L-128.36,-50"
    ".77L-125.76,-50.3L-123.51,-48.51ZM-121.54,-74.45L-117.56,-74.19L-115.51,-73.48L-119.22,-72.5"
    "2L-120.46,-71.38L-123.09,-70.9L-125.93,-71.87L-123.94,-73.68L-124.92,-74.29L-121.54,-74.45ZM"
    "-107.82,-75.85L-105.88,-75.97L-105.7,-75.48L-106.31,-75.01L-112.22,-74.42L-113.87,-74.72L-11"
    "1.79,-75.16L-117.71,-75.22L-115.4,-76.48L-109.07,-75.47L-110.5,-76.43L-109.58,-76.79L-108.55"
    ",-76.68L-107.82,-75.85ZM-106.52,-73.08L-105.4,-72.67L-104.46,-70.99L-100.98,-70.02L-101.09,-"
    "69.58L-102.73,-69.5L-102.09,-69.12L-102.43,-68.75L-105.96,-69.18L-113.31,-68.54L-117.34,-69."
    "96L-112.42,-70.37L-117.9,-70.54L-118.43,-70.91L-116.11,-71.31L-119.4,-71.56L-117.87,-72.71L-"
    "115.19,-73.31L-114.17,-73.12L-114.67,-72.65L-112.44,-72.96L-111.05,-72.45L-109.92,-72.96L-10"
    "8.19,-71.65L-107.69,-72.07L-108.4,-73.09L-106.52,-73.08ZM-100.44,-72.71L-101.54,-73.36L-100."
    "36,-73.84L-97.38,-73.76L-97.12,-73.47L-98.05,-72.99L-96.54,-72.56L-96.72,-71.66L-98.36,-71.2"
    "7L-102.5,-72.51L-100.44,-72.71ZM-98.5,-76.72L-97.74,-76.26L-98.16,-75L-102.5,-75.56L-102.57,"
    "-76.34L-98.5,-76.72ZM-96.02,-80.6L-94.3,-80.98L-94.74,-81.21L-92.41,-81.26L-87.81,-80.32L-85"
    ".81,-79.34L-89.04,-78.29L-92.88,-78.34L-93.95,-78.75L-93.15,-79.38L-94.97,-79.37L-96.71,-80."
    "16L-96.02,-80.6ZM-91.59,-81.89L-85.5,-82.65L-83.18,-82.32L-82.42,-82.86L-79.31,-83.13L-61.85"
    ",-82.63L-67.66,-81.5L-65.48,-81.51L-71.18,-79.8L-76.91,-79.32L-75.53,-79.2L-76.22,-79.02L-75"
    ".39,-78.53L-79.76,-77.21L-77.89,-76.78L-80.56,-76.18L-89.49,-76.47L-89.62,-76.95L-87.77,-77."
    "18L-88.26,-77.9L-84.98,-77.54L-87.96,-78.37L-85.09,-79.35L-86.93,-80.25L-81.85,-80.46L-87.6,"
    "-80.52L-91.59,-81.89ZM-75.22,-67.44L-76.99,-67.1L-77.24,-67.59L-75.9,-68.29L-75.11,-68.01L-7"
    "5.22,-67.44ZM-96.26,-69.49L-95.65,-69.11L-96.27,-68.76L-99.8,-69.4L-98.22,-70.14L-96.26,-69."
    "49ZM-122.84,-49L-95.16,-49L-94.82,-49.39L-94.33,-48.67L-91.64,-48.14L-88.38,-48.3L-82.55,-45"
    ".35L-82.14,-43.57L-83.12,-42.08L-82.69,-41.68L-78.94,-42.86L-79.17,-43.47L-78.72,-43.63L-76."
    "82,-43.63L-74.87,-45L-71.51,-45.01L-69.24,-47.45L-67.79,-47.07L-67.79,-45.7L-66.96,-44.81L-7"
    "0.12,-43.68L-70.83,-42.34L-69.97,-41.64L-73.71,-40.93L-71.94,-40.93L-73.95,-40.75L-74.91,-38"
    ".94L-75.53,-39.5L-75.06,-38.4L-75.94,-37.22L-75.72,-37.94L-76.35,-39.15L-76.33,-38.08L-76.99"
    ",-38.24L-76.3,-37.92L-75.73,-35.55L-81.34,-31.44L-81.31,-30.04L-80.06,-26.88L-80.38,-25.21L-"
    "81.17,-25.2L-81.71,-25.87L-83.71,-29.94L-85.11,-29.64L-86.4,-30.4L-89.59,-30.16L-89.41,-29.1"
    "6L-93.23,-29.78L-94.69,-29.48L-97.14,-27.83L-97.14,-25.87L-97.53,-25.84L-99.02,-26.37L-100.9"
    "6,-29.38L-102.48,-29.76L-103.11,-28.97L-103.94,-29.27L-106.51,-31.75L-111.02,-31.33L-114.72,"
    "-32.72L-117.13,-32.54L-118.52,-34.03L-120.62,-34.61L-124.4,-40.31L-124.53,-42.77L-123.9,-45."
    "52L-124.69,-48.18L-123.12,-48.04L-122.59,-47.1L-122.84,-49ZM-153.23,-57.97L-152.14,-57.59L-1"
    "54.01,-56.73L-154.52,-56.99L-154.67,-57.46L-153.23,-57.97ZM-140.99,-69.71L-141,-60.31L-139.0"
    "4,-60L-137.45,-58.91L-135.48,-59.79L-131.71,-56.55L-130.01,-55.92L-130.54,-54.8L-131.97,-55."
    "5L-134.08,-58.12L-136.63,-58.21L-139.87,-59.54L-147.11,-60.88L-148.22,-60.67L-148.02,-59.98L"
    "-151.72,-59.16L-151.41,-60.73L-150.35,-61.03L-150.62,-61.28L-154.02,-59.35L-153.29,-58.86L-1"
    "54.23,-58.15L-158.43,-55.99L-164.94,-54.57L-158.68,-57.02L-157.72,-57.57L-157.04,-58.92L-159"
    ".06,-58.42L-160.36,-59.07L-161.97,-58.67L-161.87,-59.63L-162.52,-59.99L-163.82,-59.8L-165.35"
    ",-60.51L-166.12,-61.5L-165.73,-62.07L-164.56,-63.15L-160.77,-63.77L-161.52,-64.4L-160.78,-64"
    ".79L-164.96,-64.45L-168.11,-65.67L-164.47,-66.58L-163.65,-66.58L-163.79,-66.08L-161.68,-66.1"
    "2L-166.76,-68.36L-166.2,-68.88L-164.43,-68.92L-161.91,-70.33L-156.58,-71.36L-154.34,-70.7L-1"
    "40.99,-69.71ZM87.36,-49.21L85.77,-48.46L85.16,-47L83.18,-47.33L82.46,-45.54L79.97,-44.92L80."
    "87,-43.18L80.18,-42.92L80.26,-42.35L74.21,-43.3L73.49,-42.5L71.19,-42.7L68.63,-40.67L66.71,-"
    "41.17L66.51,-41.99L66.02,-41.99L66.1,-43L64.9,-43.73L62.01,-43.5L58.5,-45.59L55.93,-45L55.97"
    ",-41.31L54.08,-42.32L52.5,-41.78L52.5,-42.79L51.34,-43.13L50.31,-44.61L51.28,-44.51L51.32,-4"
    "5.25L53.04,-45.26L53.04,-46.85L51.19,-47.05L49.1,-46.4L48.06,-47.74L46.47,-48.39L47.55,-50.4"
    "5L48.58,-49.87L48.7,-50.61L50.77,-51.69L52.33,-51.72L55.72,-50.62L56.78,-51.04L61.34,-50.8L6"
    "1.59,-51.27L59.97,-51.96L61.7,-52.98L60.98,-53.66L61.44,-54.01L69.07,-55.39L70.87,-55.17L71."
    "18,-54.13L73.51,-54.04L73.43,-53.49L76.89,-54.49L76.53,-54.18L80.04,-50.86L80.57,-51.39L81.9"
    "5,-50.81L83.38,-51.07L87.36,-49.21ZM55.97,-41.31L55.93,-45L58.5,-45.59L62.01,-43.5L64.9,-43."
    "73L66.1,-43L66.02,-41.99L66.51,-41.99L66.71,-41.17L67.99,-41.14L68.26,-40.66L70.96,-42.27L70"
    ".42,-41.52L73.06,-40.87L71.77,-40.15L70.6,-40.22L70.67,-40.96L69.33,-40.73L68.54,-39.53L67.7"
    ",-39.58L67.44,-39.14L68.18,-38.9L68.39,-38.16L67.83,-37.14L66.52,-37.36L66.55,-37.97L64.17,-"
    "38.89L62.37,-40.05L61.88,-41.08L60.47,-41.22L59.98,-42.22L58.63,-42.75L56.93,-41.83L57.1,-41"
    ".32L55.97,-41.31ZM141,2.6L144.58,3.86L145.98,5.47L147.65,6.08L147.89,6.61L146.97,6.72L147.19"
    ",7.39L150.69,10.58L147.91,10.13L146.05,8.07L144.74,7.63L143.29,8.25L143.41,8.98L142.63,9.33L"
    "141.03,9.12L141,2.6ZM151.3,5.84L149.71,6.32L148.32,5.75L149.85,5.51L150.14,5L150.24,5.53L150"
    ".81,5.46L151.65,4.76L151.54,4.17L152.14,4.15L152.32,4.87L151.3,5.84ZM141,2.6L141.03,9.12L140"
    ".14,8.3L137.61,8.41L138.67,7.32L137.93,5.39L133.66,3.54L132.98,4.11L131.99,2.82L133.7,2.21L1"
    "32.23,2.21L130.52,0.94L132.38,0.37L133.99,0.78L134.42,2.77L135.46,3.37L137.44,1.7L141,2.6ZM1"
    "24.97,8.89L124.44,10.14L123.46,10.24L123.98,9.29L124.97,8.89ZM117.88,-4.14L117.31,-3.23L117."
    "88,-1.83L119,-0.9L117.81,-0.78L117.52,0.8L116.56,1.49L116.15,4.01L116,3.66L114.86,4.11L113.2"
    "6,3.12L112.07,3.48L111.7,2.99L110.22,2.93L108.95,-0.42L109.66,-2.01L110.51,-0.77L112.86,-1.5"
    "L113.81,-1.22L114.62,-1.43L115.87,-4.31L117.88,-4.14ZM129.37,2.8L130.47,3.09L130.83,3.86L127"
    ".9,3.39L128.14,2.84L129.37,2.8ZM127.93,-2.17L128.69,-1.13L128.1,0.9L127.4,-1.01L127.93,-2.17"
    "ZM122.93,-0.88L125.24,-1.42L123.69,-0.24L120.18,-0.24L120.04,0.52L120.94,1.41L123.34,0.62L12"
    "1.51,1.9L123.16,5.34L122.24,5.28L122.72,4.46L121.49,4.57L120.97,2.63L120.31,2.93L120.43,5.53"
    "L119.8,5.67L119.37,5.38L119.5,3.49L118.77,2.8L119.83,-0.15L120.89,-1.31L122.93,-0.88ZM121.34"
    ",8.54L122.9,8.09L122.76,8.65L119.92,8.81L120.72,8.24L121.34,8.54ZM108.49,6.42L112.61,6.95L11"
    "2.98,7.59L115.71,8.37L114.56,8.75L105.37,6.85L106.05,5.9L108.49,6.42ZM104.37,1.08L104.89,2.3"
    "4L106.11,3.06L105.82,5.85L104.71,5.87L102.58,4.22L98.6,-1.82L95.29,-5.48L97.48,-5.25L100.64,"
    "-2.1L102.5,-1.4L103.84,-0.1L103.44,0.71L104.37,1.08ZM-68.63,52.64L-67.75,53.85L-65.05,54.7L-"
    "66.45,55.25L-68.63,54.87L-68.63,52.64ZM-57.63,30.22L-58.5,34.43L-57.23,35.29L-56.79,36.9L-57"
    ".75,38.18L-59.23,38.72L-62.34,38.83L-62.15,40.68L-62.75,41.03L-65.12,41.06L-64.98,42.06L-63."
    "76,42.04L-63.46,42.56L-65.18,43.5L-65.57,45.04L-67.29,45.55L-67.58,46.3L-65.64,47.24L-65.99,"
    "48.13L-69.14,50.73L-68.15,52.35L-71.91,52.01L-72.31,50.68L-73.33,50.38L-73.42,49.32L-72.33,4"
    "8.24L-71.66,44.97L-71.22,44.78L-72.15,42.25L-71.41,38.92L-70.81,38.55L-71.12,36.66L-70.36,36"
    ".01L-69.82,34.19L-70.54,31.37L-69.66,28.46L-68.3,26.9L-68.42,24.52L-67.33,24.03L-67.11,22.74"
    "L-66.27,21.83L-64.96,22.08L-64.38,22.8L-63.99,21.99L-62.85,22.03L-60.85,23.88L-57.78,25.16L-"
    "58.62,27.12L-55.7,27.39L-54.13,25.55L-53.65,26.92L-57.63,30.22ZM-68.63,52.64L-68.63,54.87L-6"
    "6.96,54.9L-68.15,55.61L-71.01,55.05L-74.66,52.84L-71.11,54.07L-70.27,52.93L-68.63,52.64ZM-69"
    ".59,17.58L-68.44,19.41L-68.76,20.37L-67.83,22.87L-66.99,22.99L-67.33,24.03L-68.42,24.52L-68."
    "3,26.9L-69.66,28.46L-70.54,31.37L-69.82,34.19L-70.36,36.01L-71.12,36.66L-70.81,38.55L-71.41,"
    "38.92L-72.15,42.25L-71.22,44.78L-71.66,44.97L-72.33,48.24L-73.42,49.32L-73.33,50.38L-72.31,5"
    "0.68L-71.91,52.01L-68.57,52.3L-70.85,52.9L-71.43,53.86L-74.95,52.26L-75.61,48.67L-74.13,46.9"
    "4L-75.64,46.65L-74.69,45.76L-74.35,44.1L-73.24,44.45L-72.72,42.38L-73.39,42.12L-73.7,43.37L-"
    "74.33,43.22L-73.22,39.26L-73.59,37.16L-73.17,37.12L-71.44,32.42L-71.49,28.86L-70.91,27.64L-7"
    "0.09,21.39L-70.37,18.35L-69.59,17.58ZM29.34,4.5L29.62,6.52L30.74,8.34L28.73,8.53L28.37,11.79"
    "L29.62,12.18L29.7,13.26L28.93,13.25L27.16,11.61L26.55,11.92L24.26,10.95L22.16,11.08L21.73,7."
    "29L20.09,6.94L19.02,7.99L17.47,8.07L16.33,5.88L12.18,5.79L13.6,4.5L14.58,4.97L16.01,3.54L16."
    "41,1.74L17.64,0.42L18.54,-4.2L19.47,-5.03L22.41,-4.03L22.84,-4.71L25.65,-5.26L27.37,-5.23L28"
    ".43,-4.29L29.72,-4.6L30.83,-3.51L30.77,-2.34L31.17,-2.2L29.88,-0.6L29.02,2.84L29.34,4.5ZM41."
    "59,1.68L40.99,0.86L40.98,-2.78L42.13,-4.23L44.96,-5L48.94,-9.45L48.95,-11.41L51.11,-12.02L50"
    ".55,-9.2L48.59,-5.34L41.59,1.68ZM39.2,4.68L37.77,3.68L37.7,3.1L33.9,0.95L33.89,-0.11L35.04,-"
    "1.91L34.01,-4.25L35.3,-5.51L36.16,-4.45L38.12,-3.6L39.56,-3.42L40.77,-4.26L41.86,-3.92L40.98"
    ",-2.78L40.99,0.86L41.59,1.68L40.26,2.57L39.2,4.68ZM24.57,-8.23L23.46,-8.95L23.55,-10.09L21.9"
    "4,-12.59L23.02,-15.68L23.89,-15.61L23.85,-20L25,-20L25,-22L36.87,-22L37.48,-18.61L38.41,-18L"
    "36.85,-16.96L36.27,-13.56L34.26,-10.63L33.97,-8.68L33.21,-12.18L32.74,-12.25L32.07,-11.97L32"
    ".4,-11.08L31.35,-9.81L30,-10.29L28.97,-9.4L26.75,-9.47L25.79,-10.41L25.07,-10.27L24.54,-8.92"
    "L23.89,-8.62L24.57,-8.23ZM23.84,-19.58L23.89,-15.61L23.02,-15.68L21.94,-12.59L22.86,-11.14L2"
    "1,-9.48L18.81,-8.98L17.96,-7.89L15.28,-7.42L14.98,-8.8L13.95,-9.55L14.17,-10.02L15.47,-9.98L"
    "14.6,-13.33L13.95,-13.35L13.54,-14.37L13.97,-15.68L15.25,-16.63L15.9,-20.39L15.1,-21.31L14.8"
    "5,-22.86L15.86,-23.41L23.84,-19.58ZM-71.71,-19.71L-71.71,-18.04L-74.46,-18.34L-72.33,-18.67L"
    "-73.19,-19.92L-71.71,-19.71ZM-71.71,-18.04L-71.59,-19.88L-69.95,-19.65L-68.32,-18.61L-68.69,"
    "-18.21L-70.67,-18.43L-71.4,-17.6L-71.71,-18.04ZM49.1,-46.4L46.68,-44.61L48.58,-41.81L47.82,-"
    "41.15L45.47,-42.5L39.96,-43.43L36.68,-45.24L38.23,-46.24L37.67,-46.64L39.15,-47.04L38.22,-47"
    ".1L38.26,-47.55L39.74,-47.9L40.07,-49.6L35.36,-50.58L35.02,-51.21L34.22,-51.26L34.39,-51.77L"
    "33.75,-52.34L31.79,-52.1L31.31,-53.07L32.69,-53.35L30.76,-54.81L30.87,-55.55L28.18,-56.17L27"
    ".29,-57.47L27.72,-57.79L27.42,-58.72L29.12,-60.03L28.07,-60.5L31.52,-62.87L30.04,-63.55L30.4"
    "4,-64.2L29.54,-64.95L30.22,-65.81L29.05,-66.94L29.98,-67.7L28.45,-68.36L28.59,-69.06L32.13,-"
    "69.91L41.06,-67.46L41.13,-66.79L38.38,-66L33.18,-66.63L34.81,-65.9L34.94,-64.41L37.01,-63.85"
    "L36.54,-64.76L37.18,-65.14L39.59,-64.52L40.44,-64.76L39.76,-65.5L42.09,-66.48L43.95,-66.07L4"
    "4.53,-66.76L43.7,-67.35L44.19,-67.95L43.45,-68.57L46.25,-68.25L46.82,-67.69L45.56,-67.57L45."
    "56,-67.01L46.35,-66.67L53.72,-68.86L54.47,-68.81L53.49,-68.2L58.8,-68.88L59.94,-68.28L61.08,"
    "-68.94L60.03,-69.52L60.55,-69.85L68.51,-68.09L69.18,-68.62L66.93,-69.45L67.26,-69.93L66.69,-"
    "71.03L69.94,-73.04L72.59,-72.78L72.8,-72.22L71.85,-71.41L72.79,-70.39L72.56,-69.02L73.67,-68"
    ".41L71.28,-66.32L72.42,-66.17L75.05,-67.76L74.47,-68.33L74.94,-68.99L73.84,-69.07L73.6,-69.6"
    "3L74.4,-70.63L73.1,-71.45L74.89,-72.12L74.66,-72.83L75.68,-72.3L75.29,-71.34L76.36,-71.15L75"
    ".9,-71.87L77.58,-72.27L81.5,-71.75L80.61,-72.58L80.51,-73.65L86.82,-73.94L86.01,-74.46L87.17"
    ",-75.12L100.76,-76.43L101.99,-77.29L104.35,-77.7L106.07,-77.37L104.7,-77.13L106.97,-76.97L10"
    "7.24,-76.48L111.08,-76.71L114.13,-75.85L113.89,-75.33L109.4,-74.18L113.02,-73.98L113.53,-73."
    "34L115.57,-73.75L123.2,-72.97L123.26,-73.74L126.98,-73.57L128.59,-73.04L129.05,-72.4L128.46,"
    "-71.98L131.29,-70.79L132.25,-71.84L133.86,-71.39L139.87,-71.49L139.15,-72.42L140.47,-72.85L1"
    "49.5,-72.2L152.97,-70.84L159,-70.87L159.83,-70.45L159.71,-69.72L160.94,-69.44L167.84,-69.58L"
    "169.58,-68.69L170.82,-69.01L170.01,-69.65L170.45,-70.1L175.72,-69.88L180,-68.96L180,-64.98L1"
    "77.41,-64.61L179.37,-62.98L179.23,-62.3L177.36,-62.52L173.68,-61.65L170.33,-59.88L168.9,-60."
    "57L166.29,-59.79L165.84,-60.16L163.54,-59.87L162.02,-58.24L163.19,-57.62L163.06,-56.16L162.1"
    "3,-56.12L161.7,-55.29L162.12,-54.86L160.37,-54.34L160.02,-53.2L158.53,-52.96L158.23,-51.94L1"
    "56.79,-51.01L155.43,-55.38L155.91,-56.77L156.81,-57.83L158.36,-58.06L163.67,-61.14L164.47,-6"
    "2.55L163.26,-62.47L162.66,-61.64L160.12,-60.54L159.3,-61.77L156.72,-61.43L154.22,-59.76L155."
    "04,-59.14L151.27,-58.78L151.34,-59.5L149.78,-59.66L148.54,-59.16L142.2,-59.04L135.13,-54.73L"
    "136.7,-54.6L138.16,-53.76L139.9,-54.19L141.35,-53.09L140.06,-48.45L134.87,-43.4L133.54,-42.8"
    "1L132.28,-43.28L130.78,-42.22L131.03,-44.97L133.1,-45.14L135.03,-48.48L130.99,-47.79L130.58,"
    "-48.73L129.4,-49.44L127.66,-49.76L125.95,-52.79L123.57,-53.46L120.18,-52.75L120.74,-51.96L11"
    "9.29,-50.14L117.88,-49.51L114.36,-50.25L110.66,-49.13L108.48,-49.28L106.89,-50.27L103.68,-50"
    ".09L102.26,-50.51L102.07,-51.26L98.86,-52.05L97.83,-51.01L98.23,-50.42L97.26,-49.73L92.23,-5"
    "0.8L87.36,-49.21L83.38,-51.07L81.95,-50.81L80.57,-51.39L80.04,-50.86L76.53,-54.18L76.89,-54."
    "49L73.43,-53.49L73.51,-54.04L71.18,-54.13L70.87,-55.17L69.07,-55.39L61.44,-54.01L60.98,-53.6"
    "6L61.7,-52.98L59.97,-51.96L61.59,-51.27L61.34,-50.8L56.78,-51.04L55.72,-50.62L52.33,-51.72L5"
    "0.77,-51.69L48.7,-50.61L48.58,-49.87L47.55,-50.45L46.47,-48.39L48.06,-47.74L49.1,-46.4ZM93.7"
    "8,-81.02L95.94,-81.25L100.19,-79.78L99.94,-78.88L97.76,-78.76L93.31,-79.43L92.55,-80.14L91.1"
    "8,-80.34L93.78,-81.02ZM102.84,-79.28L105.37,-78.71L105.08,-78.31L99.44,-77.92L101.26,-79.23L"
    "102.84,-79.28ZM138.83,-76.14L145.09,-75.56L144.3,-74.82L138.96,-74.61L136.97,-75.26L137.51,-"
    "75.95L138.83,-76.14ZM148.22,-75.35L150.73,-75.08L149.58,-74.69L146.12,-75.17L148.22,-75.35ZM"
    "139.86,-73.37L142.06,-73.86L143.6,-73.21L139.86,-73.37ZM44.85,-80.59L51.52,-80.7L47.59,-80.0"
    "1L46.5,-80.25L47.07,-80.56L44.85,-80.59ZM22.73,-54.33L19.66,-54.43L21.27,-55.19L22.76,-54.86"
    "L22.73,-54.33ZM53.51,-73.75L55.9,-74.63L55.63,-75.08L61.17,-76.25L68.16,-76.94L68.85,-76.54L"
    "58.48,-74.31L55.42,-72.37L55.62,-71.54L57.54,-70.72L53.68,-70.76L51.6,-71.47L51.46,-72.01L52"
    ".48,-72.23L52.44,-72.77L54.43,-73.63L53.51,-73.75ZM142.91,-53.7L143.24,-51.76L144.65,-48.98L"
    "143.17,-49.31L142.56,-47.86L143.53,-46.84L143.51,-46.14L142.75,-46.74L142.09,-45.97L142.18,-"
    "50.95L141.59,-51.94L141.68,-53.3L142.61,-53.76L142.21,-54.23L142.65,-54.37L142.91,-53.7ZM-17"
    "4.93,-67.21L-175.01,-66.58L-174.34,-66.34L-174.57,-67.06L-171.86,-66.91L-169.9,-65.98L-172.5"
    "3,-65.44L-172.96,-64.25L-176.21,-65.36L-178.36,-65.39L-178.9,-65.74L-178.69,-66.11L-179.88,-"
    "65.87L-179.43,-65.4L-180,-64.98L-180,-68.96L-174.93,-67.21ZM-178.69,-70.89L-180,-70.83L-180,"
    "-71.52L-177.58,-71.27L-178.69,-70.89ZM33.44,-45.97L36.53,-45.47L33.88,-44.36L33.33,-44.56L33"
    ".55,-45.03L32.45,-45.33L33.44,-45.97ZM-61.2,51.85L-58.55,51.1L-57.75,51.55L-59.4,52.2L-61.2,"
    "51.85ZM15.14,-79.67L16.99,-80.05L21.54,-78.96L19.03,-78.56L17.12,-76.81L15.91,-76.77L13.76,-"
    "77.38L14.67,-77.74L11.22,-78.87L10.44,-79.65L15.14,-79.67ZM31.1,-69.56L28.59,-69.06L29.02,-6"
    "9.77L27.73,-70.16L26.18,-69.83L24.74,-68.65L21.24,-69.37L20.03,-69.07L19.88,-68.41L17.99,-68"
    ".57L17.73,-68.01L16.77,-68.01L13.56,-64.79L13.92,-64.45L13.57,-64.05L12.58,-64.07L11.93,-63."
    "13L11.99,-61.8L12.63,-61.29L12.3,-60.12L11.03,-58.86L10.36,-59.47L8.38,-58.31L7.05,-58.08L5."
    "67,-58.59L4.99,-61.97L10.53,-64.49L14.76,-67.81L19.18,-69.82L23.02,-70.2L24.55,-71.03L28.17,"
    "-71.19L31.29,-70.45L30.01,-70.19L31.1,-69.56ZM27.41,-80.06L23.02,-79.4L17.37,-80.32L22.92,-8"
    "0.66L27.41,-80.06ZM24.72,-77.85L20.73,-77.68L21.42,-77.94L20.81,-78.25L22.88,-78.45L24.72,-7"
    "7.85ZM-46.76,-82.63L-38.62,-83.55L-27.1,-83.52L-20.85,-82.73L-31.9,-82.2L-22.07,-81.73L-23.1"
    "7,-81.15L-15.77,-81.91L-12.21,-81.29L-20.05,-80.18L-17.73,-80.13L-19.7,-78.75L-19.67,-77.64L"
    "-18.47,-76.99L-21.68,-76.63L-19.83,-76.1L-19.6,-75.25L-20.67,-75.16L-19.37,-74.3L-21.59,-74."
    "22L-20.43,-73.82L-20.76,-73.46L-23.57,-73.31L-22.3,-72.18L-24.79,-72.33L-22.13,-71.47L-21.75"
    ",-70.66L-23.54,-70.47L-25.54,-71.43L-25.2,-70.75L-26.36,-70.23L-22.35,-70.13L-27.75,-68.47L-"
    "31.78,-68.12L-34.2,-66.68L-39.81,-65.46L-41.19,-63.48L-42.82,-62.68L-42.42,-61.9L-43.38,-60."
    "1L-48.26,-60.86L-51.63,-63.63L-52.28,-65.18L-53.66,-66.1L-53.3,-66.84L-53.97,-67.19L-52.98,-"
    "68.36L-51.48,-68.73L-50.87,-69.93L-53.46,-69.28L-54.68,-69.61L-54.36,-70.82L-51.39,-70.57L-5"
    "5.83,-71.65L-54.72,-72.59L-58.59,-75.52L-61.27,-76.1L-68.5,-76.06L-71.4,-77.01L-66.76,-77.38"
    "L-73.3,-78.04L-73.16,-78.43L-65.71,-79.39L-65.32,-79.76L-68.02,-80.12L-62.23,-81.32L-62.65,-"
    "81.77L-57.21,-82.19L-53.04,-81.89L-50.39,-82.44L-44.52,-81.66L-46.9,-82.2L-46.76,-82.63ZM68."
    "94,48.62L70.56,49.26L70.28,49.71L68.75,49.77L68.94,48.62ZM124.97,8.89L127.34,8.4L125.09,9.39"
    "L124.97,8.89ZM16.34,28.58L16.82,28.08L18.46,29.05L19.89,28.46L19.9,24.77L20.89,26.83L21.61,2"
    "6.73L23.31,25.27L25.66,25.49L27.12,23.57L29.43,22.09L31.19,22.25L31.93,24.37L31.84,25.84L31."
    "04,25.73L30.69,26.74L31.28,27.29L32.83,26.74L32.2,28.75L28.22,32.77L25.78,33.94L22.57,33.86L"
    "20.07,34.8L18.38,34.14L17.93,32.61L18.22,31.66L16.34,28.58ZM28.98,28.96L28.07,28.85L27,29.88"
    "L28.11,30.55L29.33,29.26L28.98,28.96ZM28.98,28.96L29.33,29.26L28.11,30.55L27,29.88L28.07,28."
    "85L28.98,28.96ZM-117.13,-32.54L-114.72,-32.72L-111.02,-31.33L-106.51,-31.75L-103.94,-29.27L-"
    "103.11,-28.97L-102.48,-29.76L-101.66,-29.78L-99.02,-26.37L-97.14,-25.87L-97.87,-22.44L-95.9,"
    "-18.83L-94.43,-18.14L-91.41,-18.88L-90.77,-19.28L-90.28,-21L-87.05,-21.54L-86.85,-20.85L-87."
    "84,-18.26L-91,-17.82L-91,-17.25L-91.45,-17.25L-90.46,-16.07L-91.75,-16.07L-92.23,-14.54L-93."
    "88,-15.94L-96.56,-15.65L-103.5,-18.29L-105.49,-19.95L-105.27,-21.42L-106.03,-22.77L-112.23,-"
    "28.95L-113.15,-31.17L-114.78,-31.8L-114.94,-31.39L-114.67,-30.16L-111.62,-26.66L-110.66,-24."
    "3L-109.41,-23.36L-109.85,-22.82L-112.18,-24.74L-112.3,-26.01L-115.06,-27.72L-114.16,-28.57L-"
    "115.52,-29.56L-117.13,-32.54ZM-57.63,30.22L-56.98,30.11L-53.79,32.05L-53.21,32.73L-53.81,34."
    "4L-56.22,34.86L-58.43,33.91L-57.63,30.22ZM-53.37,33.77L-53.65,33.2L-53.21,32.73L-53.79,32.05"
    "L-56.98,30.11L-57.63,30.22L-53.65,26.92L-53.63,26.12L-54.13,25.55L-54.63,25.74L-54.29,24.02L"
    "-55.4,23.96L-55.8,22.36L-57.94,22.09L-58.17,20.18L-57.5,18.17L-58.28,17.27L-58.24,16.3L-60.1"
    "6,16.26L-60.5,13.78L-64.32,12.46L-65.4,11.57L-65.34,9.76L-66.65,9.93L-68.27,11.01L-70.55,11."
    "01L-70.48,9.49L-72.18,10.05L-73.23,9.46L-73.02,9.03L-73.99,7.52L-73.12,6.63L-72.89,5.27L-69."
    "89,4.3L-69.42,1.12L-70.02,-0.54L-69.22,-0.99L-69.8,-1.09L-69.82,-1.71L-67.54,-2.04L-67.07,-1"
    ".13L-65.55,-0.79L-63.37,-2.2L-64.27,-2.5L-64.82,-4.06L-63.09,-3.77L-60.97,-4.54L-60.73,-5.2L"
    "-59.98,-5.01L-59.54,-3.96L-59.97,-2.76L-59.03,-1.32L-56,-1.82L-55.97,-2.51L-52.94,-2.12L-51."
    "32,-4.2L-50.51,-1.9L-49.97,-1.74L-50.7,-0.22L-50.39,0.08L-48.62,0.24L-48.58,1.24L-47.82,0.58"
    "L-44.91,1.55L-44.58,2.69L-43.42,2.38L-39.98,2.87L-37.22,4.82L-35.6,5.15L-34.73,7.34L-35.13,9"
    "L-38.67,13.06L-39.27,17.87L-40.94,21.94L-41.99,22.97L-44.65,23.35L-47.65,24.89L-48.5,25.88L-"
    "48.89,28.67L-53.37,33.77ZM-69.53,10.95L-68.27,11.01L-66.65,9.93L-65.34,9.76L-65.4,11.57L-64."
    "32,12.46L-60.5,13.78L-60.16,16.26L-58.24,16.3L-58.28,17.27L-57.5,18.17L-57.85,19.97L-59.12,1"
    "9.36L-61.79,19.63L-62.69,22.25L-63.99,21.99L-64.38,22.8L-64.96,22.08L-66.27,21.83L-67.83,22."
    "87L-68.76,20.37L-68.44,19.41L-69.59,17.58L-68.96,16.5L-69.34,14.95L-68.67,12.56L-69.53,10.95"
    "ZM-69.89,4.3L-72.89,5.27L-73.12,6.63L-73.99,7.52L-73.02,9.03L-73.23,9.46L-72.18,10.05L-70.48"
    ",9.49L-70.55,11.01L-69.53,10.95L-68.67,12.56L-69.34,14.95L-68.96,16.5L-70.37,18.35L-76.01,14"
    ".65L-79.76,7.19L-81.25,6.14L-80.93,5.69L-81.41,4.74L-80.3,3.4L-80.44,4.43L-79.21,4.96L-78.64"
    ",4.55L-77.84,3L-75.54,1.56L-75.11,0.06L-73.07,2.31L-70.81,2.26L-70.05,2.73L-70.69,3.74L-69.8"
    "9,4.3ZM-66.88,-1.25L-67.54,-2.04L-69.82,-1.71L-69.8,-1.09L-69.22,-0.99L-70.02,-0.54L-69.42,1"
    ".12L-69.89,4.3L-70.69,3.74L-70.05,2.73L-70.81,2.26L-73.07,2.31L-75.11,0.06L-77.42,-0.4L-78.9"
    "9,-1.69L-77.13,-3.85L-77.88,-7.22L-77.24,-7.94L-77.47,-8.52L-75.67,-9.44L-74.91,-11.08L-73.4"
    "1,-11.23L-71.4,-12.38L-71.33,-11.78L-72.91,-10.45L-73.3,-9.15L-72.79,-9.09L-71.96,-6.99L-70."
    "09,-6.96L-69.39,-6.1L-67.34,-6.1L-67.82,-4.5L-67.3,-3.32L-67.81,-2.82L-66.88,-1.25ZM-77.35,-"
    "8.67L-77.24,-7.94L-77.88,-7.22L-78.18,-8.32L-79.12,-9L-80.38,-8.3L-80,-7.55L-80.89,-7.22L-81"
    ".72,-8.11L-82.85,-8.07L-82.93,-9.48L-81.44,-8.79L-79.02,-9.55L-77.35,-8.67ZM-82.55,-9.57L-82"
    ".97,-8.23L-84.98,-10.09L-85.11,-9.56L-85.66,-9.93L-85.94,-10.9L-83.66,-10.94L-82.55,-9.57ZM-"
    "83.66,-10.94L-85.71,-11.09L-87.67,-12.91L-84.92,-14.79L-83.15,-15L-83.66,-10.94ZM-83.15,-15L"
    "-84.92,-14.79L-87.32,-12.98L-87.86,-13.89L-89.35,-14.42L-87.9,-15.86L-84.98,-16L-83.15,-15ZM"
    "-89.35,-14.42L-87.72,-13.79L-87.9,-13.15L-90.1,-13.74L-89.35,-14.42ZM-92.23,-14.54L-91.75,-1"
    "6.07L-90.46,-16.07L-91.45,-17.25L-91,-17.25L-91,-17.82L-89.14,-17.81L-89.23,-15.89L-88.23,-1"
    "5.73L-89.35,-14.42L-90.1,-13.74L-92.23,-14.54ZM-89.14,-17.81L-88.11,-18.35L-88.93,-15.89L-89"
    ".14,-17.81ZM-60.73,-5.2L-60.97,-4.54L-63.09,-3.77L-64.82,-4.06L-64.27,-2.5L-63.37,-2.2L-66.3"
    "3,-0.72L-67.81,-2.82L-67.3,-3.32L-67.82,-4.5L-67.34,-6.1L-69.39,-6.1L-70.09,-6.96L-71.96,-6."
    "99L-72.79,-9.09L-73.3,-9.15L-72.91,-10.45L-71.33,-11.78L-71.95,-11.42L-71.63,-10.45L-72.07,-"
    "9.87L-71.26,-9.14L-71.4,-10.97L-70.16,-11.38L-69.94,-12.16L-68.19,-10.55L-66.23,-10.65L-64.8"
    "9,-10.08L-64.32,-10.64L-61.88,-10.72L-62.73,-10.42L-59.76,-8.37L-61.41,-5.96L-60.73,-5.2ZM-5"
    "6.54,-1.9L-58.54,-1.27L-59.65,-1.79L-59.98,-5.01L-61.41,-5.96L-59.76,-8.37L-57.15,-5.97L-58."
    "04,-4.06L-56.54,-1.9ZM-54.52,-2.31L-55.97,-2.51L-56,-1.82L-56.54,-1.9L-57.6,-3.33L-58.04,-4."
    "06L-57.15,-5.97L-53.96,-5.76L-54.48,-4.9L-54.01,-3.62L-54.52,-2.31ZM-51.66,-4.16L-52.94,-2.1"
    "2L-54.52,-2.31L-54.01,-3.62L-54.48,-4.9L-53.96,-5.76L-51.66,-4.16ZM6.19,-49.46L8.1,-49.02L7."
    "47,-47.62L6.04,-46.73L6.84,-45.99L7.44,-43.69L6.53,-43.13L3.1,-43.08L2.99,-42.47L1.83,-42.34"
    "L-1.9,-43.42L-1.19,-46.01L-2.96,-47.57L-4.49,-47.95L-4.59,-48.68L-1.62,-48.64L-1.93,-49.78L-"
    "0.99,-49.35L1.34,-50.13L1.64,-50.95L2.51,-51.15L4.29,-49.91L6.19,-49.46ZM-75.37,0.15L-75.54,"
    "1.56L-77.84,3L-78.64,4.55L-79.21,4.96L-80.44,4.43L-79.77,2.66L-80.97,2.25L-80.93,1.06L-80.09"
    ",-0.77L-78.86,-1.38L-75.37,0.15ZM-82.27,-23.19L-78.35,-22.51L-74.18,-20.28L-77.76,-19.86L-77"
    ".09,-20.41L-78.14,-20.74L-78.72,-21.6L-82.17,-22.39L-81.8,-22.64L-84.97,-21.9L-82.27,-23.19Z"
    "M31.19,22.25L28.02,21.49L25.26,17.74L27.04,17.94L28.95,16.04L30.27,15.51L32.85,16.71L32.66,2"
    "0.3L31.19,22.25ZM29.43,22.09L27.12,23.57L25.66,25.49L23.31,25.27L21.61,26.73L20.89,26.83L19."
    "9,24.77L19.9,21.85L20.88,21.81L20.91,18.25L25.26,17.74L28.02,21.49L29.43,22.09ZM19.9,24.77L1"
    "9.89,28.46L18.46,29.05L16.82,28.08L16.34,28.58L15.21,27.09L14.26,22.11L11.73,17.3L13.46,16.9"
    "7L14.06,17.42L18.26,17.31L21.38,17.93L24.03,17.3L25.08,17.58L23.58,18.28L23.2,17.87L20.91,18"
    ".25L20.88,21.81L19.9,21.85L19.9,24.77ZM-16.71,-13.59L-17.63,-14.73L-16.12,-16.46L-14.58,-16."
    "6L-12.17,-14.62L-11.51,-12.44L-16.68,-12.38L-16.84,-13.15L-13.84,-13.51L-16.71,-13.59ZM-11.5"
    "1,-12.44L-12.17,-14.62L-11.67,-15.39L-5.54,-15.5L-6.45,-24.96L-4.92,-24.97L3.15,-19.69L3.16,"
    "-19.06L4.27,-19.16L4.27,-16.85L3.64,-15.57L-1.07,-14.97L-4.01,-13.47L-5.22,-11.71L-5.4,-10.3"
    "7L-8.03,-10.21L-9.13,-12.31L-10.17,-11.84L-11.51,-12.44ZM-17.06,-21L-12.93,-21.33L-12.87,-23"
    ".28L-11.94,-23.37L-11.97,-25.93L-8.69,-25.88L-8.68,-27.4L-4.92,-24.97L-6.45,-24.96L-5.54,-15"
    ".5L-11.67,-15.39L-12.17,-14.62L-14.58,-16.6L-16.46,-16.14L-16.28,-20.09L-17.06,-21ZM2.69,-6."
    "26L1.87,-6.14L1.66,-9.13L0.77,-10.47L1.45,-11.55L2.85,-12.24L3.8,-10.73L2.72,-8.51L2.69,-6.2"
    "6ZM14.85,-22.86L15.1,-21.31L15.9,-20.39L15.25,-16.63L13.97,-15.68L13.54,-14.37L13.95,-13.35L"
    "14.6,-13.33L14.18,-12.48L13.08,-13.6L12.3,-13.04L10.99,-13.39L9.01,-12.83L5.44,-13.87L4.11,-"
    "13.53L3.61,-11.66L2.85,-12.24L2.15,-11.94L2.18,-12.63L1.02,-12.85L0.37,-14.93L3.64,-15.57L4."
    "27,-16.85L4.27,-19.16L12,-23.47L14.14,-22.49L14.85,-22.86ZM2.69,-6.26L2.72,-8.51L3.71,-10.06"
    "L3.68,-12.55L4.37,-13.75L9.01,-12.83L10.99,-13.39L12.3,-13.04L13.08,-13.6L14.58,-12.09L11.75"
    ",-6.98L11.06,-6.64L10.12,-7.04L9.23,-6.44L8.5,-4.77L5.9,-4.26L4.33,-6.27L2.69,-6.26ZM14.5,-1"
    "2.86L15.47,-9.98L14.17,-10.02L13.95,-9.55L15.44,-7.69L14.54,-6.23L14.48,-4.73L15.86,-3.01L15"
    ".94,-1.73L14.34,-2.23L9.65,-2.28L9.8,-3.07L8.49,-4.5L8.76,-5.48L10.12,-7.04L11.06,-6.64L11.7"
    "5,-6.98L14.42,-11.57L14.5,-12.86ZM0.9,-11L1.66,-9.13L1.87,-6.14L1.06,-5.93L-0.05,-10.71L0.9,"
    "-11ZM0.02,-11.02L1.06,-5.93L-1.96,-4.71L-2.86,-4.99L-3.24,-6.25L-2.56,-8.22L-2.94,-10.96L0.0"
    "2,-11.02ZM-8.03,-10.21L-6.21,-10.52L-4.33,-9.61L-2.83,-9.64L-2.56,-8.22L-3.24,-6.25L-2.86,-4"
    ".99L-4.65,-5.17L-7.71,-4.36L-7.57,-5.71L-8.6,-6.47L-7.83,-8.58L-8.03,-10.21ZM-13.7,-12.59L-1"
    "0.17,-11.84L-9.13,-12.31L-8.03,-10.21L-7.83,-8.58L-8.28,-7.69L-9.21,-7.31L-9.76,-8.54L-10.51"
    ",-8.35L-11.12,-10.05L-12.43,-9.84L-13.25,-8.9L-15.13,-11.04L-13.74,-11.81L-13.7,-12.59ZM-16."
    "68,-12.38L-13.7,-12.59L-13.74,-11.81L-15.13,-11.04L-16.68,-12.38ZM-8.44,-7.69L-8.6,-6.47L-7."
    "57,-5.71L-7.71,-4.36L-11.44,-6.79L-10.23,-8.41L-9.76,-8.54L-9.21,-7.31L-8.44,-7.69ZM-13.25,-"
    "8.9L-12.43,-9.84L-11.12,-10.05L-10.23,-8.41L-11.44,-6.79L-12.95,-7.8L-13.25,-8.9ZM-5.4,-10.3"
    "7L-4.28,-13.23L-1.07,-14.97L0.37,-14.93L1.02,-12.85L2.18,-12.63L1.94,-11.64L0.9,-11L-2.94,-1"
    "0.96L-2.83,-9.64L-4.33,-9.61L-5.4,-10.37ZM27.37,-5.23L24.41,-5.11L22.84,-4.71L22.41,-4.03L19"
    ".47,-5.03L18.45,-3.5L17.13,-3.73L16.01,-2.27L14.46,-5.45L15.28,-7.42L17.96,-7.89L18.81,-8.98"
    "L21,-9.48L22.86,-11.14L23.55,-10.09L23.46,-8.95L27.37,-5.23ZM18.45,-3.5L17.64,0.42L16.41,1.7"
    "4L16.01,3.54L14.58,4.97L12.62,4.44L11.91,5.04L11.09,3.98L11.86,3.43L11.48,2.77L12.58,1.95L13"
    ".99,2.47L14.43,1.33L13.84,-0.04L14.28,-1.2L13.28,-1.31L13.08,-2.27L15.94,-1.73L17.13,-3.73L1"
    "8.45,-3.5ZM11.28,-2.26L12.95,-2.32L13.28,-1.31L14.28,-1.2L13.84,-0.04L14.43,1.33L13.99,2.47L"
    "12.58,1.95L11.48,2.77L11.86,3.43L11.09,3.98L8.8,1.11L9.49,-1.01L11.29,-1.06L11.28,-2.26ZM9.6"
    "5,-2.28L11.28,-2.26L11.29,-1.06L9.49,-1.01L9.65,-2.28ZM30.74,8.34L33.23,9.68L33.31,12.44L32."
    "69,13.71L33.21,13.97L30.18,14.8L30.27,15.51L28.95,16.04L27.04,17.94L23.22,17.52L21.89,16.08L"
    "21.93,12.9L24.02,12.91L23.91,10.93L25.75,11.78L27.16,11.61L28.93,13.25L29.7,13.26L29.62,12.1"
    "8L28.37,11.79L28.45,9.16L29,8.41L30.74,8.34ZM32.76,9.23L33.74,9.42L34.28,10.16L34.56,13.58L3"
    "5.69,14.61L35.77,15.9L35.03,16.8L34.38,16.18L34.46,14.61L32.69,13.71L33.49,10.53L32.76,9.23Z"
    "M34.56,11.52L37.47,11.57L40.32,10.32L40.78,14.69L39.45,16.72L37.41,17.59L34.79,19.78L35.56,2"
    "2.09L35.46,24.12L33.01,25.36L32.57,25.73L32.83,26.74L32.07,26.73L31.19,22.25L32.66,20.3L32.8"
    "5,16.71L30.34,15.88L30.18,14.8L33.21,13.97L34.46,14.61L34.38,16.18L35.03,16.8L35.77,15.9L35."
    "69,14.61L34.56,13.58L34.56,11.52ZM32.07,26.73L31.28,27.29L30.69,26.74L31.04,25.73L31.84,25.8"
    "4L32.07,26.73ZM12.32,6.1L16.33,5.88L17.47,8.07L19.02,7.99L20.09,6.94L21.73,7.29L22.16,11.08L"
    "24.02,11.24L24.02,12.91L21.93,12.9L21.89,16.08L23.22,17.52L21.38,17.93L18.26,17.31L14.06,17."
    "42L13.46,16.97L11.73,17.3L12.18,14.45L13.74,11.3L12.32,6.1ZM30.47,2.41L30.75,3.36L29.34,4.5L"
    "29.02,2.84L30.47,2.41ZM35.72,-32.71L34.97,-31.87L35.42,-31.1L34.92,-29.5L34.27,-31.22L35.1,-"
    "33.08L35.82,-33.28L35.72,-32.71ZM49.54,12.47L50.38,15.71L49.67,15.71L49.77,16.88L47.1,24.94L"
    "45.41,25.6L44.04,24.99L43.35,22.78L43.43,21.34L44.46,19.44L43.96,17.41L44.45,16.22L46.31,15."
    "78L47.71,14.59L49.19,12.04L49.54,12.47ZM9.48,-30.31L9.06,-32.1L7.61,-33.34L7.52,-34.1L8.14,-"
    "34.66L8.42,-36.95L9.51,-37.35L10.21,-37.23L10.18,-36.72L11.03,-37.09L10.6,-36.41L10.81,-34.8"
    "3L10.15,-34.33L11.49,-33.14L11.43,-32.37L9.95,-31.38L9.97,-30.54L9.48,-30.31ZM-8.68,-27.4L-8"
    ".67,-28.84L-5.24,-30L-3.69,-30.9L-3.65,-31.64L-1.31,-32.26L-2.17,-35.17L-1.21,-35.71L1.47,-3"
    "6.61L8.42,-36.95L8.14,-34.66L7.52,-34.1L7.61,-33.34L9.06,-32.1L9.81,-29.42L9.32,-26.09L10.3,"
    "-24.38L10.77,-24.56L12,-23.47L5.68,-19.6L3.16,-19.06L3.15,-19.69L-8.68,-27.4ZM35.55,-32.39L3"
    "6.83,-32.31L38.79,-33.38L39.2,-32.16L37,-31.51L38,-30.51L36.07,-29.2L34.92,-29.5L35.55,-32.3"
    "9ZM51.58,-24.25L54.01,-24.12L56.07,-26.06L56.26,-25.71L56.4,-24.92L55.89,-24.92L55.98,-24.13"
    "L55.01,-22.5L52,-23L51.58,-24.25ZM47.97,-29.98L48.42,-28.55L46.57,-29.1L47.3,-30.06L47.97,-2"
    "9.98ZM39.2,-32.16L38.79,-33.38L41.01,-34.42L41.29,-36.36L42.78,-37.39L44.77,-37.17L46.08,-35"
    ".68L45.42,-33.97L47.33,-32.47L48.57,-29.93L47.3,-30.06L46.57,-29.1L44.71,-29.18L41.89,-31.19"
    "L39.2,-32.16ZM55.21,-22.71L55.89,-24.92L56.4,-24.92L59.81,-22.31L57.83,-20.24L57.69,-18.94L5"
    "4.79,-16.95L53.11,-16.65L52,-19L55,-20L55.67,-22L55.21,-22.71ZM102.58,-12.19L102.35,-13.39L1"
    "02.99,-14.23L106.04,-13.88L106.5,-14.57L107.61,-13.54L107.49,-12.34L105.81,-11.57L106.25,-10"
    ".96L103.5,-10.63L102.58,-12.19ZM105.22,-14.27L102.99,-14.23L102.35,-13.39L102.58,-12.19L100."
    "83,-12.63L100.98,-13.41L100.1,-13.41L99.22,-9.24L99.87,-9.21L100.46,-7.43L102.14,-6.22L101.1"
    "5,-5.69L98.15,-8.35L99.59,-11.89L98.19,-15.12L98.9,-16.18L97.38,-18.45L98.25,-19.71L100.12,-"
    "20.42L100.61,-19.51L101.28,-19.46L101.06,-17.51L103.2,-18.31L104.72,-17.43L104.78,-16.44L105"
    ".59,-15.57L105.22,-14.27ZM107.38,-14.2L106.5,-14.57L106.04,-13.88L105.22,-14.27L105.59,-15.5"
    "7L103.96,-18.24L101.06,-17.51L101.28,-19.46L100.61,-19.51L100.12,-20.42L101.18,-21.44L101.8,"
    "-21.17L101.65,-22.32L102.17,-22.46L103.2,-20.77L104.44,-20.76L104.82,-19.89L103.9,-19.27L105"
    ".09,-18.67L107.31,-15.91L107.38,-14.2ZM100.12,-20.42L98.25,-19.71L97.38,-18.45L98.9,-16.18L9"
    "8.19,-15.12L99.59,-11.89L98.55,-9.93L98.51,-13.12L97.16,-16.93L95.37,-15.71L94.19,-16.04L94."
    "32,-18.21L93.66,-19.73L92.37,-20.67L92.3,-21.48L93.17,-22.28L93.33,-24.08L94.11,-23.85L95.12"
    ",-26.57L96.42,-27.26L97.13,-27.08L97.33,-28.26L98.68,-27.51L98.67,-25.92L97.72,-25.08L97.6,-"
    "23.9L98.66,-24.06L98.9,-23.14L99.53,-22.95L99.24,-22.12L100.42,-21.56L101.15,-21.85L100.12,-"
    "20.42ZM104.33,-10.49L106.25,-10.96L105.81,-11.57L107.49,-12.34L107.56,-15.2L105.09,-18.67L10"
    "3.9,-19.27L104.82,-19.89L104.44,-20.76L103.2,-20.77L102.17,-22.46L105.33,-23.35L108.05,-21.5"
    "5L106.72,-20.7L105.66,-19.06L108.88,-15.28L109.2,-11.67L105.16,-8.6L105.08,-9.92L104.33,-10."
    "49ZM130.64,-42.4L129.67,-41.6L129.71,-40.88L127.53,-39.76L127.39,-39.21L128.21,-38.37L125.28"
    ",-37.67L124.71,-38.11L125.39,-39.39L124.27,-39.93L125.08,-40.57L126.87,-41.82L128.21,-41.47L"
    "128.05,-41.99L129.99,-42.99L130.64,-42.4ZM126.17,-37.75L128.35,-38.61L129.46,-36.78L129.09,-"
    "35.08L126.49,-34.39L126.12,-36.73L126.86,-36.89L126.17,-37.75ZM87.75,-49.3L92.23,-50.8L97.26"
    ",-49.73L98.23,-50.42L97.83,-51.01L98.86,-52.05L102.07,-51.26L102.26,-50.51L103.68,-50.09L106"
    ".89,-50.27L108.48,-49.28L110.66,-49.13L114.36,-50.25L116.68,-49.89L115.49,-48.14L115.74,-47."
    "73L118.06,-48.07L119.77,-47.05L117.42,-46.67L113.46,-44.81L111.87,-45.1L111.35,-44.46L111.83"
    ",-43.74L110.41,-42.87L104.96,-41.6L100.85,-42.66L96.35,-42.73L95.31,-44.24L90.95,-45.29L90.5"
    "9,-45.72L90.97,-46.89L90.28,-47.69L88.01,-48.6L87.75,-49.3ZM97.33,-28.26L97.13,-27.08L96.42,"
    "-27.26L95.12,-26.57L94.11,-23.85L93.33,-24.08L93.17,-22.28L92.67,-22.04L92.15,-23.63L91.71,-"
    "22.99L91.16,-23.5L92.38,-24.98L89.92,-25.27L89.83,-25.97L88.56,-26.45L88.21,-25.77L88.93,-25"
    ".24L88.08,-24.5L88.7,-24.23L88.89,-21.69L86.98,-21.5L86.5,-20.15L85.06,-19.48L82.19,-16.56L8"
    "0.32,-15.9L79.86,-10.36L77.54,-7.97L76.59,-8.9L73.53,-15.99L72.63,-21.36L70.47,-20.88L69.16,"
    "-22.09L69.64,-22.45L69.35,-22.84L68.18,-23.69L68.84,-24.36L71.04,-24.36L69.51,-26.94L70.62,-"
    "27.99L71.78,-27.91L75.26,-32.27L74.45,-32.76L73.75,-34.32L74.24,-34.75L76.87,-34.65L77.84,-3"
    "5.49L78.91,-34.32L79.21,-32.99L79.18,-32.48L78.46,-32.62L78.74,-31.52L81.11,-30.18L80.09,-28"
    ".79L83.3,-27.36L88.06,-26.41L88.12,-27.88L88.73,-28.09L88.84,-27.1L89.74,-26.72L92.03,-26.84"
    "L91.7,-27.77L94.57,-29.28L96.12,-29.45L96.59,-28.83L96.25,-28.41L97.33,-28.26ZM92.67,-22.04L"
    "92.37,-20.67L91.42,-22.77L90.5,-22.81L90.27,-21.84L89.03,-22.06L88.7,-24.23L88.08,-24.5L88.9"
    "3,-25.24L88.21,-25.77L88.56,-26.45L89.83,-25.97L89.92,-25.27L92.38,-24.98L91.16,-23.5L91.71,"
    "-22.99L92.15,-23.63L92.67,-22.04ZM91.7,-27.77L92.03,-26.84L88.84,-27.1L90.02,-28.3L91.7,-27."
    "77ZM88.12,-27.88L88.06,-26.41L87.23,-26.4L80.09,-28.79L81.53,-30.42L85.82,-28.2L88.12,-27.88"
    "ZM77.84,-35.49L76.87,-34.65L74.24,-34.75L73.75,-34.32L74.45,-32.76L75.26,-32.27L71.78,-27.91"
    "L70.62,-27.99L69.51,-26.94L71.04,-24.36L68.84,-24.36L68.18,-23.69L66.37,-25.43L61.5,-25.08L6"
    "1.87,-26.24L63.32,-26.76L62.73,-28.26L60.87,-29.83L62.55,-29.32L66.35,-29.89L66.94,-31.3L69."
    "32,-31.9L69.26,-32.5L70.32,-33.36L69.93,-34.02L70.88,-33.99L71.61,-35.15L71.26,-36.07L71.85,"
    "-36.51L75.16,-37.13L76.19,-35.9L77.84,-35.49ZM66.52,-37.36L69.2,-37.15L70.81,-38.49L71.84,-3"
    "6.74L73.26,-37.5L75.16,-37.13L71.26,-36.07L71.61,-35.15L70.88,-33.99L69.93,-34.02L70.32,-33."
    "36L69.26,-32.5L69.32,-31.9L66.94,-31.3L66.35,-29.89L62.55,-29.32L60.87,-29.83L61.78,-30.74L6"
    "0.54,-32.98L61.21,-35.65L62.98,-35.4L64.55,-36.31L64.75,-37.11L65.75,-37.66L66.52,-37.36ZM67"
    ".83,-37.14L68.39,-38.16L68.18,-38.9L67.44,-39.14L67.7,-39.58L68.54,-39.53L69.33,-40.73L70.67"
    ",-40.96L70.46,-40.5L71.01,-40.24L69.56,-40.1L69.46,-39.53L73.68,-39.43L73.93,-38.51L74.86,-3"
    "8.38L74.98,-37.42L73.26,-37.5L71.84,-36.74L70.81,-38.49L69.2,-37.15L67.83,-37.14ZM70.96,-42."
    "27L71.84,-42.85L73.49,-42.5L74.21,-43.3L80.26,-42.35L76.9,-41.07L76.53,-40.43L74.78,-40.37L7"
    "3.68,-39.43L69.46,-39.53L69.56,-40.1L71.77,-40.15L73.06,-40.87L70.42,-41.52L70.96,-42.27ZM52"
    ".5,-41.78L54.08,-42.32L55.46,-41.26L57.1,-41.32L56.93,-41.83L58.63,-42.75L59.98,-42.22L60.47"
    ",-41.22L61.88,-41.08L62.37,-40.05L64.17,-38.89L66.55,-37.97L66.52,-37.36L65.75,-37.66L64.75,"
    "-37.11L64.55,-36.31L62.23,-35.27L61.21,-35.65L61.12,-36.49L57.33,-38.03L55.51,-37.96L53.92,-"
    "37.2L53.88,-38.95L53.1,-39.29L53.36,-39.98L52.69,-40.03L52.92,-40.88L54.74,-40.95L53.72,-42."
    "12L52.92,-41.87L52.81,-41.14L52.5,-41.78ZM48.57,-29.93L47.33,-32.47L45.42,-33.97L46.08,-35.6"
    "8L44.23,-37.97L44.11,-39.43L44.79,-39.71L46.14,-38.74L48.06,-39.58L48.01,-38.79L49.2,-37.58L"
    "50.84,-36.87L53.83,-36.97L56.62,-38.12L61.12,-36.49L60.54,-32.98L61.78,-30.74L60.87,-29.83L6"
    "2.73,-28.26L63.32,-26.76L61.87,-26.24L61.5,-25.08L57.4,-25.74L56.97,-26.97L54.72,-26.48L53.4"
    "9,-26.81L51.52,-27.87L50.12,-30.15L48.57,-29.93ZM35.72,-32.71L36.61,-34.2L35.91,-35.41L36.74"
    ",-36.82L42.35,-37.23L41.29,-36.36L41.01,-34.42L36.83,-32.31L35.72,-32.71ZM46.51,-38.77L43.66"
    ",-40.25L43.58,-41.09L45.56,-40.81L46.51,-38.77ZM11.03,-58.86L12.3,-60.12L12.63,-61.29L11.99,"
    "-61.8L11.93,-63.13L12.58,-64.07L13.57,-64.05L13.92,-64.45L13.56,-64.79L16.77,-68.01L17.73,-6"
    "8.01L17.99,-68.57L19.88,-68.41L20.03,-69.07L20.65,-69.11L23.54,-67.94L23.9,-66.01L22.18,-65."
    "72L21.21,-65.03L21.37,-64.41L17.85,-62.75L17.12,-61.34L18.79,-60.08L17.87,-58.95L16.83,-58.7"
    "2L15.88,-56.1L14.67,-56.2L14.1,-55.41L12.94,-55.36L11.03,-58.86ZM28.18,-56.17L30.87,-55.55L3"
    "0.76,-54.81L32.69,-53.35L31.31,-53.07L31.79,-52.1L30.93,-52.04L30.56,-51.32L25.33,-51.91L23."
    "53,-51.58L23.48,-53.91L25.54,-54.28L26.49,-55.62L28.18,-56.17ZM31.79,-52.1L33.75,-52.34L34.3"
    "9,-51.77L34.22,-51.26L35.02,-51.21L35.36,-50.58L40.07,-49.6L39.74,-47.9L34.96,-46.27L35.01,-"
    "45.74L31.68,-46.71L30.75,-46.58L29.6,-45.29L28.68,-45.3L28.23,-45.49L28.86,-46.44L30.02,-46."
    "42L28.67,-48.12L27.52,-48.47L24.87,-47.74L22.71,-47.88L22.09,-48.42L22.78,-49.03L22.52,-49.4"
    "8L23.92,-50.42L23.53,-51.58L25.33,-51.91L30.56,-51.32L30.93,-52.04L31.79,-52.1ZM23.48,-53.91"
    "L23.8,-52.69L23.2,-52.49L24.03,-50.71L22.52,-49.48L22.78,-49.03L21.61,-49.47L19.83,-49.22L17"
    ".55,-50.36L16.18,-50.42L15.02,-51.11L14.07,-52.98L14.12,-53.76L17.62,-54.85L23.48,-53.91ZM16"
    ".98,-48.12L16.01,-46.68L14.63,-46.43L12.15,-47.12L11.05,-46.75L9.48,-47.1L9.9,-47.58L12.93,-"
    "47.47L12.88,-48.29L13.6,-48.88L16.5,-48.79L16.98,-48.12ZM22.09,-48.42L22.71,-47.88L21.02,-46"
    ".32L18.46,-45.76L16.2,-46.85L16.98,-48.12L17.86,-47.76L20.8,-48.62L22.09,-48.42ZM26.62,-48.2"
    "2L28.67,-48.12L30.02,-46.42L28.86,-46.44L28.23,-45.49L28.13,-46.81L26.62,-48.22ZM28.23,-45.4"
    "9L29.6,-45.29L28.84,-44.91L28.56,-43.71L27.24,-44.18L22.94,-43.82L22.71,-44.58L21.56,-44.77L"
    "20.22,-46.13L23.14,-48.1L24.87,-47.74L26.62,-48.22L28.13,-46.81L28.23,-45.49ZM26.49,-55.62L2"
    "5.54,-54.28L23.48,-53.91L22.76,-54.86L21.27,-55.19L21.06,-56.03L24.86,-56.37L26.49,-55.62ZM2"
    "7.29,-57.47L28.18,-56.17L26.49,-55.62L24.86,-56.37L21.06,-56.03L21.58,-57.41L22.52,-57.75L23"
    ".32,-57.01L24.12,-57.03L24.31,-57.79L25.16,-57.97L27.29,-57.47ZM27.98,-59.48L27.29,-57.47L24"
    ".31,-57.79L24.43,-58.38L23.43,-58.61L23.34,-59.19L27.98,-59.48ZM14.12,-53.76L15.02,-51.11L12"
    ".24,-50.27L13.6,-48.88L12.88,-48.29L12.93,-47.47L7.47,-47.62L8.1,-49.02L6.66,-49.2L6.04,-50."
    "13L5.99,-51.85L6.84,-52.23L7.1,-53.69L8.12,-53.53L8.8,-54.02L8.53,-54.96L9.92,-54.98L10.94,-"
    "54.01L12.52,-54.47L14.12,-53.76ZM22.66,-44.23L22.94,-43.82L27.24,-44.18L28.56,-43.71L27.67,-"
    "42.58L28,-42.01L26.12,-41.83L26.11,-41.33L22.95,-41.34L22.38,-42.32L22.99,-43.21L22.66,-44.2"
    "3ZM22.95,-41.34L26.6,-41.56L26.06,-40.82L23.71,-40.69L24.41,-40.12L22.63,-40.26L24.04,-37.66"
    "L23.12,-37.92L23.41,-37.41L22.77,-37.31L23.15,-36.42L22.49,-36.41L21.67,-36.84L20.15,-39.62L"
    "21.02,-40.84L22.95,-41.34ZM44.77,-37.17L36.74,-36.82L36.15,-35.82L35.78,-36.27L36.16,-36.65L"
    "34.71,-36.8L34.03,-36.22L30.62,-36.68L29.7,-36.14L27.64,-36.66L26.32,-38.21L26.8,-38.99L26.1"
    "7,-39.46L27.28,-40.42L28.82,-40.46L29.24,-41.22L31.15,-41.09L33.51,-42.02L35.17,-42.04L38.35"
    ",-40.95L42.62,-41.58L44.79,-39.71L44.11,-39.43L44.77,-37.17ZM26.12,-41.83L28,-42.01L28.99,-4"
    "1.3L26.36,-40.15L26.06,-40.82L26.6,-41.56L26.12,-41.83ZM21.02,-40.84L20.15,-39.62L19.41,-40."
    "25L19.3,-42.2L19.74,-42.69L20.52,-42.22L21.02,-40.84ZM16.56,-46.5L18.83,-45.91L19.39,-45.24L"
    "19.01,-44.86L15.96,-45.23L15.75,-44.82L18.45,-42.48L16.02,-43.51L14.9,-45.08L13.66,-45.14L15"
    ".33,-45.45L16.56,-46.5ZM9.59,-47.53L10.36,-46.48L7.27,-45.78L6.02,-46.27L6.74,-47.54L9.59,-4"
    "7.53ZM6.16,-50.8L5.67,-49.53L2.51,-51.15L4.97,-51.48L6.16,-50.8ZM6.91,-53.48L6.84,-52.23L5.9"
    "9,-51.85L6.16,-50.8L4.97,-51.48L3.31,-51.35L4.71,-53.09L6.91,-53.48ZM-9.03,-41.88L-8.26,-42."
    "28L-6.39,-41.38L-7.5,-39.63L-7.03,-38.08L-7.86,-36.84L-8.9,-36.87L-8.84,-38.27L-9.53,-38.74L"
    "-8.77,-40.76L-9.03,-41.88ZM-7.45,-37.1L-7.03,-38.08L-7.5,-39.63L-6.39,-41.38L-8.26,-42.28L-9"
    ".03,-41.88L-9.39,-43.03L-7.98,-43.75L-1.9,-43.42L0.34,-42.58L2.99,-42.47L3.04,-41.89L2.09,-4"
    "1.23L0.81,-41.01L-0.28,-39.31L0.11,-38.74L-2.15,-36.67L-4.37,-36.68L-5.38,-35.95L-7.45,-37.1"
    "ZM-6.2,-53.87L-6.03,-53.15L-6.79,-52.26L-9.98,-51.82L-9.17,-52.86L-9.69,-53.88L-7.57,-55.13L"
    "-7.57,-54.06L-6.2,-53.87ZM165.78,21.08L167.12,22.16L165.47,21.68L164.03,20.11L165.78,21.08ZM"
    "176.89,40.07L176.01,41.29L175.24,41.69L174.65,41.28L175.23,40.46L173.82,39.51L174.57,38.8L17"
    "4.7,37.38L172.64,34.53L174.33,35.27L175.96,37.56L178.52,37.7L177.97,39.17L177.21,39.15L176.8"
    "9,40.07ZM169.67,43.56L172.8,40.49L173.25,41.33L173.96,40.93L174.25,41.35L172.71,43.37L173.08"
    ",43.85L171.45,44.24L170.62,45.91L169.33,46.64L166.68,46.22L167.05,45.11L169.67,43.56ZM147.69"
    ",40.81L148.29,40.88L147.91,43.21L146.05,43.55L144.74,40.7L146.36,41.14L147.69,40.81ZM126.15,"
    "32.22L124.22,32.96L123.66,33.89L119.89,33.98L118.02,35.06L116.63,35.03L115.03,34.2L115.8,32."
    "21L113.34,26.12L113.78,26.55L113.44,25.62L114.23,26.3L113.39,24.38L113.74,22.48L114.15,21.76"
    "L114.23,22.52L116.71,20.7L120.86,19.68L123.01,16.41L123.43,17.27L123.86,17.07L123.5,16.6L123"
    ".82,16.11L124.26,16.33L125.69,14.23L127.07,13.82L128.36,14.87L129.62,14.97L129.41,14.42L130."
    "62,12.54L132.58,12.11L131.82,11.27L132.36,11.13L135.3,12.25L136.49,11.86L136.95,12.35L135.96"
    ",13.32L135.5,15L140.22,17.71L141.27,16.39L141.69,12.41L142.52,10.67L143.92,14.55L144.56,14.1"
    "7L145.37,14.98L146.39,18.96L148.85,20.39L149.68,22.34L150.73,22.4L150.9,23.46L152.86,25.27L1"
    "53.57,28.11L152.89,31.64L150.33,35.67L150,37.43L146.32,39.04L144.88,38.42L145.03,37.9L143.61"
    ",38.81L140.64,38.02L139.57,36.14L138.12,35.61L138.21,34.38L136.83,35.26L137.89,33.64L137.81,"
    "32.9L135.99,34.89L135.21,34.48L134.27,32.62L131.33,31.5L126.15,32.22ZM81.79,-7.52L81.64,-6.4"
    "8L80.35,-5.97L79.7,-8.2L80.15,-9.82L81.79,-7.52ZM109.48,-18.2L108.66,-18.51L108.63,-19.37L11"
    "0.79,-20.08L110.34,-18.68L109.48,-18.2ZM80.26,-42.35L80.18,-42.92L80.87,-43.18L79.97,-44.92L"
    "82.46,-45.54L83.18,-47.33L85.16,-47L85.77,-48.46L87.75,-49.3L88.01,-48.6L90.28,-47.69L90.97,"
    "-46.89L90.59,-45.72L90.95,-45.29L95.31,-44.24L96.35,-42.73L100.85,-42.66L104.96,-41.6L109.24"
    ",-42.52L111.83,-43.74L111.35,-44.46L111.87,-45.1L113.46,-44.81L117.42,-46.67L119.66,-46.69L1"
    "18.06,-48.07L115.74,-47.73L115.49,-48.14L116.68,-49.89L117.88,-49.51L119.29,-50.14L120.74,-5"
    "1.96L120.18,-52.75L122.25,-53.43L125.95,-52.79L127.66,-49.76L129.4,-49.44L130.58,-48.73L130."
    "99,-47.79L135.03,-48.48L133.1,-45.14L131.03,-44.97L131.14,-42.93L130.63,-42.9L130.64,-42.4L1"
    "29.99,-42.99L128.05,-41.99L128.21,-41.47L126.87,-41.82L124.27,-39.93L121.05,-38.9L122.17,-40"
    ".42L121.64,-40.95L117.53,-38.74L119.7,-37.16L120.82,-37.87L122.36,-37.45L122.52,-36.93L121.1"
    ",-36.65L119.15,-34.91L120.23,-34.36L121.91,-31.69L121.89,-30.95L121.26,-30.68L122.09,-29.83L"
    "121.68,-28.23L121.13,-28.14L118.66,-24.55L115.89,-22.78L110.79,-21.4L110.44,-20.34L109.89,-2"
    "0.28L109.86,-21.4L107.04,-21.81L106.73,-22.79L105.33,-23.35L101.65,-22.32L101.8,-21.17L101.2"
    "7,-21.2L101.15,-21.85L100.42,-21.56L99.24,-22.12L99.53,-22.95L98.9,-23.14L98.66,-24.06L97.6,"
    "-23.9L97.72,-25.08L98.67,-25.92L98.68,-27.51L97.91,-28.34L96.25,-28.41L96.59,-28.83L96.12,-2"
    "9.45L94.57,-29.28L92.5,-27.9L90.02,-28.3L88.81,-27.3L88.73,-28.09L85.82,-28.2L78.74,-31.52L7"
    "8.46,-32.62L79.18,-32.48L79.21,-32.99L78.91,-34.32L77.84,-35.49L76.19,-35.9L74.98,-37.42L74."
    "86,-38.38L73.93,-38.51L73.68,-39.43L74.78,-40.37L76.53,-40.43L76.9,-41.07L80.26,-42.35ZM121."
    "78,-24.39L120.75,-21.97L120.11,-23.56L121.5,-25.3L121.78,-24.39ZM10.44,-46.89L12.15,-47.12L1"
    "3.81,-46.51L13.94,-45.59L12.33,-45.38L12.59,-44.09L15.14,-41.96L15.93,-41.96L15.89,-41.54L18"
    ".38,-40.36L18.29,-39.81L16.87,-40.44L16.45,-39.8L17.17,-39.42L17.05,-38.9L15.68,-37.91L16.11"
    ",-38.96L15.41,-40.05L11.19,-42.36L10.2,-43.92L8.89,-44.37L7.44,-43.69L6.84,-45.99L8.97,-46.0"
    "4L10.44,-46.89ZM14.76,-38.14L15.52,-38.23L15.1,-36.62L12.43,-37.61L12.57,-38.13L14.76,-38.14"
    "ZM8.71,-40.9L9.21,-41.21L9.81,-40.5L9.67,-39.18L8.81,-38.91L8.16,-40.95L8.71,-40.9ZM9.92,-54"
    ".98L8.53,-54.96L8.09,-56.54L10.58,-57.73L10.25,-56.89L10.91,-56.46L9.65,-55.47L9.92,-54.98ZM"
    "12.37,-56.11L12.69,-55.61L12.09,-54.8L11.04,-55.36L10.9,-55.78L12.37,-56.11ZM-6.2,-53.87L-7."
    "57,-54.06L-7.57,-55.13L-5.66,-54.55L-6.2,-53.87ZM-3.09,-53.4L-2.95,-53.98L-3.63,-54.62L-5.08"
    ",-55.06L-5.05,-55.78L-5.59,-55.31L-6.15,-56.79L-5.01,-58.63L-3.01,-58.63L-4.07,-57.55L-1.96,"
    "-57.68L-3.12,-55.97L-2.09,-55.91L0.47,-52.93L1.68,-52.74L1.05,-51.81L1.45,-51.29L0.55,-50.77"
    "L-5.78,-50.16L-3.41,-51.43L-5.27,-51.99L-4.22,-52.3L-4.77,-52.84L-4.58,-53.5L-3.09,-53.4ZM-1"
    "4.51,-66.46L-14.74,-65.81L-13.61,-65.13L-18.66,-63.5L-22.76,-63.96L-21.78,-64.4L-23.96,-64.8"
    "9L-22.23,-65.38L-24.33,-65.61L-23.65,-66.26L-22.13,-66.41L-20.58,-65.73L-19.06,-66.28L-14.51"
    ",-66.46ZM46.4,-41.86L47.82,-41.15L48.58,-41.81L50.39,-40.26L49.57,-40.18L48.88,-38.32L48.01,"
    "-38.79L48.06,-39.58L46.51,-38.77L46.48,-39.46L45.61,-39.9L45.89,-40.22L44.97,-41.25L46.5,-41"
    ".06L46.4,-41.86ZM39.96,-43.43L45.47,-42.5L46.64,-41.18L41.55,-41.54L41.45,-42.65L39.96,-43.4"
    "3ZM122.59,-9.98L122.95,-10.88L123.5,-10.94L123.34,-10.27L124.08,-11.23L123,-9.02L122.59,-9.9"
    "8ZM126.38,-8.41L126.54,-7.19L126.2,-6.27L125.83,-7.29L125.36,-6.79L125.4,-5.58L124.22,-6.16L"
    "124.24,-7.36L123.61,-7.83L121.92,-7.19L123.49,-8.69L123.84,-8.24L125.47,-8.99L125.41,-9.76L1"
    "26.22,-9.29L126.38,-8.41ZM122.34,-18.22L122.52,-17.09L121.66,-15.93L121.73,-14.33L123.95,-13"
    ".78L124.08,-12.54L122.93,-13.55L122.67,-13.19L122.03,-13.78L120.63,-13.86L120.99,-14.53L120."
    "56,-14.4L119.92,-15.41L120.72,-18.51L122.34,-18.22ZM125.5,-12.16L125.78,-11.05L125.01,-11.31"
    "L125.28,-10.36L124.8,-10.13L124.3,-11.5L124.88,-11.79L124.27,-12.56L125.5,-12.16ZM100.09,-6."
    "46L101.08,-6.2L101.15,-5.69L102.14,-6.22L102.96,-5.52L104.23,-1.29L103.52,-1.23L101.39,-2.76"
    "L100.09,-6.46ZM117.88,-4.14L115.87,-4.31L114.62,-1.43L110.51,-0.77L109.83,-1.34L109.66,-2.01"
    "L111.17,-1.85L111.37,-2.7L113,-3.1L114.2,-4.53L114.66,-4.01L115.35,-4.32L115.45,-5.45L116.73"
    ",-6.92L119.18,-5.41L117.88,-4.14ZM13.81,-46.51L16.56,-46.5L15.33,-45.45L13.72,-45.5L13.81,-4"
    "6.51ZM28.59,-69.06L28.45,-68.36L29.98,-67.7L29.05,-66.94L30.22,-65.81L29.54,-64.95L30.44,-64"
    ".2L30.04,-63.55L31.52,-62.87L31.14,-62.36L28.07,-60.5L22.87,-59.85L21.32,-60.72L21.54,-61.71"
    "L21.06,-62.61L21.54,-63.19L25.4,-65.11L23.57,-66.4L23.54,-67.94L20.65,-69.11L24.74,-68.65L26"
    ".18,-69.83L27.73,-70.16L29.02,-69.77L28.59,-69.06ZM22.56,-49.09L21.87,-48.32L20.8,-48.62L17."
    "86,-47.76L16.88,-48.47L18.55,-49.5L22.56,-49.09ZM15.02,-51.11L18.85,-49.5L16.96,-48.6L15.25,"
    "-49.04L14.34,-48.56L12.52,-49.55L12.24,-50.27L15.02,-51.11ZM36.43,-14.42L36.85,-16.96L38.41,"
    "-18L39.27,-15.92L43.08,-12.7L42.35,-12.54L40.03,-14.52L37.91,-14.96L37.59,-14.21L36.43,-14.4"
    "2ZM141.88,-39.18L140.96,-38.17L140.25,-35.14L137.22,-34.61L135.79,-33.46L135.12,-33.85L135.0"
    "8,-34.6L130.99,-33.89L132,-33.15L131.33,-31.45L130.69,-31.03L130.2,-31.42L130.45,-32.32L129."
    "41,-33.3L132.62,-35.43L135.68,-35.53L136.72,-37.3L137.39,-36.83L139.43,-38.22L140.31,-41.2L1"
    "41.37,-41.38L141.88,-39.18ZM144.61,-43.96L145.32,-44.38L145.54,-43.26L144.06,-42.99L143.18,-"
    "42L141.61,-42.68L141.07,-41.58L139.96,-41.57L139.82,-42.56L140.31,-43.33L141.38,-43.39L141.9"
    "7,-45.55L144.61,-43.96ZM132.37,-33.46L133.9,-34.36L134.77,-33.81L134.2,-33.2L133.28,-33.29L1"
    "33.01,-32.7L132.37,-33.46ZM-58.17,20.18L-57.94,22.09L-55.8,22.36L-55.4,23.96L-54.29,24.02L-5"
    "4.79,26.62L-55.7,27.39L-58.62,27.12L-57.78,25.16L-60.85,23.88L-62.69,22.25L-61.79,19.63L-59."
    "12,19.36L-58.17,20.18ZM52,-19L53.11,-16.65L52.39,-16.38L52.17,-15.6L44.99,-12.7L43.48,-12.64"
    "L42.6,-15.21L43.38,-17.58L47,-16.95L49.12,-18.62L52,-19ZM34.96,-29.36L36.07,-29.2L37.5,-30L3"
    "8,-30.51L37,-31.51L39.2,-32.16L41.89,-31.19L44.71,-29.18L47.46,-29L50.15,-26.69L50.24,-25.61"
    "L51.39,-24.63L52,-23L55.21,-22.71L55.67,-22L55,-20L49.12,-18.62L47,-16.95L43.38,-17.58L42.78"
    ",-16.35L40.94,-19.49L39.14,-21.29L38.49,-23.69L37.48,-24.29L35.13,-28.06L34.63,-28.06L34.96,"
    "-29.36ZM-2.17,-35.17L-1.31,-32.26L-3.65,-31.64L-3.69,-30.9L-5.24,-30L-8.67,-28.84L-8.79,-27."
    "12L-11.39,-26.88L-12.5,-24.77L-13.89,-23.69L-14.75,-21.5L-17.02,-21.42L-14.44,-26.25L-9.56,-"
    "29.93L-9.81,-31.18L-8.66,-33.24L-6.91,-34.11L-5.93,-35.76L-2.17,-35.17ZM36.87,-22L25,-22L24."
    "7,-30.04L25.16,-31.57L28.91,-30.87L30.98,-31.56L31.96,-30.93L34.27,-31.22L34.92,-29.5L34.15,"
    "-27.82L32.32,-29.76L35.69,-23.93L35.53,-23.1L36.87,-22ZM25,-22L25,-20L23.85,-20L23.84,-19.58"
    "L15.86,-23.41L14.14,-22.49L10.77,-24.56L10.3,-24.38L9.32,-26.09L9.95,-31.38L11.43,-32.37L11."
    "49,-33.14L15.25,-32.27L15.71,-31.38L19.09,-30.27L20.05,-30.99L19.82,-31.75L20.85,-32.71L24.9"
    "2,-31.9L25,-22ZM47.79,-8L44.96,-5L43.66,-4.96L41.86,-3.92L40.77,-4.26L39.56,-3.42L36.16,-4.4"
    "5L34.71,-6.59L32.95,-7.78L33.83,-8.38L34.26,-10.63L35.86,-12.58L36.43,-14.42L37.59,-14.21L37"
    ".91,-14.96L40.03,-14.52L41.6,-13.45L42.35,-12.54L41.76,-11.05L42.78,-10.93L42.56,-10.57L43.6"
    "8,-9.18L47.79,-8ZM42.35,-12.54L43.32,-12.39L42.78,-10.93L41.76,-11.05L42.35,-12.54ZM48.95,-1"
    "1.41L48.94,-9.45L47.79,-8L43.68,-9.18L42.56,-10.57L43.15,-11.46L44.12,-10.45L48.95,-11.41ZM3"
    "3.9,0.95L29.58,1.34L29.88,-0.6L31.17,-2.2L30.77,-2.34L31.25,-3.78L33.39,-3.79L34.01,-4.25L34"
    ".48,-3.56L35.04,-1.91L33.89,-0.11L33.9,0.95ZM30.42,1.13L30.76,2.29L29.02,2.84L29.29,1.62L30."
    "42,1.13ZM18.56,-42.65L16.46,-44.04L15.75,-44.82L15.96,-45.23L19.37,-44.86L19.45,-43.57L18.56"
    ",-42.65ZM22.38,-42.32L22.95,-41.34L20.61,-41.09L20.76,-42.05L22.38,-42.32ZM18.83,-45.91L20.2"
    "2,-46.13L21.56,-44.77L22.71,-44.58L22.41,-44.01L22.99,-43.21L22.55,-42.46L21.58,-42.25L21.78"
    ",-42.68L20.81,-43.27L20.26,-42.81L19.22,-43.52L19.6,-44.04L18.83,-45.91ZM20.07,-42.59L19.37,"
    "-41.88L18.45,-42.48L19.22,-43.52L20.34,-42.9L20.07,-42.59ZM20.59,-41.86L20.07,-42.59L20.64,-"
    "43.22L21.78,-42.68L20.59,-41.86ZM30.83,-3.51L29.72,-4.6L27.98,-4.41L23.89,-8.62L24.54,-8.92L"
    "25.07,-10.27L25.79,-10.41L26.75,-9.47L28.97,-9.4L30,-10.29L31.35,-9.81L32.4,-11.08L32.07,-11"
    ".97L33.21,-12.18L33.97,-8.68L32.95,-7.78L34.08,-7.23L35.3,-5.51L33.39,-3.79L30.83,-3.51Z"
)


# IATA -> (lat, lon) for every large airport plus the outstations the FIS
# network actually reaches (OurAirports, public domain — the same source
# tools/load_airports.py uses for the ICAO-keyed `airports` table, which has
# no IATA column to join on). Generous on purpose: a new LH destination gets
# a marker without a code change. Anything still unknown is reported by
# /api/book/map as `unplaced` rather than silently dropped from the map.
_AIRPORT_LL = {
    'AAC': (31.055, 33.828), 'AAE': (36.827, 7.813), 'AAL': (57.095, 9.85),
    'AAN': (24.262, 55.609), 'AAR': (56.303, 10.618), 'ABA': (53.74, 91.385),
    'ABB': (6.204, 6.665), 'ABD': (30.368, 48.23), 'ABJ': (5.261, -3.926),
    'ABQ': (35.04, -106.609), 'ABV': (9.007, 7.263), 'ABZ': (57.202, -2.198),
    'ACA': (16.757, -99.753), 'ACC': (5.605, -0.167), 'ACE': (28.945, -13.605),
    'ADA': (36.982, 35.28), 'ADB': (38.292, 27.157), 'ADD': (8.978, 38.799),
    'ADE': (12.83, 45.03), 'ADJ': (31.973, 35.992), 'ADL': (-34.948, 138.533),
    'ADZ': (12.584, -81.711), 'AEP': (-34.559, -58.416), 'AER': (43.45, 39.957),
    'AES': (62.56, 6.111), 'AEY': (65.657, -18.072), 'AGA': (30.322, -9.412),
    'AGP': (36.675, -4.499), 'AGT': (-25.457, -54.84), 'AGU': (21.7, -102.318),
    'AHB': (18.24, 42.657), 'AJF': (29.783, 40.101), 'AKL': (-37.012, 174.786),
    'AKX': (50.248, 57.204), 'ALA': (43.354, 77.043), 'ALB': (42.748, -73.802),
    'ALC': (38.282, -0.558), 'ALG': (36.694, 3.215), 'ALP': (36.181, 37.227),
    'AMD': (23.077, 72.635), 'AMM': (31.723, 35.993), 'AMQ': (-3.71, 128.089),
    'AMS': (52.309, 4.764), 'ANC': (61.179, -149.993), 'ANF': (-23.445, -70.445),
    'ANU': (17.137, -61.793), 'AOE': (39.812, 30.519), 'AOJ': (40.734, 140.689),
    'APL': (-15.106, 39.282), 'APW': (-13.83, -172.008), 'AQI': (28.336, 46.127),
    'AQJ': (29.612, 35.018), 'AQP': (-16.341, -71.569), 'ARN': (59.648, 17.929),
    'ASB': (37.987, 58.361), 'ASF': (46.283, 48.011), 'ASK': (6.903, -5.366),
    'ASR': (38.77, 35.495), 'ASU': (-25.24, -57.519), 'ASW': (23.961, 32.82),
    'ATH': (37.936, 23.945), 'ATL': (33.637, -84.428), 'ATQ': (31.71, 74.797),
    'ATZ': (27.046, 31.013), 'AUA': (12.501, -70.014), 'AUH': (24.441, 54.649),
    'AUS': (30.198, -97.662), 'AVV': (-38.04, 144.467), 'AWA': (7.101, 38.396),
    'AWZ': (31.336, 48.764), 'AYT': (36.899, 30.801), 'AZI': (24.427, 54.46),
    'BAH': (26.267, 50.638), 'BAQ': (10.89, -74.781), 'BAV': (40.56, 109.997),
    'BAX': (53.361, 83.54), 'BBI': (20.251, 85.815), 'BBK': (-17.832, 25.166),
    'BBU': (44.503, 26.103), 'BCD': (10.776, 123.019), 'BCM': (46.522, 26.91),
    'BCN': (41.297, 2.078), 'BCU': (10.483, 9.744), 'BDA': (32.364, -64.678),
    'BDJ': (-3.44, 114.761), 'BDL': (41.939, -72.688), 'BDQ': (22.336, 73.226),
    'BDS': (40.658, 17.947), 'BEG': (44.818, 20.309), 'BEL': (-1.379, -48.476),
    'BEM': (32.402, -6.316), 'BEN': (32.097, 20.27), 'BER': (52.362, 13.502),
    'BES': (48.448, -4.419), 'BEW': (-19.796, 34.908), 'BEY': (33.82, 35.487),
    'BFN': (-29.093, 26.302), 'BFS': (54.658, -6.216), 'BGF': (4.398, 18.519),
    'BGI': (13.075, -59.491), 'BGO': (60.293, 5.218), 'BGW': (33.263, 44.235),
    'BGY': (45.669, 9.709), 'BHK': (39.775, 64.482), 'BHM': (33.563, -86.751),
    'BHO': (23.288, 77.337), 'BHX': (52.454, -1.748), 'BIA': (42.553, 9.484),
    'BIO': (43.301, -2.911), 'BJA': (36.713, 5.07), 'BJL': (13.338, -16.652),
    'BJM': (-3.324, 29.319), 'BJV': (37.249, 27.664), 'BJX': (20.993, -101.48),
    'BKI': (5.933, 116.049), 'BKK': (13.681, 100.747), 'BKO': (12.534, -7.95),
    'BLA': (10.111, -64.692), 'BLJ': (35.752, 6.309), 'BLL': (55.74, 9.157),
    'BLQ': (44.535, 11.289), 'BLR': (13.198, 77.706), 'BLZ': (-15.677, 34.972),
    'BME': (-17.949, 122.228), 'BNA': (36.124, -86.678), 'BND': (27.218, 56.378),
    'BNE': (-27.384, 153.117), 'BNX': (44.941, 17.298), 'BOD': (44.829, -0.715),
    'BOG': (4.702, -74.147), 'BOI': (43.564, -116.223), 'BOJ': (42.57, 27.515),
    'BOM': (19.089, 72.868), 'BON': (12.131, -68.269), 'BOO': (67.269, 14.365),
    'BOS': (42.362, -71.008), 'BOY': (11.16, -4.331), 'BPN': (-1.268, 116.895),
    'BPS': (-16.438, -39.081), 'BQT': (52.108, 23.897), 'BRC': (-41.151, -71.158),
    'BRE': (53.047, 8.789), 'BRI': (41.139, 16.761), 'BRM': (10.043, -69.359),
    'BRS': (51.382, -2.716), 'BRU': (50.901, 4.484), 'BSA': (11.275, 49.139),
    'BSB': (-15.869, -47.921), 'BSG': (1.905, 9.806), 'BSK': (34.793, 5.739),
    'BSL': (47.601, 7.521), 'BSR': (30.549, 47.662), 'BSZ': (43.061, 74.478),
    'BTH': (1.121, 104.119), 'BTJ': (5.525, 95.42), 'BTS': (48.17, 17.213),
    'BUD': (47.43, 19.262), 'BUF': (42.94, -78.732), 'BUQ': (-20.016, 28.623),
    'BUR': (34.203, -118.358), 'BUS': (41.609, 41.6), 'BVA': (49.454, 2.113),
    'BVB': (2.846, -60.691), 'BVC': (16.136, -22.889), 'BWA': (27.505, 83.41),
    'BWI': (39.175, -76.668), 'BWN': (4.944, 114.928), 'BXY': (45.622, 63.211),
    'BZE': (17.54, -88.304), 'BZV': (-4.252, 15.253), 'CAG': (39.251, 9.054),
    'CAI': (30.112, 31.397), 'CAN': (23.392, 113.299), 'CAP': (19.726, -72.201),
    'CAY': (4.82, -52.361), 'CBB': (-17.421, -66.177), 'CCJ': (11.136, 75.955),
    'CCK': (-12.192, 96.834), 'CCP': (-36.772, -73.063), 'CCS': (10.602, -66.991),
    'CCU': (22.654, 88.448), 'CDG': (49.009, 2.554), 'CEB': (10.309, 123.98),
    'CEI': (19.952, 99.883), 'CEK': (55.303, 61.505), 'CFE': (45.787, 3.169),
    'CFK': (36.217, 1.341), 'CFU': (39.601, 19.912), 'CGB': (-15.653, -56.117),
    'CGH': (-23.628, -46.655), 'CGK': (-6.126, 106.656), 'CGN': (50.866, 7.143),
    'CGO': (34.526, 113.849), 'CGP': (22.25, 91.813), 'CGQ': (43.996, 125.685),
    'CGY': (8.612, 124.456), 'CHC': (-43.489, 172.532), 'CHQ': (35.531, 24.151),
    'CHS': (32.896, -80.038), 'CIA': (41.799, 12.595), 'CIT': (42.365, 69.476),
    'CIX': (-6.789, -79.828), 'CJB': (11.03, 77.043), 'CJJ': (36.716, 127.5),
    'CJS': (31.637, -106.429), 'CJU': (33.512, 126.493), 'CKG': (29.712, 106.652),
    'CKY': (9.577, -13.612), 'CLE': (41.412, -81.85), 'CLJ': (46.786, 23.686),
    'CLO': (3.543, -76.382), 'CLT': (35.214, -80.943), 'CMB': (7.181, 79.884),
    'CMH': (39.998, -82.892), 'CMN': (33.367, -7.59), 'CMW': (21.42, -77.848),
    'CND': (44.362, 28.488), 'CNF': (-19.636, -43.967), 'CNN': (11.916, 75.545),
    'CNS': (-16.879, 145.749), 'CNX': (18.767, 98.963), 'COK': (10.151, 76.401),
    'COO': (6.357, 2.384), 'COR': (-31.312, -64.208), 'COS': (38.806, -104.701),
    'COV': (36.891, 35.071), 'CPH': (55.618, 12.656), 'CPT': (-33.974, 18.604),
    'CRA': (44.318, 23.889), 'CRD': (-45.787, -67.463), 'CRK': (15.186, 120.56),
    'CRL': (50.462, 4.46), 'CRZ': (38.931, 63.564), 'CSX': (28.189, 113.22),
    'CTA': (37.467, 15.066), 'CTG': (10.442, -75.513), 'CTS': (42.775, 141.69),
    'CTU': (30.558, 103.946), 'CUL': (24.765, -107.475), 'CUN': (21.041, -86.873),
    'CUR': (12.189, -68.96), 'CUU': (28.703, -105.964), 'CUZ': (-13.536, -71.939),
    'CVG': (39.049, -84.668), 'CWB': (-25.529, -49.176), 'CWL': (51.397, -3.343),
    'CXI': (1.986, -157.35), 'CXR': (11.998, 109.219), 'CZL': (36.276, 6.62),
    'CZM': (20.515, -86.929), 'DAC': (23.843, 90.398), 'DAD': (16.044, 108.199),
    'DAL': (32.845, -96.848), 'DAM': (33.411, 36.516), 'DAR': (-6.873, 39.207),
    'DAT': (40.061, 113.481), 'DBB': (30.924, 28.462), 'DBV': (42.562, 18.266),
    'DCA': (38.852, -77.038), 'DEB': (47.489, 21.616), 'DEL': (28.556, 77.095),
    'DEN': (39.86, -104.674), 'DFW': (32.897, -97.038), 'DHA': (26.265, 50.152),
    'DIA': (25.259, 51.566), 'DIL': (-8.547, 125.525), 'DIR': (9.624, 41.855),
    'DJE': (33.874, 10.777), 'DJG': (24.285, 9.464), 'DJJ': (-2.58, 140.52),
    'DKR': (14.742, -17.479), 'DLA': (4.006, 9.719), 'DLC': (38.966, 121.538),
    'DLM': (36.713, 28.793), 'DMB': (42.854, 71.304), 'DME': (55.409, 37.906),
    'DMK': (13.913, 100.607), 'DMM': (26.469, 49.798), 'DNA': (26.352, 127.769),
    'DNH': (40.162, 94.813), 'DOH': (25.273, 51.608), 'DPS': (-8.748, 115.167),
    'DQM': (19.502, 57.634), 'DRP': (13.112, 123.677), 'DRS': (51.134, 13.768),
    'DRW': (-12.415, 130.882), 'DSM': (41.534, -93.657), 'DSN': (39.494, 109.86),
    'DSS': (14.671, -17.073), 'DSY': (10.914, 103.227), 'DTM': (51.518, 7.612),
    'DTW': (42.214, -83.354), 'DUB': (53.429, -6.262), 'DUR': (-29.614, 31.12),
    'DUS': (51.29, 6.767), 'DVO': (7.126, 125.646), 'DWC': (24.896, 55.162),
    'DXB': (25.25, 55.371), 'DXN': (28.18, 77.612), 'DYG': (29.105, 110.443),
    'DYU': (38.544, 68.823), 'DZA': (-12.809, 45.282), 'DZN': (47.709, 67.738),
    'EBB': (0.042, 32.444), 'EBL': (36.236, 43.947), 'ECN': (35.153, 33.507),
    'EDI': (55.95, -3.372), 'EDL': (0.404, 35.239), 'EDO': (39.553, 27.01),
    'EES': (23.98, 35.46), 'EHU': (30.341, 115.039), 'EIN': (51.45, 5.375),
    'EIS': (18.445, -64.542), 'ELP': (31.81, -106.376), 'ELQ': (26.303, 43.774),
    'ELS': (-33.036, 27.826), 'EMA': (52.831, -1.328), 'ENO': (-27.228, -55.838),
    'ENU': (6.474, 7.56), 'ERF': (50.978, 10.961), 'ESB': (40.128, 32.995),
    'ESM': (0.979, -79.627), 'ETM': (29.727, 35.014), 'EUN': (27.142, -13.225),
    'EVE': (68.491, 16.678), 'EVN': (40.149, 44.398), 'EWR': (40.689, -74.171),
    'EZE': (-34.822, -58.536), 'FAE': (62.063, -7.276), 'FAO': (37.016, -7.971),
    'FAT': (36.776, -119.718), 'FBM': (-11.591, 27.531), 'FCO': (41.805, 12.252),
    'FDF': (14.591, -61.003), 'FDH': (47.671, 9.511), 'FEZ': (33.927, -4.978),
    'FIH': (-4.386, 15.445), 'FJR': (25.108, 56.328), 'FKB': (48.779, 8.081),
    'FKI': (0.482, 25.338), 'FLL': (26.073, -80.153), 'FLN': (-27.67, -48.553),
    'FLR': (43.809, 11.203), 'FMM': (47.988, 10.238), 'FMO': (52.134, 7.688),
    'FNA': (8.616, -13.195), 'FNC': (32.698, -16.775), 'FNJ': (39.224, 125.67),
    'FOC': (25.929, 119.673), 'FOR': (-3.776, -38.532), 'FPO': (26.558, -78.696),
    'FRA': (50.027, 8.558), 'FRW': (-21.159, 27.469), 'FSC': (41.502, 9.097),
    'FSZ': (34.795, 138.191), 'FUE': (28.453, -13.864), 'FUK': (33.586, 130.451),
    'GAN': (-0.693, 73.153), 'GAU': (26.107, 91.585), 'GBE': (-24.555, 25.918),
    'GCM': (19.293, -81.358), 'GDL': (20.523, -103.31), 'GDN': (54.378, 18.466),
    'GEG': (47.62, -117.534), 'GEO': (6.499, -58.254), 'GES': (6.057, 125.096),
    'GHV': (45.706, 25.523), 'GIB': (36.152, -5.35), 'GIG': (-22.81, -43.251),
    'GJL': (36.794, 5.874), 'GLA': (55.872, -4.433), 'GMP': (37.558, 126.791),
    'GND': (12.004, -61.785), 'GNJ': (40.739, 46.32), 'GNY': (37.446, 38.896),
    'GOA': (44.412, 8.841), 'GOH': (64.191, -51.679), 'GOI': (15.38, 73.833),
    'GOJ': (56.227, 43.785), 'GOM': (-1.667, 29.238), 'GOT': (57.663, 12.28),
    'GOU': (9.335, 13.372), 'GOX': (15.744, 73.861), 'GRJ': (-34.006, 22.379),
    'GRO': (41.905, 2.762), 'GRQ': (53.119, 6.578), 'GRR': (42.881, -85.523),
    'GRU': (-23.431, -46.47), 'GRV': (43.388, 45.7), 'GRZ': (46.991, 15.44),
    'GSM': (26.755, 55.902), 'GSO': (36.099, -79.937), 'GSV': (51.713, 46.171),
    'GUA': (14.583, -90.528), 'GUM': (13.485, 144.797), 'GUW': (47.121, 51.82),
    'GVA': (46.238, 6.109), 'GWD': (25.297, 62.499), 'GXF': (15.966, 48.788),
    'GYD': (40.473, 50.051), 'GYE': (-2.157, -79.884), 'GYN': (-16.632, -49.221),
    'GZT': (36.947, 37.479), 'HAH': (-11.534, 43.272), 'HAJ': (52.461, 9.685),
    'HAK': (19.935, 110.459), 'HAM': (53.63, 9.988), 'HAN': (21.221, 105.807),
    'HAQ': (6.743, 73.167), 'HAS': (27.438, 41.686), 'HAV': (22.989, -82.409),
    'HBA': (-42.837, 147.513), 'HBE': (30.932, 29.696), 'HDY': (6.933, 100.393),
    'HEA': (34.21, 62.228), 'HEL': (60.318, 24.963), 'HER': (35.34, 25.18),
    'HET': (40.85, 111.825), 'HFE': (31.988, 116.977), 'HGA': (9.514, 44.083),
    'HGH': (30.236, 120.429), 'HHN': (49.946, 7.262), 'HIA': (33.793, 119.127),
    'HIJ': (34.436, 132.919), 'HIR': (-9.428, 160.055), 'HKD': (41.77, 140.822),
    'HKG': (22.312, 113.915), 'HKT': (8.113, 98.317), 'HLA': (-25.939, 27.927),
    'HLD': (49.209, 119.822), 'HLP': (-6.267, 106.89), 'HMB': (26.343, 31.743),
    'HMO': (29.093, -111.053), 'HND': (35.55, 139.787), 'HNL': (21.318, -157.926),
    'HOF': (25.285, 49.485), 'HOG': (20.785, -76.316), 'HOU': (29.645, -95.277),
    'HPH': (20.817, 106.724), 'HRB': (45.623, 126.25), 'HRE': (-17.932, 31.093),
    'HRG': (27.177, 33.797), 'HSA': (43.311, 68.55), 'HSG': (33.15, 130.302),
    'HSN': (29.934, 122.362), 'HSR': (22.379, 71.039), 'HSS': (29.186, 75.741),
    'HTA': (52.025, 113.306), 'HUN': (24.023, 121.618), 'HUX': (15.775, -96.26),
    'HWR': (30.749, 75.63), 'HYD': (17.231, 78.43), 'IAD': (38.944, -77.456),
    'IAH': (29.984, -95.341), 'IAR': (57.561, 40.157), 'IAS': (47.18, 27.621),
    'IBR': (36.181, 140.414), 'IBZ': (38.873, 1.373), 'ICN': (37.469, 126.451),
    'IDR': (22.721, 75.801), 'IFN': (32.755, 51.884), 'IGU': (-25.594, -54.489),
    'IKA': (35.416, 51.152), 'IKT': (52.267, 104.396), 'IKU': (42.586, 76.701),
    'ILO': (10.833, 122.493), 'ILR': (8.44, 4.494), 'IMF': (24.76, 93.897),
    'INC': (38.323, 106.393), 'IND': (39.717, -86.294), 'INI': (43.337, 21.856),
    'INN': (47.26, 11.344), 'IOM': (54.083, -4.624), 'IPC': (-27.165, -109.421),
    'IPH': (4.567, 101.092), 'IQQ': (-20.536, -70.181), 'IQT': (-3.785, -73.309),
    'ISB': (33.549, 72.826), 'ISK': (20.119, 73.913), 'ISL': (40.972, 28.824),
    'IST': (41.275, 28.732), 'ITM': (34.781, 135.441), 'IVL': (68.607, 27.405),
    'IXB': (26.681, 88.329), 'IXC': (30.674, 76.788), 'IXE': (12.955, 74.887),
    'IXZ': (11.64, 92.729), 'JAF': (9.792, 80.07), 'JAI': (26.824, 75.812),
    'JAX': (30.492, -81.688), 'JCL': (48.948, 14.428), 'JED': (21.68, 39.157),
    'JFK': (40.639, -73.779), 'JGN': (39.859, 98.339), 'JHB': (1.641, 103.67),
    'JHG': (21.975, 100.762), 'JIB': (11.547, 43.16), 'JIJ': (9.332, 42.912),
    'JJN': (24.796, 118.589), 'JNB': (-26.14, 28.247), 'JPA': (-7.149, -34.951),
    'JRO': (-3.427, 37.074), 'JTR': (36.4, 25.479), 'JUB': (4.872, 31.601),
    'JUJ': (-24.393, -65.098), 'JUL': (-15.468, -70.157), 'KAD': (10.696, 7.32),
    'KAN': (12.046, 8.524), 'KBL': (34.566, 69.212), 'KBV': (8.096, 98.989),
    'KCH': (1.487, 110.353), 'KCZ': (33.545, 133.67), 'KDH': (31.506, 65.848),
    'KDU': (35.339, 75.539), 'KEF': (63.985, -22.606), 'KEJ': (55.27, 86.107),
    'KER': (30.271, 56.95), 'KGD': (54.892, 20.599), 'KGF': (49.671, 73.334),
    'KGL': (-1.969, 30.14), 'KGS': (36.795, 27.091), 'KHG': (39.542, 76.02),
    'KHH': (22.577, 120.35), 'KHI': (24.907, 67.161), 'KHN': (28.865, 115.903),
    'KIH': (26.525, 53.98), 'KIJ': (37.954, 139.112), 'KIK': (35.47, 44.349),
    'KIM': (-28.805, 24.765), 'KIN': (17.936, -76.787), 'KIS': (-0.086, 34.729),
    'KIX': (34.427, 135.244), 'KJA': (56.176, 92.486), 'KKJ': (33.846, 131.035),
    'KLO': (11.679, 122.376), 'KLU': (46.643, 14.338), 'KLV': (50.203, 12.915),
    'KMG': (25.11, 102.937), 'KMI': (31.877, 131.449), 'KMJ': (32.837, 130.855),
    'KMQ': (36.393, 136.407), 'KMS': (6.715, -1.591), 'KNO': (3.638, 98.871),
    'KOA': (19.739, -156.046), 'KOJ': (31.803, 130.719), 'KOS': (10.571, 103.632),
    'KOV': (53.329, 69.595), 'KQT': (37.866, 68.864), 'KRK': (50.078, 19.785),
    'KRN': (67.822, 20.337), 'KRR': (45.034, 39.174), 'KRS': (58.204, 8.085),
    'KRT': (15.589, 32.553), 'KSA': (5.357, 162.958), 'KSF': (51.418, 9.392),
    'KSN': (53.207, 63.55), 'KTI': (11.36, 104.921), 'KTM': (27.697, 85.359),
    'KTT': (67.701, 24.847), 'KTW': (50.476, 19.081), 'KUF': (53.505, 50.164),
    'KUL': (2.746, 101.71), 'KUN': (54.964, 24.086), 'KUO': (63.007, 27.798),
    'KUT': (42.177, 42.485), 'KVA': (40.913, 24.619), 'KWE': (26.542, 106.804),
    'KWI': (29.224, 47.97), 'KWL': (25.22, 110.04), 'KYA': (37.979, 32.562),
    'KZN': (55.606, 49.279), 'KZO': (44.707, 65.592), 'LAD': (-8.858, 13.231),
    'LAE': (-6.568, 146.727), 'LAO': (18.175, 120.531), 'LAQ': (32.789, 21.955),
    'LAS': (36.083, -115.152), 'LAX': (33.943, -118.408), 'LBA': (53.866, -1.661),
    'LBD': (40.215, 69.695), 'LBG': (48.962, 2.437), 'LBV': (0.459, 9.412),
    'LCA': (34.875, 33.625), 'LCJ': (51.722, 19.398), 'LED': (59.8, 30.263),
    'LEJ': (51.421, 12.233), 'LFW': (6.166, 1.255), 'LGA': (40.777, -73.873),
    'LGB': (33.817, -118.15), 'LGK': (6.33, 99.729), 'LGW': (51.149, -0.186),
    'LHE': (31.522, 74.404), 'LHR': (51.471, -0.46), 'LHW': (36.515, 103.62),
    'LIH': (21.974, -159.337), 'LIL': (50.567, 3.102), 'LIM': (-12.022, -77.114),
    'LIN': (45.445, 9.277), 'LIR': (10.593, -85.544), 'LIS': (38.781, -9.136),
    'LJG': (26.677, 100.245), 'LJU': (46.224, 14.458), 'LKO': (26.761, 80.889),
    'LLA': (65.544, 22.122), 'LLW': (-13.789, 33.781), 'LNZ': (48.235, 14.188),
    'LOP': (-8.76, 116.278), 'LOS': (6.577, 3.321), 'LPA': (27.932, -15.387),
    'LPB': (-16.51, -68.189), 'LPI': (58.405, 15.684), 'LPL': (53.335, -2.85),
    'LPP': (61.045, 28.145), 'LPQ': (19.904, 102.167), 'LPX': (56.518, 21.097),
    'LRL': (9.767, 1.091), 'LRM': (18.452, -68.911), 'LTH': (10.773, 107.041),
    'LTN': (51.875, -0.368), 'LTO': (25.99, -111.348), 'LUN': (-15.331, 28.453),
    'LUX': (49.627, 6.212), 'LUZ': (51.24, 22.713), 'LVI': (-17.822, 25.82),
    'LWN': (40.75, 43.859), 'LWO': (49.812, 23.956), 'LXA': (29.298, 90.912),
    'LXR': (25.671, 32.706), 'LYA': (34.741, 112.388), 'LYG': (34.414, 119.179),
    'LYP': (31.365, 72.995), 'LYS': (45.726, 5.09), 'MAA': (12.99, 80.169),
    'MAD': (40.493, -3.572), 'MAH': (39.863, 4.219), 'MAJ': (7.065, 171.272),
    'MAN': (53.349, -2.28), 'MAO': (-3.039, -60.05), 'MAR': (10.558, -71.729),
    'MBA': (-4.035, 39.594), 'MBJ': (18.503, -77.913), 'MCI': (39.302, -94.714),
    'MCO': (28.429, -81.309), 'MCT': (23.6, 58.285), 'MCX': (42.817, 47.652),
    'MCY': (-26.593, 153.083), 'MCZ': (-9.513, -35.792), 'MDC': (1.549, 124.926),
    'MDE': (6.165, -75.423), 'MDL': (21.702, 95.978), 'MDW': (41.786, -87.752),
    'MDZ': (-32.832, -68.793), 'MED': (24.553, 39.705), 'MEL': (-37.671, 144.838),
    'MEM': (35.044, -89.976), 'MEX': (19.436, -99.07), 'MFM': (22.15, 113.592),
    'MFU': (-13.259, 31.937), 'MGA': (12.142, -86.168), 'MGQ': (2.014, 45.305),
    'MHD': (36.235, 59.643), 'MIA': (25.796, -80.29), 'MID': (20.93, -89.645),
    'MIU': (11.854, 13.081), 'MJI': (32.892, 13.288), 'MJN': (-15.667, 46.351),
    'MKE': (42.947, -87.897), 'MLA': (35.846, 14.492), 'MLE': (4.192, 73.529),
    'MLM': (19.85, -101.025), 'MMK': (68.782, 32.751), 'MMX': (55.536, 13.376),
    'MNI': (16.792, -62.193), 'MNL': (14.509, 121.02), 'MPL': (43.576, 3.963),
    'MPM': (-25.921, 32.573), 'MPN': (-51.823, -58.446), 'MQF': (53.392, 58.755),
    'MQP': (-25.383, 31.105), 'MRS': (43.438, 5.213), 'MRU': (-20.43, 57.684),
    'MRV': (44.225, 43.082), 'MSP': (44.88, -93.222), 'MSQ': (53.888, 28.04),
    'MST': (50.911, 5.769), 'MSU': (-29.456, 27.554), 'MSY': (29.993, -90.265),
    'MTY': (25.779, -100.107), 'MUB': (-19.97, 23.431), 'MUC': (48.354, 11.786),
    'MUH': (31.324, 27.222), 'MUX': (30.203, 71.419), 'MVD': (-34.836, -56.026),
    'MWX': (34.991, 126.383), 'MWZ': (-2.447, 32.936), 'MXP': (45.631, 8.728),
    'MYJ': (33.827, 132.7), 'MYR': (33.68, -78.928), 'MZG': (23.569, 119.628),
    'MZR': (36.704, 67.21), 'MZT': (23.163, -106.265), 'NAG': (21.092, 79.047),
    'NAJ': (39.189, 45.458), 'NAN': (-17.762, 177.438), 'NAP': (40.886, 14.291),
    'NAS': (25.039, -77.466), 'NAT': (-5.77, -35.367), 'NAV': (38.772, 34.535),
    'NBJ': (-9.051, 13.499), 'NBO': (-1.319, 36.928), 'NCE': (43.658, 7.216),
    'NCL': (55.038, -1.69), 'NCU': (42.488, 59.623), 'NDB': (20.932, -17.03),
    'NDG': (47.23, 123.914), 'NDJ': (12.134, 15.034), 'NDR': (34.989, -3.028),
    'NGB': (29.827, 121.462), 'NGO': (34.858, 136.805), 'NGS': (32.917, 129.914),
    'NIM': (13.482, 2.184), 'NJC': (60.949, 76.484), 'NJF': (31.991, 44.405),
    'NKC': (18.31, -15.97), 'NKG': (31.735, 118.866), 'NLA': (-12.965, 28.516),
    'NLU': (19.744, -99.015), 'NMA': (40.985, 71.558), 'NMI': (18.985, 73.065),
    'NNG': (22.598, 108.182), 'NOC': (53.91, -8.817), 'NOS': (-13.312, 48.315),
    'NOU': (-22.015, 166.213), 'NQN': (-38.949, -68.156), 'NQZ': (51.027, 71.467),
    'NRN': (51.601, 6.141), 'NRT': (35.769, 140.389), 'NSI': (3.723, 11.553),
    'NSK': (69.308, 87.326), 'NTE': (47.153, -1.611), 'NTL': (-32.796, 151.835),
    'NUE': (49.499, 11.078), 'NUM': (27.924, 35.294), 'NVT': (-26.879, -48.651),
    'NYO': (58.79, 16.911), 'NYT': (19.624, 96.201), 'OAK': (37.72, -122.221),
    'OAX': (16.999, -96.726), 'OCS': (0.911, 9.33), 'ODE': (55.475, 10.327),
    'ODS': (46.427, 30.673), 'OEC': (-9.198, 124.338), 'OGG': (20.896, -156.432),
    'OHD': (41.18, 20.742), 'OHS': (24.386, 56.625), 'OKA': (26.192, 127.64),
    'OKC': (35.393, -97.598), 'OKJ': (34.757, 133.855), 'OLB': (40.899, 9.518),
    'OMA': (41.303, -95.894), 'OMO': (43.282, 17.846), 'OMR': (47.025, 21.902),
    'OMS': (54.963, 73.312), 'ONT': (34.056, -117.601), 'OOL': (-28.166, 153.507),
    'OPO': (41.248, -8.681), 'ORD': (41.979, -87.905), 'ORF': (36.895, -76.201),
    'ORK': (51.841, -8.491), 'ORN': (35.621, -0.622), 'ORU': (-17.956, -67.076),
    'ORY': (48.729, 2.359), 'OSL': (60.194, 11.1), 'OSM': (36.306, 43.147),
    'OSR': (49.696, 18.111), 'OSS': (40.609, 72.793), 'OST': (51.2, 2.875),
    'OTP': (44.572, 26.103), 'OUA': (12.353, -1.512), 'OUD': (34.79, -1.926),
    'OUL': (64.93, 25.355), 'OVB': (55.02, 82.619), 'OVD': (43.564, -6.035),
    'OXB': (11.894, -15.654), 'OZG': (30.266, -5.861), 'OZH': (47.867, 35.315),
    'OZZ': (30.939, -6.909), 'PAD': (51.613, 8.617), 'PAP': (18.58, -72.293),
    'PBC': (19.158, -98.372), 'PBH': (27.403, 89.425), 'PBI': (26.683, -80.096),
    'PBM': (5.453, -55.188), 'PCL': (-8.378, -74.574), 'PDG': (-0.786, 100.28),
    'PDL': (37.741, -25.698), 'PDV': (42.068, 24.851), 'PDX': (45.589, -122.598),
    'PED': (50.015, 15.74), 'PEE': (57.915, 56.021), 'PEG': (43.096, 12.513),
    'PEK': (40.077, 116.597), 'PEN': (5.296, 100.276), 'PER': (-31.94, 115.967),
    'PEV': (45.989, 18.242), 'PEW': (33.994, 71.515), 'PFO': (34.718, 32.486),
    'PHC': (5.015, 6.95), 'PHE': (-20.383, 118.63), 'PHH': (28.184, 84.015),
    'PHL': (39.872, -75.241), 'PHX': (33.435, -112.006), 'PIE': (27.91, -82.687),
    'PIK': (55.501, -4.577), 'PIO': (-13.745, -76.22), 'PIT': (40.492, -80.233),
    'PKC': (53.169, 158.451), 'PKX': (39.501, 116.414), 'PKZ': (15.134, 105.78),
    'PLQ': (55.973, 21.094), 'PLS': (21.774, -72.268), 'PLX': (50.351, 80.234),
    'PLZ': (-33.99, 25.617), 'PMC': (-41.443, -73.094), 'PMI': (39.552, 2.739),
    'PMO': (38.176, 13.091), 'PMV': (10.913, -63.967), 'PNH': (11.547, 104.845),
    'PNK': (-0.152, 109.404), 'PNQ': (18.582, 73.92), 'PNR': (-4.816, 11.887),
    'PNS': (30.473, -87.187), 'POA': (-29.994, -51.167), 'POG': (-0.712, 8.754),
    'POM': (-9.443, 147.22), 'POS': (10.595, -61.338), 'POZ': (52.422, 16.823),
    'PPG': (-14.331, -170.71), 'PPK': (54.776, 69.187), 'PPS': (9.742, 118.759),
    'PPT': (-17.553, -149.607), 'PQC': (10.17, 103.994), 'PRG': (50.101, 14.26),
    'PRN': (42.573, 21.036), 'PSA': (43.684, 10.393), 'PSD': (31.279, 32.241),
    'PSP': (33.83, -116.507), 'PSR': (42.431, 14.183), 'PTG': (-23.845, 29.459),
    'PTP': (16.265, -61.533), 'PTY': (9.071, -79.383), 'PUJ': (18.567, -68.365),
    'PUQ': (-53.003, -70.855), 'PUS': (35.18, 128.938), 'PUY': (44.894, 13.922),
    'PVD': (41.725, -71.426), 'PVG': (31.143, 121.805), 'PVH': (-8.708, -63.902),
    'PVR': (20.68, -105.254), 'PWM': (43.646, -70.309), 'PWQ': (52.195, 77.073),
    'PYK': (35.776, 50.827), 'PZO': (8.289, -62.76), 'PZU': (19.435, 37.234),
    'QRO': (20.619, -100.186), 'RAI': (14.941, -23.485), 'RAK': (31.605, -8.036),
    'RAR': (-21.203, -159.806), 'RBA': (34.051, -6.752), 'RBR': (-9.869, -67.894),
    'RDU': (35.879, -78.787), 'REC': (-8.127, -34.923), 'RES': (-27.45, -59.056),
    'REU': (41.148, 1.168), 'RGL': (-51.609, -69.309), 'RGN': (16.907, 96.133),
    'RHO': (36.405, 28.086), 'RIC': (37.505, -77.32), 'RIX': (56.921, 23.971),
    'RIY': (14.662, 49.375), 'RJK': (45.216, 14.571), 'RKT': (25.614, 55.939),
    'RKZ': (29.351, 89.299), 'RMF': (25.556, 34.592), 'RMI': (44.02, 12.612),
    'RML': (6.822, 79.886), 'RMO': (46.928, 28.932), 'RMQ': (24.265, 120.621),
    'RMU': (37.803, -1.125), 'RNO': (39.499, -119.768), 'ROB': (6.234, -10.362),
    'ROC': (43.119, -77.672), 'ROP': (14.173, 145.241), 'ROR': (7.367, 134.544),
    'ROS': (-32.904, -60.785), 'ROV': (47.494, 39.925), 'RSI': (25.628, 37.089),
    'RSW': (26.535, -81.753), 'RTB': (16.317, -86.523), 'RTM': (51.957, 4.437),
    'RUH': (24.958, 46.699), 'RUN': (-20.89, 55.519), 'RVN': (66.563, 25.83),
    'RZE': (50.11, 22.024), 'RZV': (41.18, 40.849), 'SAG': (19.689, 74.374),
    'SAH': (15.476, 44.22), 'SAI': (13.37, 104.224), 'SAL': (13.444, -89.056),
    'SAN': (32.734, -117.19), 'SAP': (15.453, -87.924), 'SAT': (29.534, -98.47),
    'SAV': (32.127, -81.2), 'SAW': (40.899, 29.309), 'SBD': (34.097, -117.237),
    'SBZ': (45.786, 24.087), 'SCL': (-33.393, -70.786), 'SCO': (43.86, 51.091),
    'SCQ': (42.896, -8.415), 'SCR': (61.165, 12.834), 'SCU': (19.975, -75.836),
    'SCV': (47.688, 26.354), 'SDF': (38.171, -85.735), 'SDJ': (38.14, 140.917),
    'SDQ': (18.43, -69.669), 'SDU': (-22.91, -43.163), 'SEA': (47.448, -122.31),
    'SEZ': (-4.674, 55.522), 'SFB': (28.774, -81.235), 'SFO': (37.62, -122.375),
    'SFS': (14.795, 120.272), 'SGC': (61.341, 73.406), 'SGN': (10.819, 106.652),
    'SHA': (31.198, 121.334), 'SHE': (41.64, 123.484), 'SHJ': (25.329, 55.517),
    'SHO': (-26.359, 31.717), 'SID': (16.741, -22.949), 'SIN': (1.35, 103.994),
    'SIP': (45.052, 33.975), 'SJC': (37.362, -121.929), 'SJD': (23.152, -109.721),
    'SJJ': (43.825, 18.331), 'SJO': (9.994, -84.209), 'SJU': (18.439, -66.002),
    'SJW': (38.281, 114.697), 'SKB': (17.311, -62.719), 'SKD': (39.702, 66.981),
    'SKG': (40.519, 22.97), 'SKO': (12.916, 5.208), 'SKP': (41.958, 21.623),
    'SKT': (32.536, 74.365), 'SKX': (54.125, 45.212), 'SLA': (-24.856, -65.486),
    'SLC': (40.789, -111.98), 'SLL': (17.039, 54.091), 'SLZ': (-2.586, -44.235),
    'SMF': (38.695, -121.591), 'SNA': (33.675, -117.869), 'SNC': (-2.21, -80.985),
    'SNN': (52.702, -8.925), 'SNU': (22.492, -79.943), 'SOC': (-7.516, 110.757),
    'SOF': (42.696, 23.418), 'SPU': (43.539, 16.298), 'SPX': (30.108, 30.896),
    'SRE': (-19.247, -65.15), 'SRG': (-6.971, 110.373), 'SRQ': (27.395, -82.554),
    'SRX': (31.059, 16.597), 'SSA': (-12.909, -38.322), 'SSG': (3.755, 8.709),
    'SSH': (27.977, 34.395), 'STI': (19.404, -70.604), 'STL': (38.749, -90.37),
    'STN': (51.885, 0.235), 'STR': (48.69, 9.222), 'STT': (18.337, -64.977),
    'STV': (21.116, 72.743), 'SUB': (-7.38, 112.787), 'SUF': (38.906, 16.246),
    'SUV': (-18.044, 178.561), 'SVD': (13.16, -61.149), 'SVG': (58.877, 5.638),
    'SVO': (55.977, 37.411), 'SVQ': (37.418, -5.893), 'SVX': (56.743, 60.803),
    'SWA': (23.552, 116.503), 'SXB': (48.538, 7.628), 'SXM': (18.041, -63.109),
    'SXR': (33.987, 74.774), 'SYD': (-33.946, 151.177), 'SYR': (43.111, -76.106),
    'SYX': (18.303, 109.412), 'SYZ': (29.539, 52.59), 'SZB': (3.131, 101.549),
    'SZG': (47.793, 13.004), 'SZX': (22.639, 113.803), 'SZZ': (53.585, 14.902),
    'TAB': (11.15, -60.831), 'TAE': (35.894, 128.657), 'TAG': (9.573, 123.77),
    'TAK': (34.215, 134.015), 'TAO': (36.362, 120.088), 'TAS': (41.258, 69.281),
    'TAZ': (41.76, 59.836), 'TBS': (41.669, 44.955), 'TBU': (-21.241, -175.149),
    'TBZ': (38.134, 46.235), 'TET': (-16.105, 33.64), 'TFN': (28.483, -16.342),
    'TFS': (28.044, -16.573), 'TFU': (30.313, 104.441), 'TGD': (42.359, 19.252),
    'THR': (35.689, 51.314), 'TIA': (41.415, 19.721), 'TIF': (21.485, 40.544),
    'TIJ': (32.541, -116.97), 'TIR': (13.632, 79.54), 'TJM': (57.179, 65.328),
    'TJU': (37.988, 69.805), 'TKK': (7.462, 151.843), 'TKS': (34.133, 134.608),
    'TKU': (60.514, 22.263), 'TLC': (19.337, -99.566), 'TLL': (59.413, 24.833),
    'TLM': (35.013, -1.457), 'TLS': (43.629, 1.364), 'TLV': (32.011, 34.887),
    'TML': (9.554, -0.866), 'TMM': (-18.114, 49.392), 'TMP': (61.414, 23.604),
    'TMR': (22.811, 5.451), 'TMS': (0.378, 6.712), 'TNA': (36.857, 117.216),
    'TNG': (35.732, -5.921), 'TNN': (22.95, 120.206), 'TNR': (-18.797, 47.479),
    'TOF': (56.38, 85.208), 'TOM': (16.73, -3.008), 'TOS': (69.683, 18.919),
    'TPA': (27.976, -82.533), 'TPE': (25.078, 121.233), 'TQO': (20.172, -87.66),
    'TRD': (63.458, 10.924), 'TRF': (59.187, 10.259), 'TRN': (45.201, 7.65),
    'TRS': (45.828, 13.467), 'TRU': (-8.082, -79.109), 'TRV': (8.482, 76.92),
    'TRW': (1.382, 173.147), 'TRZ': (10.763, 78.718), 'TSA': (25.067, 121.553),
    'TSF': (45.648, 12.194), 'TSN': (39.124, 117.346), 'TSR': (45.81, 21.338),
    'TTU': (35.594, -5.32), 'TUC': (-26.837, -65.104), 'TUK': (25.985, 63.029),
    'TUL': (36.197, -95.886), 'TUN': (36.851, 10.227), 'TUS': (32.115, -110.938),
    'TUU': (28.371, 36.625), 'TXN': (29.733, 118.256), 'TYN': (37.747, 112.628),
    'TYS': (35.811, -83.994), 'TZL': (44.46, 18.724), 'UBN': (47.647, 106.82),
    'UDJ': (48.634, 22.263), 'UET': (30.251, 66.938), 'UFA': (54.557, 55.874),
    'UGC': (41.583, 60.643), 'UIO': (-0.125, -78.354), 'UKB': (34.633, 135.224),
    'UKK': (50.035, 82.496), 'ULH': (26.484, 38.117), 'ULN': (47.843, 106.767),
    'UME': (63.792, 20.283), 'UPG': (-5.076, 119.554), 'URA': (51.152, 51.544),
    'URC': (43.914, 87.479), 'USM': (9.548, 100.062), 'UTH': (17.386, 102.789),
    'UTP': (12.68, 101.005), 'UUD': (51.809, 107.44), 'UUS': (46.885, 142.717),
    'UVF': (13.733, -60.953), 'UYU': (-20.441, -66.858), 'VAA': (63.05, 21.763),
    'VAR': (43.232, 27.825), 'VAV': (-18.585, -173.962), 'VBY': (57.663, 18.346),
    'VCA': (10.083, 105.709), 'VCE': (45.505, 12.352), 'VCP': (-23.007, -47.135),
    'VER': (19.14, -96.189), 'VFA': (-18.097, 25.837), 'VGA': (16.53, 80.805),
    'VIE': (48.11, 16.57), 'VIL': (23.718, -15.932), 'VIX': (-20.258, -40.285),
    'VKO': (55.591, 37.262), 'VLC': (39.489, -0.481), 'VLI': (-17.699, 168.32),
    'VLN': (10.15, -67.928), 'VNO': (54.634, 25.286), 'VNS': (25.452, 82.863),
    'VOG': (48.781, 44.339), 'VRA': (23.034, -81.435), 'VRN': (45.395, 10.887),
    'VSA': (17.994, -92.818), 'VST': (59.589, 16.634), 'VTE': (17.985, 102.567),
    'VTZ': (17.724, 83.228), 'VVI': (-17.645, -63.135), 'VVO': (43.396, 132.148),
    'VXE': (16.833, -25.055), 'WAW': (52.166, 20.967), 'WDH': (-22.48, 17.471),
    'WLG': (-41.327, 174.807), 'WLS': (-13.239, -176.199), 'WMI': (52.451, 20.652),
    'WNZ': (27.911, 120.853), 'WRO': (51.104, 16.882), 'WSI': (-33.883, 150.713),
    'WTB': (-27.558, 151.793), 'WUH': (30.775, 114.214), 'WUX': (31.497, 120.43),
    'WVB': (-22.979, 14.647), 'XBJ': (32.897, 59.281), 'XIY': (34.442, 108.762),
    'XMN': (24.544, 118.127), 'XNN': (36.528, 102.04), 'XPL': (14.382, -87.621),
    'YAP': (9.499, 138.083), 'YCU': (35.118, 111.034), 'YEG': (53.31, -113.58),
    'YHZ': (44.881, -63.509), 'YIA': (-7.905, 110.057), 'YIW': (29.342, 120.031),
    'YKS': (62.093, 129.771), 'YLW': (49.956, -119.378), 'YNB': (24.144, 38.063),
    'YNT': (37.66, 120.978), 'YNY': (38.06, 128.67), 'YNZ': (33.428, 120.205),
    'YOW': (45.322, -75.669), 'YQB': (46.791, -71.393), 'YUL': (45.468, -73.742),
    'YVR': (49.194, -123.184), 'YWG': (49.91, -97.24), 'YXE': (52.171, -106.701),
    'YYC': (51.119, -114.01), 'YYJ': (48.647, -123.428), 'YYT': (47.619, -52.752),
    'YYZ': (43.676, -79.629), 'ZAD': (44.097, 15.354), 'ZAG': (45.743, 16.069),
    'ZAH': (29.476, 60.906), 'ZAM': (6.922, 122.06), 'ZAZ': (41.666, -1.042),
    'ZCO': (-38.926, -72.651), 'ZHA': (21.482, 110.59), 'ZIA': (55.553, 38.15),
    'ZIH': (17.602, -101.461), 'ZNZ': (-6.222, 39.225), 'ZQN': (-45.019, 168.746),
    'ZRH': (47.458, 8.548), 'ZSA': (24.063, -74.523), 'ZSE': (-21.319, 55.423),
    'ZUH': (22.006, 113.376), 'ZYL': (24.964, 91.865),
}

@app.route("/book/world.json")
def book_world():
    """Basemap for /book map mode. Static, so it caches for a day — the page
    fetches it once and never talks to a tile server (no third party sees a
    visitor, which is what the Datenschutz page promises)."""
    resp = jsonify({"path": _WORLD_OUTLINE, "proj": "equirectangular"})
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


@app.route("/api/book/map")
def api_book_map():
    """Departure airports for /book map mode — every airport with an upcoming
    published departure, with its position and what leaves from it.

    Reads the same snapshot as the card list (_latest_assignments, unfiltered),
    so a marker and the flights you get when you click it can never disagree.
    Airports missing from _AIRPORT_LL come back under `unplaced` instead of
    quietly vanishing, so a new destination is visible even before it is
    mappable."""
    try:
        conn = _db()
    except Exception as e:
        return jsonify({"error": str(e)}), 503
    try:
        rows = _latest_assignments(conn)
        names = {a["code"]: a["name"] for a in _book_airports(conn)}
    finally:
        conn.close()

    agg = {}
    for (fdate, _airline, _fnum, _reg, atype, d, a, _dep_t, _arr_t, _st) in rows:
        if not d:
            continue
        e = agg.setdefault(d, {"flights": 0, "dests": set(),
                               "types": set(), "next": None})
        e["flights"] += 1
        if a:
            e["dests"].add(a)
        short = _CANON_SHORT.get(atype, atype)
        if short:
            e["types"].add(short)
        if e["next"] is None or fdate < e["next"]:
            e["next"] = fdate

    airports, unplaced = [], []
    for code, e in sorted(agg.items()):
        rec = {"code": code, "name": names.get(code) or code,
               "flights": e["flights"], "dests": sorted(e["dests"]),
               "types": sorted(e["types"]),
               "next": e["next"].isoformat() if e["next"] else None}
        ll = _AIRPORT_LL.get(code)
        if ll:
            rec["lat"], rec["lon"] = ll
            airports.append(rec)
        else:
            unplaced.append(rec)
    return jsonify({"airports": airports, "unplaced": unplaced,
                    "generated": datetime.now(timezone.utc).isoformat()})


# Insights tab → member aircraft types. Family tabs aggregate every variant,
# incl. the not-yet-delivered ones (same rationale as _SCHEDULE_TYPES): they
# show up the day the collector first sees one. Single ICAO codes still work.
_INSIGHT_FAMILIES = {
    "B787": ("B788", "B789", "B78X"),
    "A350": ("A359", "A35K"),
}


@app.route("/api/insights")
def api_insights():
    """Descriptive fleet analytics for one aircraft type or family (optionally
    one tail): route frequency, rotation transitions, per-airframe profiles,
    and — for types we collect schedule data on — reliability. Purely
    backward-looking; no prediction. Drives the parameterised /insights page."""
    atype = (request.args.get("type") or "B748").strip().upper()
    reg = (request.args.get("reg") or "").strip().upper() or None
    members = list(_INSIGHT_FAMILIES.get(atype, (atype,)))
    short = _CANON_SHORT.get(atype, atype)

    scope = "a.aircraft_type = ANY(%s) AND NOT f.needs_review"
    sp = [members]
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
                   MIN(f.flight_date), MAX(f.flight_date), MIN(a.aircraft_type)
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
                WHERE o.found AND a.aircraft_type = ANY(%s)
                ORDER BY o.flight_date, o.flight_number, o.observed_at DESC
            )
            SELECT COALESCE(overall_status, 'UNKNOWN'), COUNT(*)
            FROM latest GROUP BY 1 ORDER BY 2 DESC
        """, [members])
        stab = _reassignment_stability(conn)

        # Reschedulings over time: per observed_date, how many flights had their
        # tail change vs the previous nightly snapshot (the "re-planned today" axis).
        reschedulings = _q(conn, """
            WITH snaps AS (
                SELECT o.observed_date, btrim(o.registration) AS reg,
                       LAG(btrim(o.registration)) OVER (
                           PARTITION BY o.flight_date, o.airline, o.flight_number
                           ORDER BY o.observed_at) AS prev_reg
                FROM flight_status_observations o
                JOIN aircraft a ON a.registration = o.registration
                WHERE o.found AND o.registration IS NOT NULL AND a.aircraft_type = ANY(%s)
            )
            SELECT observed_date,
                   COUNT(*) FILTER (WHERE prev_reg IS NOT NULL AND reg <> prev_reg) AS changes
            FROM snaps GROUP BY observed_date ORDER BY observed_date
        """, [members])
    finally:
        conn.close()

    ground = {r[0]: r[1] for r in grounding}
    resched = [{"date": r[0].isoformat(), "n": r[1]} for r in reschedulings]
    hold = _merge_hold(stab["type"].get(_CANON_SHORT.get(t, t)) for t in members)
    reliability = None
    if ontime or hold or resched:
        reliability = {
            "ontime": [{"status": s, "n": n} for s, n in ontime],
            "hold_by_lead": hold or stab["overall"],
            "churn_by_route": stab["route"],
            "reschedulings": resched,
        }

    return jsonify({
        "type": atype, "short": short, "reg": reg, "members": members,
        "meta": {"flights": meta[0], "tails": meta[1],
                 "first": meta[2].isoformat() if meta[2] else None,
                 "last": meta[3].isoformat() if meta[3] else None},
        "routes": [{"route": r[0], "n": r[1], "median_min": int(r[2]) if r[2] is not None else None}
                   for r in routes],
        "airframes": [{"reg": r[0], "legs": r[1], "hours": float(r[2]) if r[2] is not None else 0.0,
                       "first": r[3].isoformat() if r[3] else None,
                       "last": r[4].isoformat() if r[4] else None,
                       "type": _CANON_SHORT.get(r[5], r[5]),
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
.gantt-row { display:flex; align-items:center; margin-bottom:6px; height:24px; }
.gantt-row.dim { opacity:.22; }
.gantt-row.typehide { display:none; }   /* type unchecked (watched rows are exempt) */
.gantt-row.watch { outline:1.5px dashed var(--fp-ink); outline-offset:-1px; }
.gantt-row.watch .gantt-label { color:var(--text-bright); }
.gantt-label .star { color:var(--fp-ink); font-size:10px; margin-right:1px; }
.gantt-label { width:122px; flex-shrink:0; font-family:var(--mono); font-size:11px; font-weight:700;
  color:var(--text-bright); padding-right:8px; display:flex; align-items:center; gap:5px; text-decoration:none; }
a.gantt-label:hover { text-decoration:underline; text-decoration-color:var(--accent); text-underline-offset:2px; }
.tbadge { font-family:var(--mono); font-size:9px; font-weight:700; padding:1px 5px; border-radius:0; color:#fff; }
.abadge { font-family:var(--mono); font-size:9px; font-weight:700; padding:1px 4px; border-radius:0;
  background:var(--fp-ink); color:#fff; letter-spacing:.4px; }   /* Allegris cabin */
.t748 .tbadge, .tbadge.t748 { background:var(--accent); }
.t388 .tbadge, .tbadge.t388 { background:var(--fp-dv-2); }
.t789 .tbadge, .tbadge.t789 { background:var(--fp-dv-4); }
.t359 .tbadge, .tbadge.t359 { background:var(--fp-dv-3); color:var(--fp-ink); }
.tbadge.tother { background:var(--surface2); color:var(--muted); }
/* type checkboxes — double as the type legend; watched tails ignore hiding */
.tchk { display:inline-flex; align-items:center; gap:5px; cursor:pointer; font-family:var(--mono);
  font-size:10px; font-weight:700; color:var(--text); text-transform:uppercase; user-select:none; }
.tchk input { accent-color:var(--fp-ink); width:13px; height:13px; margin:0; cursor:pointer; }
.tchk .sw { width:12px; height:12px; }
.tchk .sw.t748 { background:var(--accent); } .tchk .sw.t388 { background:var(--fp-dv-2); }
.tchk .sw.t789 { background:var(--fp-dv-4); } .tchk .sw.t359 { background:var(--fp-dv-3); }
.tchk .sw.tother { background:var(--fp-gray); }
.tchk.off { color:var(--muted); }
.tchk.off .sw { opacity:.3; }
.gantt-track { flex:1; position:relative; height:22px; background:color-mix(in srgb,var(--fp-gray) 26%,#fff); border-radius:0; overflow:visible; }
/* per-type colour tokens — saturated hue / light tint / deep text (white text on hue, deep on tint).
   One hue per family: 787 variants share t789, A350 variants share t359; the badge names the variant. */
.gantt-flight.t748, .tie.t748 { --_s:var(--fp-sage); --_t:var(--fp-sage-tint); --_d:color-mix(in srgb,var(--fp-sage) 72%,#000); }
.gantt-flight.t388, .tie.t388 { --_s:var(--fp-dv-2);  --_t:color-mix(in srgb,var(--fp-dv-2) 20%,#fff); --_d:color-mix(in srgb,var(--fp-dv-2) 74%,#000); }
.gantt-flight.t789, .tie.t789 { --_s:var(--fp-dv-4);  --_t:color-mix(in srgb,var(--fp-dv-4) 20%,#fff); --_d:color-mix(in srgb,var(--fp-dv-4) 74%,#000); }
.gantt-flight.t359, .tie.t359 { --_s:var(--fp-dv-3);  --_t:color-mix(in srgb,var(--fp-dv-3) 20%,#fff); --_d:color-mix(in srgb,var(--fp-dv-3) 74%,#000); }
.gantt-flight.tother, .tie.tother { --_s:var(--fp-gray); --_t:color-mix(in srgb,var(--fp-gray) 34%,#fff); --_d:var(--fp-body); }

.gantt-flight { position:absolute; top:1px; height:20px; border-radius:0; overflow:hidden; display:flex;
  align-items:center; opacity:1; transition:opacity .15s, box-shadow .15s; }
.gantt-flight.dim { opacity:.12; }
.gantt-flight .lbl { font-family:var(--mono); font-size:10.5px; font-weight:700; white-space:nowrap; padding:0 7px;
  letter-spacing:.2px; display:flex; align-items:baseline; gap:5px; }
.gantt-flight .lbl .fl { font-weight:500; font-size:9.5px; opacity:.85; }
.clk { cursor:pointer; }
.clk:hover { z-index:3; outline:2px solid var(--fp-ink); outline-offset:-2px; }   /* outline composes with status box-shadows */

/* PAST — saturated fill + bold white label (incl. flight #) */
.gantt-flight.st-tracked, .gantt-flight.st-actual, .gantt-flight.st-extra { background:var(--_s); }
.gantt-flight.st-tracked .lbl, .gantt-flight.st-actual .lbl, .gantt-flight.st-extra .lbl {
  color:#fff; text-shadow:0 1px 1.5px rgba(0,0,0,.45); }
/* ochre (A350/t359) is light — its past-bar labels go ink for contrast */
.gantt-flight.t359.st-tracked .lbl, .gantt-flight.t359.st-actual .lbl, .gantt-flight.t359.st-extra .lbl {
  color:var(--fp-ink); text-shadow:none; }
/* a deviation (what actually flew) now renders as a normal solid bar — no colour cap */
.gantt-flight.st-extra  { background:repeating-linear-gradient(45deg,var(--_s) 0 6px,color-mix(in srgb,var(--_s) 60%,#fff) 6px 11px); }

/* FUTURE — light tint + bold deep label */
.gantt-flight.st-planned { background:var(--_t); }
.gantt-flight.st-planned .lbl { color:var(--_d); }

/* PLANNED SLOT — a scheduled slot not yet tracked, or one vacated by a deviation. Light grey dashed */
.gantt-flight.st-missing, .gantt-flight.st-ghost { background:color-mix(in srgb,var(--fp-gray) 20%,#fff); border:1.5px dashed var(--fp-gray); }
.gantt-flight.st-missing .lbl, .gantt-flight.st-ghost .lbl { color:var(--fp-muted); }

/* SWAP / reassignment */
.gantt-flight.is-swap { box-shadow:inset 0 0 0 2px var(--fp-dv-6); }
.gantt-flight .swapchip { position:absolute; right:0; top:0; height:100%; text-align:center; line-height:1;
  writing-mode:vertical-rl; transform:rotate(180deg);   /* "SWAP" reads bottom-to-top: a thin vertical strip */
  font-family:var(--sans); font-size:6px; font-weight:700; letter-spacing:-.02em; color:var(--fp-ink); background:var(--fp-dv-6); padding:0 2px; }

/* rotation tie — the "stay" parked away from base; hover-only, drawn behind bars */
.tie { position:absolute; top:10px; height:3px; background:var(--_s); opacity:.4; z-index:0; cursor:help; }
.tie::before, .tie::after { content:''; position:absolute; top:-2px; width:2px; height:7px; background:var(--_s); opacity:.75; }
.tie::before { left:0; } .tie::after { right:0; }
.tielab { position:absolute; top:-7px; left:50%; transform:translateX(-50%); font-family:var(--mono);
  font-size:10px; font-weight:800; letter-spacing:.2px; color:var(--_d); background:var(--surface); padding:0 5px; white-space:nowrap; }
.now-line { position:absolute; top:0; bottom:0; width:2px; background:var(--fp-ink); z-index:6; pointer-events:none; }
.now-line::after { content:'NOW'; position:absolute; top:-13px; left:50%; transform:translateX(-50%); background:var(--fp-ink); color:#fff;
  font-family:var(--mono); font-size:8px; font-weight:700; letter-spacing:.08em; line-height:1; padding:1px 3px; white-space:nowrap; }
/* phone: don't compact — keep the timeline wide & scroll sideways; pin the reg column */
@media (max-width:640px){
  .gantt-inner { min-width:1040px; }
  .gantt-row { height:40px; align-items:flex-start; padding-top:2px; }
  .gantt-label { position:sticky; left:0; z-index:7; background:var(--surface); align-self:stretch; align-items:center; }
  .gantt-row.watch .gantt-label { background:var(--surface); }
  .gantt-axis-row .gantt-label { background:var(--surface); }
  .gantt-flight .lbl .fl { display:none; }     /* drop flight numbers on phone */
  .tie { top:25px; }                            /* connecting line moved below the bars */
  .tielab { top:3px; font-size:9.5px; }
}
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
/* confidence tiers are monochrome — the % and wording carry the level (no traffic-light hue) */
.conf-chip.cg, .conf-chip.ca, .conf-chip.cr { background:var(--surface); }
.conf-chip.cg .cp, .conf-chip.ca .cp, .conf-chip.cr .cp { color:var(--fp-ink); }
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
      <span id="typechk" style="display:contents"></span>
      <label class="tchk" id="allegchk" title="Show only airframes with the Allegris cabin"><input type="checkbox" id="alleg-only"><span class="abadge">A</span>Allegris only</label>
      <span><span class="sw" style="background:var(--surface);outline:1.5px dashed var(--fp-ink);outline-offset:-2px"></span>watched</span>
      <span style="opacity:0.4">|</span>
      <span><span class="sw" style="background:var(--accent)"></span>flew as planned</span>
      <span><span class="sw" style="background:repeating-linear-gradient(45deg,var(--accent) 0 4px,color-mix(in srgb,var(--accent) 60%,#fff) 4px 7px)"></span>unplanned</span>
      <span><span class="sw" style="background:var(--accent);box-shadow:inset 0 0 0 2px var(--fp-dv-6)"></span>swap</span>
      <span><span class="sw" style="background:color-mix(in srgb,var(--line) 20%,#fff);border:1.5px dashed var(--line)"></span>planned slot</span>
      <span><span class="sw" style="background:var(--accent-dim)"></span>planned (future)</span>
    </div>
  </div>
  <div class="gantt"><div class="gantt-inner" id="gantt"><div class="empty">Loading…</div></div></div>
  <div class="meta" style="margin-top:10px">Times in Frankfurt local; bar length = real flight duration. The ink <b>NOW</b> line splits past from plan: to its <b>left</b>, solid bars are what each tail actually did (ADS-B) and a hatched bar is an unplanned flight; to the <b>right</b>, pale bars are the plan, and grey dashed bars are <b>planned slots</b> not yet tracked. Watched tails are boxed with a dashed rule. A faint tie-line links a tail&rsquo;s out &amp; back legs across the time it&rsquo;s <b>parked away from base</b>. Click a leg for details; click a tail to open it in the Fleet&nbsp;DB.</div>
</div>
<div class="modal-bg" id="fl-modal"><div class="modal" id="fl-modal-body"></div></div>
<footer>
  <a href="/impressum">Impressum</a> <span style="margin:0 6px">&middot;</span> <a href="/datenschutz">Datenschutz</a>
</footer>
<script>
const $ = id => document.getElementById(id);
function fmt(iso){ if(!iso) return '?'; const d=new Date(iso);
  return d.toLocaleString('en-GB',{weekday:'short',day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
// hue per family (variants share it; the badge text names the exact variant)
const TFAM={'748':'t748','388':'t388','788':'t789','789':'t789','78X':'t789','359':'t359','35K':'t359'};
function tcls(t){ return TFAM[t] || 'tother'; }
const TYPE_ORDER=['748','388','788','789','78X','359','35K'];   // fixed checkbox/legend order
const HIDE_KEY='sched.hiddenTypes';
const ALLEG_KEY='sched.allegrisOnly';
const HUBS=['FRA','MUC'];                       // German bases these widebodies rotate from
function legDisplay(dep,arr){                   // destination-led label: arrow + the non-hub endpoint
  const dh=HUBS.includes(dep), ah=HUBS.includes(arr);
  if(dh&&!ah) return {arrow:'\\u2192', stn:arr};
  if(!dh&&ah) return {arrow:'\\u2190', stn:dep};
  return {arrow:'\\u2192', stn:arr||dep||'?'};
}
const legByKey={};                              // num|fdate -> leg, so the modal can show the actual track

async function init(){
  let d;
  try { d = await (await fetch('/api/schedule')).json(); }
  catch(e){ $('gantt').innerHTML='<div class="empty">Failed to load schedule.</div>'; return; }
  if(d.error){ $('gantt').innerHTML='<div class="empty">'+d.error+'</div>'; return; }
  if(!d.airframes || !d.airframes.length){ $('gantt').innerHTML='<div class="empty">No upcoming schedule data yet — the nightly collector has not populated future flights.</div>'; $('meta').textContent=''; return; }

  const t0=new Date(d.window.start).getTime(), t1=new Date(d.window.end).getTime();
  const range=t1-t0, dayMs=86400000;
  const frac = ms => (ms - t0)/range;                        // 0..1 across the time area
  const oLeft = f => 'calc(122px + (100% - 122px) * '+f+')'; // overlay coord (rows incl. label)
  const dayName = ms => new Date(ms).toLocaleDateString('en-GB',{weekday:'short',day:'numeric',month:'short',timeZone:'UTC'});

  // midnight ticks (fake-UTC midnights are epoch multiples of a day)
  const ticks=[]; for(let m=Math.ceil(t0/dayMs)*dayMs; m<t1; m+=dayMs) ticks.push(m);

  // axis: day labels at the left edge + each midnight. Drop the partial first-day
  // label when the first midnight is so close to the left edge that the two would
  // overlap (e.g. when "now-24h" lands late in the day) — that day is still named by
  // the midnight tick just to its right.
  let html='<div class="gantt-axis-row"><div class="gantt-label"></div><div class="gantt-axis">';
  const firstTickPct = ticks.length ? frac(ticks[0])*100 : 100;
  if(firstTickPct >= 8)
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
    const lblInner=(a.watch?'<span class="star">\\u2605</span>':'')+a.reg+'<span class="tbadge '+tc+'">'+a.type+'</span>'
      +(a.allegris?'<span class="abadge" title="Allegris cabin">A</span>':'');
    html+='<div class="gantt-row'+(a.watch?' watch':'')+'" data-reg="'+a.reg+'" data-type="'+a.type+'" data-allegris="'+(a.allegris?'1':'0')+'" data-dests="'+a.legs.map(l=>l.dep+' '+l.arr).join(' ')+'" data-fls="'+a.legs.map(l=>l.fl).join(' ')+'">';
    html+= a.icao24
      ? '<a class="gantt-label" href="/fleet/'+a.icao24+'" title="Open '+a.reg+' in Fleet DB">'+lblInner+'</a>'
      : '<div class="gantt-label">'+lblInner+'</div>';
    html+='<div class="gantt-track">';

    // rotation ties ("stays"): consecutive legs where the tail sits at an outstation (out & back)
    a.legs.forEach((l,i)=>{
      const n=a.legs[i+1]; if(!n) return;
      if(l.arr && n.dep && l.arr===n.dep && !HUBS.includes(l.arr)){
        const ge=new Date(l.end).getTime(), gs=new Date(n.start).getTime();
        let gl=Math.max(0,Math.min(100,(ge-t0)/range*100)), gr=Math.max(0,Math.min(100,(gs-t0)/range*100));
        if(gr-gl>0.3){
          const hrs=Math.max(0,Math.round((gs-ge)/3600000));
          const tip=a.reg+' parked at '+l.arr+'\\nturnaround '+hrs+'h'
            +'\\n'+(l.fl||'?')+' \\u2192 '+(n.fl||'?')
            +'\\non ground '+fmt(l.end)+' \\u2192 '+fmt(n.start);
          html+='<div class="tie '+tc+'" style="left:'+gl+'%;width:'+(gr-gl)+'%" title="'+tip.replace(/"/g,'&quot;')+'">'
            +(hrs>=10?'<span class="tielab">@'+l.arr+' '+hrs+'H</span>':'')+'</div>';   // short stays: line only, label in tooltip
        }
      }
    });

    a.legs.forEach(l=>{
      const s=new Date(l.start).getTime(), e=new Date(l.end).getTime();
      // clip to the visible window: keep the true end position, cut the start at the edge
      const left=Math.max(0,(s-t0)/range*100), right=Math.min(100,(e-t0)/range*100);
      const width=Math.max(0.5,right-left);
      const dur=l.dur?(Math.floor(l.dur/60)+'h'+String(l.dur%60).padStart(2,'0')):'';
      const st=l.status||'planned';
      // hue = aircraft type; status = a presentation class (cap / texture / tint), never a text-colour swap
      const stCls = st==='tracked'?'st-tracked' : st==='missing'?'st-missing'
        : st==='deviation'?'st-ghost' : st==='extra'?'st-extra' : 'st-planned';
      const dsp=legDisplay(l.dep,l.arr), flnum=(l.fl||'').replace('LH','');
      let title;
      if(st==='extra'){
        title=(l.fl||'actual')+'  '+l.dep+' \\u2192 '+l.arr+'\\n\\u2713 recorded (ADS-B) \\u00b7 no matching plan'
          +'\\n'+fmt(l.start)+' \\u2192 '+fmt(l.end);
      } else {
        title=l.fl+'  '+l.dep+' \\u2192 '+l.arr
          +'\\nDep '+fmt(l.dep_t)+' ('+l.dep+')'+(l.arr_t?'\\nArr '+fmt(l.arr_t)+' ('+l.arr+')':'')
          +(dur?'\\nFlight '+dur:'')+'  \\u00b7  '+a.type
          +(l.prev?'\\nprev: '+l.prev:'')+(l.lead!=null?'\\nlead: +'+l.lead+'d':'')+(l.swap?'  \\u00b7  \\u26a0 reassigned':'');
        if(st==='deviation' && l.act){
          title+='\\n\\u26a0 planned slot \\u2014 tail actually flew '+l.act.dep+'\\u2192'+l.act.arr;
        } else if(st==='tracked' && l.act){
          const dm=l.act.delta, ds=(dm>0?'+':'')+dm+'m';
          title+='\\n\\u2713 tracked: '+l.act.dep+'\\u2192'+l.act.arr+(l.act.cs?' ('+l.act.cs+')':'')+'  ['+ds+' vs plan]';
        } else if(st==='missing'){ title+='\\n\\u2014 no ADS-B track found yet'; }
      }
      if(l.num && l.fdate) legByKey[l.num+'|'+l.fdate]=l;   // remember for the enriched modal
      // unplanned "extra" legs have no flight number → carry the actual track on data-* for openActual()
      const actAttrs = st==='extra'
        ? ' data-cs="'+(l.fl||'')+'" data-dep="'+l.dep+'" data-arr="'+l.arr+'" data-start="'+l.start+'" data-end="'+l.end+'"'
        : '';
      html+='<div class="gantt-flight '+tc+' '+stCls+(l.swap?' is-swap':'')+' clk" style="left:'+left+'%;width:'+width+'%"'
        +' data-dest="'+l.dep+' '+l.arr+'" data-fl="'+(l.fl||'')+'" data-num="'+(l.num||'')+'" data-fdate="'+(l.fdate||'')+'"'+actAttrs
        +' title="'+title.replace(/"/g,'&quot;')+'">'
        +(l.swap?'<span class="swapchip">SWAP</span>':'')
        +'<span class="lbl">'+dsp.arrow+' '+dsp.stn+(flnum?'<span class="fl">'+flnum+'</span>':'')+'</span></div>';
      // deviation: also draw the ACTUAL route the tail really flew (clickable → actual detail)
      if(st==='deviation' && l.act){
        const as=new Date(l.act.start).getTime(), ae=new Date(l.act.end).getTime();
        const al=Math.max(0,(as-t0)/range*100), ar=Math.min(100,(ae-t0)/range*100);
        const aw=Math.max(0.5,ar-al), ad=legDisplay(l.act.dep,l.act.arr);
        const at=a.reg+' actually flew\\n'+l.act.dep+' \\u2192 '+l.act.arr+(l.act.cs?' ('+l.act.cs+')':'')
          +'\\n'+fmt(l.act.start)+' \\u2192 '+fmt(l.act.end)+'\\n(planned '+l.fl+' '+l.dep+'\\u2192'+l.arr+')';
        html+='<div class="gantt-flight '+tc+' st-actual clk" style="left:'+al+'%;width:'+aw+'%"'
          +' data-cs="'+(l.act.cs||'')+'" data-dep="'+l.act.dep+'" data-arr="'+l.act.arr+'" data-start="'+l.act.start+'" data-end="'+l.act.end+'"'
          +' data-planfl="'+(l.fl||'')+'" data-plandep="'+l.dep+'" data-planarr="'+l.arr+'"'
          +' title="'+at.replace(/"/g,'&quot;')+'">'
          +'<span class="lbl">'+ad.arrow+' '+ad.stn+'</span></div>';
      }
    });
    html+='</div></div>';
  });
  html+='</div>';
  $('gantt').innerHTML=html;
  const sw=d.swaps?(' \\u00b7 '+d.swaps+' reassignment'+(d.swaps>1?'s':'')):'';
  $('meta').textContent=d.airframes.length+' airframes \\u00b7 last 24h + plan \\u00b7 updated '
    +new Date(d.generated).toLocaleString('en-GB',{timeZone:'UTC',hour12:false})+' UTC'+sw;

  // type checkboxes — the type legend and the visibility control in one.
  // Hiding a type hides its rows EXCEPT watched (starred) tails; persisted.
  const present=[...new Set(d.airframes.map(a=>a.type))];
  const types=TYPE_ORDER.filter(t=>present.includes(t))
    .concat(present.filter(t=>!TYPE_ORDER.includes(t)).sort());
  let hidden;
  try { hidden=new Set(JSON.parse(localStorage.getItem(HIDE_KEY)||'[]')); } catch(e){ hidden=new Set(); }
  $('typechk').innerHTML=types.map(t=>
    '<label class="tchk'+(hidden.has(t)?' off':'')+'" title="Show/hide '+t+' rows (watched tails stay visible)">'
    +'<input type="checkbox" data-t="'+t+'"'+(hidden.has(t)?'':' checked')+'>'
    +'<span class="sw '+tcls(t)+'"></span>'+t+'</label>').join('');
  // "Allegris only" is strict on purpose (no watched-tail exemption): it's an
  // explicit cabin lens, not a row-decluttering toggle like the type boxes.
  let allegOnly=false;
  try { allegOnly = localStorage.getItem(ALLEG_KEY)==='1'; } catch(e){}
  $('alleg-only').checked=allegOnly;
  const applyTypes=()=>{
    document.querySelectorAll('.gantt-row[data-type]').forEach(r=>{
      const typeHide = hidden.has(r.dataset.type) && !r.classList.contains('watch');
      r.classList.toggle('typehide', typeHide || (allegOnly && r.dataset.allegris!=='1'));
    });
  };
  $('typechk').addEventListener('change', e=>{
    const t=e.target.dataset.t; if(!t) return;
    if(e.target.checked) hidden.delete(t); else hidden.add(t);
    e.target.closest('.tchk').classList.toggle('off', !e.target.checked);
    try { localStorage.setItem(HIDE_KEY, JSON.stringify([...hidden])); } catch(err){}
    applyTypes();
  });
  $('alleg-only').addEventListener('change', e=>{
    allegOnly=e.target.checked;
    try { localStorage.setItem(ALLEG_KEY, allegOnly?'1':'0'); } catch(err){}
    applyTypes();
  });
  applyTypes();
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
  const leg=legByKey[num+'|'+fdate]||null;   // already-loaded leg carries the ADS-B actual
  const b=$('fl-modal-body'); b.innerHTML='<div class="empty">Loading\\u2026</div>'; $('fl-modal').classList.add('show');
  fetch('/api/schedule/flight/LH/'+num+'/'+fdate).then(r=>r.json()).then(d=>renderFlight(d,leg))
    .catch(()=>{ b.innerHTML='<button class="close" onclick="closeFl()">\\u00d7</button><div class="empty">Failed to load.</div>'; });
}
function actualBlock(leg){   // plan-vs-actual section for a leg whose departure is in the past
  if(!leg || !['tracked','deviation','missing'].includes(leg.status)) return '';
  let h='<div class="hist-title">Actual track (ADS-B)</div>';
  if(leg.act){
    const dm=leg.act.delta, ds=(dm>0?'+':'')+dm+'m', dev=leg.status==='deviation';
    h+='<div class="det-grid">';
    h+=row('Flew', leg.act.dep+' \\u2192 '+leg.act.arr+(leg.act.cs?'  \\u00b7  '+leg.act.cs:''));
    h+=row('Off / On', fmt(leg.act.start)+' \\u2192 '+fmt(leg.act.end));
    h+=row('vs plan', ds+(dev?'  \\u00b7  \\u26a0 deviation from '+leg.dep+'\\u2192'+leg.arr:'  \\u00b7  on planned route'));
    h+='</div>';
  } else { h+='<div class="sub">\\u2014 no ADS-B track found yet for this leg.</div>'; }
  return h;
}
function openActual(ds){   // detail for a recorded (ADS-B) leg with no FIS plan
  const b=$('fl-modal-body'); $('fl-modal').classList.add('show');
  let h='<button class="close" onclick="closeFl()">\\u00d7</button>';
  h+='<h3>'+(ds.cs||'Recorded flight')+(ds.dep?'  \\u00b7  '+ds.dep+'\\u2192'+ds.arr:'')+'</h3>';
  h+='<div class="sub">'+(ds.planfl?'Deviation \\u2014 the tail flew this instead of its plan':'Unplanned \\u2014 recorded ADS-B track, no matching plan')+'</div>';
  h+='<div class="det-grid">';
  if(ds.cs) h+=row('Callsign', ds.cs);
  h+=row('Route', (ds.dep||'?')+' \\u2192 '+(ds.arr||'?'));
  h+=row('Off blocks', fmt(ds.start));
  h+=row('On blocks', fmt(ds.end));
  const mins=Math.round((new Date(ds.end)-new Date(ds.start))/60000);
  if(mins>0) h+=row('Duration', Math.floor(mins/60)+'h'+String(mins%60).padStart(2,'0'));
  if(ds.planfl) h+=row('Planned', ds.planfl+'  '+ds.plandep+'\\u2192'+ds.planarr);
  h+='</div>';
  b.innerHTML=h;
}
function renderFlight(d,leg){
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
    h+=row('Aircraft',(d.current_reg||'?')+(d.current_type?' \\u00b7 '+d.current_type:'')+(d.allegris?' <span class="abadge">ALLEGRIS</span>':''));
    if(d.cabin){
      const cb=[]; if(d.cabin.F)cb.push('<b>First '+d.cabin.F+'</b>'); if(d.cabin.C)cb.push('Business '+d.cabin.C);
      if(d.cabin.E)cb.push('Prem Eco '+d.cabin.E); if(d.cabin.M)cb.push('Economy '+d.cabin.M);
      h+=row('Cabin',cb.join(' \\u00b7 '));
    }
    h+=row('Departure',fmt(d.dep_sched)+(d.dep_term?' \\u00b7 T'+d.dep_term:'')+(d.dep_gate?' \\u00b7 Gate '+d.dep_gate:''));
    h+=row('Arrival',fmt(d.arr_sched)+(d.arr_term?' \\u00b7 T'+d.arr_term:'')+(d.arr_gate?' \\u00b7 Gate '+d.arr_gate:''));
    if(dur) h+=row('Flight time',dur);
    if(d.status) h+=row('Status',d.status);
    if(d.codeshares&&d.codeshares.length) h+=row('Codeshare',d.codeshares.join(', '));
    if(d.prev) h+=row('Previous leg',d.prev+(d.prev_date?' ('+fmtD(d.prev_date)+')':''));
    h+='</div>';
  } else { h+='<div class="sub">No current assignment for this date.</div>'; }
  h+=actualBlock(leg);
  if(d.history&&d.history.length){
    h+='<div class="hist-title">Assignment history</div>';
    let pr=null;
    d.history.forEach((x,i)=>{
      const ch = pr!==null && x.reg!==pr;
      const tags=[]; if(i===0) tags.push('originally planned'); if(i===d.history.length-1) tags.push('current');
      h+='<div class="hist-row'+(ch?' changed':'')+'"><span class="obs">'+fmtD(x.observed)+'</span>'
        +'<span class="reg">'+(x.reg||'\\u2014')+'</span>'+(x.allegris?'<span class="abadge" title="Allegris cabin">A</span>':'')+'<span class="tag">'
        +[x.type,tags.join(' \\u00b7 ')].filter(Boolean).join('  \\u00b7  ')+'</span></div>';
      pr=x.reg;
    });
  }
  b.innerHTML=h;
}
$('gantt').addEventListener('click', e=>{ const f=e.target.closest('.gantt-flight'); if(!f) return;
  if(f.dataset.num) openFlight(f.dataset.num, f.dataset.fdate);
  else if(f.dataset.dep) openActual(f.dataset); });
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
.searchbar { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:8px; align-items:stretch; }
.searchbar > input { background:var(--surface); border:1.5px solid var(--fp-ink); padding:9px 14px; font-size:14px; font-family:var(--sans); color:var(--text-bright); width:200px; text-transform:uppercase; }
.searchbar > input::placeholder { color:var(--muted); text-transform:none; }
.searchbar > input:focus { outline:none; border-color:var(--accent); }
.fp-btn:hover { opacity:.88; }  /* search button uses .fp-btn--solid */
/* buttons stretch with the row so they match the input/tokbox height */
.searchbar .fp-btn { display:inline-flex; align-items:center; padding-top:0; padding-bottom:0; }
/* filter drawer — tucked behind the Filters button */
.filterbar { display:none; align-items:center; flex-wrap:wrap; gap:9px 14px; border:1.5px solid var(--fp-ink);
  background:var(--surface); padding:10px 14px; margin-bottom:8px; }
.filterbar.show { display:flex; }
.filterbar .fp-seg { margin-bottom:0; }
.filterbar .fp-btn { font-size:.72rem; padding:.45em .8em; }
.flabel { font-family:var(--sans); font-size:10px; text-transform:uppercase; letter-spacing:.6px; color:var(--muted); }
.fchk { display:inline-flex; align-items:center; gap:5px; cursor:pointer; font-family:var(--mono); font-size:11px; font-weight:700; color:var(--text); user-select:none; }
.fchk input { accent-color:var(--fp-ink); width:13px; height:13px; margin:0; cursor:pointer; }
.fsep { width:1.5px; height:18px; background:var(--border); }
.fnote { font-family:var(--sans); font-size:11px; color:var(--muted); padding:8px 2px 0; }
/* multi-airport token inputs (route mode) — chips + inline input + suggestions */
.tokbox { display:flex; align-items:center; flex-wrap:wrap; gap:5px; background:var(--surface);
  border:1.5px solid var(--fp-ink); padding:5px 8px; width:280px; position:relative; cursor:text; }
.tokbox:focus-within { border-color:var(--accent); }
.tokbox .boxlbl { font-family:var(--sans); font-size:10px; text-transform:uppercase; letter-spacing:.5px; color:var(--muted); margin-right:2px; }
.tok { display:inline-flex; align-items:center; gap:5px; background:var(--fp-ink); color:#fff;
  font-family:var(--mono); font-size:11px; font-weight:700; padding:3px 7px; }
.tok button { border:none; background:none; color:rgba(255,255,255,.7); cursor:pointer; font-size:13px; line-height:1; padding:0; }
.tok button:hover { color:#fff; }
.tokbox input { flex:1; min-width:76px; border:none; background:none; padding:4px 2px; font-size:14px;
  font-family:var(--sans); color:var(--text-bright); text-transform:uppercase; }
.tokbox input:focus { outline:none; }
.tokbox input::placeholder { color:var(--muted); text-transform:none; }
.sugg { display:none; position:absolute; top:calc(100% + 1.5px); left:-1.5px; right:-1.5px; z-index:60;
  background:var(--surface); border:1.5px solid var(--fp-ink); max-height:264px; overflow-y:auto; }
.sugg.show { display:block; }
.sugg .sitem { display:flex; align-items:baseline; gap:9px; padding:7px 11px; cursor:pointer; }
.sugg .sitem .code { font-family:var(--mono); font-weight:700; font-size:12px; color:var(--text-bright); }
.sugg .sitem .nm { font-size:11px; color:var(--muted); }
.sugg .sitem:hover, .sugg .sitem.active { background:var(--surface2); }
/* ── Map mode ── the basemap is an inline equirectangular SVG (x=lon, y=-lat),
   so pan/zoom is just the viewBox and no tile server ever sees a visitor.
   Marker radius and label size are set in JS: CSS px inside a scaled viewBox
   are user units, which would grow with zoom. Strokes use non-scaling-stroke. */
.mapwrap { display:none; position:relative; border:1.5px solid var(--fp-ink); background:var(--surface); margin-bottom:8px; }
.mapwrap.show { display:block; }
.maphead { display:flex; align-items:center; gap:6px 12px; flex-wrap:wrap; padding:8px 12px; border-bottom:1.5px solid var(--fp-ink); }
.maphead .t { font-family:var(--sans); font-size:11px; color:var(--muted); }
.maphead .pick { font-family:var(--mono); font-size:12px; font-weight:700; color:var(--text-bright); }
/* figure/ground: sea takes the page grey, land the sage tint, so the coastline
   reads at a glance and a sage marker still sits clearly on top of it */
.mapsvg { display:block; width:100%; height:clamp(300px,52vh,470px); touch-action:none;
  cursor:grab; background:var(--bg); }
/* a portrait canvas would fit the world's width into a sliver and pad it with
   empty ocean — keep the map box landscape on narrow screens */
@media (max-width:640px){ .mapsvg { height:clamp(230px,62vw,320px); } }
.mapsvg.grabbing { cursor:grabbing; }
.land { fill:var(--fp-sage-tint); stroke:color-mix(in srgb,var(--fp-sage) 42%,var(--fp-sage-tint));
  stroke-width:.8; vector-effect:non-scaling-stroke; }
.mk { fill:var(--accent); stroke:var(--fp-bg); stroke-width:1.3; vector-effect:non-scaling-stroke; cursor:pointer; }
.mk:hover { fill:var(--fp-ink); }
.mk.sel { fill:var(--fp-terra); stroke:var(--fp-ink); stroke-width:1.8; }
/* the UA focus ring is an `outline:auto` drawn in SVG user space, so it blows up
   with the zoom — replace it with a stroke, which honours non-scaling-stroke */
.mk:focus { outline:none; }
.mk:focus-visible { stroke:var(--fp-ink); stroke-width:2.6; }
.mklbl { font-family:var(--mono); font-weight:700; fill:var(--text-bright); pointer-events:none;
  paint-order:stroke; stroke:var(--surface); stroke-linejoin:round; }
.mapctl { position:absolute; top:52px; right:10px; display:flex; flex-direction:column; gap:5px; }
.mapctl button { width:29px; height:29px; padding:0; border:1.5px solid var(--fp-ink); background:var(--surface);
  color:var(--text-bright); font-family:var(--mono); font-size:14px; font-weight:700; line-height:1; cursor:pointer; }
.mapctl button:hover { background:var(--surface2); }
.mapctl button.sm { font-size:8.5px; letter-spacing:.4px; }
.maptip { display:none; position:absolute; z-index:20; pointer-events:none; max-width:215px;
  background:var(--fp-ink); color:#fff; padding:6px 9px; font-family:var(--sans); font-size:11px; line-height:1.5; }
.maptip.show { display:block; }
.maptip b { font-family:var(--mono); font-size:11.5px; }
.maptip .d { color:rgba(255,255,255,.72); }
.mapnote { font-family:var(--sans); font-size:11px; color:var(--muted); padding:7px 12px; border-top:1.5px solid var(--border); }
.hint { font-family:var(--sans); font-size:11px; color:var(--muted); margin-bottom:18px; }
.results { display:flex; flex-direction:column; gap:8px; }
.fcard { display:flex; align-items:center; gap:14px; border:1.5px solid var(--fp-ink); background:var(--surface); padding:12px 14px; cursor:pointer; transition:border-color .12s; }
.fcard:hover { border-color:var(--accent); }
.fcard .when { width:96px; font-family:var(--mono); font-size:11px; color:var(--muted); flex-shrink:0; line-height:1.5; }
.fcard .when b { display:block; color:var(--text-bright); font-size:13px; }
.fcard .route { flex:1; min-width:0; }
.fcard .route .pair { font-size:15px; font-weight:700; color:var(--text-bright); }
.fcard .route .sub { font-family:var(--sans); font-size:11px; color:var(--muted); margin-top:1px; }
.fcard .tail { font-family:var(--mono); font-weight:700; font-size:13px; color:var(--text-bright); display:flex; align-items:center; flex-wrap:wrap; gap:5px; min-width:90px; }
.fcard .tail .star { color:var(--amber); }
.tbadge { font-family:var(--mono); font-size:9px; font-weight:700; padding:1px 5px; color:#fff; }
.tbadge.t748 { background:var(--accent); } .tbadge.t388 { background:var(--fp-dv-2); }
.tbadge.t789 { background:var(--fp-dv-4); }
.tbadge.t359 { background:var(--fp-dv-3); color:var(--fp-ink); } .tbadge.tother { background:var(--surface2); color:var(--muted); }
.abadge { font-family:var(--mono); font-size:9px; font-weight:700; padding:1px 4px;
  background:var(--fp-ink); color:#fff; letter-spacing:.4px; }   /* Allegris cabin */
/* cabin-config badges: F<n> = First (amber, the catch), C<n> = Business */
.cbadge { font-family:var(--mono); font-size:9px; font-weight:700; padding:1px 4px; letter-spacing:.4px;
  border:1.5px solid var(--line); color:var(--text); background:var(--surface); }
.cbadge.cf { border-color:var(--amber); background:var(--amber-dim); color:color-mix(in srgb,var(--fp-dv-3) 72%,var(--fp-ink)); }
.miniconf { display:flex; flex-direction:column; align-items:center; justify-content:center; min-width:62px; padding:5px 8px; border:1.5px solid var(--fp-ink); flex-shrink:0; }
.miniconf .p { font-family:var(--mono); font-weight:700; font-size:17px; line-height:1; }
.miniconf .cn { font-family:var(--sans); font-size:9px; text-transform:uppercase; letter-spacing:.4px; color:var(--muted); margin-top:3px; }
/* confidence tiers are monochrome — the % carries the level (no traffic-light hue) */
.miniconf.cg, .miniconf.ca, .miniconf.cr { background:var(--surface); }
.miniconf.cg .p, .miniconf.ca .p, .miniconf.cr .p { color:var(--fp-ink); }
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
/* confidence tiers are monochrome — the % and wording carry the level (no traffic-light hue) */
.conf-chip.cg, .conf-chip.ca, .conf-chip.cr { background:var(--surface); }
.conf-chip.cg .cp, .conf-chip.ca .cp, .conf-chip.cr .cp { color:var(--fp-ink); }
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
    <div class="meta">Find an upcoming flight by <b>airframe</b>, <b>route</b> or <b>location</b>, with the currently published tail and a measured chance it still holds by departure. Schedule is published ~4 days out, so check back closer to your date.</div>
  <div class="fp-seg">
    <button id="m-tail" class="active" onclick="setMode('tail')">By tail</button>
    <button id="m-route" onclick="setMode('route')">By route</button>
    <button id="m-map" onclick="setMode('map')">By location</button>
  </div>
  <div class="searchbar">
    <input id="tail-in" type="text" placeholder="registration, e.g. D-ABYN">
    <div class="tokbox" id="dep-box" style="display:none">
      <span class="boxlbl">from</span>
      <input id="dep-in" type="text" placeholder="add airport, e.g. FRA" autocomplete="off">
      <div class="sugg" id="dep-sugg"></div>
    </div>
    <div class="tokbox" id="arr-box" style="display:none">
      <span class="boxlbl">to</span>
      <input id="arr-in" type="text" placeholder="add airport, e.g. HND" autocomplete="off">
      <div class="sugg" id="arr-sugg"></div>
    </div>
    <button class="fp-btn fp-btn--solid" id="search-btn" onclick="search()">Search</button>
    <button class="fp-btn" id="filter-btn" onclick="toggleFilters()">Filters</button>
  </div>
  <div class="mapwrap" id="mapwrap">
    <div class="maphead">
      <span class="t">Click an airport to see what departs</span>
      <span class="pick" id="map-pick"></span>
      <span class="t" id="map-count" style="margin-left:auto"></span>
    </div>
    <svg class="mapsvg" id="mapsvg" role="group"
         aria-label="World map of airports with upcoming Lufthansa departures"></svg>
    <div class="mapctl">
      <button type="button" id="map-in" aria-label="Zoom in" title="Zoom in">+</button>
      <button type="button" id="map-out" aria-label="Zoom out" title="Zoom out">&minus;</button>
      <button type="button" class="sm" id="map-fit" aria-label="Reset view" title="Reset view">FIT</button>
    </div>
    <div class="maptip" id="maptip"></div>
    <div class="mapnote" id="mapnote" style="display:none"></div>
  </div>
  <div class="filterbar" id="filterbar">
    <span class="flabel">Type</span>
    <label class="fchk"><input type="checkbox" data-fam="747" checked>747-8</label>
    <label class="fchk"><input type="checkbox" data-fam="A380" checked>A380</label>
    <label class="fchk"><input type="checkbox" data-fam="787" checked>787</label>
    <label class="fchk"><input type="checkbox" data-fam="A350" checked>A350</label>
    <span class="fsep"></span>
    <span class="flabel">Allegris</span>
    <div class="fp-seg" id="f-alleg"><button class="active" data-v="any">any</button><button data-v="yes">yes</button><button data-v="no">no</button></div>
    <span class="fsep"></span>
    <span class="flabel">First class</span>
    <div class="fp-seg" id="f-first"><button class="active" data-v="any">any</button><button data-v="yes">yes</button><button data-v="no">no</button></div>
    <span class="fsep"></span>
    <button class="fp-btn" id="f-reset">Reset</button>
  </div>
  <div class="hint" id="hint"><span id="hint-lead">Tip: route mode takes <b>several airports per side</b> &mdash; type a code or city for suggestions, Enter or comma adds it.</span> Watched airframes are starred; <span class="abadge">ALLEGRIS</span> marks the new cabin; <span class="cbadge cf">F8</span> <span class="cbadge">C80</span> = First / Business seat count (cabin follows the assigned tail, so mind the hold %).</div>
  <div class="results" id="results"><div class="empty">Search a tail (e.g. D-ABYN), a route (e.g. FRA &rarr; HND), or pick an airport off the map.</div></div>
</div>
<div class="modal-bg" id="fl-modal"><div class="modal" id="fl-modal-body"></div></div>
<footer>
  <a href="/impressum">Impressum</a> <span style="margin:0 6px">&middot;</span> <a href="/datenschutz">Datenschutz</a>
</footer>
<script>
const $ = id => document.getElementById(id);
let mode = 'tail';
const HINTS = {
  tail: 'Tip: search a registration to see every upcoming leg that airframe is published on.',
  route: 'Tip: route mode takes <b>several airports per side</b> &mdash; type a code or city for suggestions, Enter or comma adds it.',
  map: 'Tip: drag to pan, scroll or +/&minus; to zoom, then click an airport. Marker size is how many departures are published there.',
};
function setMode(m){
  mode = m;
  $('m-tail').classList.toggle('active', m==='tail');
  $('m-route').classList.toggle('active', m==='route');
  $('m-map').classList.toggle('active', m==='map');
  $('tail-in').style.display = m==='tail' ? '' : 'none';
  $('dep-box').style.display = m==='route' ? '' : 'none';
  $('arr-box').style.display = m==='route' ? '' : 'none';
  // in map mode the marker *is* the query, so the free-text search button goes
  $('search-btn').style.display = m==='map' ? 'none' : '';
  $('mapwrap').classList.toggle('show', m==='map');
  $('hint-lead').innerHTML = HINTS[m] || '';
  if(m==='map'){ mapInit(); if(MAP.marks.length) mapDraw(); }   // redraw: it sizes to a visible box
  else { (m==='tail' ? $('tail-in') : $('dep-in')).focus(); }
}

/* ── Multi-airport token inputs with suggestions ─────────────────
   The suggestion list is the collected FIS network (code + city name),
   so everything offered is actually searchable. Filtering is local. */
let AIRPORTS = [];
fetch('/api/book/airports').then(r=>r.json()).then(d=>{ AIRPORTS = d.airports||[]; }).catch(()=>{});

function makeTok(boxId){
  const box = $(boxId), input = box.querySelector('input'), sugg = box.querySelector('.sugg');
  const codes = [];
  let items = [], act = -1;
  function renderChips(){
    box.querySelectorAll('.tok').forEach(t=>t.remove());
    codes.forEach(c=>{
      const el = document.createElement('span');
      el.className = 'tok';
      el.innerHTML = c + '<button type="button" aria-label="remove '+c+'">&times;</button>';
      el.querySelector('button').addEventListener('click', ev=>{
        ev.stopPropagation(); codes.splice(codes.indexOf(c),1); renderChips(); });
      box.insertBefore(el, input);
    });
  }
  function add(c){
    c = (c||'').trim().toUpperCase();
    if(/^[A-Z0-9]{3}$/.test(c) && !codes.includes(c)){ codes.push(c); renderChips(); }
    input.value = ''; hide();
  }
  function matches(q){
    q = q.trim().toUpperCase(); if(!q) return [];
    const ql = q.toLowerCase();
    return AIRPORTS
      .filter(a => !codes.includes(a.code) && (a.code.startsWith(q) || (a.name||'').toLowerCase().includes(ql)))
      .sort((a,b) => (b.code.startsWith(q) - a.code.startsWith(q)) || (b.n - a.n))
      .slice(0,8);
  }
  function show(){
    items = matches(input.value); act = -1;
    if(!items.length){ hide(); return; }
    sugg.innerHTML = items.map((a,i)=>'<div class="sitem" data-i="'+i+'"><span class="code">'+a.code+'</span><span class="nm">'+(a.name||'')+'</span></div>').join('');
    sugg.classList.add('show');
    sugg.querySelectorAll('.sitem').forEach(el=>{
      el.addEventListener('mousedown', ev=>{ ev.preventDefault(); add(items[+el.dataset.i].code); });
    });
  }
  function hide(){ sugg.classList.remove('show'); items = []; act = -1; }
  function mark(){ sugg.querySelectorAll('.sitem').forEach((el,i)=>el.classList.toggle('active', i===act)); }
  function commit(){   // turn whatever is typed into a chip (best match wins)
    const v = input.value.trim().toUpperCase();
    if(!v) return;
    if(/^[A-Z0-9]{3}$/.test(v)) add(v);
    else { const m = matches(v); if(m.length) add(m[0].code); else input.value=''; }
  }
  input.addEventListener('input', show);
  input.addEventListener('keydown', e=>{
    if(e.key==='ArrowDown' && items.length){ act = (act+1)%items.length; mark(); e.preventDefault(); }
    else if(e.key==='ArrowUp' && items.length){ act = (act-1+items.length)%items.length; mark(); e.preventDefault(); }
    else if(e.key===','){ e.preventDefault(); commit(); }
    else if(e.key==='Enter'){
      e.preventDefault();
      if(act>=0) add(items[act].code);
      else if(input.value.trim()) commit();
      else search();
    }
    else if(e.key==='Escape' && items.length){ hide(); e.stopPropagation(); }
    else if(e.key==='Backspace' && !input.value && codes.length){ codes.pop(); renderChips(); }
  });
  input.addEventListener('blur', ()=>{ commit(); hide(); });
  box.addEventListener('click', e=>{ if(e.target===box) input.focus(); });
  return {
    codes: ()=>codes.slice(),
    set(list){ codes.length = 0; input.value=''; (list||[]).forEach(c=>{
      c = (c||'').trim().toUpperCase();
      if(/^[A-Z0-9]{3}$/.test(c) && !codes.includes(c)) codes.push(c); }); renderChips(); },
  };
}
const tokDep = makeTok('dep-box'), tokArr = makeTok('arr-box');
function fmtDay(iso){ if(!iso) return '?'; return new Date(iso).toLocaleDateString('en-GB',{weekday:'short',day:'2-digit',month:'short',timeZone:'UTC'}); }
function fmtClock(iso){ if(!iso) return ''; return new Date(iso).toLocaleTimeString('en-GB',{hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'UTC'}); }
function tcls(t){ return ({'748':'t748','388':'t388','788':'t789','789':'t789','78X':'t789','359':'t359','35K':'t359'})[t] || 'tother'; }

function miniChip(hold){
  if(!hold) return '<div class="miniconf"><span class="p" style="color:var(--muted)">&mdash;</span><span class="cn">no data</span></div>';
  const p = Math.round(hold.p*100);
  const cls = p>=85 ? 'cg' : (p>=60 ? 'ca' : 'cr');
  return '<div class="miniconf '+cls+'" title="chance the published tail still holds by departure ('+hold.lead+'d out, '+hold.basis+', n='+hold.n+')">'
    +'<span class="p">'+p+'%</span><span class="cn">holds</span></div>';
}

/* cabin config {F,C,E,M} — F/C badges on the card, full breakdown as tooltip */
function cabinText(c){
  const t = [];
  if(c.F) t.push('First '+c.F); if(c.C) t.push('Business '+c.C);
  if(c.E) t.push('Prem Eco '+c.E); if(c.M) t.push('Economy '+c.M);
  return t.join(' \\u00b7 ');
}
function cabinBadges(c){
  if(!c) return '';
  const full = 'Cabin: '+cabinText(c);
  return (c.F ? '<span class="cbadge cf" title="'+full+'">F'+c.F+'</span>' : '')
       + (c.C ? '<span class="cbadge" title="'+full+'">C'+c.C+'</span>' : '');
}

/* ── Result filters (type / Allegris / First) — client-side over the last fetch ── */
const FILT = { fams:null, alleg:'any', first:'any' };   // fams null = all types
let LAST = null;
function famOf(t){ return ({'748':'747','388':'A380','788':'787','789':'787','78X':'787','359':'A350','35K':'A350'})[t] || 'other'; }
function toggleFilters(){ $('filterbar').classList.toggle('show'); }
function passes(f){
  if(FILT.fams && !FILT.fams.has(famOf(f.type))) return false;
  if(FILT.alleg!=='any' && !!f.allegris !== (FILT.alleg==='yes')) return false;
  if(FILT.first!=='any' && !!(f.cabin && f.cabin.F) !== (FILT.first==='yes')) return false;
  return true;
}
function syncFilters(){
  const n = (FILT.fams?1:0) + (FILT.alleg!=='any'?1:0) + (FILT.first!=='any'?1:0);
  $('filter-btn').textContent = n ? 'Filters \\u00b7 '+n : 'Filters';
  if(LAST) drawResults();
}
document.querySelectorAll('.fchk input').forEach(cb => cb.addEventListener('change', ()=>{
  const boxes = [...document.querySelectorAll('.fchk input')], on = boxes.filter(x=>x.checked);
  FILT.fams = on.length===boxes.length ? null : new Set(on.map(x=>x.dataset.fam));
  syncFilters();
}));
function segWire(id, key){
  $(id).querySelectorAll('button').forEach(b => b.addEventListener('click', ()=>{
    FILT[key] = b.dataset.v;
    $(id).querySelectorAll('button').forEach(x=>x.classList.toggle('active', x===b));
    syncFilters();
  }));
}
segWire('f-alleg','alleg'); segWire('f-first','first');
$('f-reset').addEventListener('click', ()=>{
  document.querySelectorAll('.fchk input').forEach(x=>{ x.checked = true; });
  ['f-alleg','f-first'].forEach(id => $(id).querySelectorAll('button')
    .forEach(x=>x.classList.toggle('active', x.dataset.v==='any')));
  FILT.fams = null; FILT.alleg = 'any'; FILT.first = 'any';
  syncFilters();
});

function renderResults(d){ LAST = d; drawResults(); }
function drawResults(){
  const R = $('results'), d = LAST;
  if(d.error){ R.innerHTML = '<div class="empty">'+d.error+'</div>'; return; }
  const all = d.flights || [];
  if(!all.length){ R.innerHTML = '<div class="empty">No upcoming flights found. The schedule is only published ~4 days ahead &mdash; try again closer to your date, or check the spelling.</div>'; return; }
  const fs = all.filter(passes);
  if(!fs.length){ R.innerHTML = '<div class="empty">All '+all.length+' flights are hidden by the filters &mdash; adjust or reset them.</div>'; return; }
  let h = '';
  fs.forEach(f => {
    const lead = f.lead===0 ? 'today' : (f.lead===1 ? 'tomorrow' : 'in '+f.lead+' days');
    const reassigned = f.reassigned ? ' &middot; <span style="color:var(--amber)">&#9888; reassigned before</span>' : '';
    h += '<div class="fcard" data-num="'+f.number+'" data-fdate="'+f.flight_date+'">'
      + '<div class="when"><b>'+fmtDay(f.dep_sched)+'</b>'+lead+'</div>'
      + '<div class="route"><div class="pair">'+f.dep+' &rarr; '+f.arr+'</div>'
      + '<div class="sub">'+f.flight+' &middot; dep '+fmtClock(f.dep_sched)+(f.arr_sched?' &middot; arr '+fmtClock(f.arr_sched):'')+reassigned+'</div></div>'
      + '<div class="tail">'+(f.watch?'<span class="star">&#9733;</span>':'')+(f.reg||'?')
      + '<span class="tbadge '+tcls(f.type)+'">'+(f.type||'?')+'</span>'
      + (f.allegris?'<span class="abadge" title="Allegris cabin">ALLEGRIS</span>':'')
      + cabinBadges(f.cabin)+'</div>'
      + miniChip(f.hold) + '</div>';
  });
  if(fs.length < all.length) h += '<div class="fnote">'+fs.length+' of '+all.length+' flights shown &mdash; '+(all.length-fs.length)+' hidden by filters</div>';
  R.innerHTML = h;
}

function search(){
  let url;
  if(mode==='tail'){
    const r = $('tail-in').value.trim().toUpperCase();
    if(!r){ return; }
    url = '/api/book?reg=' + encodeURIComponent(r);
  } else if(mode==='map'){
    if(!MAP.sel){ return; }              // the marker is the query
    url = '/api/book?dep=' + encodeURIComponent(MAP.sel);
  } else {
    const dp = tokDep.codes().join(','), ar = tokArr.codes().join(',');
    if(!dp && !ar){ return; }
    url = '/api/book?dep=' + encodeURIComponent(dp) + '&arr=' + encodeURIComponent(ar);
  }
  $('results').innerHTML = '<div class="empty">Searching&hellip;</div>';
  fetch(url).then(r=>r.json()).then(renderResults)
    .catch(()=>{ $('results').innerHTML = '<div class="empty">Search failed.</div>'; });
}

// route-mode inputs handle Enter themselves (suggestion pick vs. search)
$('tail-in').addEventListener('keydown', e => { if(e.key==='Enter') search(); });

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
    h += row('Aircraft',(d.current_reg||'?')+(d.current_type?' &middot; '+d.current_type:'')+(d.allegris?' <span class="abadge">ALLEGRIS</span>':''));
    if(d.cabin){
      const cb=[]; if(d.cabin.F)cb.push('<b>First '+d.cabin.F+'</b>'); if(d.cabin.C)cb.push('Business '+d.cabin.C);
      if(d.cabin.E)cb.push('Prem Eco '+d.cabin.E); if(d.cabin.M)cb.push('Economy '+d.cabin.M);
      const tot = (d.cabin.F||0)+(d.cabin.C||0)+(d.cabin.E||0)+(d.cabin.M||0);
      h += row('Cabin',cb.join(' &middot; ')+' &middot; '+tot+' seats');
    }
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
        + '<span class="reg">'+(x.reg||'&mdash;')+'</span>'+(x.allegris?'<span class="abadge" title="Allegris cabin">A</span>':'')+'<span class="tag">'
        + [x.type,tags.join(' &middot; ')].filter(Boolean).join('  &middot;  ')+'</span></div>';
      pr = x.reg;
    });
  }
  b.innerHTML = h;
}
$('results').addEventListener('click', e => { const c=e.target.closest('.fcard'); if(c) openFlight(c.dataset.num, c.dataset.fdate); });
$('fl-modal').addEventListener('click', e => { if(e.target.id==='fl-modal') closeFl(); });
document.addEventListener('keydown', e => { if(e.key==='Escape') closeFl(); });

/* ── Map mode ─────────────────────────────────────────────────────────
   Basemap and markers share one coordinate system — x = longitude,
   y = -latitude (equirectangular) — so pan and zoom are nothing but the
   viewBox, and a marker needs no projection maths. Both payloads come from
   this app, so the page makes no third-party request. Clicking a marker runs
   the ordinary dep= search: results, filters and the modal are shared with
   route mode, and the marker can never disagree with the cards it opens. */
const SVGNS = 'http://www.w3.org/2000/svg';
// minW caps the zoom: the outline is Natural Earth 1:110m, so past roughly a
// 12-degree window it degrades into abstract polygons. 12 degrees still puts
// ~50px between neighbours as close as HND/NRT or JFK/EWR.
const MAP = { on:false, svg:null, marks:[], byCode:{}, sel:null, pending:null,
              moved:0, minW:12, V:{x:-180,y:-70,w:360} };

function esc(s){ return String(s==null?'':s).replace(/[&<>"]/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'})[c]); }
function svgEl(tag, attrs){
  const e = document.createElementNS(SVGNS, tag);
  for(const k in attrs) e.setAttribute(k, attrs[k]);
  return e;
}
/* when the viewport outgrows the content, centre instead of clamping */
function clampRange(v, lo, hi){ return lo > hi ? (lo+hi)/2 : Math.min(hi, Math.max(lo, v)); }

/* Sets the viewBox, then re-sizes everything that must stay constant on
   screen: inside a scaled viewBox a CSS px is a user unit, so radii and
   label sizes would otherwise grow with the zoom. */
function mapDraw(){
  const svg = MAP.svg, r = svg.getBoundingClientRect(), V = MAP.V;
  const wpx = Math.max(r.width, 1);
  V.w = Math.min(360, Math.max(MAP.minW, V.w));
  const h = V.w * (r.height / wpx);
  V.x = clampRange(V.x, -186, 186 - V.w);
  V.y = clampRange(V.y, -88, 62 - h);
  svg.setAttribute('viewBox', V.x+' '+V.y+' '+V.w+' '+h);
  const k = V.w / wpx;                       // user units per screen pixel
  MAP.marks.forEach(m => {
    m.c.setAttribute('r', (m.r * k).toFixed(4));
    // label the hubs and the pick always, everything else once zoomed in —
    // and never label what is scrolled off the canvas
    const inView = m.lon > V.x - 2 && m.lon < V.x + V.w + 2
                && -m.lat > V.y - 2 && -m.lat < V.y + h + 2;
    const show = inView && (V.w < 100 || m.big || m.code === MAP.sel);
    m.t.style.display = show ? '' : 'none';
    if(show){
      m.t.setAttribute('x', (m.lon + (m.r + 3.5) * k).toFixed(4));
      m.t.setAttribute('y', (-m.lat + 3.7 * k).toFixed(4));
      m.t.setAttribute('font-size', (10.5 * k).toFixed(4));
      m.t.setAttribute('stroke-width', (3 * k).toFixed(4));
    }
  });
}

function mapFit(){
  const r = MAP.svg.getBoundingClientRect(), asp = r.height / Math.max(r.width, 1);
  if(!MAP.marks.length){ MAP.V = {x:-180, y:-70, w:360}; mapDraw(); return; }
  let x0=180, x1=-180, y0=90, y1=-90;
  MAP.marks.forEach(m => {
    x0 = Math.min(x0, m.lon); x1 = Math.max(x1, m.lon);
    y0 = Math.min(y0, -m.lat); y1 = Math.max(y1, -m.lat);
  });
  const spanX = (x1-x0) * 1.10, spanY = (y1-y0) * 1.18;
  let w = Math.max(spanX, spanY / Math.max(asp, 0.01), 20);
  // on a tall canvas, fitting every longitude buys mostly empty ocean — cap the
  // latitude shown at ~1.9x what the network needs and let the user pan instead
  w = Math.max(Math.min(w, (spanY * 1.9) / Math.max(asp, 0.01)), 20, MAP.minW);
  MAP.V = { x:(x0+x1)/2 - w/2, y:(y0+y1)/2 - (w*asp)/2, w:w };
  mapDraw();
}

function mapZoomAt(cx, cy, f){
  const r = MAP.svg.getBoundingClientRect(), V = MAP.V;
  const wpx = Math.max(r.width, 1), k = V.w / wpx;
  const wx = V.x + (cx - r.left) * k, wy = V.y + (cy - r.top) * k;
  const nw = Math.min(360, Math.max(MAP.minW, V.w * f)), nk = nw / wpx;
  V.x = wx - (cx - r.left) * nk;
  V.y = wy - (cy - r.top) * nk;
  V.w = nw;
  mapDraw();
}
function mapZoomCentre(f){
  const r = MAP.svg.getBoundingClientRect();
  mapZoomAt(r.left + r.width/2, r.top + r.height/2, f);
}

function mapTip(m, ev){
  const tip = $('maptip'), a = m.a, nd = a.dests.length;
  tip.innerHTML = '<b>'+esc(a.code)+'</b> '+esc(a.name)
    + '<br>'+a.flights+' departure'+(a.flights===1?'':'s')
    + ' &middot; '+nd+' destination'+(nd===1?'':'s')
    + (a.types.length ? '<br><span class="d">'+esc(a.types.join(', '))
        + (a.next ? ' &middot; from '+fmtD(a.next) : '')+'</span>' : '');
  tip.classList.add('show');
  const wr = $('mapwrap').getBoundingClientRect();
  const x = Math.min(ev.clientX - wr.left + 14, wr.width - tip.offsetWidth - 8);
  const y = Math.min(ev.clientY - wr.top + 14, wr.height - tip.offsetHeight - 8);
  tip.style.left = Math.max(6, x)+'px';
  tip.style.top = Math.max(6, y)+'px';
}
function mapHideTip(){ $('maptip').classList.remove('show'); }

function mapBuild(world, data){
  const svg = MAP.svg;
  svg.innerHTML = '';
  svg.appendChild(svgEl('path', {d:world.path, class:'land'}));
  const gMk = svgEl('g', {}), gLbl = svgEl('g', {});
  // busiest first, so the small outstations paint on top and stay clickable
  const A = (data.airports||[]).slice().sort((a,b) => b.flights - a.flights);
  const max = A.length ? A[0].flights : 1;
  MAP.marks = []; MAP.byCode = {};
  A.forEach((a, i) => {
    const c = svgEl('circle', {cx:a.lon, cy:-a.lat, r:0, class:'mk', tabindex:'0',
      role:'button', 'aria-label':a.code+' '+a.name+', '+a.flights+' departures'});
    const t = svgEl('text', {x:a.lon, y:-a.lat, class:'mklbl'});
    t.textContent = a.code;
    const m = {code:a.code, lon:a.lon, lat:a.lat, a:a, c:c, t:t,
               r:3.4 + 3.8 * Math.sqrt(a.flights / max), big:i < 8};
    c.addEventListener('click', () => { if(MAP.moved < 5) mapSelect(a.code); });
    c.addEventListener('keydown', e => {
      if(e.key === 'Enter' || e.key === ' '){ e.preventDefault(); mapSelect(a.code); } });
    c.addEventListener('pointerenter', ev => mapTip(m, ev));
    c.addEventListener('pointermove', ev => mapTip(m, ev));
    c.addEventListener('pointerleave', mapHideTip);
    gMk.appendChild(c); gLbl.appendChild(t);
    MAP.marks.push(m); MAP.byCode[a.code] = a;
  });
  svg.appendChild(gMk); svg.appendChild(gLbl);
  $('map-count').textContent = A.length + ' airports with published departures';
  const un = data.unplaced || [];
  $('mapnote').style.display = un.length ? '' : 'none';
  if(un.length){
    $('mapnote').innerHTML = un.length+' airport'+(un.length===1?'':'s')
      + ' not placed on the map ('+un.map(u => esc(u.code)).join(', ')
      + ') &mdash; reachable from route mode.';
  }
  mapFit();
}

function mapWire(){
  const svg = MAP.svg, ptrs = new Map();
  let last = null, pinch = 0;
  function pinchState(){
    const v = [...ptrs.values()];
    return {x:(v[0].x+v[1].x)/2, y:(v[0].y+v[1].y)/2,
            d:Math.hypot(v[0].x-v[1].x, v[0].y-v[1].y)};
  }
  // Deliberately no setPointerCapture: capturing retargets the compatibility
  // click to the <svg>, so a marker's own click would never fire. Tracking the
  // drag on window instead keeps clicks intact and still follows a pointer
  // that leaves the map mid-drag.
  svg.addEventListener('pointerdown', e => {
    ptrs.set(e.pointerId, {x:e.clientX, y:e.clientY});
    MAP.moved = 0;
    if(ptrs.size === 1){ last = {x:e.clientX, y:e.clientY}; svg.classList.add('grabbing'); }
    else if(ptrs.size === 2){ pinch = pinchState().d; last = null; }
  });
  window.addEventListener('pointermove', e => {
    if(!ptrs.has(e.pointerId)) return;
    ptrs.set(e.pointerId, {x:e.clientX, y:e.clientY});
    if(ptrs.size >= 2){                       // pinch zoom about the midpoint
      const p = pinchState();
      if(pinch > 0 && p.d > 0){ mapZoomAt(p.x, p.y, pinch / p.d); MAP.moved += 10; }
      pinch = p.d;
      return;
    }
    if(!last) return;
    const r = svg.getBoundingClientRect(), k = MAP.V.w / Math.max(r.width, 1);
    const dx = e.clientX - last.x, dy = e.clientY - last.y;
    MAP.moved += Math.abs(dx) + Math.abs(dy);   // tells a drag from a click
    if(MAP.moved > 4) mapHideTip();
    MAP.V.x -= dx * k; MAP.V.y -= dy * k;
    last = {x:e.clientX, y:e.clientY};
    mapDraw();
  });
  function up(e){
    if(!ptrs.delete(e.pointerId)) return;
    if(ptrs.size < 2) pinch = 0;
    if(ptrs.size === 0){ last = null; svg.classList.remove('grabbing'); }
  }
  window.addEventListener('pointerup', up);
  window.addEventListener('pointercancel', up);
  svg.addEventListener('wheel', e => {
    e.preventDefault();
    mapZoomAt(e.clientX, e.clientY, Math.exp(e.deltaY * 0.0016));
  }, {passive:false});
  $('map-in').addEventListener('click', () => mapZoomCentre(1/1.5));
  $('map-out').addEventListener('click', () => mapZoomCentre(1.5));
  $('map-fit').addEventListener('click', mapFit);
  window.addEventListener('resize', () => { if(MAP.marks.length) mapDraw(); });
}

function mapSelect(code){
  if(!MAP.byCode[code]){ MAP.pending = code; return; }   // still loading
  MAP.sel = code;
  MAP.marks.forEach(m => m.c.classList.toggle('sel', m.code === code));
  const a = MAP.byCode[code];
  $('map-pick').innerHTML = esc(a.code)+' &middot; '+esc(a.name);
  mapDraw();                   // the pick keeps its label at any zoom
  search();
}

function mapInit(){
  if(MAP.on) return;
  MAP.on = true;
  MAP.svg = $('mapsvg');
  mapWire();
  Promise.all([
    fetch('/book/world.json').then(r => r.json()),
    fetch('/api/book/map').then(r => r.json()),
  ]).then(([w, d]) => {
    if(d.error){ $('map-count').textContent = d.error; return; }
    mapBuild(w, d);
    if(MAP.pending){ const c = MAP.pending; MAP.pending = null; mapSelect(c); }
  }).catch(() => { $('map-count').textContent = 'Map failed to load.'; });
}

/* Prefill + auto-search from URL (?reg=, ?dep=&arr=, or ?loc=) so links land on results */
(function(){
  const p = new URLSearchParams(location.search);
  if(p.get('reg')){ setMode('tail'); $('tail-in').value = p.get('reg'); search(); }
  else if(p.get('loc')){ setMode('map'); mapSelect(p.get('loc').trim().toUpperCase()); }
  else if(p.get('dep') || p.get('arr')){ setMode('route');
    tokDep.set((p.get('dep')||'').split(/[,\\s]+/));
    tokArr.set((p.get('arr')||'').split(/[,\\s]+/)); search(); }
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
      <a href="/insights?type=B787" id="seg-B787">787</a>
      <a href="/insights?type=A350" id="seg-A350">A350</a>
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
  const mixed = new Set(af.map(a=>a.type)).size > 1;  // family tab with >1 variant
  let h='<table><tr><th>Tail</th>'+(mixed?'<th>Type</th>':'')+'<th class="r">Legs</th><th class="r">Hours</th><th class="r">Longest gap</th><th class="r">Last seen</th></tr>';
  af.forEach(a=>{
    h+='<tr><td class="tail-reg">'+(a.watch?'<span class="star">&#9733;</span>':'')+a.reg+'</td>'
      +(mixed?'<td>'+(a.type||'')+'</td>':'')
      +'<td class="r">'+a.legs+'</td><td class="r">'+a.hours+'h</td>'
      +'<td class="r">'+(a.max_ground_days!=null?a.max_ground_days+'d':'—')+'</td>'
      +'<td class="r">'+shortDate(a.last)+'</td></tr>';
  });
  return h+'</table>';
}
function reschedChart(rel){
  const data = (rel && rel.reschedulings) || [];
  if(!data.length) return '<div class="empty">No schedule data collected for this type yet.</div>';
  const max = Math.max(1, ...data.map(d=>d.n));
  let bars='';
  data.forEach((d,i)=>{
    const px = Math.round(d.n/max*148);
    const isBase = i===0 && d.n===0;
    const tip = shortDate(d.date)+': '+d.n+' reschedulings'
      + (isBase ? '  (collection start)' : '');
    bars += '<div class="rs-col'+(isBase?' rs-base':'')+'" title="'+esc(tip)+'">'
      + '<div class="rs-n">'+d.n+'</div>'
      + '<div class="rs-barwrap"><div class="rs-bar" style="height:'+Math.max(2,px)+'px"></div></div>'
      + '<div class="rs-date">'+shortDate(d.date)+'</div></div>';
  });
  return '<div class="rs-chart">'+bars+'</div>'
    + '<div class="legend"><span><span class="sw" style="background:var(--accent)"></span>reschedulings/day</span></div>';
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
  html += module('Schedule reliability', 'reschedulings',
      '<div class="rs-note">Bars = tails reassigned each day vs the night before. Early days are thin.</div>'
      + reschedChart(d.reliability)
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
