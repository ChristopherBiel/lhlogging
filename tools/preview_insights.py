"""
Preview /insights locally, on real legs, without a database.

A push to main deploys, and the dashboard cannot run here (no Postgres), so
this is the way to look at the page before it is live: it serves the page
shell out of dashboard/app.py, the real static files, and /api/insights
computed by the same dashboard/leg_stats.insights() from the offline leg
export. Stdlib only — app.py is read, never imported, so Flask is not needed.

"Now" is the newest look in the export, so the page shows what the dashboard
showed when the data was pulled.

Input: tmp/leg_outcomes.csv from tools/build_leg_outcomes.py.

Usage:
    ./tools/pull_fis_history.sh
    python3 tools/build_leg_outcomes.py --since 2026-07-21
    python3 tools/preview_insights.py            # http://127.0.0.1:8791/insights
    python3 tools/preview_insights.py --port 9000 --dump tmp/insights   # also write the payloads
"""
import argparse
import ast
import csv
import json
import sys
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
sys.path.insert(0, str(DASH))
import booking_model as bm  # noqa: E402
import leg_stats  # noqa: E402

DEFAULT_CSV = ROOT / "tmp" / "leg_outcomes.csv"
TYPES = {".css": "text/css", ".js": "text/javascript", ".woff2": "font/woff2",
         ".svg": "image/svg+xml"}


def _constant(name):
    """A module-level literal out of dashboard/app.py, without importing it."""
    for node in ast.parse((DASH / "app.py").read_text()).body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == name:
            return ast.literal_eval(node.value)
    raise SystemExit(f"{name} not found in dashboard/app.py")


def load(path):
    """Leg dicts shaped like dashboard/app.py's _leg_from_row."""
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"No leg export at {path} — run tools/build_leg_outcomes.py first.")
    when = lambda s: datetime.fromisoformat(s) if s else None
    legs = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            legs.append({
                "flight_date": date.fromisoformat(r["flight_date"]), "airline": "LH",
                "flight_number": r["flight_number"], "dep": r["dep_airport"], "arr": r["arr_airport"],
                "dep_utc": when(r["dep_scheduled_utc"]), "fleet_type": r["fleet_type"] or "",
                "duration_min": int(r["duration_min"]) if r.get("duration_min") else None,
                "segments": json.loads(r["segments"]) if r.get("segments") else None,
                "seat_config": r.get("seat_config") or None, "allegris": r.get("allegris") == "1",
                "truth_tail": r["truth_tail"] or "", "cancelled": r["cancelled"] == "1",
                "latest_status": r.get("latest_status") or "",
                "latest_observed_at": when(r.get("latest_observed_at")),
                "first_lead_h": float(r["first_lead_h"]) if r["first_lead_h"] else None,
                "timeline": json.loads(r["timeline"] or "[]"),
            })
    return legs


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(DEFAULT_CSV))
    ap.add_argument("--port", type=int, default=8791)
    ap.add_argument("--dump", metavar="DIR", help="also write each family's payload as JSON")
    args = ap.parse_args()

    legs = load(args.csv)
    now = max(l["latest_observed_at"] for l in legs if l["latest_observed_at"])
    families = _constant("_INSIGHT_FAMILIES")
    # what the dashboard's model cache holds: every fleet, the model's weeks
    hist = [l for l in legs if l["dep_utc"] and now - timedelta(weeks=bm.HISTORY_WEEKS) <= l["dep_utc"] < now]

    def payload(asked):
        asked = (asked or "B748").upper()
        family = next((f for f, m in families.items() if asked == f or asked in m), "B748")
        mine = [l for l in legs if l["fleet_type"] in families[family]]
        return leg_stats.insights(mine, hist, families, family, now)

    if args.dump:
        out = Path(args.dump)
        out.mkdir(parents=True, exist_ok=True)
        for family in families:
            (out / f"{family}.json").write_text(json.dumps(payload(family)))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def reply(self, body, ctype, code=200):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path in ("/", "/insights"):
                # read on every request, so an edit shows on reload
                html = _constant("_INSIGHTS_HTML").replace("{{ v }}", "preview")
                return self.reply(html.encode(), "text/html; charset=utf-8")
            if url.path == "/api/insights":
                asked = (parse_qs(url.query).get("type") or [""])[0]
                return self.reply(json.dumps(payload(asked)).encode(), "application/json")
            f = None
            if url.path.startswith("/static/"):
                f = (DASH / url.path.lstrip("/")).resolve()
            elif url.path.startswith("/faceplate/"):
                f = DASH / "dist" / Path(url.path).name
            elif url.path == "/favicon.svg":
                f = DASH / "static" / "favicon.svg"
            if f and f.is_file() and DASH in f.parents:
                return self.reply(f.read_bytes(), TYPES.get(f.suffix, "application/octet-stream"))
            self.reply(b"not in the preview", "text/plain", 404)

    print(f"{len(legs)} legs, as of {now:%Y-%m-%d %H:%M} UTC")
    print(f"http://127.0.0.1:{args.port}/insights   (Ctrl-C to stop)")
    try:
        ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
