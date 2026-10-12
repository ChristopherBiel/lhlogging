"""
Preview /insights locally, on real legs, without a database.

A push to main deploys, and the dashboard cannot run here (no Postgres), so
this is the way to look at the page before it is live: it serves the page as
the dashboard builds it (tools/_preview.py), the real static files, and
/api/insights computed by the same dashboard/leg_stats.insights() from the
offline leg export. Stdlib only — app.py is read, never imported, so Flask is
not needed. ?intensity=02 shows the page at another level of Faceplate's dial.

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
import csv
import json
import sys
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _preview  # noqa: E402
from _preview import DASH, ROOT, constant as _constant  # noqa: E402
import booking_model as bm  # noqa: E402
import leg_stats  # noqa: E402

DEFAULT_CSV = ROOT / "tmp" / "leg_outcomes.csv"


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
                # built on every request, so an edit shows on reload
                html = _preview.page("/insights?" + url.query)
                return self.reply(html.encode(), "text/html; charset=utf-8")
            if url.path == "/api/insights":
                asked = (parse_qs(url.query).get("type") or [""])[0]
                return self.reply(json.dumps(payload(asked)).encode(), "application/json")
            found = _preview.asset(url.path)
            if found:
                return self.reply(*found)
            self.reply(b"not in the preview", "text/plain", 404)

    print(f"{len(legs)} legs, as of {now:%Y-%m-%d %H:%M} UTC")
    print(f"http://127.0.0.1:{args.port}/insights   (Ctrl-C to stop)")
    try:
        ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
