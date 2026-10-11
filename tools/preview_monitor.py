"""
Preview the monitor (the start page) locally, on real rows, without a database.

A push to main deploys, and the dashboard cannot run here (no Postgres), so
this is the way to look at the page before it is live. `--pull` sends the very
queries the dashboard runs (dashboard/monitor_stats.QUERIES) to the production
database, read-only, and keeps the rows in tmp/monitor/; without it the page is
served from the rows of the last pull. The payload comes from the same
monitor_stats functions the dashboard calls. Stdlib only — app.py is read,
never imported, so Flask is not needed.

"Now" is the moment of the pull, so the page shows what the dashboard showed
then.

Configure the pull like tools/pull_data.sh, via environment or the gitignored
.env.local: LHLOGGING_SSH, LHLOGGING_REMOTE_DIR, [LHLOGGING_DB_USER],
[LHLOGGING_DB_NAME].

Usage:
    python3 tools/preview_monitor.py --pull      # fetch, then serve
    python3 tools/preview_monitor.py             # http://127.0.0.1:8792/
    python3 tools/preview_monitor.py --dump tmp/monitor/payload.json --no-serve
"""
import argparse
import ast
import csv
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
sys.path.insert(0, str(DASH))
import monitor_stats  # noqa: E402

DATA = ROOT / "tmp" / "monitor"
TYPES = {".css": "text/css", ".js": "text/javascript", ".woff2": "font/woff2",
         ".svg": "image/svg+xml"}
# columns that stay text although they can look like a number or a flag
TEXT = {"icao24", "registration", "type", "run_type", "status", "detail", "hub", "kind", "db_size"}
FLAGS = {"long"}
STAMP = re.compile(r"^(\d{4}-\d\d-\d\d) (\d\d:\d\d:\d\d)(\.\d+)?([+-]\d\d)$")


def _constant(name):
    """A module-level literal out of dashboard/app.py, without importing it."""
    for node in ast.parse((DASH / "app.py").read_text()).body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == name:
            return ast.literal_eval(node.value)
    raise SystemExit(f"{name} not found in dashboard/app.py")


def _env():
    """LHLOGGING_* from the environment, else from .env.local."""
    env = dict(os.environ)
    local = ROOT / ".env.local"
    if local.exists():
        for line in local.read_text().splitlines():
            m = re.match(r"\s*(?:export\s+)?(LHLOGGING_\w+)=(.*)$", line)
            if m:
                env.setdefault(m.group(1), m.group(2).strip().strip("'\""))
    for need in ("LHLOGGING_SSH", "LHLOGGING_REMOTE_DIR"):
        if not env.get(need):
            raise SystemExit(f"set {need} — see the header of tools/pull_data.sh")
    return env


def pull():
    """Run every monitor query on the server, read-only, into tmp/monitor/."""
    env = _env()
    remote = ("cd %s && docker compose exec -T db psql -U %s -d %s -X -q -v ON_ERROR_STOP=1" % (
        env["LHLOGGING_REMOTE_DIR"], env.get("LHLOGGING_DB_USER", "lhlogging"),
        env.get("LHLOGGING_DB_NAME", "lhlogging")))
    DATA.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc)
    for name, sql in monitor_stats.QUERIES.items():
        done = subprocess.run(["ssh", "-o", "BatchMode=yes", env["LHLOGGING_SSH"], remote],
                              input="COPY (%s) TO STDOUT WITH CSV HEADER" % sql,
                              capture_output=True, text=True)
        if done.returncode or done.stderr.strip():
            raise SystemExit(f"{name}: {done.stderr.strip() or 'ssh failed'}")
        (DATA / f"{name}.csv").write_text(done.stdout)
        print(f"{name}: {max(done.stdout.count(chr(10)) - 1, 0)} rows")
    (DATA / "pulled_at.txt").write_text(stamp.isoformat())


def _value(key, s):
    """A CSV cell as psycopg would have handed it over."""
    if s == "":
        return None
    if key in FLAGS:
        return s == "t"
    if key in TEXT:
        return s
    m = STAMP.match(s)
    if m:
        return datetime.fromisoformat("%sT%s%s:00" % (m.group(1), m.group(2), m.group(4)))
    if re.fullmatch(r"\d{4}-\d\d-\d\d", s):
        return date.fromisoformat(s)
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    if re.fullmatch(r"-?\d+\.\d+", s):
        return float(s)
    return s


def load():
    stamp = DATA / "pulled_at.txt"
    if not stamp.exists():
        raise SystemExit(f"No rows in {DATA} — run with --pull first.")
    rows = {}
    for name in monitor_stats.QUERIES:
        with open(DATA / f"{name}.csv", newline="") as f:
            rows[name] = [{k: _value(k, v) for k, v in r.items()} for r in csv.DictReader(f)]
    return rows, datetime.fromisoformat(stamp.read_text().strip())


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pull", action="store_true", help="fetch fresh rows from the server first")
    ap.add_argument("--port", type=int, default=8792)
    ap.add_argument("--dump", metavar="FILE", help="also write the payload as JSON")
    ap.add_argument("--no-serve", action="store_true")
    args = ap.parse_args()

    if args.pull:
        pull()
    rows, now = load()
    families = _constant("_INSIGHT_FAMILIES")
    payload = dict(monitor_stats.collector(rows, now, families), **monitor_stats.data(rows, now, families))
    payload["generated"] = now.isoformat()
    body = json.dumps(payload).encode()
    if args.dump:
        Path(args.dump).write_bytes(body)
    if args.no_serve:
        return

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def reply(self, data, ctype, code=200):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/":
                # read on every request, so an edit shows on reload
                html = _constant("_HTML").replace("{{ v }}", "preview")
                return self.reply(html.encode(), "text/html; charset=utf-8")
            if url.path == "/api/monitor":
                return self.reply(body, "application/json")
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

    print(f"rows as of {now:%Y-%m-%d %H:%M} UTC, {len(body) // 1024} kB payload")
    print(f"http://127.0.0.1:{args.port}/   (Ctrl-C to stop)")
    try:
        ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
