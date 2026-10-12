"""
Preview any page of the dashboard locally, on the live site's data.

A push to main deploys, and the dashboard cannot run here (no Postgres). This
serves every page as the dashboard builds it (tools/_preview.py) with the
static files of this checkout, and passes whatever a page fetches (/api/…,
/book/world.json) on to the live site, read-only: so a changed page shows with
real rows before it is deployed. Stdlib only — app.py is read, never imported,
so Flask is not needed. Nothing but GET is passed on.

The monitor and /insights have previews of their own, which compute the
payload here from local rows and so also show a changed query or statistic:
tools/preview_monitor.py, tools/preview_insights.py.

?intensity=02 shows a page at another level of Faceplate's dial (01 to 04).

Usage:
    python3 tools/preview_site.py                 # http://127.0.0.1:8790/schedule
    python3 tools/preview_site.py --live https://lhlogging.biels.net --port 8790
"""
import argparse
import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
import _preview  # noqa: E402

PASSED_ON = ("/api/", "/book/world.json")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--live", default="https://lhlogging.biels.net", help="the site whose data the pages show")
    ap.add_argument("--port", type=int, default=8790)
    args = ap.parse_args()
    live = args.live.rstrip("/")

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
            path = urlparse(self.path).path
            if path.startswith(PASSED_ON):
                try:
                    with urllib.request.urlopen(live + self.path, timeout=60) as r:
                        return self.reply(r.read(), r.headers.get("Content-Type", "application/json"))
                except urllib.error.HTTPError as e:
                    return self.reply(e.read(), e.headers.get("Content-Type", "text/plain"), e.code)
                except OSError as e:
                    return self.reply(str(e).encode(), "text/plain", 502)
            html = _preview.page(self.path)        # built on every request, so an edit shows on reload
            if html is not None:
                return self.reply(html.encode(), "text/html; charset=utf-8")
            found = _preview.asset(path)
            if found:
                return self.reply(*found)
            self.reply(b"not in the preview", "text/plain", 404)

    print(f"pages from this checkout, data from {live}")
    print(f"http://127.0.0.1:{args.port}/schedule   (Ctrl-C to stop)")
    try:
        ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
