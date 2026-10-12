"""
What the preview tools share: the pages of the dashboard, built as the
dashboard builds them but without importing it (so neither Flask nor a database
is needed), and the files those pages link.

A page is chassis.page() around the spec that dashboard/app.py names for its
route in _PAGES; both are literals and are read with ast. For review only,
?intensity=02 shows a page at another level of Faceplate's dial (01 to 04);
the site's own level is the one attribute in dashboard/chassis.html.
"""
import ast
import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
sys.path.insert(0, str(DASH))
import chassis  # noqa: E402

TYPES = {".css": "text/css", ".js": "text/javascript", ".woff2": "font/woff2", ".svg": "image/svg+xml",
         ".txt": "text/plain; charset=utf-8", ".json": "application/json"}
_parsed = [0.0, None]


def constant(name):
    """A module-level literal out of dashboard/app.py, without importing it."""
    src = DASH / "app.py"
    if src.stat().st_mtime != _parsed[0]:              # read again after an edit, so a reload shows it
        _parsed[:] = [src.stat().st_mtime, ast.parse(src.read_text())]
    for node in _parsed[1].body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == name:
            return ast.literal_eval(node.value)
    raise SystemExit(f"{name} not found in dashboard/app.py")


def page(url):
    """The page at this URL as HTML, or None if no route of _PAGES matches."""
    url = urlparse(url)
    for route, spec in constant("_PAGES").items():
        if re.fullmatch(re.sub(r"<\w+>", "[^/]+", route), url.path):
            html = chassis.page(**constant(spec))
            level = parse_qs(url.query).get("intensity", [""])[0]
            if re.fullmatch(r"0[1-4]", level):
                html = re.sub(r'data-fp-intensity="\d+"', 'data-fp-intensity="%s"' % level, html)
            return html
    return None


def asset(path):
    """(bytes, content type) of a file a page links, or None: the static
    folder, the favicon and what the dashboard serves under /faceplate/."""
    f = None
    if path.startswith("/static/"):
        f = (DASH / path.lstrip("/")).resolve()
        if DASH / "static" not in f.parents:
            f = None
    elif path.startswith("/faceplate/"):
        f = chassis.asset(path[len("/faceplate/"):])
    elif path == "/favicon.svg":
        f = DASH / "static" / "favicon.svg"
    if f and f.is_file():
        return f.read_bytes(), TYPES.get(f.suffix, "application/octet-stream")
    return None
