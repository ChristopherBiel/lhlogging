"""
The page chassis every route shares.

Faceplate builds every page on one chassis: a sheet of twelve columns with a
top bar (the project's glyph and name, the pages, a stamp), the lead, the
sections and a footer that carries the signature. chassis.html is LH·Fleet's
copy of it — a real file, so `faceplate check` reads the markup, and the one
place the site's intensity is declared — and page() fills it.

Pure and stdlib-only: the dashboard imports it, and so do the preview tools,
which run without Flask.
"""
import html
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
STATIC = HERE / "static"

# The pages of the top bar, in order: (path, name).
PAGES = (("/", "Monitor"), ("/schedule", "Schedule"), ("/book", "Book"),
         ("/insights", "Insights"), ("/fleet", "Fleet DB"))

# What /faceplate/<name> hands out: the vendored files the pages link, by the
# folder they sit in. The package carries far more (LaTeX, KiCad, the logo
# set); none of that is served.
_FONTS = ("manrope-variable.woff2", "ibm-plex-mono-400.woff2", "ibm-plex-mono-500.woff2",
          "ibm-plex-mono-600.woff2", "OFL-Manrope.txt", "OFL-IBMPlexMono.txt")
ASSETS = {"faceplate.tokens.css": "dist", "faceplate.components.css": "dist", "fonts.css": ".",
          **{"fonts/" + f: "." for f in _FONTS}}


def asset(name):
    """The vendored file behind /faceplate/<name>, or None if it is not served."""
    folder = ASSETS.get(name)
    return HERE / folder / name if folder else None


def _inline(path, cls):
    """An SVG file as inline markup: drawn with currentColor, it takes the
    colour of whatever it stands on."""
    src = re.sub(r"<!--.*?-->", "", path.read_text(encoding="utf-8"), flags=re.S)
    head, body = re.search(r"<svg\b([^>]*)>(.*)</svg>", src, re.S).groups()
    keep = re.findall(r'(?<![\w-])(?:viewBox|fill|stroke|stroke-width|stroke-linecap|stroke-linejoin)="[^"]*"', head)
    if cls:
        keep.insert(0, 'class="%s"' % cls)
    return '<svg %s aria-hidden="true">%s</svg>' % (" ".join(keep), " ".join(body.split()))


_SHELL = (HERE / "chassis.html").read_text(encoding="utf-8")
# the project's glyph, drawn to the icon spec, and the mark of the signature
_GLYPH = _inline(STATIC / "icons" / "lh-fleet.svg", "fp-top__glyph")
_MARK = _inline(HERE / "dist" / "assets" / "logo" / "mark.svg", None)


def _version(files):
    """The newest mtime among a page's files, for their URLs: a browser must
    never lay this week's markup over last week's stylesheet."""
    try:
        return int(max(f.stat().st_mtime for f in files))
    except OSError:
        return 0


def page(title, current, main, styles=(), scripts=(), lang="en"):
    """One page as HTML: `main` (the lead and the sections) between the top bar
    and the footer. `current` is the path of the page's entry in the top bar;
    `styles` and `scripts` are file names under static/css and static/js, in
    the order they load."""
    css = ["app.css", *styles]
    v = _version([HERE / "chassis.html", *(HERE / d / n for n, d in ASSETS.items() if n.endswith(".css")),
                  *(STATIC / "css" / f for f in css), *(STATIC / "js" / f for f in scripts)])
    slots = {
        "lang": lang,
        "title": html.escape(title),
        "v": str(v),
        "styles": "\n".join('<link rel="stylesheet" href="/static/css/%s?v=%d">' % (f, v) for f in styles),
        "glyph": _GLYPH,
        "nav": "".join('<a href="%s"%s>%s</a>' % (path, ' aria-current="page"' if path == current else "", name)
                       for path, name in PAGES),
        "main": main.strip("\n"),
        "mark": _MARK,
        "scripts": "\n".join('<script src="/static/js/%s?v=%d"></script>' % (f, v) for f in scripts),
    }
    return re.sub(r"\{\{(\w+)\}\}", lambda m: slots[m.group(1)], _SHELL)
