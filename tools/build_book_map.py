#!/usr/bin/env python3
"""
Regenerate the two map constants inlined in dashboard/app.py for /book map mode:

  _WORLD_OUTLINE   land + country outlines as one SVG path
  _AIRPORT_LL      IATA -> (lat, lon)

Both are inlined rather than shipped as files because dashboard/Dockerfile
copies app.py and nothing else (same reason _FACEPLATE_CSS lives there), and
because a self-hosted basemap is what keeps the page from calling a tile
server — the Datenschutz page promises no third party sees a visitor.

Sources, both public domain:
  Natural Earth 1:110m admin_0_countries  (coastlines + borders)
  OurAirports airports.csv                (the same source tools/load_airports.py
                                           uses; the `airports` table is keyed by
                                           ICAO and has no IATA column to join on)

The output is equirectangular by construction: x = longitude, y = -latitude, so
the page needs no projection maths and pan/zoom is just the SVG viewBox.

Usage:
    ./tools/build_book_map.py [--out consts.py] [--tol 0.35] [--min-area 1.2]

Then paste the two blocks over the existing constants in dashboard/app.py.
Downloads both sources to tmp/ on first run and reuses them afterwards.
"""
import argparse
import csv
import json
import math
import sys
import textwrap
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TMP = ROOT / "tmp"

NE_URL = ("https://raw.githubusercontent.com/nvkelso/natural-earth-vector/"
          "master/geojson/ne_110m_admin_0_countries.geojson")
OA_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"

# No scheduled service below this, and equirectangular smears the pole across
# the whole canvas — clip it away rather than spend pixels on it.
LAT_FLOOR = -58.0


def fetch(url, name):
    TMP.mkdir(exist_ok=True)
    path = TMP / name
    if not path.exists():
        print("downloading %s -> %s" % (url, path), file=sys.stderr)
        urllib.request.urlretrieve(url, path)
    return path


# ── geometry ──────────────────────────────────────────────────────────────
def _perp(p, a, b):
    (x, y), (x1, y1), (x2, y2) = p, a, b
    dx, dy = x2 - x1, y2 - y1
    if dx == 0 and dy == 0:
        return math.hypot(x - x1, y - y1)
    t = max(0.0, min(1.0, ((x - x1) * dx + (y - y1) * dy) / (dx * dx + dy * dy)))
    return math.hypot(x - (x1 + t * dx), y - (y1 + t * dy))


def simplify(pts, tol):
    """Douglas-Peucker in degrees — planar is fine at this scale."""
    if len(pts) < 3:
        return pts
    dmax, idx = 0.0, 0
    for i in range(1, len(pts) - 1):
        d = _perp(pts[i], pts[0], pts[-1])
        if d > dmax:
            dmax, idx = d, i
    if dmax <= tol:
        return [pts[0], pts[-1]]
    return simplify(pts[:idx + 1], tol)[:-1] + simplify(pts[idx:], tol)


def ring_area(pts):
    """Shoelace area in square degrees — drops islands too small to read."""
    s = 0.0
    for i in range(len(pts)):
        x1, y1 = pts[i]
        x2, y2 = pts[(i + 1) % len(pts)]
        s += x1 * y2 - x2 * y1
    return abs(s) / 2


def build_world(geojson, tol, min_area):
    d = json.load(open(geojson))
    out = []
    for f in d["features"]:
        if f["properties"].get("ADMIN") == "Antarctica":
            continue
        geom = f["geometry"]
        polys = ([geom["coordinates"]] if geom["type"] == "Polygon"
                 else geom["coordinates"])
        for poly in polys:
            for ring in poly:
                pts = [(float(x), float(y)) for x, y in ring]
                if ring_area(pts) < min_area:
                    continue
                pts = [(x, max(y, LAT_FLOOR)) for x, y in simplify(pts, tol)]
                if len(pts) < 3:
                    continue
                out.append("M" + "L".join("%g,%g" % (round(x, 2), round(-y, 2))
                                          for x, y in pts) + "Z")
    return "".join(out)


# ── airports ──────────────────────────────────────────────────────────────
def build_airports(csv_path, extra):
    """Every large airport with an IATA code, plus `extra` (codes the FIS
    network reaches that OurAirports classes smaller). Generous on purpose:
    a new destination gets a marker without a code change."""
    rows = {}
    with open(csv_path, newline="") as fh:
        for r in csv.DictReader(fh):
            ia = (r["iata_code"] or "").strip().upper()
            if len(ia) != 3:
                continue
            if not (r["type"] == "large_airport" or ia in extra):
                continue
            rank = (r["type"] == "large_airport", r["scheduled_service"] == "yes")
            if ia not in rows or rank > rows[ia][0]:
                rows[ia] = (rank, round(float(r["latitude_deg"]), 3),
                            round(float(r["longitude_deg"]), 3))
    return {k: (v[1], v[2]) for k, v in rows.items()}


def emit(world, airports):
    o = ['_WORLD_OUTLINE = (']
    for ln in textwrap.wrap(world, 92, drop_whitespace=False, break_on_hyphens=False):
        o.append('    "%s"' % ln)
    o.append(')\n\n')
    o.append('_AIRPORT_LL = {')
    line = "   "
    for ia in sorted(airports):
        lat, lon = airports[ia]
        add = " %r: (%g, %g)," % (ia, lat, lon)
        if len(line) + len(add) > 96:
            o.append(line)
            line = "   " + add
        else:
            line += add
    o.append(line)
    o.append("}")
    return "\n".join(o)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tol", type=float, default=0.35,
                    help="Douglas-Peucker tolerance in degrees (default 0.35)")
    ap.add_argument("--min-area", type=float, default=1.2,
                    help="drop rings smaller than this, sq degrees (default 1.2)")
    ap.add_argument("--out", help="write here instead of stdout")
    ap.add_argument("--extra", default="",
                    help="comma-separated IATA codes to force in (non-large fields)")
    a = ap.parse_args()

    ne = fetch(NE_URL, "ne_110m_countries.geojson")
    oa = fetch(OA_URL, "ourairports.csv")

    world = build_world(ne, a.tol, a.min_area)
    extra = {c.strip().upper() for c in a.extra.split(",") if c.strip()}
    airports = build_airports(oa, extra)
    text = emit(world, airports)

    print("world outline: %.1f KB | airports: %d"
          % (len(world) / 1024, len(airports)), file=sys.stderr)
    missing = sorted(extra - set(airports))
    if missing:
        print("WARNING: not found in OurAirports: %s" % ", ".join(missing),
              file=sys.stderr)
    if a.out:
        Path(a.out).write_text(text)
        print("wrote %s" % a.out, file=sys.stderr)
    else:
        print(text)


if __name__ == "__main__":
    main()
