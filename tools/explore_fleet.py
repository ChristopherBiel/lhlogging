"""
Poke at any aircraft or route in a flights CSV export (tools/pull_data.sh) —
the ad-hoc queries from the rotation debugging, made reusable. Routes are
resolved via the callsign reference, so legs the detector left as UNKN still
show their real destination.

Usage:
    python3 tools/explore_fleet.py --reg D-ABYN          # one tail's flight log + route mix
    python3 tools/explore_fleet.py --route EDDF-FAOR     # who flies a route, and when
    python3 tools/explore_fleet.py --type B748 --routes  # route frequency for a type
"""
import argparse
import collections
from datetime import date

import _lhdata as lh


def show_reg(rows, cmap, reg):
    flights = sorted((r for r in rows if r["registration"] == reg), key=lambda r: r["first_seen"])
    if not flights:
        raise SystemExit(f"{reg} not found.")
    print(f"== {reg}: {len(flights)} flights ==")
    routes = collections.Counter()
    for r in flights:
        dep, arr = lh.canon_route(r, cmap)
        dep, arr = dep or "?", arr or "?"
        routes[f"{dep}-{arr}"] += 1
        print(f"  {r['flight_date']}  {dep:5}->{arr:5}  {(r['callsign'] or '').strip():8}"
              f"  {r['first_seen'][11:16]}")
    print("\n  route frequency:")
    for rt, c in routes.most_common(15):
        print(f"    {rt:12} {c}")


def show_route(rows, cmap, route):
    dep_t, arr_t = route.upper().split("-")
    hits = []
    for r in rows:
        dep, arr = lh.canon_route(r, cmap)
        if dep == dep_t and arr == arr_t:
            hits.append(r)
    hits.sort(key=lambda r: r["flight_date"], reverse=True)
    print(f"== {route}: {len(hits)} flights ==")
    tails = collections.Counter(r["registration"] for r in hits)
    for r in hits[:40]:
        print(f"  {r['flight_date']}  {r['registration']:8}  {(r['callsign'] or '').strip():8}")
    if len(hits) > 40:
        print(f"  ... ({len(hits) - 40} more)")
    print("\n  by tail:", dict(tails.most_common()))


def show_type_routes(rows, cmap, ac_type):
    routes = collections.Counter()
    for r in rows:
        if r["aircraft_type"] != ac_type:
            continue
        dep, arr = lh.canon_route(r, cmap)
        routes[f"{dep or '?'}-{arr or '?'}"] += 1
    print(f"== {ac_type}: top routes ==")
    for rt, c in routes.most_common(30):
        print(f"  {rt:12} {c}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default=str(lh.DEFAULT_CSV))
    ap.add_argument("--reg", help="show one registration's flight log + route mix")
    ap.add_argument("--route", help="show a route's history (ICAO, e.g. EDDF-FAOR)")
    ap.add_argument("--type", dest="ac_type", help="with --routes: aircraft type to summarize")
    ap.add_argument("--routes", action="store_true", help="route frequency for --type")
    args = ap.parse_args()

    rows = lh.load(args.csv)
    cmap = lh.build_callsign_routes(rows)

    if args.reg:
        show_reg(rows, cmap, args.reg)
    elif args.route:
        show_route(rows, cmap, args.route)
    elif args.routes and args.ac_type:
        show_type_routes(rows, cmap, args.ac_type)
    else:
        ap.error("give one of: --reg REG | --route DEP-ARR | --type T --routes")


if __name__ == "__main__":
    main()
