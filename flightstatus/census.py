"""
Census — which flight numbers to ask the feed for that the catalog has never
seen.

The catalog (fis_flight_catalog) learns a number once a widebody has been seen
on it: from the ADS-B log after it flew, from its even/odd sibling, or from a
known flight's `previousFlight`. All three look back, so a flight new to the
timetable enters the sweeps about when it first operates — and at a change of
timetable (the last Sunday of March and of October) a whole season's worth of
new numbers does. The feed, though, lists a flight eight days ahead to whoever
asks for its number. Lufthansa's long-haul numbers sit in one block (128 of
the 138 widebody numbers of summer 2026 are LH400-799, 97% of the legs; the
rest are the Frankfurt-Munich shuttles), so the census asks for the numbers of
that block the catalog does not sweep, a slice a day. A widebody found on one
is an observation like any other, and the next sweep's catalog refresh picks
the number up from there (fetch_flightstatus.refresh_catalog, source A).

Stdlib only, nothing here touches a database or a browser: it loads in the
collector image and in the test runner alike (as legs.py does).
"""
from __future__ import annotations


def parse_block(spec):
    """'400-799' or '90-125,400-799' -> the numbers, sorted, each once. A part
    that does not parse is left out."""
    out = set()
    for part in (spec or "").split(","):
        lo, dash, hi = part.strip().partition("-")
        hi = hi if dash else lo
        if lo.isdigit() and hi.isdigit():
            out.update(range(int(lo), int(hi) + 1))
    return sorted(out)


def census_slice(block, known, day, cycle):
    """The numbers of `block` to ask for on `day` (a date): those not in
    `known` (the numbers the catalog sweeps, as str or int), one `cycle`-th of
    them — number n on the days where (day + n) divides by `cycle`.

    Every number comes round once in `cycle` days. With the default of six,
    the weekday it is asked about moves one back each round, so a flight that
    only runs on Thursdays is met within six rounds and a daily one in the
    first. cycle = 1 asks for the whole block."""
    known = {int(k) for k in known if str(k).isdigit()}
    cycle = max(int(cycle), 1)
    return [n for n in block if n not in known and (day.toordinal() + n) % cycle == 0]
