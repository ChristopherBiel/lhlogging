"""
The read plan — which flight days the schedule reader asks Lufthansa's feed
for, and in which order (2026-11-01).

Until then the collector ran fifteen passes a day, each reading a fixed set of
days (the sweeps everything near, the pulses the 747-8/A380 one and two days
out, the far pass further): ~2,270 lookups a day. Since 27 October the feed has
been shut most of the day (door.py), and those passes could not finish. This
plan asks for about a third of that, and for the most valuable first. It runs
every hour (crontab): each run asks for what has fallen due since the last.

Pools. Tails swap almost only within their pool (93-100% of the changes,
12-26 Oct 2026), so the fleet is read pool by pool, and a pool is read
completely or not at all — what the fleet simulation and the hold rates need:

    pool            tails  legs a day  level   read to
    747-8             17       21      close     D+9
    A380               8       10      close     D+9
    A350 Allegris     10       14      near      D+9
    787               20       31      near      D+4
    A350 Munich       16       24      near      D+4
    A350 Frankfurt     5        6      off        -

A tail's pool comes from its type, its cabin (Allegris) and the hub it flies
from (pool_of). A flight day is read by the highest of its tail's pool and the
pools that fly its number (a third of its legs or more, number_pools), so a
747-8 on an A350 number is read as a 747-8, and an A350 on a 747-8 number too.

Levels. Every flight day of a pool is read once a day out to the day the pool
is read to ("day": the data stays whole, and the pages can trust a look for
36 hours). On top:
    near   a read within 12 h and within 3 h of a hub departure: the tail it
           leaves with is the one that flies, 99.3-100% instead of 95-98.5%
    close  near, and every 6 h one to three days out: a change then shows
           within hours (747-8: dated to 5.4 h instead of 24 h, 90% seen
           within 3.3 h instead of 18 h), the timing the model learns from
(Scored by replaying the October looks, docs/flightstatus_schedule.md, "The
read plan".)

Return legs. A flight's previousFlight is the airframe's flight before it. At an
outstation that is the outbound that brought it there, and the two carry the
same tail (99.8%): the outbound's daily and 6-hourly reads are its return
leg's (`covers`). Only its first read, its near reads and its flown tail ask
for the outbound itself.

Watched tails (FIS_WATCH_TAILS, and every widebody the feed shows in a special
livery) are read every 6 h from today to D+3, in any pool, and within 12 h and
3 h of a hub departure. A tail ADS-B has not seen fly for 36 h is likely to
lose its flights (a change within 24 h 36% of the time against 18%): its
flights get the 6-hourly reads too.

The flown tail comes from ADS-B (fis_legs: the callsign DLH<number> finds it on
91% of the 747-8/A380 legs and 83% of the others, never a wrong one). A flight
ADS-B did not see is asked for after it has landed ("flown").

When the feed lets a run read only part of what is due, the order is (CLASS):
    0  near reads of every pool and of the watched tails, and where a watched
       tail went after a swap (a run's own follow-ups)
    1  the 747-8 and A380 daily reads (D0..D+9), and flights not read yet
    2  their 6-hourly reads, the watched and the idle tails'
    3  A350 Allegris, to D+4
    4  787 and A350 Munich, to D+2
    5  flown tails ADS-B did not see (the feed keeps a few days of the past)
    6  the far days: Allegris D+5..D+9, 787 and A350 Munich D+3..D+4
    7  flight numbers no pool has flown lately
inside a class the nearest day first, then the earliest flight. About 350
lookups a day keep the 747-8 and the A380 complete. Until 2026-11-05 the near
reads of the other pools came in their pool's class; while the feed let a few
hundred lookups a day through, the 747-8/A380 days to D+9 then took them all,
and a 787 leaving in 3 h was not read.

Stdlib only, nothing here touches a database or a browser: the collector hands
in rows (fetch_flightstatus.run_due), the tests their own.
"""
from __future__ import annotations

from collections import Counter, defaultdict, namedtuple
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")
# a departure from a German hub is where a tail can still be swapped (mirrors
# legs.DE_HUBS); a flight leaving anywhere else names its outbound
HUBS = frozenset({"FRA", "MUC", "DUS", "BER", "HAM", "STR", "CGN", "NUE", "LEJ", "TXL"})
TERMINAL = ("ARRIVED", "DIVERTED", "CANCELLED")

OFF, NEAR, CLOSE = 0, 1, 2
LEVELS = {"off": OFF, "near": NEAR, "close": CLOSE}

# name, level, read to D+days, rank (who goes first), the days that count as
# near for the order (beyond them: a far day, class FAR)
Pool = namedtuple("Pool", "name level days rank near_days")
POOLS = {
    "b748": Pool("747-8", CLOSE, 9, 0, 9),
    "a388": Pool("A380", CLOSE, 9, 0, 9),
    "a350-allegris": Pool("A350 Allegris", NEAR, 9, 1, 4),
    "b787": Pool("787", NEAR, 4, 2, 2),
    "a350-muc": Pool("A350 Munich", NEAR, 4, 2, 2),
    "a350-fra": Pool("A350 Frankfurt", OFF, 0, 3, 0),
}
# the fleet type a read is logged with when no tail is known yet (the
# observation's seed_type; the monitor sorts an unanswered lookup by it)
POOL_TYPE = {"b748": "B748", "a388": "A388", "a350-allegris": "A359", "b787": "B789",
             "a350-muc": "A359", "a350-fra": "A359"}

# the order (see above)
NEAR_TOP, DAY_TOP, CLOSE_TOP, FIRST, SECOND, FLOWN, FAR, FIND = range(8)
CLASS_NAME = ("near: every pool, watched", "daily: 747-8, A380", "6-hourly: 747-8, A380, watched, idle",
              "A350 Allegris", "787, A350 Munich", "flown", "far days", "new numbers")

DAY = timedelta(hours=24)            # every flight day of a pool: a read this often
CLOSE_EVERY = timedelta(hours=6)     # close, watched and idle tails
CLOSE_DAYS = (1, 3)                  # close: D+1..D+3
WATCH_DAYS = (0, 3)                  # watched and idle tails: today..D+3
NEAR_MARKS = (timedelta(hours=3), timedelta(hours=12))   # latest mark first
# what falls due this soon is read now, so that no run opens a session for it a
# few minutes later; a near mark counts a read that early as its own
SLACK = timedelta(minutes=30)
# The feed lists a flight from about nine days before it leaves; asked earlier,
# it says "no such flight" for a flight it lists later that day (14% of the
# day-9 lookups at 22:00). A day nine out is asked for once the number's
# usual departure is that close.
FEED_REACH = timedelta(days=9) - timedelta(minutes=30)
FLOWN_AFTER = timedelta(hours=3)     # after the arrival: ADS-B has logged it by then
FLOWN_RETRY = timedelta(hours=12)
FLOWN_UNTIL = timedelta(hours=72)    # the feed keeps a few days of the past
SHARE = 0.3                          # a number belongs to every pool flying this share of it,
MIN_LEGS = 3                         # if it flew this often (the feed's days and ADS-B's together):
                                     # a charter or ferry flown once (LH9913) is no flight of a pool
FIND_DAYS = (1, 3)                   # a number in no pool: the nearest day of these not answered
FOLLOW_UPS = 3                       # reads after a watched tail moved


def configure(spec, pools=None):
    """'a350-fra=near:4,b787=near:9' -> POOLS with those levels and days; a
    part that does not parse is left out. The order (rank) stays."""
    out = dict(pools or POOLS)
    for part in (spec or "").split(","):
        pid, _, rule = part.strip().partition("=")
        level, _, days = rule.partition(":")
        if pid in out and level in LEVELS:
            out[pid] = out[pid]._replace(
                level=LEVELS[level], days=0 if level == "off" else int(days) if days.isdigit() else out[pid].days)
    return out


def pool_of(fleet_type, allegris=False, hub=None):
    """A tail's pool, from its type (and an A350's cabin and hub); None for a
    type no pool reads."""
    t = (fleet_type or "").strip().upper()
    if t == "B748":
        return "b748"
    if t == "A388":
        return "a388"
    if t in ("B788", "B789", "B78X"):
        return "b787"
    if t in ("A359", "A35K"):
        if allegris:
            return "a350-allegris"
        return "a350-fra" if hub == "FRA" else "a350-muc"
    return None


def berlin_day(t):
    return t.astimezone(BERLIN).date()


def midnight(d):
    """The start of Berlin day `d`, as an aware UTC datetime."""
    return datetime(d.year, d.month, d.day, tzinfo=BERLIN).astimezone(timezone.utc)


def number_pools(counts, pools=POOLS):
    """{pool: share} of the pools that flew a number's legs lately (counts:
    {pool or None: legs}), the pools with SHARE or more of them; none for a
    number flown fewer than MIN_LEGS times."""
    total = sum(counts.values())
    if total < MIN_LEGS:
        return {}
    return {p: n / total for p, n in counts.items() if p in pools and n / total >= SHARE}


def governing(candidates, pools=POOLS):
    """The pool a flight day is read by: of the candidates, the highest level,
    then the furthest day, then the most important. None if there is none."""
    cands = [p for p in candidates if p in pools]
    if not cands:
        return None
    return max(cands, key=lambda p: (pools[p].level, pools[p].days, -pools[p].rank))


def class_of(why, pool, lead, pools=POOLS):
    """The CLASS of a reason to read a flight day `lead` days ahead that
    `pool` reads (None: a watched tail no pool reads, or a new number)."""
    if why == "flown":
        return FLOWN
    if why == "find":
        return FIND
    if why in ("12h", "3h", "watch-near", "swap"):
        return NEAR_TOP
    if why == "watch":
        return CLOSE_TOP
    p = pools[pool]
    if p.rank == 0:
        return {"6h": CLOSE_TOP, "idle": CLOSE_TOP}.get(why, DAY_TOP)
    if why in ("day", "new") and lead > p.near_days:
        return FAR
    return FIRST if p.rank == 1 else SECOND


def plan(now, days, tails, numbers, catalog=(), watched=(), idle=(), types=None, pools=POOLS):
    """What a run asks the feed for, in order, and what fell due.

    now      aware datetime
    days     {(number, date): flight day}: what the feed has told us about the
             flight days from three days back to nine ahead, each a dict of
               answered_at  the newest answer (a flight, or "no such flight")
               absent       that answer is "no such flight", and is trusted
               found_at     the newest look that found it, and from that look:
                            tail, dep, arr, dep_utc, arr_utc (aware UTC),
                            status, prev ((number, date) of its previousFlight
                            when that is a Lufthansa flight)
               truth        the tail that flew it (fis_legs: the feed or ADS-B)
               cancelled
             (a flight day the feed only named as a previousFlight has none)
    tails    {tail: pool}; types: {tail: fleet type}, for the logged seed_type
    numbers  {number: {pool or None: legs}}: who flew each number lately
    catalog  the catalog's numbers: one no pool has flown lately is asked about
             once a day, so a new flight finds its pool
    watched  tails read every 6 h; idle: tails ADS-B has not seen fly for 36 h

    Returns (work, due): work = [item], an item {flight_number, flight_date,
    seed_type, cls, why, due, pool, lead, covers}, `covers` the outbound days
    its read stands in for; due = {pool: {why: flight days}}, each flight day
    that fell due by its first reason (what the run is asked to read)."""
    today = berlin_day(now)
    soon = now + SLACK
    watched, idle, types = set(watched), set(idle), types or {}
    npools = {n: number_pools(c, pools) for n, c in numbers.items()}

    # the return leg standing in for each outbound: a flight leaving an
    # outstation names the flight that brought its airframe there
    follower = {}
    for key, fd in days.items():
        prev = fd.get("prev")
        if prev and fd.get("found_at") and fd.get("dep") not in HUBS and not fd.get("absent"):
            if prev not in follower or fd["found_at"] > days[follower[prev]]["found_at"]:
                follower[prev] = key
    # a flight named as an airframe's previous one that the feed was never
    # asked about: the airframe's tail is its tail (the chain a read follows)
    named = {}
    for key, fd in days.items():
        prev = fd.get("prev")
        if (prev and prev not in days and fd.get("found_at") and fd.get("tail")
                and 0 <= (prev[1] - today).days <= 9):
            named[prev] = {"tail": fd["tail"]}
    days = {**named, **days}
    # how long after its Berlin midnight a number's flight leaves, by its
    # newest day with a known departure (for the day nine out)
    offset = {}
    for (n, d), fd in sorted(days.items(), key=lambda kv: kv[0][1]):
        if fd.get("dep_utc"):
            offset[n] = fd["dep_utc"] - midnight(d)

    # every flight day a pool reads: the known ones, and each day ahead of a
    # number a pool flies, which the feed may not have been asked about yet
    keys = set(days)
    for n, ps in npools.items():
        g = governing(ps, pools)
        if g is not None and pools[g].level != OFF:
            keys.update((n, today + timedelta(days=i)) for i in range(pools[g].days + 1))

    reasons = defaultdict(list)       # target -> [(cls, at, why, pool, flight day, seed, departure)]

    def due_now(target, why, at, pool, key, seed, dep):
        if at <= soon:
            lead = (key[1] - today).days
            reasons[target].append((class_of(why, pool, lead, pools), at, why, pool, key, seed, dep))

    for key in sorted(keys):
        n, d = key
        lead = (d - today).days
        fd = days.get(key) or {}
        tail = fd.get("tail")
        tpool = tails.get(tail) if tail else None
        pool = governing([tpool, *npools.get(n, {})], pools)
        level = pools[pool].level if pool else OFF
        horizon = pools[pool].days if level != OFF else -1
        is_watched = tail in watched
        if level == OFF and not is_watched:
            continue
        seed = types.get(tail) or POOL_TYPE.get(pool)
        T = fd.get("dep_utc")

        # flown: the tail that flew it, where neither the feed nor ADS-B has it
        if lead < 0 or (T is not None and now >= T):
            if (not fd.get("found_at") or fd.get("truth") or fd.get("cancelled") or T is None
                    or (fd.get("status") or "").upper() in TERMINAL or now > T + FLOWN_UNTIL):
                continue
            at = (fd.get("arr_utc") or T + timedelta(hours=12)) + FLOWN_AFTER
            if fd.get("answered_at"):
                at = max(at, fd["answered_at"] + FLOWN_RETRY)
            due_now(key, "flown", at, pool, key, seed, T)
            continue

        if lead > max(horizon, WATCH_DAYS[1] if is_watched else -1):
            continue
        if fd.get("absent") or (fd.get("status") or "").upper() == "CANCELLED":
            continue

        # not asked about yet: ask once the feed lists it
        if not fd.get("answered_at"):
            if lead > horizon:
                continue
            at = now
            if lead >= 9:
                if n not in offset:
                    continue
                at = midnight(d) + offset[n] - FEED_REACH
            due_now(key, "new", at, pool, key, seed, T)
            continue

        own = fd["answered_at"]
        f = follower.get(key)
        cover = max(own, days[f]["found_at"]) if f else own
        # the return leg reads for it unless it lies beyond what the pool reads
        stand_in = f if f and (f[1] - today).days <= max(horizon, 0) else None
        later = []
        if level != OFF:
            later.append(("day", cover + DAY))
            if level == CLOSE and CLOSE_DAYS[0] <= lead <= CLOSE_DAYS[1]:
                later.append(("6h", cover + CLOSE_EVERY))
        if WATCH_DAYS[0] <= lead <= WATCH_DAYS[1]:
            if is_watched:
                later.append(("watch", cover + CLOSE_EVERY))
            elif tail in idle and level != OFF:
                later.append(("idle", cover + CLOSE_EVERY))
        for why, at in later:
            due_now(stand_in or key, why, at, pool, key, seed, T)

        # near: the tail it leaves a hub with, from its own read
        if T is not None and fd.get("dep") in HUBS:
            for mark in NEAR_MARKS:
                at = T - mark
                if at <= soon and own < at - SLACK:
                    why = "watch-near" if is_watched and level != CLOSE else "%dh" % (mark // timedelta(hours=1))
                    due_now(key, why, at, pool, key, seed, T)
                    break

    # numbers no pool has flown lately: the nearest unanswered day, once a day
    asked = defaultdict(list)
    for (n, _d), fd in days.items():
        if fd.get("answered_at"):
            asked[n].append(fd["answered_at"])
    for n in sorted(set(catalog) - set(numbers)):
        if asked.get(n) and max(asked[n]) > now - DAY:
            continue
        for i in range(FIND_DAYS[0], FIND_DAYS[1] + 1):
            key = (n, today + timedelta(days=i))
            if not (days.get(key) or {}).get("answered_at"):
                reasons[key].append((FIND, now, "find", None, key, None, None))
                break

    work, due = [], defaultdict(Counter)
    counted = set()
    for target, rs in reasons.items():
        rs.sort(key=lambda r: (r[0], r[1]))
        cls, at, why, pool, _key, seed, _dep = rs[0]
        covers = sorted({r[4] for r in rs if r[4] != target})
        for r in rs:
            if r[4] not in counted:        # a flight day counts once, by its first reason
                counted.add(r[4])
                due[r[3] or "-"][r[2]] += 1
        own_seed = next((r[5] for r in rs if r[4] == target), seed)
        work.append({"flight_number": target[0], "flight_date": target[1], "seed_type": own_seed,
                     "cls": cls, "why": why, "due": at, "pool": pool, "lead": (target[1] - today).days,
                     "covers": covers,
                     "_at": min((r[6] or r[1]) for r in rs)})
    work.sort(key=lambda it: (it["cls"], abs(it["lead"]), it["lead"] < 0, it["_at"]))
    for it in work:
        del it["_at"]
    return work, {p: dict(c) for p, c in due.items()}


def follow_ups(days, tail, done, today, limit=FOLLOW_UPS):
    """After a read showed a watched tail leave or take a flight: the flight
    days the tail that took its place there is shown on (the other side of the
    swap, if it is one: a tail that moved is found again this way about half
    the time). `done` the flight days this run has read. Today..D+3, the
    nearest first."""
    out = []
    for key in sorted(days, key=lambda k: (k[1], days[k].get("dep_utc") or midnight(k[1]))):
        fd = days[key]
        if (fd.get("tail") == tail and key not in done and not fd.get("absent")
                and WATCH_DAYS[0] <= (key[1] - today).days <= WATCH_DAYS[1]):
            out.append(key)
            if len(out) == limit:
                break
    return out
