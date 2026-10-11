"""
The monitor: how the site runs, and the big picture of what it has logged.

Pure functions over query rows, like leg_stats.py — the dashboard runs QUERIES
and hands the rows to monitor(); tools/preview_monitor.py does the same with
rows it pulled into CSV files, so the page can be looked at without a database.

Two halves:

  the collector   which runs read which flight days for which fleets (the plan
                  below mirrors flightstatus/crontab; tests/test_monitor_stats
                  fails when the two drift apart), what each run of the last
                  days actually brought back, and what that leaves known about
                  every day ahead;
  the data        the hubs' day (departures and arrivals by quarter-hour) and
                  how hard each aircraft is worked — from the ADS-B flights,
                  the only source that covers the whole fleet.

All clock times are Europe/Berlin: the collector's cron runs on it, and so do
the hubs.
"""
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from statistics import median
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")

# ── the collector's plan (flightstatus/crontab, fetch_flightstatus.py) ──────
# The two tiers the plan tells apart. A flight number in neither (the chained
# short-haul legs) gets the near window only and is not drawn.
TIERS = (
    ("deep", "747-8 · A380", ("B748", "A388")),
    ("wide", "787 · A350", ("B788", "B789", "B78X", "A359", "A35K")),
)
TIER_NAME = {t[0]: t[1] for t in TIERS}
# the fleet families of /insights, as the page names them
FLEET_NAME = {"B748": "747-8", "A388": "A380", "B787": "787", "A350": "A350"}
# kind of pass -> the flight days it reads, per tier, counted from its own day
READS = {
    "pulse": {"deep": (1, 2)},
    "probe": {"deep": (1, 2, 5, 6, 7, 8, 9)},
    "watch": {"deep": (0,), "wide": (0,)},
    "far": {"deep": (1, 2, 3, 4), "wide": (3, 4)},
    "lite": {"deep": (-2, -1, 0, 1, 2), "wide": (-2, -1, 0, 1, 2)},
    "full": {"deep": (-2, -1, 0, 1, 2, 3), "wide": (-2, -1, 0, 1, 2, 3)},
}
KIND_NAME = {"pulse": "pulse", "probe": "pulse and probe", "watch": "watch",
             "far": "far pass", "lite": "morning sweep", "full": "evening sweep"}
# the 15 slots of a day, Berlin time
SLOTS = (
    ("00:10", "pulse"), ("02:00", "pulse"), ("02:45", "watch"), ("03:30", "far"),
    ("05:45", "lite"), ("09:00", "pulse"), ("10:30", "watch"), ("11:35", "pulse"),
    ("13:00", "watch"), ("14:10", "pulse"), ("15:30", "watch"), ("16:45", "pulse"),
    ("18:15", "full"), ("21:15", "watch"), ("22:00", "probe"),
)
# each start is delayed by a random wait of up to this many minutes
JITTER_MIN = {"pulse": 10, "probe": 10, "watch": 15, "far": 30, "lite": 30, "full": 30}
# how long a pass of each kind takes when nothing is known yet (minutes)
TYPICAL_MIN = {"pulse": 11, "probe": 45, "watch": 6, "far": 60, "lite": 85, "full": 100}
FIRST_LEAD, LAST_LEAD = -2, 9

# a run that has not started this long after its latest possible start was skipped
GRACE = timedelta(minutes=20)
# a run still marked running after this was killed (a deploy rebuilds the container)
RUNNING_MAX = timedelta(hours=4)
READS_BACK = timedelta(hours=48)
READS_AHEAD = timedelta(hours=24)
RECORD_DAYS = 28
# a fleet's day counts as known when this share of its usual flights has a
# tail, and as complete from the second on (a fleet's days differ: the A380
# flies 10 to 12 legs depending on the weekday)
KNOWN_SHARE = 0.5
FULL_SHARE = 0.75
HUBS = (("EDDF", "FRA", "Frankfurt"), ("EDDM", "MUC", "Munich"))
HUB_DAYS = 56
BINS = 96                                    # quarter-hours of a day
USE_DAYS = 28
LONG_HAUL = ("A332", "A333", "A343", "A346", "A359", "A35K", "A388",
             "B744", "B748", "B788", "B789", "B78X", "B77W", "B772")

# ── what the dashboard asks the database ───────────────────────────────────
# No parameters, so tools/preview_monitor.py can send the same text to psql.
# Column names are what the functions below read.
QUERIES = {
    "runs": """
        SELECT id, status, started_at, finished_at, aircraft_total AS total,
               aircraft_ok AS found, aircraft_error AS blocked, error_detail AS detail
        FROM batch_runs
        WHERE run_type = 'flightstatus' AND started_at >= NOW() - INTERVAL '30 days'
        ORDER BY started_at""",
    # one row per (run, flight day, aircraft type): what a run asked for and got
    "lookups": """
        SELECT o.run_id, o.flight_date, COALESCE(a.aircraft_type, o.seed_type) AS type,
               count(*) AS asked, count(*) FILTER (WHERE o.found) AS found,
               min(o.observed_at) AS first_at, max(o.observed_at) AS last_at
        FROM flight_status_observations o
        LEFT JOIN (
            SELECT DISTINCT ON (registration) registration, aircraft_type
            FROM aircraft
            ORDER BY registration, is_active DESC, last_seen_date DESC NULLS LAST
        ) a ON a.registration = btrim(o.registration)
        WHERE o.observed_at >= NOW() - INTERVAL '4 days' AND o.run_id IS NOT NULL
        GROUP BY 1, 2, 3""",
    # flights with a tail, per fleet type and flight day
    "known": """
        SELECT flight_date, fleet_type AS type, count(*) AS legs,
               max(latest_observed_at) AS newest
        FROM fis_legs
        WHERE flight_date BETWEEN CURRENT_DATE - 32 AND CURRENT_DATE + 10
          AND NOT COALESCE(cancelled, FALSE)
          AND COALESCE(NULLIF(truth_tail, ''), NULLIF(latest_tail, '')) IS NOT NULL
        GROUP BY 1, 2""",
    "collectors": """
        SELECT DISTINCT ON (run_type) run_type, status, started_at, finished_at,
               aircraft_total AS total, aircraft_ok AS found, aircraft_error AS blocked,
               flights_upserted AS stored, error_detail AS detail
        FROM batch_runs
        WHERE started_at >= NOW() - INTERVAL '30 days'
        ORDER BY run_type, started_at DESC""",
    "ledger": """
        SELECT (SELECT count(*) FROM flights
                 WHERE departure_airport_icao IS NOT NULL AND NOT needs_review) AS flights,
               (SELECT min(first_seen) FROM flights) AS flights_since,
               (SELECT count(*) FROM flights
                 WHERE first_seen >= NOW() - INTERVAL '7 days'
                   AND departure_airport_icao IS NOT NULL AND NOT needs_review) AS flights_week,
               (SELECT count(*) FROM aircraft WHERE is_active) AS aircraft,
               (SELECT count(*) FROM fis_legs) AS legs,
               (SELECT min(flight_date) FROM fis_legs) AS legs_since,
               (SELECT count(*) FROM flight_status_observations) AS lookups,
               (SELECT min(observed_at) FROM flight_status_observations) AS lookups_since,
               (SELECT count(DISTINCT icao24) FROM positions
                 WHERE on_ground = FALSE
                   AND captured_at = (SELECT max(captured_at) FROM positions)) AS airborne,
               (SELECT max(captured_at) FROM positions) AS positions_at,
               pg_size_pretty(pg_database_size(current_database())) AS db_size""",
    # movements at the hubs by local quarter-hour; a flight that left and
    # landed at the same field is a detection artefact, not a movement
    "hubs": """
        SELECT hub, kind, t::date AS day,
               EXTRACT(HOUR FROM t)::int * 4 + EXTRACT(MINUTE FROM t)::int / 15 AS bin,
               long, count(*) AS n
        FROM (
            SELECT f.departure_airport_icao AS hub, 'dep' AS kind,
                   f.first_seen AT TIME ZONE 'Europe/Berlin' AS t,
                   a.aircraft_type IN ({long}) AS long
            FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
            WHERE f.departure_airport_icao IN ({hubs}) AND NOT f.needs_review
              AND f.departure_airport_icao IS DISTINCT FROM f.arrival_airport_icao
              AND f.first_seen >= NOW() - INTERVAL '{days} days'
            UNION ALL
            SELECT f.arrival_airport_icao, 'arr',
                   f.last_seen AT TIME ZONE 'Europe/Berlin',
                   a.aircraft_type IN ({long})
            FROM flights f JOIN aircraft a ON a.icao24 = f.icao24
            WHERE f.arrival_airport_icao IN ({hubs}) AND NOT f.needs_review
              AND f.departure_airport_icao IS DISTINCT FROM f.arrival_airport_icao
              AND f.last_seen >= NOW() - INTERVAL '{days} days'
        ) m
        GROUP BY 1, 2, 3, 4, 5""".format(
            long=", ".join("'%s'" % t for t in LONG_HAUL),
            hubs=", ".join("'%s'" % h[0] for h in HUBS), days=HUB_DAYS + 2),
    # every active aircraft and what it flew in the last full days
    "use": """
        SELECT a.icao24, btrim(a.registration) AS registration, a.aircraft_type AS type,
               count(f.id) AS sectors, COALESCE(sum(f.duration_minutes), 0) AS minutes
        FROM aircraft a
        LEFT JOIN flights f ON f.icao24 = a.icao24 AND NOT f.needs_review
             AND f.arrival_airport_icao IS NOT NULL
             AND f.duration_minutes BETWEEN 15 AND 1200
             AND f.first_seen >= date_trunc('day', NOW()) - INTERVAL '{days} days'
             AND f.first_seen < date_trunc('day', NOW())
        WHERE a.is_active
        GROUP BY 1, 2, 3""".format(days=USE_DAYS),
}


# ── the plan ───────────────────────────────────────────────────────────────
def tier_of(type_):
    for tid, _name, types in TIERS:
        if type_ in types:
            return tid
    return None


def _at(day, hhmm):
    h, m = hhmm.split(":")
    return datetime.combine(day, time(int(h), int(m)), tzinfo=BERLIN)


def slots_between(start, end):
    """Every planned slot whose nominal start lies in [start, end)."""
    out = []
    day = start.astimezone(BERLIN).date()
    while day <= end.astimezone(BERLIN).date():
        for i, (hhmm, kind) in enumerate(SLOTS):
            at = _at(day, hhmm)
            if start <= at < end:
                out.append({"at": at, "kind": kind, "slot": i})
        day += timedelta(days=1)
    return out


def reads_per_day():
    """{tier: {lead day: planned reads a day}}."""
    out = {tid: defaultdict(int) for tid, _n, _t in TIERS}
    for _hhmm, kind in SLOTS:
        for tid, leads in READS[kind].items():
            for lead in leads:
                out[tid][lead] += 1
    return {tid: dict(v) for tid, v in out.items()}


def readers(tier, lead):
    """The slots (indices into SLOTS) that read this tier's day `lead`."""
    return [i for i, (_h, kind) in enumerate(SLOTS) if lead in READS[kind].get(tier, ())]


def horizon(tier):
    return max(lead for _h, kind in SLOTS for lead in READS[kind].get(tier, (0,)))


# ── runs against the plan ──────────────────────────────────────────────────
def outcome(run, now):
    """ok | cut (stopped early, or a tenth of it blocked) | failed (nothing came
    back) | running | lost (never finished) | skipped (no run for the slot)."""
    if run is None:
        return "skipped"
    if run["status"] == "running":
        return "running" if now - run["started_at"] < RUNNING_MAX else "lost"
    total, found, blocked = run["total"] or 0, run["found"] or 0, run["blocked"] or 0
    if run["status"] == "error" or (total and not found):
        return "failed"
    if run["status"] == "blocked" or (blocked >= 5 and blocked >= 0.1 * total):
        return "cut"
    return "ok"


BAD = ("failed", "cut", "lost", "skipped")


def match(runs, start, end, now):
    """The planned slots in [start, end) with the run that served each, and
    the runs no slot accounts for (started by hand).

    A slot starts after a random wait, so its run is the first one beginning
    between the nominal time and the longest wait. A slot still inside that
    window, or in the future, is `planned`."""
    runs = sorted((r for r in runs if r.get("started_at")), key=lambda r: r["started_at"])
    used, out = set(), []
    for s in slots_between(start, end):
        latest = s["at"] + timedelta(minutes=JITTER_MIN[s["kind"]])
        run = next((r for r in runs if r["id"] not in used
                    and s["at"] - timedelta(minutes=1) <= r["started_at"] <= latest + timedelta(minutes=5)), None)
        if run is not None:
            used.add(run["id"])
            state = outcome(run, now)
        elif now < latest + GRACE:
            state = "planned"
        else:
            state = "skipped"
        out.append(dict(s, run=run, state=state))
    extra = [r for r in runs if r["id"] not in used and start <= r["started_at"] < end]
    return out, extra


def _typical(slots):
    """Minutes a pass of each kind takes, from the runs that went through."""
    took = defaultdict(list)
    for s in slots:
        r = s["run"]
        if s["state"] == "ok" and r and r.get("finished_at"):
            took[s["kind"]].append((r["finished_at"] - r["started_at"]).total_seconds() / 60)
    return {k: (median(took[k]) if took.get(k) else TYPICAL_MIN[k]) for k in TYPICAL_MIN}


def _iso(dt):
    return dt.astimezone(BERLIN).isoformat(timespec="minutes") if dt else None


def _run_view(run):
    if run is None:
        return None
    return {"start": _iso(run["started_at"]), "end": _iso(run.get("finished_at")),
            "total": run["total"], "found": run["found"], "blocked": run["blocked"],
            "status": run["status"], "detail": run.get("detail")}


# ── Fig. 1: which flight days were read, and when ──────────────────────────
def reads(slots, lookups, now, typical=None):
    """The slots of the last two days and the next, each with the flight days
    it read per tier: `read`, `failed` (a run that went wrong asked and under
    half came back), `missed` (planned, never asked: the run stopped or did not
    start) or `planned`. A run that went through and asked nothing for a day
    had nothing left to ask (the sweeps skip flights that have landed), so
    that day is not drawn."""
    typical = typical or TYPICAL_MIN
    today = now.astimezone(BERLIN).date()
    got = defaultdict(lambda: {"asked": 0, "found": 0, "first": None, "last": None})
    for r in lookups:
        tid = tier_of(r["type"])
        if tid is None:
            continue
        g = got[(r["run_id"], tid, r["flight_date"])]
        g["asked"] += r["asked"]
        g["found"] += r["found"]
        g["first"] = min(filter(None, (g["first"], r["first_at"])))
        g["last"] = max(filter(None, (g["last"], r["last_at"])))
    lo, hi = now - READS_BACK, now + READS_AHEAD
    out = []
    for s in slots:
        if not lo <= s["at"] < hi:
            continue
        run, day0 = s["run"], s["at"].date()
        begin = run["started_at"] if run else s["at"]
        end = (run.get("finished_at") if run else None) or begin + timedelta(minutes=typical[s["kind"]])
        planned = {(tid, day0 + timedelta(days=lead))
                   for tid, leads in READS[s["kind"]].items() for lead in leads}
        mine = {(tid, d): dict(g) for (rid, tid, d), g in got.items() if run and rid == run["id"]}
        # A lookup that found nothing has no aircraft, so its tier is a guess
        # from the flight number's seed type. Where that guess falls outside
        # what this pass reads and the other tier's same day is inside, it was
        # the other tier's lookup.
        for (tid, d) in [k for k, g in mine.items() if k not in planned and not g["found"]]:
            for other, _name, _t in TIERS:
                if (other, d) in planned:
                    g, o = mine.pop((tid, d)), mine.setdefault((other, d), {"asked": 0, "found": 0, "first": None, "last": None})
                    o["asked"] += g["asked"]
                    o["first"] = min(filter(None, (o["first"], g["first"])))
                    o["last"] = max(filter(None, (o["last"], g["last"])))
                    break
        cells = []
        for tid, d in sorted(planned | set(mine)):
            g = mine.get((tid, d))
            if g:
                state = "failed" if s["state"] in BAD and g["found"] < 0.5 * g["asked"] else "read"
                x0, x1 = g["first"], g["last"]
            elif s["state"] == "ok":
                continue
            else:
                state = "planned" if s["state"] in ("planned", "running") else "missed"
                x0, x1 = begin, end
            cells.append({"tier": tid, "day": d.isoformat(), "lead": (d - day0).days, "state": state,
                          "asked": g["asked"] if g else 0, "found": g["found"] if g else 0,
                          "from": _iso(x0), "to": _iso(x1)})
        out.append({"at": _iso(s["at"]), "kind": s["kind"], "slot": s["slot"], "state": s["state"],
                    "run": _run_view(run), "cells": cells})
    per_day = reads_per_day()
    return {"from": _iso(lo), "to": _iso(hi), "now": _iso(now), "today": today.isoformat(),
            "days": [(today + timedelta(days=i)).isoformat() for i in range(FIRST_LEAD - 1, LAST_LEAD + 1)],
            "tiers": [{"id": tid, "name": name, "horizon": horizon(tid),
                       "per_day": {str(k): v for k, v in sorted(per_day[tid].items())}}
                      for tid, name, _t in TIERS],
            "slots": out}


# ── what is known about each day ahead ─────────────────────────────────────
def _due(tier, now, back=11):
    """The flight days this tier's plan should have read by now: a day is due
    once a slot that reads it lies behind us (start, longest wait, typical run)."""
    due = set()
    for s in slots_between(now - timedelta(days=back), now):
        done = s["at"] + timedelta(minutes=JITTER_MIN[s["kind"]] + TYPICAL_MIN[s["kind"]])
        if done <= now:
            for lead in READS[s["kind"]].get(tier, ()):
                due.add(s["at"].date() + timedelta(days=lead))
    return due


def coverage(known, now, families):
    """Per fleet family and flight day (two back, nine ahead): flights with a
    tail against the fleet's usual day, the newest look, and whether the plan
    says the day should have been read by now."""
    today = now.astimezone(BERLIN).date()
    legs, newest = defaultdict(int), {}
    fam_of = {t: fam for fam, members in families.items() for t in members}
    for r in known:
        fam = fam_of.get(r["type"])
        if fam is None:
            continue
        key = (fam, r["flight_date"])
        legs[key] += r["legs"]
        if r.get("newest") and (key not in newest or r["newest"] > newest[key]):
            newest[key] = r["newest"]
    days = [today + timedelta(days=i) for i in range(FIRST_LEAD, LAST_LEAD + 1)]
    out = []
    for fam, members in families.items():
        tid = tier_of(members[0])
        due = _due(tid, now) if tid else set()
        past = [legs[(fam, today - timedelta(days=i))] for i in range(3, 31)]
        past = [n for n in past if n]
        usual = median(past) if past else 0
        cells = []
        for d in days:
            n = legs[(fam, d)]
            share = min(n / usual, 1.0) if usual else 0.0
            state = ("full" if share >= FULL_SHARE else "part" if share >= KNOWN_SHARE
                     else "thin" if n else "missing" if d in due else "unread")
            cells.append({"day": d.isoformat(), "lead": (d - today).days, "legs": n, "share": round(share, 3),
                          "state": state, "due": d in due, "newest": _iso(newest.get((fam, d)))})
        ahead = [c for c in cells if c["lead"] >= 0]
        through = None
        for c in ahead:
            if c["share"] < KNOWN_SHARE:
                break
            through = c["day"]
        due_ahead = [d for d in due if d >= today]
        out.append({"id": fam, "tier": tid, "usual": usual, "cells": cells, "through": through,
                    "due_through": max(due_ahead).isoformat() if due_ahead else None})
    return out


# ── Fig. 2: the collector's record ─────────────────────────────────────────
def record(slots, extra, now, days=RECORD_DAYS):
    """Every run of the last `days` days on the clock: when it started, how
    long it took and how it ended — and per slot, how often it went wrong."""
    today = now.astimezone(BERLIN).date()
    first = today - timedelta(days=days - 1)
    idx = lambda d: (d - first).days
    minute = lambda dt: dt.astimezone(BERLIN).hour * 60 + dt.astimezone(BERLIN).minute
    runs, skipped = [], []
    stats = [{"at": hhmm, "kind": kind, "n": 0, "bad": 0, "streak": 0, "last_ok": None}
             for hhmm, kind in SLOTS]
    open_streak = [True] * len(SLOTS)

    def bar(run, state, slot):
        start = run["started_at"].astimezone(BERLIN)
        end = run.get("finished_at") or (now if state == "running" else run["started_at"])
        end = end.astimezone(BERLIN)
        last = 24 * 60 if end.date() > start.date() else minute(end)
        out = {"day": idx(start.date()), "slot": slot, "state": state,
               "s": minute(start), "e": max(last, minute(start)),
               "total": run["total"] or 0, "found": run["found"] or 0, "blocked": run["blocked"] or 0}
        if run.get("detail"):
            out["detail"] = run["detail"]
        return out

    for s in sorted(slots, key=lambda s: s["at"], reverse=True):     # newest first, for the streaks
        if not first <= s["at"].date() <= today or s["state"] in ("planned", "running"):
            if s["state"] == "running" and s["run"]:
                runs.append(bar(s["run"], "running", s["slot"]))
            continue
        st = stats[s["slot"]]
        st["n"] += 1
        if s["state"] in BAD:
            st["bad"] += 1
            if open_streak[s["slot"]]:
                st["streak"] += 1
        else:
            open_streak[s["slot"]] = False
            if st["last_ok"] is None:
                st["last_ok"] = s["at"].date().isoformat()
        if s["run"]:
            runs.append(bar(s["run"], s["state"], s["slot"]))
        else:
            skipped.append({"day": idx(s["at"].date()), "slot": s["slot"],
                            "s": s["at"].hour * 60 + s["at"].minute})
    for r in extra:
        if first <= r["started_at"].astimezone(BERLIN).date() <= today:
            runs.append(bar(r, outcome(r, now), -1))
    runs = sorted((r for r in runs if 0 <= r["day"] < days), key=lambda r: (r["day"], r["s"]))
    per_day = [{"total": 0, "found": 0, "blocked": 0} for _ in range(days)]
    for r in runs:
        for k in per_day[r["day"]]:
            per_day[r["day"]][k] += r[k]
    return {"first": first.isoformat(), "days": days, "slots": stats, "runs": runs,
            "skipped": skipped, "per_day": per_day, "now": minute(now)}


# ── the status, in sentences ───────────────────────────────────────────────
def _join(words):
    words = list(words)
    return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " and " + words[-1]


def _day(d):
    if isinstance(d, str):
        d = date.fromisoformat(d)
    return "%s %d %s" % (d.strftime("%a"), d.day, d.strftime("%b"))


def _ago(delta):
    m = int(delta.total_seconds() // 60)
    if m < 90:
        return "%d min" % m
    if m < 48 * 60:
        return "%d h" % round(m / 60)
    return "%d days" % round(m / 1440)


def headline(slots, rec, cov, now):
    """What a visitor should know first, as sentences: [{level, parts}], where
    parts alternate plain strings and [bold] ones. Levels: ok, warn, bad."""
    out = []
    done = [s for s in slots if s["run"] and s["state"] not in ("planned", "running")]
    done.sort(key=lambda s: s["run"]["started_at"])
    good = [s for s in done if s["state"] == "ok"]
    if not done:
        out.append({"level": "bad", "parts": ["No run of the schedule reader in the last days."]})
    else:
        last = done[-1]
        when = last["run"]["started_at"].astimezone(BERLIN)
        if last["state"] == "ok":
            age = now - last["run"]["started_at"]
            out.append({"level": "ok" if age < timedelta(hours=4) else "warn", "parts": [
                "Schedule last read ", [_ago(age) + " ago"],
                ": the %s %s, %d of %d found." % (when.strftime("%H:%M"), KIND_NAME[last["kind"]],
                                                   last["run"]["found"] or 0, last["run"]["total"] or 0)]})
        else:
            parts = ["The latest read, the %s %s, " % (when.strftime("%H:%M"), KIND_NAME[last["kind"]]),
                     ["failed" if last["state"] != "cut" else "stopped early"], "."]
            if good:
                g = good[-1]["run"]
                parts.append(" The last complete one started %s ago." % _ago(now - g["started_at"]))
            out.append({"level": "bad", "parts": parts})

    for s in slots:
        if s["state"] == "running":
            out[0]["parts"].append(" The %s %s is running." % (s["at"].strftime("%H:%M"), KIND_NAME[s["kind"]]))

    # how far ahead each fleet's plan is known
    by_through = defaultdict(list)
    for c in cov:
        by_through[(c["through"], c["due_through"])].append(c)
    for (through, due), fams in by_through.items():
        who = _join(FLEET_NAME.get(c["id"], c["id"]) for c in fams)
        if through is None:
            out.append({"level": "bad", "parts": [[who], ": no flight with a tail today."]})
            continue
        short = (date.fromisoformat(due) - date.fromisoformat(through)).days if due else 0
        # days the plan reads but the pages cannot show yet are not a gap
        missing = [x for c in fams for x in c["cells"] if x["state"] == "missing" and x["day"] > through]
        if short > 0 and missing:
            out.append({"level": "bad", "parts": [
                [who], ": plan known through ", [_day(through)], ", ",
                ["%d day%s short" % (short, "" if short == 1 else "s")],
                " of what should have been read by now."]})
        else:
            out.append({"level": "ok", "parts": [[who], ": plan known through ", [_day(through)], "."]})

    # slots that keep failing, and the days nothing else reads
    failing = [i for i, st in enumerate(rec["slots"]) if st["streak"] >= 2]
    if failing:
        streaks = {rec["slots"][i]["streak"] for i in failing}
        label = lambda i: "%s %s" % (SLOTS[i][0], KIND_NAME[SLOTS[i][1]])
        parts = ["The ", [_join(label(i) for i in failing)],
                 " %s failed %s%d days running." % ("has" if len(failing) == 1 else "have",
                                                    "" if len(streaks) == 1 else "up to ", max(streaks))]
        # a lost day only matters where the plan reads nothing further out
        for tid, _name, _t in TIERS:
            lead = horizon(tid)
            who = readers(tid, lead)
            if who and set(who) <= set(failing):
                fleets = _join(TIER_NAME[tid].split(" · "))
                parts.append(" %s day %d for the %s." % (
                    "It is the only read of" if len(failing) == 1
                    else "The %s is the only read of" % label(who[0]) if len(who) == 1
                    else "Nothing else reads", lead, fleets))
        out.append({"level": "bad", "parts": parts})
    elif not any(o["level"] == "bad" for o in out):
        day = [s for s in slots if now - timedelta(hours=24) <= s["at"] <= now and s["state"] != "planned"]
        bad = [s for s in day if s["state"] in BAD]
        out.append({"level": "ok" if not bad else "warn", "parts": [
            "%d of %d runs in the last 24 hours went through." % (len(day) - len(bad), len(day))]})
    return out


# what the other collectors do, and how stale each may get before it is flagged
COLLECTORS = (
    ("flightstatus", "Schedule reader", "Lufthansa flight status: which tail is planned on which flight",
     "15 runs a day", timedelta(hours=4)),
    ("state_poller", "Position poller", "ADS-B positions of the whole fleet", "every 2 min", timedelta(minutes=10)),
    ("flight_detector", "Flight detector", "turns positions into flights", "twice an hour", timedelta(minutes=45)),
    ("fleet_discovery", "Fleet discovery", "finds aircraft new to the fleet", "twice an hour", timedelta(hours=7)),
    ("fleet_refresh", "Fleet refresh", "re-reads every aircraft's registry entry", "Mondays", timedelta(days=8)),
)


def collectors(rows, now):
    by = {r["run_type"]: r for r in rows}
    out = []
    for key, name, does, cadence, stale in COLLECTORS:
        r = by.get(key)
        if r is None:
            out.append({"id": key, "name": name, "does": does, "cadence": cadence, "state": "bad",
                        "last": None, "result": "no run in 30 days"})
            continue
        age = now - r["started_at"]
        if key == "flightstatus":
            state = outcome(dict(r, id=0), now)
            state = "ok" if state in ("ok", "running") else "bad"
            result = "%d of %d found" % (r["found"] or 0, r["total"] or 0)
            if r["blocked"]:
                result += ", %d blocked" % r["blocked"]
        else:
            state = "ok" if r["status"] in ("ok", "running") else "bad"
            result = {"state_poller": "%d seen, %d stored" % (r["found"] or 0, r["stored"] or 0),
                      "flight_detector": "%d flights updated" % (r["stored"] or 0),
                      "fleet_discovery": "%d new" % (r["found"] or 0),
                      "fleet_refresh": "%d of %d updated" % (r["found"] or 0, r["total"] or 0)}[key]
        if r["status"] == "running":
            # the counts are written when a run ends; until then they say nothing
            result = ("running since %s" % r["started_at"].astimezone(BERLIN).strftime("%H:%M")
                      if state == "ok" else "never finished")
        elif r["status"] != "ok":
            result = r["detail"] or r["status"]
        if state == "ok" and age > stale:
            state = "stale"
        out.append({"id": key, "name": name, "does": does, "cadence": cadence, "state": state,
                    "last": _iso(r["started_at"]), "ago": _ago(age), "result": result})
    return out


def ledger(row, now):
    if not row:
        return None
    out = dict(row)
    for k in ("flights_since", "lookups_since", "positions_at"):
        out[k] = _iso(out.get(k))
    if out.get("legs_since"):
        out["legs_since"] = out["legs_since"].isoformat()
    return out


# ── Fig. 3: the hubs' day ──────────────────────────────────────────────────
def _quantile(values, q):
    v = sorted(values)
    if not v:
        return 0
    i = q * (len(v) - 1)
    lo = int(i)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (i - lo)


def hubs(rows, now, days=HUB_DAYS):
    """Per hub: departures by day and quarter-hour, the day's profile (median
    and the band between the 10th and 90th percentile of a quarter-hour, for
    departures and arrivals, and the long-haul part of each), and the day's
    totals. The current day is drawn but left out of the profile."""
    today = now.astimezone(BERLIN).date()
    first = today - timedelta(days=days)
    span = days + 1
    out = []
    for icao, iata, name in HUBS:
        grid = {k: [[0] * BINS for _ in range(span)] for k in ("dep", "arr", "dep_long", "arr_long")}
        for r in rows:
            i = (r["day"] - first).days
            if r["hub"] != icao or not 0 <= i < span or not 0 <= r["bin"] < BINS:
                continue
            grid[r["kind"]][i][r["bin"]] += r["n"]
            if r["long"]:
                grid[r["kind"] + "_long"][i][r["bin"]] += r["n"]
        full = range(span - 1)                                      # every day but today
        profile = {}
        for k in grid:
            cols = [[grid[k][i][b] for i in full] for b in range(BINS)]
            profile[k] = {"med": [round(median(c), 1) if c else 0 for c in cols],
                          "lo": [round(_quantile(c, 0.1), 1) for c in cols],
                          "hi": [round(_quantile(c, 0.9), 1) for c in cols]}
        totals = {k: [sum(grid[k][i]) for i in range(span)] for k in ("dep", "arr")}
        deps = [totals["dep"][i] for i in full]
        busiest = max(range(BINS), key=lambda b: profile["dep"]["med"][b])
        active = [b for b in range(BINS) if profile["dep"]["hi"][b] > 0]
        low = min(full, key=lambda i: totals["dep"][i]) if deps else 0
        high = max(full, key=lambda i: totals["dep"][i]) if deps else 0
        out.append({"id": iata, "name": name, "first": first.isoformat(), "days": span,
                    "dep": grid["dep"], "profile": profile, "totals": totals,
                    "median": median(deps) if deps else 0,
                    "long_share": round(sum(map(sum, grid["dep_long"][:span - 1])) / max(sum(deps), 1), 3),
                    "busiest": busiest, "open": [min(active), max(active)] if active else None,
                    "low": {"day": (first + timedelta(days=low)).isoformat(), "n": totals["dep"][low]},
                    "high": {"day": (first + timedelta(days=high)).isoformat(), "n": totals["dep"][high]}})
    return {"hubs": out, "bins": BINS}


# ── Fig. 4: how hard each aircraft is worked ───────────────────────────────
def use(rows, families, days=USE_DAYS, min_type=4):
    """Every active aircraft as sectors a day against flight hours a day, and
    each type's median aircraft. Types of fewer than `min_type` aircraft are
    kept as points but not named."""
    fam_of = {t: fam for fam, members in families.items() for t in members}
    points, idle = [], []
    by_type = defaultdict(list)
    for r in rows:
        t = r["type"] or ""
        p = {"reg": r["registration"] or r["icao24"], "icao24": r["icao24"], "type": t,
             "family": fam_of.get(t), "sectors": round(r["sectors"] / days, 3),
             "hours": round(float(r["minutes"]) / 60 / days, 3)}
        if not r["sectors"]:
            idle.append(p)
            continue
        points.append(p)
        by_type[t].append(p)
    types = []
    for t, ps in by_type.items():
        if not t or len(ps) < min_type:
            continue
        types.append({"type": t, "family": fam_of.get(t), "n": len(ps),
                      "sectors": round(median(p["sectors"] for p in ps), 2),
                      "hours": round(median(p["hours"] for p in ps), 2)})
    types.sort(key=lambda x: -x["hours"])
    points.sort(key=lambda p: (p["family"] is not None, p["hours"]))
    return {"days": days, "points": points, "types": types,
            "idle": sorted(idle, key=lambda p: (p["type"], p["reg"]))}


# ── the page's payload ─────────────────────────────────────────────────────
def collector(rows, now, families):
    """The status and the two collector figures (cheap; refreshed every minute)."""
    start = now - timedelta(days=RECORD_DAYS + 1)
    slots, extra = match(rows["runs"], start, now + READS_AHEAD, now)
    rec = record(slots, extra, now)
    cov = coverage(rows["known"], now, families)
    return {"headline": headline(slots, rec, cov, now),
            "collectors": collectors(rows["collectors"], now),
            "ledger": ledger(rows["ledger"][0] if rows["ledger"] else None, now),
            "reads": reads(slots, rows["lookups"], now, _typical(slots)),
            "coverage": cov, "record": rec}


def data(rows, now, families):
    """The two figures on what was flown (heavier; refreshed every quarter-hour)."""
    return {"hubs": hubs(rows["hubs"], now), "use": use(rows["use"], families)}
