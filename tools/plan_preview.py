"""
What the read plan would ask the feed for (flightstatus/plan.py), on production
rows, without the collector's container.

The collector runs `fetch_flightstatus.py --due` every hour. Its dry run needs
Python inside the production container, which is not for tools to start; this
sends the plan's own queries (fetch_flightstatus.DUE_SQL) to the production
database instead, read-only, and runs the planner here on what comes back —
the same code, the same inputs. Stdlib only: fetch_flightstatus is loaded with
its database and browser libraries stubbed out (as the tests load it).

    python3 tools/plan_preview.py               # what the next run would read
    python3 tools/plan_preview.py --days 3      # ...and three days of hourly runs, as if
                                                # the feed were always open: lookups a day
    python3 tools/plan_preview.py --rows tmp/plan/rows.json --days 3   # again, without pulling

The replay assumes every read finds the tail it found last time, a day not
asked about yet runs if it ran a week before, and ADS-B sees the flown tail
of seven legs in eight (the share measured in October): it counts what the
plan asks for, not what the feed would answer.

Configure the pull like tools/pull_data.sh: LHLOGGING_SSH, LHLOGGING_REMOTE_DIR
(environment or the gitignored .env.local).
"""
import argparse
import csv
import io
import json
import os
import re
import subprocess
import sys
import types
import zlib
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "flightstatus"))
OUT = ROOT / "tmp" / "plan"


def _collector():
    """fetch_flightstatus without psycopg and playwright (its SQL and its pure
    functions are all that is used here)."""
    stubs = {"psycopg": types.ModuleType("psycopg"), "psycopg.errors": types.ModuleType("psycopg.errors"),
             "psycopg.types": types.ModuleType("psycopg.types"),
             "psycopg.types.json": types.ModuleType("psycopg.types.json"),
             "playwright": types.ModuleType("playwright"),
             "playwright.sync_api": types.ModuleType("playwright.sync_api")}
    stubs["psycopg"].Connection = object
    stubs["psycopg"].errors = stubs["psycopg.errors"]
    stubs["psycopg.errors"].UndefinedTable = stubs["psycopg.errors"].UndefinedColumn = type("E", (Exception,), {})
    stubs["psycopg.types.json"].Jsonb = object
    stubs["playwright.sync_api"].sync_playwright = None
    sys.modules.update(stubs)
    os.environ.setdefault("POSTGRES_USER", "preview")
    os.environ.setdefault("POSTGRES_PASSWORD", "preview")
    import fetch_flightstatus
    return fetch_flightstatus


fs = _collector()
import plan  # noqa: E402

# the type of each column of each DUE_SQL query, in order
TYPES = {
    "days": "date str ts bool ts str str str ts ts str str str date str",
    "truth": "date str str bool",
    "tails": "str str int int ts",
    "allegris": "str",
    "numbers": "str str int",
    "catalog": "str",
    "liveries": "str",
}


def _env():
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


def _literal(v):
    if isinstance(v, date):
        return "'%s'::date" % v.isoformat()
    if isinstance(v, (list, tuple)):
        return "ARRAY[%s]::text[]" % ", ".join("'%s'" % x for x in v)
    return str(int(v))


def _ts(s):
    s = s.strip()
    if re.search(r"[+-]\d\d$", s):
        s += "00"
    for fmt in ("%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(s)


def _cell(kind, s):
    if s == "":
        return None
    return {"str": str, "int": int, "bool": lambda x: x == "t", "date": date.fromisoformat, "ts": _ts}[kind](s)


def pull(now):
    """DUE_SQL on the production database, read-only: {name: [row tuple]}."""
    env = _env()
    remote = ("cd %s && docker compose exec -T db psql -U %s -d %s -X -q -v ON_ERROR_STOP=1" % (
        env["LHLOGGING_REMOTE_DIR"], env.get("LHLOGGING_DB_USER", "lhlogging"),
        env.get("LHLOGGING_DB_NAME", "lhlogging")))
    params = fs.due_params(now)
    rows = {}
    for name, sql in fs.DUE_SQL.items():
        text = re.sub(r"%\((\w+)\)s", lambda m: _literal(params[m.group(1)]), sql).replace("%%", "%")
        done = subprocess.run(["ssh", "-o", "BatchMode=yes", env["LHLOGGING_SSH"], remote],
                              input="SET default_transaction_read_only = on; SET TIME ZONE 'UTC';\n"
                                    "COPY (%s) TO STDOUT WITH CSV" % text,
                              capture_output=True, text=True)
        if done.returncode or "ERROR" in done.stderr:
            raise SystemExit(f"{name}: {done.stderr.strip()}")
        kinds = TYPES[name].split()
        rows[name] = [tuple(_cell(k, v) for k, v in zip(kinds, r)) for r in csv.reader(io.StringIO(done.stdout))]
    return rows


def _dump(rows, now, path):
    def enc(v):
        return v.isoformat() if isinstance(v, (date, datetime)) else v
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"now": now.isoformat(),
                                "rows": {k: [[enc(v) for v in r] for r in rs] for k, rs in rows.items()}}))


def _load(path):
    raw = json.loads(Path(path).read_text())

    def dec(kind, v):
        if v is None:
            return None
        return {"date": date.fromisoformat, "ts": datetime.fromisoformat}.get(kind, lambda x: x)(v)
    rows = {k: [tuple(dec(kind, v) for kind, v in zip(TYPES[k].split(), r)) for r in rs] for k, rs in raw["rows"].items()}
    return rows, datetime.fromisoformat(raw["now"])


def show(work, due, state, limit):
    print(fs._due_summary(work, due, state))
    by = Counter((w["pool"] or "-", w["why"]) for w in work)
    for (pool, why), n in sorted(by.items()):
        print("  %-15s %-10s %4d" % (plan.POOLS[pool].name if pool in plan.POOLS else pool, why, n))
    for w in work[:limit]:
        print("  %d %-14s LH%-5s %s D%+d %-6s%s" % (
            w["cls"], w["pool"] or "-", w["flight_number"], w["flight_date"], w["lead"], w["why"],
            " for " + ", ".join("LH%s %s" % c for c in w["covers"]) if w["covers"] else ""))
    if len(work) > limit:
        print("  ... %d more" % (len(work) - limit))


def replay(state, now, days):
    """Every hour for `days` days, as if the feed answered every read: what
    the plan asks for, per day, pool and reason."""
    days_state = {k: dict(v) for k, v in state["days"].items()}
    per_day = defaultdict(Counter)
    covers = Counter()
    t = now
    end = now + timedelta(days=days)
    while t < end:
        work, _due = plan.plan(t, **dict(state, days=days_state))
        today = plan.berlin_day(t)
        for w in work:
            key = (w["flight_number"], w["flight_date"])
            per_day[today][(w["pool"] or "-", w["why"])] += 1
            covers[today] += len(w["covers"])
            fd = days_state.setdefault(key, {})
            if w["why"] == "flown" or (w["why"] == "find"):
                fd.update(answered_at=t, absent=w["why"] == "find")
                if w["why"] == "flown":
                    fd["truth"] = fd.get("tail")
                continue
            if not fd.get("found_at"):
                # a day not asked about yet: it runs if it ran a week before
                week = days_state.get((key[0], key[1] - timedelta(days=7))) or {}
                if week.get("found_at") or not week:
                    src = next((v for (n, d), v in sorted(days_state.items(), key=lambda kv: kv[0][1], reverse=True)
                                if n == key[0] and v.get("dep_utc")), None)
                    if src is None:
                        fd.update(answered_at=t, absent=True)
                        continue
                    # the same flight as on its latest known day, moved to this one
                    src_day = next(d for (n, d), v in days_state.items() if v is src)
                    shift = timedelta(days=(key[1] - src_day).days)
                    prev = src.get("prev")
                    fd.update(src, answered_at=t, found_at=t, dep_utc=src["dep_utc"] + shift,
                              arr_utc=(src.get("arr_utc") or src["dep_utc"]) + shift, truth=None,
                              prev=(prev[0], prev[1] + shift) if prev else None)
                else:
                    fd.update(answered_at=t, absent=True)
                continue
            fd.update(answered_at=t, found_at=t)
        # legs that have left: ADS-B sees seven in eight
        for key, fd in days_state.items():
            dep = fd.get("dep_utc")
            if dep and dep < t and not fd.get("truth") and zlib.crc32(repr(key).encode()) % 8:
                fd["truth"] = fd.get("tail")
        t += timedelta(hours=1)
    for d in sorted(per_day):
        c = per_day[d]
        pools = Counter()
        for (pool, _why), n in c.items():
            pools[pool] += n
        print("\n%s  %d lookups (%d outbound days through their return legs)" % (d, sum(c.values()), covers[d]))
        for pool, n in pools.most_common():
            whys = ", ".join("%s %d" % (why, m) for (p, why), m in sorted(c.items()) if p == pool)
            print("  %-15s %4d   %s" % (plan.POOLS[pool].name if pool in plan.POOLS else pool, n, whys))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", help="rows of an earlier pull (tmp/plan/rows.json) instead of pulling")
    ap.add_argument("--days", type=int, default=0, help="replay this many days of hourly runs")
    ap.add_argument("--show", type=int, default=40, help="how many of the next run's lookups to list")
    args = ap.parse_args()
    if args.rows:
        rows, now = _load(args.rows)
    else:
        now = datetime.now(timezone.utc)
        rows = pull(now)
        _dump(rows, now, OUT / "rows.json")
        print("rows of %s in %s" % (now.isoformat(timespec="minutes"), OUT / "rows.json"))
    state = fs.due_inputs(rows, now)
    work, due = plan.plan(now, pools=fs.POOLS, **state)
    show(work, due, state, args.show)
    if args.days:
        replay(state, now, args.days)


if __name__ == "__main__":
    main()
