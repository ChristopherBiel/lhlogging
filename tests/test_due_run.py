"""A run of the read plan end to end (fetch_flightstatus._read_due), on a fake
feed and a fake database: what it asks, in which order, what it follows up
and what it records.

    python3 -m unittest discover tests
"""
import json
import os
import sys
import tempfile
import types
import unittest
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "flightstatus"))

UTC = timezone.utc
NOW = datetime(2026, 11, 2, 9, 0, tzinfo=UTC)
TODAY = date(2026, 11, 2)


def day(n):
    return TODAY + timedelta(days=n)


def flight(reg, dep, arr, prev=None):
    """A FIS answer with one flight."""
    out = {"aircraftInfo": {"aircraftRegistration": reg.replace("-", "")},
           "legs": [{"overallStatus": "ONTIME",
                     "departure": {"departureAirport": dep, "departureScheduledTime": "2026-11-03T14:05:00.000+0000"},
                     "arrival": {"arrivalAirport": arr, "arrivalScheduledTime": "2026-11-04T09:45:00.000+0000"},
                     "flightDuration": "PT12H40M"}]}
    if prev:
        out["flightDetails"] = {"previousFlight": {"operatingCarrier": "LH", "operatingCarrierFlightNumber": prev[0],
                                                   "flightDate": prev[1].isoformat()}}
    return {"flights": [out]}


class Feed:
    """The browser: every page load answered `nav`; a lookup gets its answer
    from `answers` ({(number, date): payload}, None: "no such flight"), or a
    refusal when `refuse`."""

    def __init__(self, answers, nav=403, refuse=False):
        self.answers, self.nav, self.refuse = answers, nav, refuse
        self.asked, self.loads = [], 0
        feed = self

        class Page:
            def goto(self, url, **kw):
                feed.loads += 1
                return types.SimpleNamespace(status=feed.nav)

            def wait_for_timeout(self, ms):
                pass

            def evaluate(self, js, url):
                q = dict(p.split("=") for p in url.split("?")[1].split("&"))
                key = (q["flightNumber"][2:], date.fromisoformat(q["date"]))
                feed.asked.append(key)
                if feed.refuse:
                    return {"status": 403, "ct": "text/html", "headers": {}, "body": "no"}
                got = feed.answers.get(key)
                if got is None:
                    return {"status": 404, "ct": "application/json", "headers": {}, "body": "{}"}
                return {"status": 200, "ct": "application/json", "headers": {}, "body": json.dumps(got)}

        class Context:
            def add_init_script(self, s):
                pass

            def new_page(self):
                return Page()

            def close(self):
                pass

        class Browser:
            def new_context(self, **kw):
                return Context()

            def close(self):
                pass

        class PW:
            chromium = types.SimpleNamespace(launch=lambda **kw: feed.launched.append(1) or Browser())

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        self.launched = []
        self.pw = PW


@unittest.skipIf(sys.version_info < (3, 10), "the collector is written for the image's Python (3.10+)")
class DueRun(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
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
        cls.saved = {k: sys.modules.get(k) for k in stubs}
        cls.env = {k: os.environ.get(k) for k in ("POSTGRES_USER", "POSTGRES_PASSWORD")}
        sys.modules.update(stubs)
        os.environ.setdefault("POSTGRES_USER", "test")
        os.environ.setdefault("POSTGRES_PASSWORD", "test")
        sys.modules.pop("fetch_flightstatus", None)
        import fetch_flightstatus
        import plan
        cls.fs, cls.plan = fetch_flightstatus, plan

    @classmethod
    def tearDownClass(cls):
        sys.modules.pop("fetch_flightstatus", None)
        for k, v in cls.saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
        for k, v in cls.env.items():
            if v is None:
                os.environ.pop(k, None)

    def setUp(self):
        fs = self.fs
        self.rows, self.finished, self.coverage, self.lines = [], [], [], []
        self.patched = {}

        def patch(name, value):
            self.patched.setdefault(name, getattr(fs, name))
            setattr(fs, name, value)
        patch("upsert_observation", lambda conn, obs, run_id=None: self.rows.append(obs))
        patch("log_batch_start", lambda conn, run_type="flightstatus": 7)
        patch("log_batch_finish", lambda conn, run_id, *a: self.finished.append(a))
        patch("store_coverage", lambda conn, run_id, summary: self.coverage.append(summary))
        patch("per_pass_schema", lambda conn: True)
        patch("_shut_before", lambda conn, run_id, now: None)
        patch("refresh_recent_legs", lambda conn: None)
        patch("log", self.lines.append)
        patch("time", types.SimpleNamespace(sleep=lambda s: None))
        patch("BLOCK_BACKOFF_S", 0.0)                      # the door's pause between sessions
        self.conn = types.SimpleNamespace(commit=lambda: None, rollback=lambda: None, close=lambda: None,
                                          cursor=None)

    def tearDown(self):
        for name, value in self.patched.items():
            setattr(self.fs, name, value)

    def item(self, number, d, cls=1, why="day", pool="b748", covers=()):
        return {"flight_number": number, "flight_date": d, "seed_type": "B748", "cls": cls, "why": why,
                "due": NOW, "pool": pool, "lead": (d - TODAY).days, "covers": list(covers)}

    def run_due(self, work, feed, days=None, watched=()):
        self.fs.sync_playwright = feed.pw
        state = {"days": days or {}, "watched": set(watched), "idle": set(), "tails": {}}
        return self.fs._read_due(self.conn, work, {"b748": {"day": len(work)}}, state, NOW)

    def test_a_return_leg_read_stands_in_for_its_outbound(self):
        feed = Feed({("717", day(3)): flight("D-ABYN", "HND", "FRA", prev=("716", day(2)))})
        self.assertEqual(self.run_due([self.item("717", day(3), covers=[("716", day(2))])], feed), 0)
        self.assertEqual(feed.asked, [("717", day(3))])
        cov = self.coverage[-1]
        self.assertEqual((cov["plan"], cov["stood_in"], cov["read"], cov["left"]),
                         ("due", 1, {"b748": {"day": 1}}, {}))
        self.assertEqual(self.finished[-1][:5], (1, 1, 0, 1, "ok"))                       # total, found, err, stored

    def test_an_outbound_its_return_leg_no_longer_names_is_read_itself(self):
        feed = Feed({("717", day(3)): flight("D-ABYN", "HND", "FRA", prev=("9990", day(2))),
                     ("716", day(2)): flight("D-ABYQ", "FRA", "HND")})
        self.run_due([self.item("717", day(3), covers=[("716", day(2))])], feed)
        self.assertEqual(feed.asked[:2], [("717", day(3)), ("716", day(2))])
        self.assertEqual(self.coverage[-1]["stood_in"], 0)

    def test_a_flight_named_that_was_never_asked_about_is_followed(self):
        feed = Feed({("717", day(3)): flight("D-ABYN", "HND", "FRA", prev=("9716", day(2)))})
        self.run_due([self.item("717", day(3))], feed)
        self.assertEqual(feed.asked, [("717", day(3)), ("9716", day(2))])
        self.assertEqual(self.coverage[-1]["chained"], 1)
        self.assertEqual([r["found"] for r in self.rows], [True, False])                 # 9716: no such flight
        self.assertEqual(self.rows[1]["raw"], "{}")

    def test_where_a_watched_tail_went(self):
        days = {("716", day(1)): {"tail": "D-ABYN"}, ("718", day(2)): {"tail": "D-ABYA"},
                ("720", day(3)): {"tail": "D-ABYR"}}
        feed = Feed({("716", day(1)): flight("D-ABYA", "FRA", "HND"),
                     ("718", day(2)): flight("D-ABYN", "FRA", "HND")})
        self.run_due([self.item("716", day(1), cls=2, why="watch")], feed, days=days, watched={"D-ABYN"})
        self.assertEqual(feed.asked, [("716", day(1)), ("718", day(2))])                # D-ABYA's other flight
        self.assertEqual(self.coverage[-1]["follow_ups"], 1)
        self.assertEqual(days[("718", day(2))]["tail"], "D-ABYN")                        # found it

    def test_where_a_watched_tail_went_comes_before_the_daily_reads(self):
        days = {("716", day(1)): {"tail": "D-ABYN"}, ("718", day(2)): {"tail": "D-ABYA"}}
        feed = Feed({("716", day(1)): flight("D-ABYA", "FRA", "HND"),
                     ("718", day(2)): flight("D-ABYN", "FRA", "HND")})
        work = [self.item("716", day(1), cls=0, why="watch-near")] + [self.item(str(400 + i), day(3)) for i in range(3)]
        self.run_due(work, feed, days=days, watched={"D-ABYN"})
        self.assertEqual(feed.asked[:2], [("716", day(1)), ("718", day(2))])

    def test_the_most_valuable_first_and_at_most_due_max(self):
        work = [self.item(str(400 + i), day(1), cls=4) for i in range(5)] + [self.item("716", day(0), cls=0, why="3h")]
        work.sort(key=lambda w: w["cls"])
        saved, self.fs.DUE_MAX = self.fs.DUE_MAX, 3
        try:
            self.run_due(work, Feed({}))
        finally:
            self.fs.DUE_MAX = saved
        self.assertEqual(self.finished[-1][0], 3)
        self.assertEqual(self.coverage[-1]["left"], {"b748": 3})
        self.assertIn("3 due left for the next run", self.finished[-1][-1])

    def test_a_shut_feed_stops_the_run_and_what_is_left_is_counted(self):
        feed = Feed({}, nav=200, refuse=True)                  # by hand: no wait, stops at the third session
        self.assertEqual(self.run_due([self.item("716", day(1)), self.item("717", day(2))], feed), 1)
        self.assertEqual((feed.loads, self.rows), (3, []))     # a lookup the door stopped leaves no row
        self.assertEqual(self.finished[-1][4], "blocked")
        self.assertTrue(self.finished[-1][5].startswith("feed shut since "))
        self.assertEqual(self.coverage[-1]["left"], {"b748": 2})

    def test_nothing_due_no_browser(self):
        feed = Feed({})
        self.assertEqual(self.run_due([], feed), 0)
        self.assertEqual((feed.launched, self.finished[-1][:5]), ([], (0, 0, 0, 0, "ok")))
        self.assertEqual(self.finished[-1][5], "nothing due")
        self.assertEqual(self.coverage[-1]["plan"], "due")

    def test_a_run_says_what_it_is_as_it_starts(self):
        self.run_due([self.item("716", day(1))], Feed({}))
        self.assertEqual(self.coverage[0], {"plan": "due", "due": {"b748": {"day": 1}}})
        self.assertEqual(len(self.coverage), 2)                                          # and what it did

    # the rows a restart of the container left on 'running' (close_lost_runs)

    def test_when_the_container_started(self):
        with tempfile.TemporaryDirectory() as proc:
            os.makedirs(os.path.join(proc, "1"))
            with open(os.path.join(proc, "stat"), "w") as f:
                f.write("cpu  1 2 3\nbtime 1762300000\nprocesses 9\n")
            fields = ["S", "0", "1", "1", "0", "-1", "4194560"] + ["0"] * 12 + ["360000", "1234", "5"]
            with open(os.path.join(proc, "1", "stat"), "w") as f:
                f.write("1 (sh -c x) " + " ".join(fields) + "\n")                       # field 22: 360000
            self.assertEqual(self.fs.container_started(proc),
                             datetime.fromtimestamp(1762300000 + 360000 / os.sysconf("SC_CLK_TCK"), UTC))
            self.assertIsNone(self.fs.container_started(os.path.join(proc, "none")))

    def test_the_rows_a_restart_left_running_are_closed(self):
        restart = datetime(2026, 11, 5, 6, 45, 51, tzinfo=UTC)
        next_run = datetime(2026, 10, 15, 7, 2, tzinfo=UTC)
        last_read = datetime(2026, 10, 15, 3, 59, 57, tzinfo=UTC)
        cur = Cursor([(201792, "waiting for the feed, next try 07:58", restart + timedelta(minutes=23), None),
                      (183213, None, next_run, last_read), (183300, None, next_run, None)])
        self.assertEqual(self.fs.close_lost_runs(Conn(cur), restart), [201792, 183213, 183300])
        (sql, params), *updates = cur.done
        self.assertIn("b.started_at < %(started)s", sql)
        self.assertEqual(params["started"], restart)
        (status, text, end, run_id), reading, before = (u[1] for u in updates)
        self.assertEqual((status, end, run_id), ("blocked", restart, 201792))           # ended by the restart
        self.assertTrue(text.startswith("feed shut") and text.endswith("next try 07:58"))
        self.assertEqual(reading[0::2], ("error", last_read))                             # at its last lookup
        self.assertEqual(before[0::2], ("error", next_run))                               # by the next run

    def test_the_next_run_knows_the_feed_was_shut(self):
        restart = datetime(2026, 11, 5, 6, 45, 51, tzinfo=UTC)
        cur = Cursor([], one=("blocked", "feed shut, and the run was stopped from outside ...", restart))
        got = self.patched["_shut_before"](Conn(cur), 201827, restart + timedelta(minutes=23))
        self.assertEqual(got, restart)

    def test_a_run_starts_even_when_closing_them_fails(self):
        cur = Cursor([], one=(42,))
        saved, self.fs.close_lost_runs = self.fs.close_lost_runs, lambda conn: 1 / 0
        try:
            self.assertEqual(self.patched["log_batch_start"](Conn(cur)), 42)
        finally:
            self.fs.close_lost_runs = saved
        self.assertTrue(any(line.startswith("could not close the runs") for line in self.lines))


class Cursor:
    """A database cursor: what was executed, `rows` for a fetchall, `one` for
    a fetchone."""

    def __init__(self, rows, one=None):
        self.rows, self.one, self.done = rows, one, []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.done.append((" ".join(sql.split()), params))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.one


def Conn(cur):
    return types.SimpleNamespace(cursor=lambda: cur, commit=lambda: None, rollback=lambda: None)


if __name__ == "__main__":
    unittest.main()
