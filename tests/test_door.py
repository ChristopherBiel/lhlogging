"""Unit tests for flightstatus/door.py (when the schedule reader asks the feed,
waits for it, or stops) and for the collector's order of work.

    python3 -m unittest discover tests
"""
import os
import sys
import types
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "flightstatus"))
import door  # noqa: E402

B = ZoneInfo("Europe/Berlin")
START = datetime(2026, 10, 29, 14, 13, tzinfo=B)
A, R, X = door.ANSWER, door.REFUSED, door.BAD


class Feed:
    """Scripted sessions: each (status of the page load, what its lookups get
    in turn — the last one repeats). Opening a session takes 8 s, a lookup 7 s."""

    def __init__(self, *sessions, start=START):
        self.sessions, self.t, self.opened, self.closed = list(sessions), start, 0, 0
        self.notes, self.lines = [], []

    def now(self):
        return self.t

    def sleep(self, s):
        self.t += timedelta(seconds=s)

    def open_session(self):
        nav, script = self.sessions[min(self.opened, len(self.sessions) - 1)]
        self.opened += 1
        self.t += timedelta(seconds=8)
        return {"script": list(script), "n": 0}, nav

    def close_session(self):
        self.closed += 1

    def ask(self, page):
        self.t += timedelta(seconds=7)
        kind = page["script"][min(page["n"], len(page["script"]) - 1)]
        page["n"] += 1
        return kind, ({"flights": []} if kind == A else None), "HTTP 403 (text/html)"

    def door(self, **kw):
        kw.setdefault("backoff_s", 45)
        return door.Door(self.open_session, self.close_session, now=self.now, sleep=self.sleep,
                         log=self.lines.append, on_wait=self.notes.append, **kw)


class Sessions(unittest.TestCase):
    def test_a_checked_session_is_answered(self):
        f = Feed((403, [A]))
        self.assertEqual(f.door().fetch("LH716", f.ask), {"flights": []})
        self.assertEqual(f.opened, 1)

    def test_when_the_check_completes_the_next_session_takes_over(self):
        f = Feed((403, [A, R]), (403, [A]))
        d = f.door()
        d.fetch("LH716", f.ask)
        self.assertEqual(d.fetch("LH717", f.ask), {"flights": []})
        self.assertEqual((f.opened, d.unchecked, d.waits), (2, 0, []))
        self.assertIn("LH717: HTTP 403 (text/html) attempt 1/3", " ".join(f.lines))   # the log's old shape

    def test_a_lookup_refused_in_open_sessions_is_given_up(self):
        f = Feed((403, [A, R]), (403, [R]))
        d = f.door()
        d.fetch("LH716", f.ask)
        self.assertIsNone(d.fetch("LH717", f.ask))            # three attempts, then the next lookup
        self.assertEqual((f.opened, d.unchecked), (3, 0))

    def test_a_reply_that_is_no_answer_is_not_asked_again(self):
        f = Feed((403, [X]))
        self.assertIsNone(f.door().fetch("LH716", f.ask))
        self.assertEqual(f.opened, 1)

    def test_a_session_without_the_check_that_is_answered_counts_as_open(self):
        f = Feed((200, [A, A, R]), (403, [A]))
        d = f.door()
        d.fetch("LH716", f.ask)
        d.fetch("LH717", f.ask)
        self.assertEqual(d.fetch("LH718", f.ask), {"flights": []})          # a retry, not a shut door
        self.assertEqual(d.unchecked, 0)

    def test_sessions_with_and_without_the_check_in_turn_keep_the_door_open(self):
        f = Feed((200, [R]), (403, [A, R]), (200, [R]), (403, [A]))
        d = f.door(wait=timedelta(minutes=15), deadline=START + timedelta(hours=1))
        self.assertEqual(d.fetch("LH716", f.ask), {"flights": []})
        self.assertEqual(d.fetch("LH717", f.ask), {"flights": []})
        self.assertEqual((d.unchecked, d.waits, f.notes), (2, [], []))


class Shut(unittest.TestCase):
    def test_a_run_by_hand_stops_after_three_shut_sessions(self):
        f = Feed((200, [R]))
        with self.assertRaises(door.Stop) as stop:
            f.door().fetch("LH716", f.ask)
        self.assertEqual((stop.exception.why, f.opened), ("shut", 3))
        self.assertLess(f.t - START, timedelta(minutes=3))   # it used to be 21 sessions in 19 min
        self.assertEqual(str(stop.exception),
                         "feed shut since 14:13: 3 sessions opened without the bot check and were refused")

    def test_a_shut_feed_is_waited_for_and_read_when_it_opens(self):
        f = Feed((200, [R]), (200, [R]), (200, [R]), (200, [R]), (403, [A]))
        d = f.door(wait=timedelta(minutes=15), deadline=START + timedelta(hours=1))
        self.assertEqual(d.fetch("LH716", f.ask), {"flights": []})
        self.assertEqual((f.opened, len(d.waits)), (5, 2))            # one session per wait
        self.assertEqual(f.notes[-1], None)                            # the monitor's line cleared
        self.assertEqual([n.strftime("%H:%M") for n in f.notes[:-1]], ["14:30", "14:45"])
        self.assertEqual(d.waited(), "14:15-14:45")
        self.assertEqual(f.closed, 2)                                  # no session held while waiting

    def test_the_run_stops_when_the_next_wait_would_end_past_the_deadline(self):
        f = Feed((200, [R]))
        d = f.door(wait=timedelta(minutes=15), deadline=START + timedelta(minutes=20))
        with self.assertRaises(door.Stop) as stop:
            d.fetch("LH716", f.ask)
        self.assertEqual((stop.exception.why, len(d.waits), f.opened), ("shut", 1, 4))
        self.assertLessEqual(f.t, START + timedelta(minutes=20))
        self.assertIn(", waited ", str(stop.exception))

    def test_a_run_that_waited_gives_way_at_the_deadline(self):
        f = Feed((200, [R]), (200, [R]), (200, [R]), (403, [A]))
        d = f.door(wait=timedelta(minutes=15), deadline=START + timedelta(minutes=19))
        done = 0
        with self.assertRaises(door.Stop) as stop:
            while True:
                d.fetch("LH%d" % done, f.ask)
                done += 1
                f.sleep(60)
        self.assertEqual(stop.exception.why, "deadline")
        self.assertGreater(done, 0)
        self.assertGreaterEqual(f.t, START + timedelta(minutes=19))
        self.assertLess(f.t, START + timedelta(minutes=21))
        self.assertTrue(str(stop.exception).startswith("gave way to the next run at "))

    def test_after_a_run_that_found_it_shut_the_first_shut_session_waits(self):
        before = START - timedelta(minutes=8)                          # the run before stopped then
        f = Feed((200, [R]), (403, [A]))
        d = f.door(wait=timedelta(minutes=15), deadline=START + timedelta(hours=1), shut_since=before)
        self.assertEqual(d.fetch("LH716", f.ask), {"flights": []})
        self.assertEqual((f.opened, len(d.waits)), (2, 1))            # one page load, not three, then the wait
        self.assertEqual(d.waited(), "14:13-14:28")
        self.assertIn("shut since 14:05", " ".join(f.lines))           # the spell the run before saw

    def test_after_a_run_that_found_it_shut_an_open_feed_reads_at_once(self):
        f = Feed((403, [A]))
        d = f.door(wait=timedelta(minutes=15), deadline=START + timedelta(hours=1),
                   shut_since=START - timedelta(minutes=8))
        self.assertEqual(d.fetch("LH716", f.ask), {"flights": []})
        self.assertEqual((f.opened, d.waits, d.shut), (1, [], 0))

    def test_a_run_that_did_not_wait_is_not_cut_at_the_deadline(self):
        # the door still answers it; the run sees that the next one is due
        # (overdue) and leaves the rest to it (fetch_flightstatus._read_due)
        f = Feed((403, [A]))
        d = f.door(wait=timedelta(minutes=15), deadline=START + timedelta(minutes=1))
        self.assertFalse(d.overdue())
        f.sleep(600)
        self.assertTrue(d.overdue())
        self.assertEqual(d.fetch("LH716", f.ask), {"flights": []})
        self.assertFalse(f.door().overdue())                         # by hand: no deadline, never due


class NextSlot(unittest.TestCase):
    CRONTAB = (ROOT / "flightstatus" / "crontab").read_text()

    def nxt(self, *args):
        return door.next_slot(self.CRONTAB, datetime(*args, tzinfo=B))

    def test_the_next_run_of_the_hour(self):
        self.assertEqual(self.nxt(2026, 11, 2, 14, 13), datetime(2026, 11, 2, 15, 7, tzinfo=B))
        self.assertEqual(self.nxt(2026, 11, 2, 14, 7), datetime(2026, 11, 2, 15, 7, tzinfo=B))
        self.assertEqual(self.nxt(2026, 11, 2, 14, 6), datetime(2026, 11, 2, 14, 7, tzinfo=B))
        self.assertEqual(self.nxt(2026, 11, 2, 8, 20), datetime(2026, 11, 2, 8, 40, tzinfo=B))   # the census

    def test_after_the_last_run_of_the_day_the_first_of_tomorrow(self):
        self.assertEqual(self.nxt(2026, 11, 2, 23, 30), datetime(2026, 11, 3, 0, 7, tzinfo=B))

    def test_the_passes_of_a_daily_crontab(self):
        daily = "10 0 * * * root run_nightly.sh --pulse\n30 15 * * * root run_nightly.sh --watch 4\n"
        nxt = lambda *a: door.next_slot(daily, datetime(*a, tzinfo=B))
        self.assertEqual(nxt(2026, 10, 29, 14, 13), datetime(2026, 10, 29, 15, 30, tzinfo=B))
        self.assertEqual(nxt(2026, 10, 29, 22, 5), datetime(2026, 10, 30, 0, 10, tzinfo=B))
        later = nxt(2026, 10, 24, 23, 0)                       # the night the clocks go back
        self.assertEqual(later - datetime(2026, 10, 24, 23, 0, tzinfo=B), timedelta(minutes=70))

    def test_no_runs_no_slot(self):
        self.assertIsNone(door.next_slot("SHELL=/bin/bash\n# 10 0 * * * root run_nightly.sh\n", START))
        self.assertIsNone(door.next_slot(None, START))


@unittest.skipIf(sys.version_info < (3, 10), "the collector is written for the image's Python (3.10+)")
class Collector(unittest.TestCase):
    """fetch_flightstatus, loaded without a database or a browser."""

    @classmethod
    def setUpClass(cls):
        stubs = {"psycopg": types.ModuleType("psycopg"), "psycopg.types": types.ModuleType("psycopg.types"),
                 "psycopg.types.json": types.ModuleType("psycopg.types.json"),
                 "playwright": types.ModuleType("playwright"),
                 "playwright.sync_api": types.ModuleType("playwright.sync_api")}
        stubs["psycopg"].Connection = object
        stubs["psycopg.types.json"].Jsonb = object
        stubs["playwright.sync_api"].sync_playwright = None
        cls.saved = {k: sys.modules.get(k) for k in stubs}
        cls.env = {k: os.environ.get(k) for k in ("POSTGRES_USER", "POSTGRES_PASSWORD")}
        sys.modules.update(stubs)
        os.environ.setdefault("POSTGRES_USER", "test")
        os.environ.setdefault("POSTGRES_PASSWORD", "test")
        sys.modules.pop("fetch_flightstatus", None)
        import fetch_flightstatus
        cls.fs = fetch_flightstatus

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

    def test_the_747_and_a380_first_then_the_787_and_a350_then_the_rest(self):
        today = date(2026, 10, 30)
        day = lambda d: today + timedelta(days=d)
        work = [(n, day(d), None, 0) for n in ("716", "400", "96") for d in (-2, -1, 0, 1, 2, 3)]
        out = self.fs.prioritised(work, deep={"716"}, wide={"400"}, today=today)
        self.assertEqual([w[0] for w in out], ["716"] * 6 + ["400"] * 6 + ["96"] * 6)
        self.assertEqual([(w[1] - today).days for w in out[:6]], [0, 1, -1, 2, -2, 3])
        self.assertEqual(sorted(out), sorted(work))

    def test_a_run_by_hand_does_not_wait(self):
        self.assertEqual(self.fs.DOOR_WAIT_MIN, 0)          # the crontab sets it for the scheduled runs
        self.assertIn("\nFIS_DOOR_WAIT_MIN=15\n", (ROOT / "flightstatus" / "crontab").read_text())


if __name__ == "__main__":
    unittest.main()
