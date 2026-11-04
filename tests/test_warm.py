"""Unit tests for dashboard/warm.py (payloads rebuilt on a timer, read from
the last copy).

    python3 -m unittest discover tests
"""
import sys
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard"))
import warm  # noqa: E402


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class Conn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class Source:
    """A build that counts its calls and can be told to fail."""

    def __init__(self):
        self.calls, self.fail, self.conns, self.opened = 0, False, [], []

    def build(self, conn):
        self.calls += 1
        self.conns.append(conn)
        if self.fail:
            raise RuntimeError("db down")
        return {"n": self.calls}

    def connect(self):
        c = Conn()
        self.opened.append(c)
        return c


def payload(every=300):
    clock, src = Clock(), Source()
    return warm.Payload(every, src.build, src.connect, clock=clock), clock, src


class PayloadTest(unittest.TestCase):
    def test_the_first_request_builds_on_its_own_connection(self):
        p, _clock, src = payload()
        mine = Conn()
        self.assertEqual(p.get(mine), {"n": 1})
        self.assertEqual(src.conns, [mine])
        self.assertEqual(src.opened, [])

    def test_a_request_reads_the_copy_until_it_is_two_rounds_old(self):
        p, clock, src = payload(every=300)
        p.get(Conn())
        clock.t += 599
        self.assertEqual(p.get(Conn()), {"n": 1})
        clock.t += 1
        self.assertEqual(p.get(Conn()), {"n": 2})

    def test_the_refresher_rebuilds_only_what_is_due(self):
        a, clock, src_a = payload(every=300)
        b = warm.Payload(900, Source().build, Source().connect, clock=clock)
        warm.refresh_due([a, b])
        self.assertEqual((a.data, b.data), ({"n": 1}, {"n": 1}))
        clock.t += 300
        warm.refresh_due([a, b])
        self.assertEqual((a.data, b.data), ({"n": 2}, {"n": 1}))
        self.assertTrue(all(c.closed for c in src_a.opened))
        self.assertEqual(len(src_a.opened), 2)

    def test_a_request_that_rebuilds_moves_the_refreshers_next_round(self):
        p, clock, src = payload(every=300)
        p.get(Conn())
        clock.t += 100
        warm.refresh_due([p])
        self.assertEqual(src.calls, 1)

    def test_a_failed_rebuild_keeps_the_copy_and_retries_a_minute_later(self):
        p, clock, src = payload(every=300)
        warm.refresh_due([p])
        src.fail = True
        clock.t += 300
        warm.refresh_due([p])
        self.assertEqual(p.data, {"n": 1})
        self.assertEqual(p.next, clock.t + warm.RETRY_S)
        clock.t += 300  # two rounds old now: a request tries itself, and gets the copy
        self.assertEqual(p.get(Conn()), {"n": 1})
        src.fail = False
        clock.t += warm.RETRY_S
        warm.refresh_due([p])
        self.assertEqual(p.data, {"n": 4})

    def test_with_no_copy_a_failure_reaches_the_request(self):
        p, _clock, src = payload()
        src.fail = True
        warm.refresh_due([p])  # the refresher swallows it
        with self.assertRaises(RuntimeError):
            p.get(Conn())

    def test_requests_that_find_it_cold_together_wait_for_one_build(self):
        calls = []

        def slow(conn):
            calls.append(conn)
            time.sleep(0.05)
            return "ok"

        p = warm.Payload(300, slow, Conn)
        got = []
        threads = [threading.Thread(target=lambda: got.append(p.get())) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(got, ["ok"] * 5)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
