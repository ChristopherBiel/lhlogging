"""
Payloads the dashboard keeps warm.

What a page reads is rebuilt on a timer matched to how often its data moves,
not when a visitor asks: a request takes the last copy, so no visit waits on a
rebuild, and the server's work stays the same however many visitors come.
Before (2026-10-14) a visit found most caches expired and waited 1-3 s; the
timers app.py sets cost about ten minutes of server time a day.

A request builds on the spot only when there is no copy yet (a fresh process)
or the refresher has fallen two rounds behind. A failed rebuild keeps the last
copy and is tried again a minute later; with no copy at all, it raises.

Pure and stdlib-only: the tests run it without Flask or a database.
"""
import threading
import time

RETRY_S = 60  # a failed rebuild is tried again after this
TICK_S = 15   # how often the refresher looks for what is due


class Payload:
    """build(conn) -> data, rebuilt every `every` seconds by start()'s thread.
    `connect()` opens the connection a background rebuild runs on."""

    def __init__(self, every, build, connect, clock=time.time):
        self.every, self.build, self.connect, self.clock = every, build, connect, clock
        self.data, self.ts, self.next = None, 0.0, 0.0
        self.lock = threading.Lock()

    def _fresh(self):
        return self.data is not None and self.clock() - self.ts < 2 * self.every

    def get(self, conn=None):
        """The last copy; built on `conn` (or a connection of its own) only if
        there is none or it is two rounds old. Requests that find it cold at
        the same moment wait for one build."""
        if self._fresh():
            return self.data
        with self.lock:
            if self._fresh():
                return self.data
            return self._rebuild(conn)

    def refresh(self):
        with self.lock:
            return self._rebuild(None)

    def _rebuild(self, conn):
        try:
            if conn is not None:
                data = self.build(conn)
            else:
                own = self.connect()
                try:
                    data = self.build(own)
                finally:
                    own.close()
        except Exception:
            self.next = self.clock() + RETRY_S
            if self.data is None:
                raise
            return self.data  # stale beats a 500
        self.data, self.ts = data, self.clock()
        self.next = self.ts + self.every
        return data


def refresh_due(payloads):
    """One round: rebuild every payload whose time has come, in order."""
    for p in payloads:
        if p.clock() >= p.next:
            try:
                p.refresh()
            except Exception:
                pass  # no copy yet: the next request builds, or the next round retries


def start(payloads, tick=TICK_S):
    """The refresher: one daemon thread, so rebuilds never overlap each other."""
    def loop():
        while True:
            refresh_due(payloads)
            time.sleep(tick)
    thread = threading.Thread(target=loop, name="payload-refresher", daemon=True)
    thread.start()
    return thread
