"""
The door — when the schedule reader asks Lufthansa's feed, and when it waits
for the feed to let it in instead.

lufthansa.com sits behind Cloudflare. A browser session opens on the timetable
page, and how that page load is answered decides the session
(docs/flightstatus_schedule.md, "When the feed is shut"):

  - 403, Cloudflare's check page: lookups are answered for about two minutes,
    until the check completes; then the next session takes over;
  - 200, the page without the check: every lookup is refused from the start,
    and the next session usually opens the same way, for hours at a time
    (nights since August 2026, from 27 October whole days as well).

Retrying in the second state only adds page loads: a run spent ~19 minutes on
21 sessions that could not be answered, then stopped, and so did every run
after it. The door goes by what happened, not by the status alone: a session
that opened without the check and had its first lookup refused counts as shut.
After `tries` of those in a row the run waits `wait`, holding no session, and
tries one more; it goes on like that until the deadline (the start of the next
run, see next_slot), and stops when the next wait would end past it. A run
that had to wait also stops reading at the deadline, so it gives way to the
next run instead of holding the lock and making that one skip. A session that
opened without the check but is answered after all counts as open: if
Cloudflare changes its ways, the door follows.

Stdlib only, no browser or database in here: the collector hands in how to open
a session and how to ask (fetch_flightstatus.py), the tests hand in fakes.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timedelta

# what one lookup got back: an answer (a flight, or the feed's "no such flight"),
# a refusal, or a reply that is neither and is not worth asking again
ANSWER, REFUSED, BAD = "answer", "refused", "bad"
# how a session's page load was answered
CHECKED, UNCHECKED = 403, 200

_RUN_LINE = re.compile(r"^(\d+)\s+(\d+|\*)\s+\*\s+\*\s+\*\s+\S+\s+.*run_nightly\.sh")


def next_slot(crontab, now):
    """The first start of a run in `crontab` (the text of the collector's
    crontab, whose runs are daily lines 'M H * * * user ... run_nightly.sh ...'
    or hourly ones, 'M * * * * ...') after `now`, an aware datetime in the
    crontab's time zone. None if there is no such line."""
    starts = []
    for line in (crontab or "").splitlines():
        m = _RUN_LINE.match(line.strip())
        if not m:
            continue
        if m.group(2) == "*":
            at = now.replace(minute=int(m.group(1)), second=0, microsecond=0)
            starts.append(at if at > now else at + timedelta(hours=1))
            continue
        at = now.replace(hour=int(m.group(2)), minute=int(m.group(1)), second=0, microsecond=0)
        starts.append(at if at > now else at + timedelta(days=1))
    return min(starts) if starts else None


class Stop(Exception):
    """The run stops here. `why` is 'shut' (the feed stayed shut, and there is
    no time left to wait) or 'deadline' (a run that waited reached the next
    run's start)."""

    def __init__(self, why, message):
        super().__init__(message)
        self.why = why


class Door:
    """One run's sessions on the feed. `open_session()` loads the timetable
    page in a fresh session and returns (page, status of the page load or
    None); `close_session()` drops it. `deadline` (aware datetime) is when the
    run has to be done if it waits; `wait` (timedelta) between tries while the
    feed is shut, none = stop at once. `tries` sessions in a row that are shut
    make the run wait; `retries` refusals of one lookup in sessions that were
    not shut give it up. `on_wait(wake)` is told when the run starts waiting
    (wake: the next try) and when the feed lets it in again (None).

    `shut_since` (aware datetime): the run before this one stopped because the
    feed was shut then. The run then counts as one shut session short of
    waiting: if its first session is shut too, it waits at once instead of
    loading the page `tries` more times (with a run every hour, 2 page loads
    an hour fewer while the feed stays shut)."""

    def __init__(self, open_session, close_session=None, *, deadline=None, wait=None,
                 tries=3, retries=3, backoff_s=45.0, now=None, sleep=time.sleep,
                 log=print, on_wait=None, shut_since=None):
        self._open, self._close = open_session, close_session
        self.deadline = deadline
        self.wait = wait or timedelta(0)
        self.tries, self.retries, self.backoff_s = max(tries, 1), max(retries, 1), backoff_s
        self._now = now or (lambda: datetime.now().astimezone())
        self._sleep, self._log, self._on_wait = sleep, log, on_wait
        self.page = self.nav = None
        self.asked = self.answered = 0  # in the current session
        self.shut = 0                   # sessions in a row that were shut
        self.unchecked = 0              # sessions of the run that were shut
        self.shut_from = None           # when the current (or last) shut spell began
        self.waits = []                 # [(from, to)], one per wait
        if shut_since is not None:
            self.shut, self.shut_from = self.tries - 1, shut_since

    def open(self):
        self.page, self.nav = self._open()
        self.asked = self.answered = 0

    def close(self):
        if self._close and self.page is not None:
            self._close()
        self.page = None

    def overdue(self):
        """Whether the run's time is up: the next run is due (the deadline)."""
        return self.deadline is not None and self._now() >= self.deadline

    def fetch(self, label, ask):
        """One lookup: `ask(page)` -> (kind, payload, note), `note` saying what
        a refusal was. Returns the payload of an answer; None if the lookup is
        given up (refused `retries` times in open sessions, or a BAD reply).
        Raises Stop when the run has to stop; the lookup is then not done."""
        attempt = 1
        while True:
            if self.waits and self.overdue():
                raise Stop("deadline", "gave way to the next run at %s, after waiting for the feed %s"
                           % (_hm(self._now()), self.waited()))
            if self.page is None:
                self.open()
            kind, payload, note = ask(self.page)
            self.asked += 1
            if kind == ANSWER:
                if self.shut >= self.tries:
                    self._log("door: the feed answers again (waited %s)" % self.waited())
                    if self._on_wait:
                        self._on_wait(None)
                self.answered += 1
                self.shut = 0
                return payload
            if kind == BAD:
                return None
            if self.nav == UNCHECKED and not self.answered:
                if not self.shut:
                    self.shut_from = self._now()
                self.shut += 1
                self.unchecked += 1
                self._log("  %s: %s, the session opened without the check (%d in a row)"
                          % (label, note, self.shut))
                if self.shut >= self.tries:
                    self._wait_for_the_door()
                else:
                    self._sleep(self.backoff_s)
                    self.open()
                continue
            self._log("  %s: %s attempt %d/%d" % (label, note, attempt, self.retries))
            if attempt >= self.retries:
                return None
            attempt += 1
            self._sleep(self.backoff_s)
            self.open()

    def _wait_for_the_door(self):
        now = self._now()
        wake = now + self.wait
        if not self.wait or self.deadline is None or wake > self.deadline:
            raise Stop("shut", "feed shut since %s: %d sessions opened without the bot check "
                       "and were refused%s" % (_hm(self.shut_from), self.unchecked,
                                               ", waited %s" % self.waited() if self.waits else ""))
        self.close()
        self._log("door: shut since %s (%d sessions in a row without the check); next try %s, "
                  "last before %s %s" % (_hm(self.shut_from), self.shut, _hm(wake), _hm(self.deadline),
                                         wake.strftime("%Z")))
        if self._on_wait:
            self._on_wait(wake)
        self._sleep(max((wake - now).total_seconds(), 0))
        self.waits.append((now, self._now()))
        self.open()

    def waited(self):
        """'14:15-15:20', the span of the run's waits; None if it did not wait."""
        if not self.waits:
            return None
        return "%s-%s" % (_hm(self.waits[0][0]), _hm(self.waits[-1][1]))


def _hm(t):
    return t.strftime("%H:%M") if t else "?"
