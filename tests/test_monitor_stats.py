"""Unit tests for dashboard/monitor_stats.py (the start page: the collector's
plan against its runs, what is known about the days ahead, hubs, fleet use).

    python3 -m unittest discover tests
"""
import re
import sys
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard"))
import monitor_stats as ms  # noqa: E402

B = ms.BERLIN
NOW = datetime(2026, 10, 18, 16, 49, tzinfo=B)          # a Sunday afternoon, six days into the present plan
TODAY = NOW.date()
FAMILIES = {"B748": ("B748",), "A388": ("A388",), "B787": ("B788", "B789", "B78X"), "A350": ("A359", "A35K")}
SLOT = {hhmm: i for i, (hhmm, _k) in enumerate(ms.SLOTS)}


def at(day, hhmm, plus=0):
    """Berlin time on `day` days from today."""
    h, m = hhmm.split(":")
    return datetime(TODAY.year, TODAY.month, TODAY.day, int(h), int(m), tzinfo=B) + timedelta(days=day, minutes=plus)


_ids = iter(range(1, 10 ** 6))


def run(day, hhmm, plus=3, took=11, total=64, found=64, blocked=0, status="ok", detail=None):
    start = at(day, hhmm, plus)
    return {"id": next(_ids), "status": status, "started_at": start,
            "finished_at": None if status == "running" else start + timedelta(minutes=took),
            "total": total, "found": found, "blocked": blocked, "detail": detail}


def stopped(day, hhmm):
    return run(day, hhmm, total=10, found=0, blocked=10, status="blocked", took=18,
               detail="stopped after 10 consecutive blocked lookups")


def healthy(days=30, but=()):
    """Every slot of the last `days` days served, except (day, 'HH:MM') in `but`."""
    out = []
    for d in range(-days, 1):
        for hhmm, _kind, _wait in ms.plan(TODAY + timedelta(days=d)):      # the slot's time that day
            if (d, hhmm) in but or at(d, hhmm, 40) > NOW:
                continue
            out.append(run(d, hhmm))
    return out


class Plan(unittest.TestCase):
    """The plan here is a copy of the collector's; these fail when it drifts."""

    def crontab(self):
        slots, jitter, probe = [], {}, None
        for ln in (ROOT / "flightstatus" / "crontab").read_text().splitlines():
            m = re.match(r"^(\d+)\s+(\d+)\s+\* \* \*\s+root\s+(.*run_nightly\.sh.*)$", ln)
            if not m:
                continue
            cmd = m.group(3)
            offsets = re.search(r"FIS_PULSE_OFFSETS=([\d,]+)", cmd)
            kind = ("probe" if "--pulse" in cmd and offsets else "pulse" if "--pulse" in cmd
                    else "watch" if "--watch" in cmd else "far" if "--far" in cmd
                    else "lite" if "FIS_LOOKAHEAD_DAYS=1" in cmd else "full")
            if offsets:
                probe = tuple(int(x) for x in offsets.group(1).split(","))
            slots.append(("%02d:%02d" % (int(m.group(2)), int(m.group(1))), kind))
            jitter.setdefault(kind, set()).add(int(re.search(r"NIGHTLY_JITTER_MAX_S=(\d+)", cmd).group(1)) // 60)
        return sorted(slots), jitter, probe

    def setting(self, name):
        src = (ROOT / "flightstatus" / "fetch_flightstatus.py").read_text()
        return re.search(r'"%s",\s*"([^"]*)"' % name, src).group(1)

    def test_the_slots_are_the_crontabs(self):
        slots, jitter, _probe = self.crontab()
        self.assertEqual(list(ms.SLOTS), slots)
        self.assertEqual({k: {v} for k, v in ms.JITTER_MIN.items()}, jitter)

    def test_the_reads_follow_the_collectors_settings(self):
        _slots, _jitter, probe = self.crontab()
        num = lambda name: int(self.setting(name))
        pulse = tuple(int(x) for x in self.setting("FIS_PULSE_OFFSETS").split(","))
        back, bonus = num("FIS_BACKFILL_DAYS"), num("FIS_DEEP_LOOKAHEAD_BONUS")
        far = tuple(range(num("FIS_FAR_MIN_DAYS"), num("FIS_FAR_DEEP_MAX_DAYS") + 1))
        sweep = lambda ahead: tuple(range(-back, ahead + bonus + 1))
        self.assertEqual(ms.READS, {
            "pulse": {"deep": pulse}, "probe": {"deep": probe},
            "watch": {"deep": (0,), "wide": (0,)},
            "far": {"deep": tuple(sorted(set(pulse) | set(far))), "wide": far},
            "lite": {"deep": sweep(1), "wide": sweep(1)},
            "full": {"deep": sweep(num("FIS_LOOKAHEAD_DAYS")), "wide": sweep(num("FIS_LOOKAHEAD_DAYS"))}})
        self.assertEqual(dict((t[0], set(t[2])) for t in ms.TIERS),
                         {"deep": set(self.setting("FIS_DEEP_TYPES").split(",")),
                          "wide": set(self.setting("FIS_WIDE_TYPES").split(","))})

    def test_how_often_a_day_is_read(self):
        per = ms.reads_per_day()
        self.assertEqual((per["deep"][1], per["deep"][2], per["deep"][4], per["deep"][9]), (10, 10, 1, 1))
        self.assertEqual((per["wide"][1], per["wide"][3], per["wide"][4]), (2, 2, 1))
        self.assertNotIn(5, per["wide"])
        self.assertEqual((ms.horizon("deep"), ms.horizon("wide")), (9, 4))
        self.assertEqual(ms.readers("wide", 4), [SLOT["11:20"]])           # one run reads their day 4

    def test_a_day_is_judged_by_the_plan_it_ran_under(self):
        # the far pass and a pulse swapped places on 12 Oct 2026
        swap = date(2026, 10, 12)
        self.assertEqual((ms.plan(swap - timedelta(days=1))[3], ms.plan(swap - timedelta(days=1))[7]),
                         (("03:30", "far", 30), ("11:35", "pulse", 10)))
        self.assertEqual((ms.plan(swap)[3], ms.plan(swap)[7]), (("03:30", "pulse", 10), ("11:20", "far", 10)))
        self.assertEqual([len(ms.plan(swap - timedelta(days=d))) for d in (1, 0)], [15, 15])
        on = lambda day, h, m: datetime(day.year, day.month, day.day, h, m, tzinfo=B)
        mk = lambda start: {"id": next(_ids), "status": "ok", "started_at": start, "finished_at": start + timedelta(minutes=11),
                            "total": 64, "found": 64, "blocked": 0, "detail": None}
        before, after = swap - timedelta(days=1), swap
        runs = [mk(on(before, 3, 58)), mk(on(before, 11, 38)), mk(on(after, 3, 33)), mk(on(after, 11, 25))]
        slots, extra = ms.match(runs, on(before, 0, 0), on(after, 23, 59), NOW)
        got = [(s["at"].strftime("%d %H:%M"), s["kind"], s["slot"]) for s in slots if s["run"]]
        self.assertEqual(got, [("11 03:30", "far", 3), ("11 11:35", "pulse", 7), ("12 03:30", "pulse", 3), ("12 11:20", "far", 7)])
        self.assertEqual(extra, [])
        rec = ms.record(slots, extra, NOW)                                 # both days are inside the 28
        self.assertEqual(rec["changes"], [{"day": "2026-10-12", "slots": [
            {"slot": 3, "was": {"at": "03:30", "kind": "far"}, "now": {"at": "03:30", "kind": "pulse"}},
            {"slot": 7, "was": {"at": "11:35", "kind": "pulse"}, "now": {"at": "11:20", "kind": "far"}}]}])
        self.assertEqual(sorted((r["at"], r["kind"]) for r in rec["runs"]),
                         [("03:30", "far"), ("03:30", "pulse"), ("11:20", "far"), ("11:35", "pulse")])
        self.assertEqual(ms.changes(swap + timedelta(days=1), swap + timedelta(days=30)), [])

    def test_the_queries_take_no_parameters(self):
        for name, sql in ms.QUERIES.items():
            self.assertNotIn("%s", sql, name)                              # psql runs the same text
            self.assertNotIn("{", sql, name)


class Runs(unittest.TestCase):
    def test_outcome(self):
        self.assertEqual(ms.outcome(run(0, "09:00"), NOW), "ok")
        self.assertEqual(ms.outcome(stopped(0, "11:20"), NOW), "failed")
        self.assertEqual(ms.outcome(run(0, "05:45", total=317, found=281, blocked=10, status="blocked"), NOW), "cut")
        self.assertEqual(ms.outcome(run(0, "11:20", total=220, found=103, blocked=112), NOW), "cut")
        self.assertEqual(ms.outcome(run(0, "02:45", total=7, found=0, blocked=7), NOW), "failed")   # under the breaker
        self.assertEqual(ms.outcome(run(0, "02:45", total=0, found=0), NOW), "ok")                  # nothing to watch
        self.assertEqual(ms.outcome(run(0, "16:45", status="running"), NOW), "running")
        self.assertEqual(ms.outcome(run(-2, "05:45", status="running"), NOW), "lost")
        self.assertEqual(ms.outcome(None, NOW), "skipped")

    def test_a_slot_finds_its_run_inside_the_random_wait(self):
        late = run(0, "05:45", plus=29)                      # a sweep may wait 30 min
        hand = run(0, "07:40")                               # nobody's slot
        slots, extra = ms.match([late, hand, run(0, "09:00")], at(0, "00:00"), at(1, "00:00"), NOW)
        by = {s["at"].strftime("%H:%M"): s for s in slots}
        self.assertIs(by["05:45"]["run"], late)
        self.assertEqual(by["09:00"]["state"], "ok")
        self.assertEqual(by["03:30"]["state"], "skipped")    # hours ago, and nothing ran
        self.assertEqual(by["16:45"]["state"], "planned")    # may still be waiting to start
        self.assertEqual(by["22:00"]["state"], "planned")
        self.assertEqual(extra, [hand])

    def test_the_record_counts_what_went_wrong_and_the_running_streak(self):
        # day -9 was under the plan before: the slot was the 11:35 pulse then, and nothing ran
        bad = {(d, "11:20") for d in (-3, -2, -1, 0)} | {(-9, "11:35")}
        runs = healthy(but=bad) + [stopped(d, "11:20") for d in (-3, -2, -1, 0)]
        slots, extra = ms.match(runs, NOW - timedelta(days=29), NOW + ms.READS_AHEAD, NOW)
        rec = ms.record(slots, extra, NOW)
        far = rec["slots"][SLOT["11:20"]]
        self.assertEqual((far["n"], far["bad"], far["streak"]), (28, 5, 4))          # one record through the change
        self.assertEqual(far["last_ok"], (TODAY - timedelta(days=4)).isoformat())
        self.assertEqual(rec["slots"][SLOT["09:00"]]["bad"], 0)
        self.assertEqual([(k["slot"], k["at"], k["kind"]) for k in rec["skipped"]], [(SLOT["11:20"], "11:35", "pulse")])
        self.assertEqual(rec["per_day"][-1]["blocked"], 10)
        self.assertEqual(len(rec["runs"]), sum(st["n"] for st in rec["slots"]) - 1)   # all but the skipped one


class Reads(unittest.TestCase):
    def lookup(self, r, lead, type_, asked, found, day=0):
        return {"run_id": r["id"], "flight_date": TODAY + timedelta(days=day + lead), "type": type_,
                "asked": asked, "found": found, "first_at": r["started_at"], "last_at": r["started_at"] + timedelta(minutes=5)}

    def cells(self, runs, lookups, hhmm):
        slots, _extra = ms.match(runs, NOW - timedelta(days=3), NOW + ms.READS_AHEAD, NOW)
        out = ms.reads(slots, lookups, NOW)
        slot = next(s for s in out["slots"] if s["at"].startswith(TODAY.isoformat() + "T" + hhmm))
        return {(c["tier"], c["lead"]): c["state"] for c in slot["cells"]}, out

    def test_a_stopped_far_pass_leaves_its_days_missed(self):
        far = stopped(0, "11:20")
        # the ten lookups it got out were day 1 of the 747-8: six seeded as such, four as A359
        cells, out = self.cells([far], [self.lookup(far, 1, "B748", 6, 0), self.lookup(far, 1, "A359", 4, 0)], "11:20")
        self.assertEqual(cells[("deep", 1)], "failed")
        self.assertNotIn(("wide", 1), cells)                 # not a 787/A350 read: the far pass has none on day 1
        self.assertEqual((cells[("wide", 3)], cells[("wide", 4)], cells[("deep", 4)]), ("missed",) * 3)
        self.assertEqual(len(out["days"]), 13)
        self.assertEqual(out["days"][3], TODAY.isoformat())  # three days back, nine ahead

    def test_a_good_run_draws_what_it_read_and_nothing_else(self):
        full = run(-1, "18:15", took=100, total=600, found=560)
        rows = [self.lookup(full, lead, "B748", 22, 22, day=-1) for lead in (-1, 0, 1, 2, 3)]   # day -2 was settled
        rows.append(self.lookup(full, 0, "A320", 12, 12, day=-1))                                # a connecting leg
        slots, _ = ms.match([full], NOW - timedelta(days=3), NOW, NOW)
        out = ms.reads(slots, rows, NOW)
        slot = next(s for s in out["slots"] if s["kind"] == "full" and s["run"])
        got = {(c["tier"], c["lead"]): c["state"] for c in slot["cells"]}
        self.assertEqual(got, {("deep", lead): "read" for lead in (-1, 0, 1, 2, 3)})

    def test_what_is_ahead_is_planned(self):
        cells, _ = self.cells([], [], "18:15")
        self.assertEqual(set(cells.values()), {"planned"})
        self.assertEqual(len(cells), 12)


class Coverage(unittest.TestCase):
    def known(self, type_, lead, legs):
        return {"flight_date": TODAY + timedelta(days=lead), "type": type_, "legs": legs, "newest": NOW - timedelta(hours=2)}

    def rows(self, wide_to=2):
        rows = [self.known(t, d, n) for t, n in (("B748", 22), ("A388", 10)) for d in range(-30, 9)]
        rows += [self.known(t, d, n) for t, n in (("B789", 30), ("A359", 46)) for d in range(-30, wide_to + 1)]
        return rows

    def test_a_day_that_should_be_known_and_is_not(self):
        cov = {c["id"]: c for c in ms.coverage(self.rows(), NOW, FAMILIES)}
        a350 = {x["lead"]: x["state"] for x in cov["A350"]["cells"]}
        self.assertEqual((a350[2], a350[3], a350[4], a350[5]), ("full", "missing", "missing", "unread"))
        self.assertEqual(cov["A350"]["through"], (TODAY + timedelta(days=2)).isoformat())
        self.assertEqual(cov["A350"]["due_through"], (TODAY + timedelta(days=4)).isoformat())
        b748 = {x["lead"]: x["state"] for x in cov["B748"]["cells"]}
        self.assertEqual((b748[8], b748[9]), ("full", "unread"))           # tonight's probe reads day 9
        self.assertEqual(cov["B748"]["usual"], 22)

    def test_before_the_far_pass_day_4_is_not_yet_due(self):
        early = at(0, "11:00")
        cov = {c["id"]: c for c in ms.coverage(self.rows(wide_to=3), early, FAMILIES)}
        self.assertEqual({x["lead"]: x["state"] for x in cov["A350"]["cells"]}[4], "unread")

    def lines(self, bad, rows):
        runs = healthy(but=bad) + [stopped(d, h) for d, h in bad]
        slots, extra = ms.match(runs, NOW - timedelta(days=29), NOW + ms.READS_AHEAD, NOW)
        lines = ms.headline(slots, ms.record(slots, extra, NOW), ms.coverage(rows, NOW, FAMILIES), NOW)
        return [(h["level"], "".join(p if isinstance(p, str) else p[0] for p in h["parts"])) for h in lines]

    def test_the_headline_names_the_failing_runs_and_the_day_only_they_read(self):
        text = self.lines({(d, h) for d in (-3, -2, -1, 0) for h in ("03:30", "11:20")}, self.rows())
        self.assertEqual(text[0][0], "ok")
        self.assertIn("Schedule last read", text[0][1])
        self.assertEqual(text[1], ("ok", "747-8 and A380: plan known through Mon 26 Oct."))
        self.assertEqual(text[2], ("bad", "787 and A350: plan known through Tue 20 Oct, 2 days short "
                                          "of what should have been read by now."))
        self.assertEqual(text[-1], ("bad", "The 03:30 and 11:20 runs have failed 4 days running. "
                                           "The 11:20 far pass is the only read of day 4 for the 787 and A350."))
        alone = self.lines({(d, "11:20") for d in (-1, 0)}, self.rows())
        self.assertEqual(alone[-1], ("bad", "The 11:20 run has failed 2 days running. "
                                            "It is the only read of day 4 for the 787 and A350."))
        night = self.lines({(d, h) for d in (-2, -1, 0) for h in ("02:00", "03:30")}, self.rows(wide_to=4))
        self.assertEqual(night[-1], ("bad", "The 02:00 and 03:30 runs have failed 3 days running."))   # no day depends on them

    def test_a_quiet_day_says_so(self):
        runs = healthy()
        slots, extra = ms.match(runs, NOW - timedelta(days=29), NOW + ms.READS_AHEAD, NOW)
        lines = ms.headline(slots, ms.record(slots, extra, NOW), ms.coverage(self.rows(wide_to=4), NOW, FAMILIES), NOW)
        self.assertEqual({h["level"] for h in lines}, {"ok"})
        self.assertIn("runs in the last 24 hours went through", lines[-1]["parts"][0])


class Collectors(unittest.TestCase):
    def test_states(self):
        row = lambda kind, ago, **kw: dict({"run_type": kind, "status": "ok", "started_at": NOW - ago, "finished_at": NOW - ago,
                                            "total": 10, "found": 10, "blocked": 0, "stored": 10, "detail": None}, **kw)
        got = {c["id"]: c for c in ms.collectors([
            row("flightstatus", timedelta(minutes=70), total=47, found=47),
            row("state_poller", timedelta(minutes=30)),                      # every 2 min: half an hour is stale
            row("flight_detector", timedelta(minutes=4), status="error", detail="boom")], NOW)}
        self.assertEqual((got["flightstatus"]["state"], got["flightstatus"]["result"]), ("ok", "47 of 47 found"))
        self.assertEqual(got["state_poller"]["state"], "stale")
        self.assertEqual((got["flight_detector"]["state"], got["flight_detector"]["result"]), ("bad", "boom"))
        self.assertEqual(got["fleet_refresh"]["state"], "bad")               # no run at all

    def test_a_run_in_progress_says_so(self):
        row = lambda ago: {"run_type": "flightstatus", "status": "running", "started_at": NOW - ago, "finished_at": None,
                           "total": None, "found": None, "blocked": None, "stored": None, "detail": None}
        live = ms.collectors([row(timedelta(minutes=10))], NOW)[0]
        self.assertEqual((live["state"], live["result"]), ("ok", "running since 16:39"))      # not "0 of 0 found"
        dead = ms.collectors([row(timedelta(hours=9))], NOW)[0]
        self.assertEqual((dead["state"], dead["result"]), ("bad", "never finished"))
        sweep = run(0, "16:45", status="running")
        slots, extra = ms.match(healthy() + [sweep], NOW - timedelta(days=29), NOW + ms.READS_AHEAD, NOW)
        first = ms.headline(slots, ms.record(slots, extra, NOW), [], NOW)[0]
        self.assertEqual(first["parts"][-1], " The 16:45 pulse is running.")
        self.assertIn("Schedule last read ", first["parts"][0])                               # the last finished one


class Flown(unittest.TestCase):
    def test_hubs(self):
        rows = []
        for d in range(1, ms.HUB_DAYS + 1):                  # every full day: 8 departures at 10:00, one long-haul
            rows.append({"hub": "EDDF", "kind": "dep", "day": TODAY - timedelta(days=d), "bin": 40, "long": False, "n": 7})
            rows.append({"hub": "EDDF", "kind": "dep", "day": TODAY - timedelta(days=d), "bin": 40, "long": True, "n": 1})
            rows.append({"hub": "EDDF", "kind": "arr", "day": TODAY - timedelta(days=d), "bin": 36, "long": None, "n": 5})
        rows.append({"hub": "EDDF", "kind": "dep", "day": TODAY - timedelta(days=1), "bin": 88, "long": False, "n": 3})
        rows.append({"hub": "EDDF", "kind": "dep", "day": TODAY, "bin": 40, "long": False, "n": 2})      # today, so far
        fra, muc = ms.hubs(rows, NOW)["hubs"]
        self.assertEqual((fra["id"], fra["days"], len(fra["dep"]), len(fra["dep"][0])), ("FRA", ms.HUB_DAYS + 1, ms.HUB_DAYS + 1, 96))
        self.assertEqual((fra["profile"]["dep"]["med"][40], fra["profile"]["dep_long"]["med"][40], fra["profile"]["arr"]["med"][36]), (8, 1, 5))
        self.assertEqual(fra["profile"]["dep"]["med"][88], 0)                # one late evening is not the median day
        self.assertEqual((fra["median"], fra["busiest"], fra["long_share"]), (8, 40, round(56 / (56 * 8 + 3), 3)))
        self.assertEqual((fra["totals"]["dep"][-1], fra["high"]["n"]), (2, 11))
        self.assertEqual(fra["high"]["day"], (TODAY - timedelta(days=1)).isoformat())
        self.assertEqual((muc["median"], muc["open"]), (0, None))

    def test_use(self):
        ac = lambda reg, type_, sectors, minutes: {"icao24": reg.lower(), "registration": reg, "type": type_,
                                                   "sectors": sectors, "minutes": minutes}
        rows = [ac("D-ABY%s" % c, "B748", 28, 28 * 60 * (10 + i)) for i, c in enumerate("ABCDE")]
        rows += [ac("D-AIM%s" % c, "A388", 42, 28 * 60 * 12) for c in "AB"]          # too few to be named
        rows += [ac("D-ABYZ", "B748", 0, 0)]
        u = ms.use(rows, FAMILIES)
        self.assertEqual([(t["type"], t["n"], t["sectors"], t["hours"]) for t in u["types"]], [("B748", 5, 1.0, 12.0)])
        self.assertEqual((len(u["points"]), [p["reg"] for p in u["idle"]]), (7, ["D-ABYZ"]))
        self.assertEqual({p["family"] for p in u["points"]}, {"B748", "A388"})


if __name__ == "__main__":
    unittest.main()
