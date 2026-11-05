"""Unit tests for flightstatus/plan.py (which flight days the schedule reader
asks the feed for, and in which order) and for what the crontab promises
about it.

    python3 -m unittest discover tests
"""
import re
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "flightstatus"))
import plan  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 11, 2, 9, 0, tzinfo=UTC)            # a Monday, 10:00 in Berlin
TODAY = date(2026, 11, 2)
TAILS = {"D-ABYN": "b748", "D-ABYA": "b748", "D-AIMH": "a388", "D-AIXL": "a350-muc",
         "D-AIXF": "a350-muc", "D-AIXR": "a350-allegris", "D-AIXA": "a350-fra", "D-ABPU": "b787"}


def day(n):
    return TODAY + timedelta(days=n)


def hours(h):
    return NOW + timedelta(hours=h)


def flight(tail, d, dep_h, dep="FRA", arr="HND", read=-2.0, block=12.0, prev=None, **kw):
    """A flight day the feed found `read` hours from NOW (negative: ago),
    leaving at dep_h UTC on day d."""
    t = datetime(d.year, d.month, d.day, tzinfo=UTC) + timedelta(hours=dep_h)
    out = {"answered_at": hours(read), "found_at": hours(read), "absent": False, "tail": tail,
           "dep": dep, "arr": arr, "dep_utc": t, "arr_utc": t + timedelta(hours=block),
           "status": "ONTIME", "prev": prev}
    out.update(kw)
    return out


def run(days, numbers=None, now=NOW, quiet=True, **kw):
    """plan.plan at `now`; `numbers` by default: each number in `days` flown
    by its tails' pool. `quiet`: the days of those numbers that `days` does not
    name were answered "no such flight" (else each would be a new day)."""
    if numbers is None:
        numbers = {}
        for (n, _d), fd in days.items():
            if fd.get("tail") in TAILS:
                numbers.setdefault(n, {})[TAILS[fd["tail"]]] = 12
    if quiet:
        days = dict(days)
        for n in {k[0] for k in days}:
            for i in range(-3, 10):
                days.setdefault((n, day(i)), {"answered_at": hours(-1), "absent": True})
    return plan.plan(now, days, TAILS, numbers, **kw)


def keys(work):
    return [(w["flight_number"], w["flight_date"]) for w in work]


def full_days(number, tail, first=0, last=9, read=-1.0, dep_h=22.0, **kw):
    """A number read an hour ago on each of its days (nothing due)."""
    return {(number, day(i)): flight(tail, day(i), dep_h, read=read, **kw) for i in range(first, last + 1)}


class Pools(unittest.TestCase):
    def test_a_tail_has_a_pool_by_type_cabin_and_hub(self):
        self.assertEqual(plan.pool_of("B748"), "b748")
        self.assertEqual(plan.pool_of("A388"), "a388")
        self.assertEqual(plan.pool_of("B789", allegris=True), "b787")
        self.assertEqual(plan.pool_of("A359", allegris=True, hub="MUC"), "a350-allegris")
        self.assertEqual(plan.pool_of("A359", hub="MUC"), "a350-muc")
        self.assertEqual(plan.pool_of("A359", hub="FRA"), "a350-fra")
        self.assertEqual(plan.pool_of("A359"), "a350-muc")              # hub unknown: most of them
        self.assertIsNone(plan.pool_of("A320"))

    def test_the_pools_as_agreed(self):
        P = plan.POOLS
        self.assertEqual({p: (P[p].level, P[p].days) for p in P}, {
            "b748": (plan.CLOSE, 9), "a388": (plan.CLOSE, 9), "a350-allegris": (plan.NEAR, 9),
            "b787": (plan.NEAR, 4), "a350-muc": (plan.NEAR, 4), "a350-fra": (plan.OFF, 0)})

    def test_configure(self):
        P = plan.configure("a350-fra=near:4, b787=near:9, x=near:1, b748=loud:3, a388=off")
        self.assertEqual((P["a350-fra"].level, P["a350-fra"].days), (plan.NEAR, 4))
        self.assertEqual(P["b787"].days, 9)
        self.assertEqual((P["a388"].level, P["a388"].days), (plan.OFF, 0))
        self.assertEqual(P["b748"], plan.POOLS["b748"])
        self.assertNotIn("x", P)

    def test_a_number_belongs_to_every_pool_flying_a_third_of_it(self):
        self.assertEqual(plan.number_pools({"a350-muc": 6, "a350-allegris": 4, None: 0}),
                         {"a350-muc": 0.6, "a350-allegris": 0.4})
        self.assertEqual(plan.number_pools({"a350-muc": 9, "a350-allegris": 1}), {"a350-muc": 0.9})
        self.assertEqual(plan.number_pools({None: 9, "a350-muc": 1}), {})
        self.assertEqual(plan.number_pools({"b748": 2}), {})                    # a charter flown once
        # read by the highest level, then the furthest day
        self.assertEqual(plan.governing(["a350-muc", "a350-allegris"]), "a350-allegris")
        self.assertEqual(plan.governing(["a350-fra", "b748"]), "b748")
        self.assertIsNone(plan.governing([None]))


class Reads(unittest.TestCase):
    def test_nothing_is_due_right_after_a_read(self):
        days = {**full_days("716", "D-ABYN", read=-0.5), **full_days("506", "D-ABPU", last=4, read=-0.5)}
        self.assertEqual(run(days, {"716": {"b748": 12}, "506": {"b787": 12}})[0], [])

    def test_a_flight_day_is_read_once_a_day(self):
        days = full_days("506", "D-ABPU", last=4, read=-1)
        days[("506", day(4))]["answered_at"] = days[("506", day(4))]["found_at"] = hours(-25)
        work, due = run(days, {"506": {"b787": 12}})
        self.assertEqual(keys(work), [("506", day(4))])
        self.assertEqual((work[0]["why"], work[0]["cls"]), ("day", plan.FAR))     # the 787's day 4 is far
        self.assertEqual(due, {"b787": {"day": 1}})

    def test_each_pool_is_read_to_its_own_day(self):
        numbers = {n: {p: 12} for n, p in (("716", "b748"), ("412", "a350-allegris"), ("506", "b787"),
                                           ("432", "a350-fra"))}
        work, _ = run({}, numbers, quiet=False)                     # nothing asked yet: every day is new
        got = {}
        for w in work:
            got.setdefault(w["flight_number"], []).append(w["lead"])
        # day nine waits until the feed lists it (no departure known to time it by)
        self.assertEqual(got, {"716": list(range(9)), "412": list(range(9)), "506": list(range(5))})

    def test_the_day_nine_out_is_asked_for_once_the_feed_lists_it(self):
        # LH716 leaves at 12:00 UTC: nine days ahead of that is 12:00 today;
        # it is asked for half an hour after, not at 09:00
        days = full_days("716", "D-ABYN", last=8, dep_h=12, read=-0.4)
        self.assertEqual(run(days, quiet=False)[0], [])
        later = run(days, now=hours(3.75), quiet=False)[0]
        self.assertEqual([(w["flight_number"], w["lead"], w["why"]) for w in later], [("716", 9, "new")])

    def test_close_reads_every_six_hours_one_to_three_days_out(self):
        days = {**full_days("716", "D-ABYN", read=-6.5), **full_days("506", "D-ABPU", last=4, read=-6.5)}
        work, due = run(days, {"716": {"b748": 12}, "506": {"b787": 12}})
        self.assertEqual(sorted(w["lead"] for w in work), [1, 2, 3])
        self.assertEqual({(w["flight_number"], w["why"], w["cls"]) for w in work},
                         {("716", "6h", plan.CLOSE_TOP)})
        self.assertEqual(due, {"b748": {"6h": 3}})

    def test_near_a_hub_departure_its_own_reads_count(self):
        # leaves FRA in 2 h: due at 3 h out, unless read since
        days = {("716", day(0)): flight("D-ABYN", day(0), 11, read=-5)}
        work, _ = run(days)
        self.assertEqual([(w["why"], w["cls"]) for w in work], [("3h", plan.NEAR_TOP)])
        days[("716", day(0))]["answered_at"] = hours(-0.9)                   # read at 2.9 h out
        self.assertEqual(run(days)[0], [])
        # leaves in 11 h, last read 14 h before departure: the 12 h mark
        days = {("716", day(0)): flight("D-ABYN", day(0), 20, read=-3)}
        self.assertEqual([w["why"] for w in run(days)[0]], ["12h"])

    def test_near_reads_come_first_in_every_pool(self):
        # a 787 3 h before it leaves goes with the 747-8's, the earliest first
        days = {("506", day(0)): flight("D-ABPU", day(0), 11, read=-5),
                ("716", day(0)): flight("D-ABYN", day(0), 11.5, read=-5)}
        work, _ = run(days)
        self.assertEqual([(w["flight_number"], w["cls"]) for w in work],
                         [("506", plan.NEAR_TOP), ("716", plan.NEAR_TOP)])
        self.assertEqual(plan.class_of("day", "b787", 1), plan.SECOND)     # its daily reads stay behind

    def test_no_near_reads_for_a_flight_leaving_an_outstation(self):
        days = {("717", day(0)): flight("D-ABYN", day(0), 11, dep="HND", arr="FRA", read=-5)}
        self.assertEqual(run(days)[0], [])

    def test_an_outbound_is_read_through_its_return_leg(self):
        out = flight("D-ABYN", day(2), 12, read=-25)
        back = flight("D-ABYN", day(3), 3, dep="HND", arr="FRA", read=-25, prev=("716", day(2)))
        work, due = run({("716", day(2)): out, ("717", day(3)): back})
        self.assertEqual(keys(work), [("717", day(3))])
        self.assertEqual((work[0]["covers"], work[0]["why"]), ([("716", day(2))], "day"))
        self.assertEqual(due, {"b748": {"day": 2}})
        # the return leg's read is the outbound's: nothing due an hour later
        back["answered_at"] = back["found_at"] = hours(-1)
        self.assertEqual(run({("716", day(2)): out, ("717", day(3)): back})[0], [])

    def test_a_return_leg_beyond_the_pools_days_does_not_stand_in(self):
        out = flight("D-ABPU", day(4), 12, arr="ORD", read=-25)
        back = flight("D-ABPU", day(5), 3, dep="ORD", arr="FRA", read=-25, prev=("506", day(4)))
        work, _ = run({("506", day(4)): out, ("507", day(5)): back})
        self.assertEqual([(k, w["covers"]) for k, w in zip(keys(work), work)], [(("506", day(4)), [])])

    def test_a_flight_named_but_never_asked_about_is_asked_for(self):
        back = flight("D-ABYN", day(3), 3, dep="HND", arr="FRA", read=-1, prev=("9716", day(2)))
        work, _ = run({("717", day(3)): back})
        self.assertIn(("9716", day(2)), keys(work))
        self.assertEqual(next(w for w in work if w["flight_number"] == "9716")["why"], "new")

    def test_no_such_flight_is_not_asked_again(self):
        days = full_days("716", "D-ABYN", last=8)
        days[("716", day(5))] = {"answered_at": hours(-30), "absent": True}
        self.assertEqual(run(days)[0], [])

    def test_a_cancelled_flight_is_not_read(self):
        days = full_days("716", "D-ABYN", last=8)
        days[("716", day(5))].update(answered_at=hours(-30), found_at=hours(-30), status="CANCELLED")
        self.assertEqual(run(days)[0], [])

    def test_the_a350_frankfurt_is_not_read(self):
        days = {("432", day(i)): flight("D-AIXA", day(i), 12, read=-30) for i in range(5)}
        self.assertEqual(run(days)[0], [])

    def test_a_747_on_an_a350_number_is_read_as_a_747(self):
        days = {("432", day(2)): flight("D-ABYA", day(2), 12, read=-7)}
        work, _ = run(days)
        self.assertEqual([(w["why"], w["pool"]) for w in work], [("6h", "b748")])


class Flown(unittest.TestCase):
    def test_a_flight_without_a_flown_tail_is_asked_for_after_it_landed(self):
        f = flight("D-ABYN", day(-1), 12, read=-24)                     # landed 9 h ago
        work, _ = run({("716", day(-1)): f})
        self.assertEqual([(w["why"], w["cls"], w["lead"]) for w in work], [("flown", plan.FLOWN, -1)])
        for known in ({"truth": "D-ABYN"}, {"status": "ARRIVED"}, {"cancelled": True}):
            self.assertEqual(run({("716", day(-1)): dict(f, **known)})[0], [], known)

    def test_not_before_it_has_landed_and_not_twice_in_twelve_hours(self):
        self.assertEqual(run({("716", day(0)): flight("D-ABYN", day(0), 4, read=-6)})[0], [])
        f = flight("D-ABYN", day(-1), 12, read=-6)                       # asked 6 h ago, still FLYING
        self.assertEqual(run({("716", day(-1)): f})[0], [])

    def test_not_after_three_days(self):
        self.assertEqual(run({("716", day(-3)): flight("D-ABYN", day(-3), 6, read=-60)})[0], [])


class Watched(unittest.TestCase):
    def test_a_watched_tail_is_read_every_six_hours_in_any_pool(self):
        days = {("432", day(i)): flight("D-AIXA", day(i), 22, read=-6.5) for i in range(6)}
        work, _ = run(days, watched={"D-AIXA"})
        self.assertEqual([(w["lead"], w["why"], w["cls"]) for w in work],
                         [(i, "watch", plan.CLOSE_TOP) for i in range(4)])
        # and near its hub departure, among the first
        days = {("432", day(0)): flight("D-AIXA", day(0), 11, read=-5)}
        self.assertEqual([(w["why"], w["cls"]) for w in run(days, watched={"D-AIXA"})[0]],
                         [("watch-near", plan.NEAR_TOP)])

    def test_an_idle_tail_gets_six_hourly_reads(self):
        days = full_days("506", "D-ABPU", last=4, read=-6.5)
        self.assertEqual(run(days, {"506": {"b787": 12}})[0], [])
        work, _ = run(days, {"506": {"b787": 12}}, idle={"D-ABPU"})
        self.assertEqual([(w["lead"], w["why"]) for w in work], [(i, "idle") for i in range(4)])

    def test_after_a_watched_tail_moved_the_other_side_of_the_swap(self):
        days = {("716", day(1)): flight("D-ABYA", day(1), 12),
                ("714", day(2)): flight("D-AIXL", day(2), 12),
                ("718", day(2)): flight("D-ABYA", day(2), 12),
                ("720", day(6)): flight("D-ABYA", day(6), 12)}
        # D-ABYN left LH716 for D-ABYA: where D-ABYA was, D-ABYN may be now
        self.assertEqual(plan.follow_ups(days, "D-ABYA", {("716", day(1))}, TODAY), [("718", day(2))])


class NewNumbers(unittest.TestCase):
    def test_a_number_no_pool_flies_is_asked_about_once_a_day(self):
        work, _ = run({}, {}, catalog={"9999"})
        self.assertEqual([(w["flight_number"], w["lead"], w["why"], w["cls"]) for w in work],
                         [("9999", 1, "find", plan.FIND)])
        asked = {("9999", day(1)): {"answered_at": hours(-3), "absent": True}}
        self.assertEqual(run(asked, {}, catalog={"9999"}, quiet=False)[0], [])
        asked[("9999", day(1))]["answered_at"] = hours(-25)
        self.assertEqual(keys(run(asked, {}, catalog={"9999"}, quiet=False)[0]), [("9999", day(2))])

    def test_a_number_flown_lately_is_no_new_number(self):
        self.assertEqual(run({}, {"100": {None: 30}}, catalog={"100"})[0], [])


class Order(unittest.TestCase):
    def test_what_goes_first_when_the_feed_is_short(self):
        days = {
            ("716", day(0)): flight("D-ABYN", day(0), 11, read=-5),                    # 747 near
            ("416", day(5)): flight("D-AIMH", day(5), 12, read=-25),                   # A380 daily
            ("716", day(2)): flight("D-ABYN", day(2), 12, read=-7),                    # 747 6-hourly
            ("412", day(1)): flight("D-AIXR", day(1), 12, arr="JFK", read=-25),        # Allegris
            ("714", day(1)): flight("D-AIXL", day(1), 12, read=-25),                   # A350 Munich
            ("506", day(-1)): flight("D-ABPU", day(-1), 2, arr="ORD", read=-26),       # flown
            ("412", day(7)): flight("D-AIXR", day(7), 12, arr="JFK", read=-25),        # Allegris far
            ("506", day(4)): flight("D-ABPU", day(4), 12, arr="ORD", read=-25),        # 787 far
        }
        work, _ = run(days, catalog={"9999"}, watched=set())
        self.assertEqual([(w["flight_number"], w["lead"], w["cls"]) for w in work if w["flight_number"] != "9999"]
                         [:8], [("716", 0, 0), ("416", 5, 1), ("716", 2, 2), ("412", 1, 3), ("714", 1, 4),
                                ("506", -1, 5), ("506", 4, 6), ("412", 7, 6)])
        self.assertEqual(work[-1]["cls"], plan.FIND)


class Crontab(unittest.TestCase):
    """What flightstatus/crontab holds for the plan."""

    def lines(self):
        return [ln for ln in (ROOT / "flightstatus" / "crontab").read_text().splitlines()
                if re.match(r"^\d+\s+(\d+|\*)\s+\* \* \*\s+root\s+.*run_nightly\.sh", ln)]

    def test_the_plan_runs_every_hour_and_alone(self):
        due = [ln for ln in self.lines() if "--due" in ln]
        self.assertEqual(len(due), 1)
        self.assertEqual(due[0].split()[1], "*")
        others = [ln for ln in self.lines() if "--due" not in ln]
        self.assertEqual([("--census" in ln) for ln in others], [True])      # and the census

    def test_the_census_fits_between_two_runs(self):
        census = next(ln for ln in self.lines() if "--census" in ln)
        due = next(ln for ln in self.lines() if "--due" in ln)
        minute = int(due.split()[0])
        cm, ch = (int(x) for x in census.split()[:2])
        wait = int(re.search(r"NIGHTLY_JITTER_MAX_S=(\d+)", census).group(1)) // 60
        self.assertGreater(cm, minute + 25)                                    # a typical run is done
        self.assertLess(cm + wait + 15, 60 + minute - 3)                       # done before the next


if __name__ == "__main__":
    unittest.main()
