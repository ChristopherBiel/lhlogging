"""Unit tests for dashboard/booking_model.py (P(target tail flies a flight)).

    python3 -m unittest discover tests
"""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dashboard"))
import booking_model as bm  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)
FLEET = ["D-ABYA", "D-ABYB", "D-ABYC", "D-ABYD"]
KNOBS = {"m": 5.0, "m_type": 20.0, "idle_near": 0.2, "idle_far": 0.5}


def leg(days_ago, fnum, truth, timeline=None, first_lead=None, dep="FRA", arr="HND",
        ftype="B748", duration=None):
    dep_utc = NOW - timedelta(days=days_ago)
    return {"flight_date": dep_utc.date(), "airline": "LH", "flight_number": fnum,
            "dep": dep, "arr": arr, "fleet_type": ftype, "dep_utc": dep_utc,
            "dep_local": dep_utc, "arr_local": dep_utc + timedelta(hours=12),
            "duration_min": duration, "truth_tail": truth, "timeline": timeline or [],
            "first_lead_h": first_lead, "cancelled": False}


def history():
    """Four weeks: LH716 mostly flown by D-ABYA; LH510 rotates; every tail active.
    Published tails at 48h held on LH716 3 times in 4."""
    legs = []
    for d in range(1, 29):
        a = "D-ABYA" if d % 4 else "D-ABYB"
        pub48 = a if d % 4 != 1 else "D-ABYC"
        legs.append(leg(d, "716", a, [[60.0, pub48]], 60.0))
        legs.append(leg(d, "510", FLEET[d % 4], [[60.0, FLEET[d % 4]]], 60.0, arr="EZE"))
    return legs


def upcoming(days_ahead, fnum="716", arr="HND"):
    return leg(-days_ahead, fnum, "", arr=arr)


class Regimes(unittest.TestCase):
    def setUp(self):
        # history() never flies the return legs, so every plan would clash:
        # these tests are about the regimes, Clash below about the plan
        self.st = bm.build_stats(history(), NOW, weeks=8, fit=False, plan=bm.PlanIndex([]))

    def p(self, tail, lg, pub=None):
        return bm.p_target(self.st, tail, lg, published=pub, now=NOW, knobs=KNOBS)

    def test_history_prefers_the_tail_that_usually_flies_the_flight(self):
        lg = upcoming(10)
        self.assertGreater(self.p("D-ABYA", lg)["p"], self.p("D-ABYD", lg)["p"])
        self.assertEqual(self.p("D-ABYA", lg)["regime"], "history")

    def test_distribution_over_the_fleet_sums_to_about_one(self):
        for pub in (None, "D-ABYC"):
            lg = upcoming(2 if pub else 10)
            total = sum(self.p(t, lg, pub)["p"] for t in FLEET)
            self.assertAlmostEqual(total, 1.0, delta=0.03, msg=pub)

    def test_published_target_gets_the_shrunk_hold_rate(self):
        r = self.p("D-ABYA", upcoming(2), pub="D-ABYA")
        self.assertEqual(r["regime"], "published")
        self.assertEqual(r["band"], 48)
        # route cell alone holds 21/28 = 0.75; shrinkage keeps it strictly inside (0, 1)
        self.assertGreater(r["p"], 0.5)
        self.assertLess(r["p"], 1.0)

    def test_other_tail_published_leaves_only_the_swap_mass(self):
        pub = self.p("D-ABYA", upcoming(2), pub="D-ABYA")["p"]
        swap = self.p("D-ABYB", upcoming(2), pub="D-ABYA")
        self.assertEqual(swap["regime"], "swap")
        self.assertLess(swap["p"], 1.0 - pub + 1e-9)

    def test_cancelled_and_departed(self):
        lg = dict(upcoming(2), cancelled=True)
        self.assertEqual(self.p("D-ABYA", lg)["p"], 0.0)
        gone = leg(0.1, "716", "")
        self.assertEqual(self.p("D-ABYA", gone, pub="D-ABYA")["regime"], "departed")
        self.assertEqual(self.p("D-ABYA", gone, pub="D-ABYA")["p"], 1.0)

    def test_no_signal_fleet_is_explained_as_an_even_share(self):
        r = self.p("D-ABYA", upcoming(10), None)
        self.assertEqual(r["basis"], "flight")
        flat = bm.p_target(self.st, "D-ABYA", upcoming(10), now=NOW,
                           knobs=dict(KNOBS, m=1e6, m_type=1e6))
        self.assertEqual(flat["basis"], "fleet")
        self.assertIn("even share", flat["why"])


class Clash(unittest.TestCase):
    """D-ABYA: LH716 FRA-HND in 24h (12h block), back on LH717 HND-FRA 14.4h
    after that (13h block, lands ~51h from now). Every publication is dated
    before NOW (timeline lead >= the flight's lead now)."""

    def plan(self, *extra):
        out = leg(-1, "716", "", [[30.0, "D-ABYA"]], 30.0, duration=720)
        back = leg(-1.6, "717", "", [[45.0, "D-ABYA"]], 45.0, dep="HND", arr="FRA",
                   duration=780)
        return [out, back] + list(extra)

    def test_a_consistent_chain_does_not_clash(self):
        nxt = leg(-2.5, "400", "", [[70.0, "D-ABYA"]], 70.0, arr="JFK", duration=500)
        idx = bm.PlanIndex(self.plan(nxt))
        self.assertIsNone(bm.plan_conflict(idx, nxt, "D-ABYA", NOW))

    def test_overlapping_flights_clash(self):
        # leaves FRA while still flying back from HND
        nxt = leg(-1.7, "400", "", [[45.0, "D-ABYA"]], 45.0, arr="JFK", duration=500)
        idx = bm.PlanIndex(self.plan(nxt))
        kind, other = bm.plan_conflict(idx, nxt, "D-ABYA", NOW)
        self.assertEqual((kind, other["flight_number"]), ("overlap", "717"))

    def test_departing_where_the_tail_is_not_clashes(self):
        # the return LH717 is published on another tail: D-ABYA stays in HND
        legs = self.plan()
        legs[1] = dict(legs[1], timeline=[[45.0, "D-ABYB"]])
        nxt = leg(-2.5, "400", "", [[70.0, "D-ABYA"]], 70.0, arr="JFK", duration=500)
        kind, other = bm.plan_conflict(bm.PlanIndex(legs + [nxt]), nxt, "D-ABYA", NOW)
        self.assertEqual((kind, other["flight_number"]), ("airport", "716"))

    def test_the_plan_is_read_as_of_then(self):
        # D-ABYB until 10h out, then D-ABYA (who is still flying LH717 then)
        nxt = leg(-1.7, "400", "", [[45.0, "D-ABYB"], [10.0, "D-ABYA"]], 45.0, arr="JFK",
                  duration=500)
        idx = bm.PlanIndex(self.plan(nxt))
        self.assertIsNone(bm.plan_conflict(idx, nxt, "D-ABYB", NOW))
        later = nxt["dep_utc"] - timedelta(hours=5)
        self.assertEqual(bm.plan_conflict(idx, nxt, "D-ABYA", later)[0], "overlap")

    def test_tail_as_of_takes_truth_only_once_settled(self):
        flown = leg(1, "716", "D-ABYB", [[30.0, "D-ABYA"]], 30.0)
        self.assertEqual(bm.tail_as_of(flown, NOW), "D-ABYA")  # truth pass not in yet
        self.assertEqual(bm.tail_as_of(flown, NOW + bm.TRUTH_LAG), "D-ABYB")

    def test_clash_and_far_cells_drive_the_hold_rate(self):
        st = bm.Stats(NOW, 8)
        st.hold[("overall", "", 48)] = [8, 10]
        st.hold[("clash:overall", "", 72)] = [3, 10]
        self.assertAlmostEqual(bm.hold_rate(st, 48, "FRA-HND", "B748")[0], 0.8)
        self.assertAlmostEqual(bm.hold_rate(st, 48, "FRA-HND", "B748", conflict=True)[0], 0.3)
        # far: the type keeps its own rate (A380-like overall vs 747-8-like type)
        st.hold[("far:overall", "", 120)] = [45, 100]
        st.hold[("far:type", "B748", 120)] = [13, 100]
        p = bm.hold_rate(st, 192, "FRA-HND", "B748")[0]
        self.assertAlmostEqual(p, (13 + bm.FAR_TYPE_M * 0.45) / (100 + bm.FAR_TYPE_M))
        self.assertIsNone(bm.hold_rate(st, 96, "FRA-HND", "B748"))  # no 96h cell at all

    def test_a_clashing_publication_says_so(self):
        nxt = leg(-1.7, "400", "", [[45.0, "D-ABYA"]], 45.0, arr="JFK", duration=500)
        st = bm.build_stats([], NOW, fit=False, plan=bm.PlanIndex(self.plan(nxt)))
        st.hold[("overall", "", 36)] = [8, 10]
        st.hold[("clash:overall", "", 72)] = [3, 10]
        r = bm.p_target(st, "D-ABYA", nxt, published="D-ABYA", now=NOW, knobs=KNOBS)
        self.assertAlmostEqual(r["p"], 0.3)
        self.assertIn("LH717", r["why"])


class Idle(unittest.TestCase):
    def test_idle_tail_weighs_less_near_than_far(self):
        legs = [l for l in history() if not (l["truth_tail"] == "D-ABYD" and
                                             (NOW - l["dep_utc"]).days < 6)]
        st = bm.build_stats(legs, NOW, weeks=8, fit=False)
        k = dict(KNOBS, m=1e6, m_type=1e6)
        near = bm.type_share(st, "D-ABYD", "B748", k, lead_days=1)
        far = bm.type_share(st, "D-ABYD", "B748", k, lead_days=10)
        self.assertLess(near, far)
        self.assertLess(near, bm.type_share(st, "D-ABYA", "B748", k, lead_days=1))


class Fit(unittest.TestCase):
    def test_fit_returns_grid_points_per_type(self):
        st = bm.build_stats(history(), NOW, weeks=8)   # 28 holdout legs: too few to fit
        self.assertEqual(st.fit, {})
        with mock.patch.object(bm, "MIN_VALIDATION_LEGS", 20):
            st = bm.build_stats(history(), NOW, weeks=8)
        self.assertIn(st.fit["B748"]["m"], bm.M_GRID)
        self.assertIn(st.fit["B748"]["idle_far"], bm.IDLE_W_GRID)


class Projection(unittest.TestCase):
    def test_weekly_flight_is_projected_and_one_off_is_not(self):
        legs = [leg(7 * w, "716", "D-ABYA") for w in (1, 2, 3)]     # same weekday, 3 weeks
        legs.append(leg(9, "9999", "D-ABYB"))                         # once
        target = (NOW + timedelta(days=7)).date()
        out = bm.project_schedule(legs, [target, target + timedelta(days=2)], NOW, ftype="B748")
        self.assertEqual([(l["flight_number"], l["flight_date"]) for l in out], [("716", target)])
        self.assertTrue(out[0]["projected"])
        self.assertEqual(out[0]["truth_tail"], "")

    def test_projection_takes_the_usual_layout_not_the_last_one(self):
        legs = [dict(leg(7 * w, "422", "D-ABYA"), seat_config=s)
                for w, s in ((1, "C88E32M244"), (2, "F8C80E32M244"), (3, "F8C80E32M244"))]
        target = (NOW + timedelta(days=7)).date()
        out = bm.project_schedule(legs, [target], NOW, ftype="B748")
        self.assertEqual(out[0]["seat_config"], "F8C80E32M244")


if __name__ == "__main__":
    unittest.main()
