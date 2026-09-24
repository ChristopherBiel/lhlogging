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
        ftype="B748"):
    dep_utc = NOW - timedelta(days=days_ago)
    return {"flight_date": dep_utc.date(), "airline": "LH", "flight_number": fnum,
            "dep": dep, "arr": arr, "fleet_type": ftype, "dep_utc": dep_utc,
            "dep_local": dep_utc, "arr_local": dep_utc + timedelta(hours=12),
            "truth_tail": truth, "timeline": timeline or [], "first_lead_h": first_lead,
            "cancelled": False}


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
        self.st = bm.build_stats(history(), NOW, weeks=8, fit=False)

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


if __name__ == "__main__":
    unittest.main()
