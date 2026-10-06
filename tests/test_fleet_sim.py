"""Unit tests for dashboard/fleet_sim.py (the fleet simulation).

    python3 -m unittest discover tests
"""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dashboard"))
import booking_model as bm  # noqa: E402
import fleet_sim as fs  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)
TAILS = ["D-ABYA", "D-ABYB", "D-ABYC", "D-ABYD"]


def leg(hours, fnum, tail="", dep="FRA", arr="HND", duration=600, truth="", pub_lead=None):
    """A leg departing `hours` from NOW; `tail` published `pub_lead` hours before
    departure (default: before NOW, and before departure for a leg that left)."""
    dep_utc = NOW + timedelta(hours=hours)
    lead = pub_lead if pub_lead is not None else max(hours, 0) + 2
    timeline = [[float(lead), tail]] if tail else []
    return {"flight_date": dep_utc.date(), "airline": "LH", "flight_number": fnum,
            "dep": dep, "arr": arr, "fleet_type": "B748", "dep_utc": dep_utc,
            "duration_min": duration, "truth_tail": truth, "timeline": timeline,
            "first_lead_h": float(lead) if tail else None, "cancelled": False}


def history():
    """Every tail flies its own FRA-HND-FRA rotation (LH70x/LH71x/...) for three
    weeks; D-ABYD's last one ended 10 days ago (its position is unknown to a
    7-day look-back)."""
    out = []
    for i, t in enumerate(TAILS):
        last = -24 * 10 if t == "D-ABYD" else -60
        for start in range(-24 * 21 + 8 * i, last, 72):
            out.append(leg(start, "7%d0" % i, dep="FRA", arr="HND", truth=t, tail=t, pub_lead=30))
            out.append(leg(start + 14, "7%d1" % i, dep="HND", arr="FRA", truth=t, tail=t,
                           pub_lead=30))
    return out


def upcoming():
    return [
        # D-ABYA is flying FRA-HND right now (left 6h ago, lands in 4h)
        leg(-6, "700", "D-ABYA", dep="FRA", arr="HND"),
        leg(8, "701", "D-ABYA", dep="HND", arr="FRA", duration=780),
        # two overlapping FRA departures, and the JFK return of the first
        leg(2, "400", "D-ABYB", dep="FRA", arr="JFK", duration=480),
        leg(3, "432", "D-ABYC", dep="FRA", arr="ORD", duration=540),
        leg(12, "401", "D-ABYB", dep="JFK", arr="FRA", duration=420),
        # nothing published yet; D-ABYB is back from JFK by then
        leg(20, "410", "", dep="FRA", arr="EWR", duration=500),
    ]


class Simulation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        hist, up = history(), upcoming()
        cls.legs = {l["flight_number"] + ("@" + l["dep"]): l for l in up}
        cls.st = bm.build_stats(hist, NOW, fit=False, plan=bm.PlanIndex(hist + up))
        # B748 B and C at FRA (last trip home 60h+ ago), A away in HND
        cls.res = fs.simulate(cls.st, hist + up, NOW, "B748", timedelta(days=2), n=400,
                              conn=fs.connections(hist, NOW), seed=1)

    def p(self, key, tail):
        return self.res.p(self.legs[key], tail)

    def test_a_tail_that_is_away_cannot_take_the_flight(self):
        self.assertEqual(self.p("400@FRA", "D-ABYA"), 0.0)
        self.assertEqual(self.p("432@FRA", "D-ABYA"), 0.0)

    def test_the_tail_at_the_outstation_flies_home(self):
        self.assertEqual(self.p("701@HND", "D-ABYA"), 1.0)

    def test_each_flight_sums_to_one(self):
        for key in ("400@FRA", "432@FRA", "401@JFK", "701@HND"):
            self.assertAlmostEqual(sum(self.res.dist(self.legs[key]).values()), 1.0, msg=key)

    def test_the_return_inherits_the_outbound(self):
        out, back = self.res.dist(self.legs["400@FRA"]), self.res.dist(self.legs["401@JFK"])
        self.assertEqual(out, back)

    def test_no_tail_on_two_overlapping_flights(self):
        for t in TAILS:
            self.assertLessEqual(self.p("400@FRA", t) + self.p("432@FRA", t), 1.0, msg=t)

    def test_an_unknown_position_does_not_exclude(self):
        st0 = fs.start_state(self.st, TAILS, NOW)
        self.assertEqual(st0["D-ABYD"], (None, None, None))
        self.assertEqual(st0["D-ABYA"][0], "HND")
        self.assertGreater(self.p("410@FRA", "D-ABYD"), 0.0)
        self.assertGreater(self.p("410@FRA", "D-ABYB"), 0.0)
        self.assertEqual(self.p("410@FRA", "D-ABYA"), 0.0)  # still flying back from HND


class Keep(unittest.TestCase):
    def test_published_tail_keeps_the_hold_rate(self):
        hist, up = history(), upcoming()
        st = bm.build_stats(hist, NOW, fit=False, plan=bm.PlanIndex(hist + up))
        st.hold.clear()
        st.hold[("overall", "", 3)] = [7, 10]       # LH400, 2h out: hold 70%
        res = fs.simulate(st, hist + up, NOW, "B748", timedelta(days=1), n=2000, seed=3)
        self.assertAlmostEqual(res.p(up[2], "D-ABYB"), 0.7, delta=0.04)

    def test_served_number_keeps_a_tail_that_is_away_near_zero(self):
        hist, up = history(), upcoming()
        st = bm.build_stats(hist, NOW, fit=False, plan=bm.PlanIndex(hist + up))
        st.hold.clear()
        st.hold[("overall", "", 3)] = [7, 10]       # 30% of LH400 is swap mass
        res = fs.simulate(st, hist + up, NOW, "B748", timedelta(days=1), n=200, seed=3)
        lh400 = up[2]
        model = {x: bm.p_target(st, x, lh400, published="D-ABYB", now=NOW)["p"] for x in TAILS}
        served = fs.blend_hard(res, lh400, model, 0.5)
        self.assertAlmostEqual(sum(served.values()), 1.0)
        # D-ABYA is over Siberia and lands in HND long after LH400 leaves: the
        # per-flight model's swap share for it is cut to a tenth
        self.assertFalse(res.reachable(lh400, "D-ABYA"))
        self.assertTrue(res.reachable(lh400, "D-ABYC"))
        self.assertGreater(model["D-ABYA"], 0.0)
        self.assertLess(served["D-ABYA"], 0.5 * model["D-ABYA"] * 0.2)
        r = fs.p_fleet(res, st, "D-ABYA", lh400, published="D-ABYB", now=NOW, fleet=TAILS)
        self.assertEqual(r["fleet"], 0.0)
        self.assertAlmostEqual(r["p"], fs.blend_hard(res, lh400, model,
                                                     fs.DEFAULTS["mix"])["D-ABYA"])
        self.assertIn("Playing the fleet forward", r["why"])


if __name__ == "__main__":
    unittest.main()
