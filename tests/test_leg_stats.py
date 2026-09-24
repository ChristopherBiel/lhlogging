"""Unit tests for dashboard/leg_stats.py (insights / airframe / network sums).

    python3 -m unittest discover tests
"""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dashboard"))
import leg_stats as ls  # noqa: E402

UTC = timezone.utc
T0 = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def leg(day, dep, arr, tail, fnum="716", ftype="B748", dur=760, timeline=None,
        cancelled=False, status="ARRIVED"):
    d = T0 + timedelta(days=day)
    return {"flight_date": d.date(), "flight_number": fnum, "dep": dep, "arr": arr,
            "fleet_type": ftype, "dep_utc": d, "duration_min": dur, "truth_tail": tail,
            "cancelled": cancelled, "timeline": timeline or [], "latest_status": status}


LEGS = [
    leg(0, "FRA", "HND", "D-ABYA"), leg(1, "HND", "FRA", "D-ABYA", "717"),
    leg(3, "FRA", "EZE", "D-ABYA", "510", dur=820), leg(4, "EZE", "FRA", "D-ABYA", "511"),
    leg(0, "FRA", "EZE", "D-ABYB", "510", dur=820), leg(1, "EZE", "FRA", "D-ABYB", "511"),
    leg(2, "FRA", "HND", "D-ABYB"),
    leg(5, "FRA", "HND", "", cancelled=True),          # never counts
    leg(6, "FRA", "HND", ""),                          # not flown yet
]


class Descriptive(unittest.TestCase):
    def test_route_counts_and_median_block(self):
        r = {x["route"]: x for x in ls.route_counts(LEGS)}
        self.assertEqual(r["FRA-HND"]["n"], 2)
        self.assertEqual(r["FRA-EZE"]["median_min"], 820)

    def test_rotation_follows_each_tail_in_time_order(self):
        rot = {(x["from"], x["to"]): x["n"] for x in ls.rotation(LEGS)}
        self.assertEqual(rot[("FRA-HND", "HND-FRA")], 1)
        self.assertEqual(rot[("HND-FRA", "FRA-EZE")], 1)
        self.assertNotIn(("EZE-FRA", "FRA-EZE"), rot)

    def test_airframe_profile_gap_is_longest_run_without_a_leg(self):
        a = {x["reg"]: x for x in ls.airframe_profiles(LEGS)}
        self.assertEqual(a["D-ABYA"]["legs"], 4)
        self.assertEqual(a["D-ABYA"]["max_ground_days"], 2)     # day 1 -> day 3
        self.assertAlmostEqual(a["D-ABYA"]["hours"], (760 * 3 + 820) / 60, places=1)

    def test_route_shares(self):
        sh = {x["route"]: x for x in ls.route_shares(LEGS, "D-ABYA")}
        self.assertEqual((sh["FRA-HND"]["k"], sh["FRA-HND"]["n"], sh["FRA-HND"]["share"]), (1, 2, 0.5))

    def test_network_merges_directions_hub_first(self):
        net = {(r["a"], r["b"]): r for r in ls.network(LEGS)}
        self.assertEqual(set(net), {("FRA", "HND"), ("FRA", "EZE")})
        self.assertEqual(net[("FRA", "EZE")]["n"], 4)
        self.assertEqual(net[("FRA", "EZE")]["tails"], {"D-ABYA": 2, "D-ABYB": 2})

    def test_reschedulings_date_changes_by_the_revealing_look(self):
        lg = leg(5, "FRA", "HND", "D-ABYC", timeline=[[100.0, "D-ABYA"], [30.0, "D-ABYC"]])
        out = {x["date"]: x["n"] for x in ls.reschedulings([lg], max_lead_h=120)}
        revealed = (lg["dep_utc"] - timedelta(hours=30)).astimezone(ls.BERLIN).date().isoformat()
        self.assertEqual(out[revealed], 1)
        self.assertEqual(sum(out.values()), 1)
        self.assertEqual(ls.reschedulings([lg], max_lead_h=24), [])   # both looks too early


class Cabins(unittest.TestCase):
    def test_short_span_is_a_glitch(self):
        spans = ls.cabin_spans([
            ("F8C80E32M244", False, "74H", T0, T0 + timedelta(days=30)),
            ("C80", False, "74H", T0 + timedelta(days=3), T0 + timedelta(days=3, hours=1)),
        ])
        self.assertEqual([s["glitch"] for s in spans], [False, True])

    def test_overlapping_layouts_are_sold_variants_not_a_refit(self):
        spans = ls.cabin_spans([
            ("F8C80E32M244", False, "74H", T0, T0 + timedelta(days=60)),
            ("C88E32M244", False, "74H", T0 + timedelta(days=20), T0 + timedelta(days=50)),
        ])
        self.assertEqual([s["concurrent"] for s in spans], [True, True])
        refit = ls.cabin_spans([
            ("C48E21M224", False, "359", T0, T0 + timedelta(days=20)),
            ("F4C38E24M201", True, "359", T0 + timedelta(days=21), T0 + timedelta(days=60)),
        ])
        self.assertEqual([s["concurrent"] for s in refit], [False, False])

    def test_first_is_counted_per_flight(self):
        legs = [dict(leg(0, "FRA", "BLR", "D-ABYA", "754"), seat_config="C88E32M244"),
                dict(leg(1, "BLR", "FRA", "D-ABYA", "755"), seat_config="C88E32M244"),
                dict(leg(2, "FRA", "HND", "D-ABYA"), seat_config="F8C80E32M244"),
                leg(3, "FRA", "HND", "D-ABYA")]                          # layout unknown
        net = {(r["a"], r["b"]): r for r in ls.network(legs, {"D-ABYA": True})}
        self.assertEqual(net[("FRA", "BLR")]["first"], {})
        self.assertEqual(net[("FRA", "HND")]["first"], {"D-ABYA": 2})   # unknown -> tail's cabin
        nf = ls.first_not_sold(legs, "B748", {"D-ABYA": True})
        self.assertEqual([(r["route"], r["without"], r["legs"]) for r in nf],
                         [("BLR-FRA", 1, 1), ("FRA-BLR", 1, 1)])

    def test_variants_group_identical_cabins_biggest_first(self):
        latest = {"D-AIXA": ("A359", "C48E21M224", False, "359"),
                  "D-AIXB": ("A359", "C48E21M224", False, "359"),
                  "D-AIVC": ("A359", "C30E26M262", False, "35S"),
                  "D-ABYA": ("B748", "F8C80E32M244", False, "74H")}
        v = ls.cabin_variants(latest, "A359")
        self.assertEqual([(x["label"], x["tails"]) for x in v],
                         [("A", ["D-AIXA", "D-AIXB"]), ("B", ["D-AIVC"])])


if __name__ == "__main__":
    unittest.main()
