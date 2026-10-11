"""Unit tests for dashboard/leg_stats.py (insights / airframe / network sums).

    python3 -m unittest discover tests
"""
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dashboard"))
import booking_model as bm  # noqa: E402
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
    def test_route_shares(self):
        sh = {x["route"]: x for x in ls.route_shares(LEGS, "D-ABYA")}
        self.assertEqual((sh["FRA-HND"]["k"], sh["FRA-HND"]["n"], sh["FRA-HND"]["share"]), (1, 2, 0.5))

    def test_network_merges_directions_hub_first(self):
        net = {(r["a"], r["b"]): r for r in ls.network(LEGS)}
        self.assertEqual(set(net), {("FRA", "HND"), ("FRA", "EZE")})
        self.assertEqual(net[("FRA", "EZE")]["n"], 4)
        self.assertEqual(net[("FRA", "EZE")]["tails"], {"D-ABYA": 2, "D-ABYB": 2})


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


class MultiLeg(unittest.TestCase):
    def test_a_flight_of_several_legs_is_one_route_and_draws_each_leg(self):
        segs = [{"dep": "FRA", "arr": "LOS"}, {"dep": "LOS", "arr": "SSG"}]
        legs = [dict(leg(d, "FRA", "SSG", "D-ABPO", "568", ftype="B789", dur=525), segments=segs)
                for d in (0, 2)]
        net = {(e["a"], e["b"]): e["n"] for e in ls.network(legs)}
        self.assertEqual(net, {("FRA", "LOS"): 2, ("LOS", "SSG"): 2})


def seen(lg, *looks):
    """`lg` with a publication history: looks = (hours out, tail), farthest first."""
    return dict(lg, timeline=[[float(h), t] for h, t in looks], first_lead_h=float(looks[0][0]))


class Trust(unittest.TestCase):
    def test_wilson_interval_brackets_the_rate_and_narrows_with_n(self):
        lo, hi = ls.wilson(5, 10)
        self.assertTrue(lo < 0.5 < hi)
        lo2, hi2 = ls.wilson(500, 1000)
        self.assertLess(hi2 - lo2, hi - lo)
        self.assertEqual(ls.wilson(0, 0), (0.0, 1.0))
        self.assertEqual(ls.wilson(0, 20)[0], 0.0)

    def test_hold_curve_counts_only_the_bands_we_looked_at(self):
        legs = [seen(leg(0, "FRA", "HND", "D-ABYA"), (80, "D-ABYB"), (30, "D-ABYA")),
                seen(leg(1, "FRA", "HND", "D-ABYA"), (50, "D-ABYA"))]
        curve = {c["band"]: c for c in ls.hold_curve(legs)}
        self.assertNotIn(96, curve)                       # nobody looked that early
        self.assertEqual((curve[72]["k"], curve[72]["n"]), (0, 1))
        self.assertEqual((curve[48]["k"], curve[48]["n"]), (1, 2))   # 48h: YB (wrong), YA
        self.assertEqual((curve[24]["k"], curve[24]["n"]), (2, 2))
        self.assertTrue(curve[48]["lo"] < curve[48]["p"] < curve[48]["hi"])

    def test_hold_curve_is_the_booking_models_all_cell(self):
        legs = [seen(leg(d, "FRA", "HND", "D-ABYA"), (100, "D-ABYB" if d % 3 else "D-ABYA"),
                     (20, "D-ABYA")) for d in range(12)]
        now = T0 + timedelta(days=20)
        st = bm.build_stats(legs, now, fit=False)
        for c in ls.hold_curve(legs):
            self.assertEqual(tuple(st.hold[("all", "B748", c["band"])]), (c["k"], c["n"]))

    def test_settling_is_the_gain_between_neighbouring_bands_per_hour(self):
        legs = [seen(leg(d, "FRA", "HND", "D-ABYA"), (60, "D-ABYB"), (40, "D-ABYA")) for d in range(4)]
        steps = {(x["far"], x["near"]): x for x in ls.settling(legs, min_n=1)}
        self.assertEqual(steps[(48, 36)]["gain"], 1.0)          # wrong at 48h, right at 36h
        self.assertAlmostEqual(steps[(48, 36)]["per_hour"], 1 / 12, places=4)
        self.assertEqual(steps[(36, 24)]["gain"], 0.0)
        self.assertEqual(ls.settling(legs, min_n=5), [])

    def test_crossing_interpolates_on_log_lead(self):
        curve = [{"band": b, "p": p, "n": 100} for b, p in ((96, 0.2), (48, 0.4), (24, 0.8), (12, 0.9))]
        self.assertAlmostEqual(ls.crossing(curve, 0.6), (24 * 48) ** 0.5, places=1)
        self.assertIsNone(ls.crossing(curve, 0.95))     # even the nearest band is below
        self.assertIsNone(ls.crossing(curve, 0.1))      # never drops that low
        thin = [dict(c, n=5) for c in curve]
        self.assertIsNone(ls.crossing(thin, 0.6))


class Calendar(unittest.TestCase):
    NOW = T0 + timedelta(days=12)

    def test_block_time_is_split_at_utc_midnight(self):
        cal = ls.calendar([leg(0, "FRA", "HND", "D-ABYA", dur=780)], self.NOW)   # 12:00 + 13h
        t = cal["tails"][0]
        self.assertEqual(cal["start"], "2026-09-01")
        self.assertEqual(t["hours"][:3], [12.0, 1.0, 0.0])
        self.assertEqual(cal["flying"][:3], [1, 1, 0])

    def test_long_stops_and_the_open_one(self):
        legs = [leg(0, "FRA", "HND", "D-ABYA"), leg(1, "HND", "FRA", "D-ABYA", "717"),
                leg(7, "FRA", "HND", "D-ABYA"),                      # five days at FRA
                leg(0, "FRA", "EZE", "D-ABYB", "510"), leg(1, "EZE", "FRA", "D-ABYB", "511")]
        cal = {t["reg"]: t for t in ls.calendar(legs, self.NOW)["tails"]}
        (stop,) = cal["D-ABYA"]["stops"]
        self.assertEqual((stop["at"], stop["open"]), ("FRA", False))
        self.assertAlmostEqual(stop["days"], 5.5, places=1)          # lands day 2 00:40, leaves day 7 noon
        (still,) = cal["D-ABYB"]["stops"]                            # nothing after day 1
        self.assertTrue(still["open"])
        self.assertIsNone(still["to"])

    def test_first_seen_needs_data_from_before(self):
        legs = [leg(0, "FRA", "HND", "D-ABYA"), leg(8, "FRA", "HND", "D-ABYB")]
        cal = {t["reg"]: t for t in ls.calendar(legs, self.NOW)["tails"]}
        self.assertIsNone(cal["D-ABYA"]["first"])
        self.assertAlmostEqual(cal["D-ABYB"]["first"], 8.5)

    def test_the_last_days_are_pending_and_the_window_is_bounded(self):
        cal = ls.calendar([leg(0, "FRA", "HND", "D-ABYA")], self.NOW)
        self.assertEqual(cal["days"], 13)
        self.assertEqual(cal["complete"], 11)                        # truth lags two days
        old = [leg(-200, "FRA", "HND", "D-ABYA"), leg(0, "FRA", "HND", "D-ABYB")]
        cal = ls.calendar(old, self.NOW, days=10)
        self.assertEqual(cal["days"], 10)
        self.assertEqual([t["reg"] for t in cal["tails"]], ["D-ABYB"])   # gone too long for a row


class Turns(unittest.TestCase):
    def legs(self, n=8):
        """Every other day the tail in from HND leaves for EZE and the one in
        from EZE leaves for HND — except on the last day, when each goes back
        where it came from."""
        out = []
        for k in range(n):
            d, usual = 2 * k, k < n - 1
            out += [leg(d, "HND", "FRA", "D-ABYA", "717", dur=120),
                    leg(d, "EZE", "FRA", "D-ABYB", "511", dur=120),
                    dict(leg(d, "FRA", "EZE", "D-ABYA" if usual else "D-ABYB", "510", dur=60),
                         dep_utc=T0 + timedelta(days=d, hours=5)),
                    dict(leg(d, "FRA", "HND", "D-ABYB" if usual else "D-ABYA", "716", dur=60),
                         dep_utc=T0 + timedelta(days=d, hours=6))]
        return out

    def test_links_join_each_arrival_to_the_same_tails_next_departure(self):
        (hub,) = ls.hub_turns(self.legs(), min_turns=1)
        self.assertEqual((hub["hub"], hub["n"], hub["long"]), ("FRA", 16, 0))
        name = lambda side, i: hub[side][i]["apt"]
        links = {(name("inbound", l["i"]), name("outbound", l["o"])): l for l in hub["links"]}
        self.assertEqual(links[("HND", "EZE")]["n"], 7)
        self.assertEqual(links[("HND", "HND")]["n"], 1)              # the last day
        self.assertEqual(links[("HND", "EZE")]["turn"], 180)         # lands 14:00, leaves 17:00
        self.assertEqual(links[("HND", "EZE")]["d"], 0)
        before, after = hub["choices"]
        self.assertLess(after, before)                               # the inbound narrows it

    def test_a_stop_beyond_the_turn_limit_is_not_a_turn(self):
        legs = [leg(0, "HND", "FRA", "D-ABYA", "717", dur=120), leg(4, "FRA", "HND", "D-ABYA")]
        self.assertEqual(ls.hub_turns(legs, min_turns=1), [])
        legs += [leg(d, "HND", "FRA", "D-ABYB", "717", dur=120) for d in range(3)]
        legs += [dict(leg(d, "FRA", "EZE", "D-ABYB", "510"), dep_utc=T0 + timedelta(days=d, hours=4))
                 for d in range(3)]
        (hub,) = ls.hub_turns(legs, min_turns=1, min_node=1)
        self.assertEqual((hub["n"], hub["long"]), (3, 1))

    def test_a_hub_with_one_destination_has_nothing_to_choose(self):
        legs = []
        for d in range(6):
            legs += [leg(d, "FRA", "MUC", "D-ABPA", "96", "B789", dur=55),
                     dict(leg(d, "MUC", "FRA", "D-ABPA", "97", "B789", dur=55),
                          dep_utc=T0 + timedelta(days=d, hours=2))]
        hubs = {h["hub"]: h for h in ls.hub_turns(legs, min_turns=1)}
        self.assertEqual(hubs["MUC"]["only"], "FRA")
        self.assertEqual(hubs["MUC"]["links"], [])


class Routes(unittest.TestCase):
    def test_hold_is_split_by_direction_from_the_hub(self):
        legs = []
        for d in range(4):
            legs.append(seen(leg(d, "FRA", "HND", "D-ABYA"), (60, "D-ABYB" if d else "D-ABYA")))
            legs.append(seen(leg(d, "HND", "FRA", "D-ABYA", "717"), (60, "D-ABYA")))
        r = ls.route_hold(legs)
        (row,) = r["routes"]
        self.assertEqual((row["a"], row["b"]), ("FRA", "HND"))
        self.assertEqual((row["out"]["48"]["k"], row["out"]["48"]["n"]), (1, 4))
        self.assertEqual((row["back"]["48"]["k"], row["back"]["48"]["n"]), (4, 4))
        self.assertNotIn("72", row["out"])                           # first look was at 60h
        self.assertEqual(r["all"]["back"]["48"]["p"], 1.0)

    def test_who_flies_where_groups_by_cabin_and_skips_one_offs(self):
        legs = []
        for d in range(6):
            legs += [dict(leg(d, "FRA", "HND", "D-ABYA"), seat_config="F8C80E32M244"),
                     dict(leg(d, "MUC", "BOS", "D-AIXP", "424", "A359"), seat_config="F4C38E24M201",
                          allegris=True)]
        legs.append(dict(leg(7, "FRA", "SIN", "D-ABYA", "778"), seat_config="C88E32M244"))  # one-off
        sp = ls.tail_routes(legs)
        self.assertEqual(sp["fleet"], 2)
        self.assertEqual({(r["a"], r["b"]) for r in sp["routes"]}, {("FRA", "HND"), ("MUC", "BOS")})
        self.assertEqual(len(sp["groups"]), 2)
        by = {r["b"]: r for r in sp["routes"]}
        self.assertEqual((by["BOS"]["allegris"], by["BOS"]["first"]), (1.0, 1.0))
        self.assertEqual((by["HND"]["allegris"], by["HND"]["tails"]), (0.0, 1))
        cells = {(sp["tails"][t]["reg"], sp["routes"][r]["b"]): n for t, r, n in sp["cells"]}
        self.assertEqual(cells, {("D-ABYA", "HND"): 6, ("D-AIXP", "BOS"): 6})


class Events(unittest.TestCase):
    NOW = T0 + timedelta(days=60)

    def base(self):
        """Two tails taking turns on a daily FRA-HND, up to the truth horizon."""
        return [leg(d, "FRA", "HND", "D-ABYB" if d % 2 else "D-ABYA") for d in range(58)]

    def kinds(self, legs):
        ev = ls.events(legs, self.NOW)
        return ev, [e["kind"] for e in ev["recent"]], [e["reg"] for e in ev["now"]]

    def test_a_tail_without_a_flight_is_listed_until_it_flies_again(self):
        legs = [l for l in self.base() if not (l["truth_tail"] == "D-ABYA" and l["dep_utc"] > T0 + timedelta(days=30))]
        ev, recent, out = self.kinds(legs)
        self.assertEqual(out, ["D-ABYA"])
        self.assertIn("no flight since", ev["now"][0]["text"])
        back = legs + [leg(55, "FRA", "HND", "D-ABYA")]
        ev, recent, out = self.kinds(back)
        self.assertEqual(out, [])
        self.assertEqual(recent, ["returned"])

    def test_new_and_paused_routes_and_new_tails(self):
        legs = self.base() + [leg(d, "FRA", "SIN", "D-ABYC", "778") for d in (52, 54, 56)]
        ev, recent, _ = self.kinds(legs)
        self.assertEqual(sorted(recent), ["new_route", "new_tail"])
        # HND stops a fortnight ago; both tails carry on to EZE
        paused = [l for l in self.base() if l["dep_utc"] < self.NOW - ls.RECENT]
        paused += [leg(d, "FRA", "EZE", "D-ABYB" if d % 2 else "D-ABYA", "510") for d in range(58)]
        ev, recent, out = self.kinds(paused)
        self.assertEqual((recent, out), (["gone_route"], []))
        self.assertIn("FRA–HND", ev["recent"][0]["text"])

    def test_diversions_and_cancellations_once_each(self):
        div = leg(55, "FRA", "HND", "D-ABYA", status="DIVERTED")
        legs = self.base() + [div, dict(div), leg(56, "FRA", "HND", "", cancelled=True)]
        _, recent, _ = self.kinds(legs)
        self.assertEqual(sorted(recent), ["cancelled", "diverted"])


class Payload(unittest.TestCase):
    def test_insights_is_plain_json_for_every_family(self):
        families = {"B748": ("B748",), "A350": ("A359", "A35K")}
        legs = [seen(leg(d, "FRA", "HND", "D-ABYA"), (60, "D-ABYA")) for d in range(10)]
        now = T0 + timedelta(days=12)
        d = ls.insights(legs, legs, families, "B748", now)
        self.assertEqual(json.loads(json.dumps(d))["meta"]["flights"], 10)
        self.assertEqual([f["id"] for f in d["trust"]["fleets"]], ["B748", "A350"])
        self.assertIsNotNone(d["trust"]["fleets"][0]["settling"])
        empty = ls.insights([], legs, families, "A350", now)       # a fleet nobody has seen yet
        self.assertEqual(json.loads(json.dumps(empty))["calendar"]["tails"], [])


if __name__ == "__main__":
    unittest.main()
