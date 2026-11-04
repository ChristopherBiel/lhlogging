"""Unit tests for flightstatus/legs.py (the shared leg builder).

    python3 -m unittest discover tests
"""
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "flightstatus"))
import legs  # noqa: E402

UTC = timezone.utc


def fis(y, mo, d, h, mi):
    """A FIS time: local wall clock stamped +0000."""
    return datetime(y, mo, d, h, mi, tzinfo=UTC)


def look(at, reg, status="ONTIME", dep=fis(2026, 9, 28, 14, 5)):
    return {"observed_at": at, "found": True, "registration": reg,
            "dep_scheduled": dep, "arr_scheduled": fis(2026, 9, 29, 9, 45),
            "dep_airport_iata": "FRA", "arr_airport_iata": "HND", "overall_status": status,
            "seed_type": "B748", "fleet_type": "B748", "fis_type": "Boeing 747-8",
            "flight_duration": "PT12H40M"}


class DepUtc(unittest.TestCase):
    def test_hub_departure_converts_berlin_summer_time(self):
        self.assertEqual(legs.dep_utc(fis(2026, 9, 28, 14, 5), None, "FRA", "HND", 760),
                         datetime(2026, 9, 28, 12, 5, tzinfo=UTC))

    def test_hub_departure_converts_berlin_winter_time(self):
        self.assertEqual(legs.dep_utc(fis(2026, 12, 1, 14, 5), None, "FRA", "HND", 760),
                         datetime(2026, 12, 1, 13, 5, tzinfo=UTC))

    def test_outstation_departure_backs_off_block_time_from_hub_arrival(self):
        # EZE 16:00 local (UTC-3) -> FRA 09:00 local next day (UTC+2), 12h block
        got = legs.dep_utc(fis(2026, 9, 28, 16, 0), fis(2026, 9, 29, 9, 0), "EZE", "FRA", 720)
        self.assertEqual(got, datetime(2026, 9, 28, 19, 0, tzinfo=UTC))

    def test_no_hub_keeps_fis_clock(self):
        t = fis(2026, 9, 28, 8, 0)
        self.assertEqual(legs.dep_utc(t, None, "SSG", "LOS", 60), t)

    def test_session_time_zone_does_not_shift_the_wall_clock(self):
        # the same instant handed back in a +02:00 session must give the same answer
        berlin_session = fis(2026, 9, 28, 14, 5).astimezone(timezone(timedelta(hours=2)))
        self.assertEqual(legs.dep_utc(berlin_session, None, "FRA", "HND", 760),
                         datetime(2026, 9, 28, 12, 5, tzinfo=UTC))


class BuildLeg(unittest.TestCase):
    def setUp(self):
        dep_utc = datetime(2026, 9, 28, 12, 5, tzinfo=UTC)
        self.obs = [
            look(dep_utc - timedelta(hours=100), "D-ABYT"),
            look(dep_utc - timedelta(hours=60), "D-ABYR"),
            look(dep_utc - timedelta(hours=30), "D-ABYQ"),
            look(dep_utc - timedelta(hours=5), "D-ABYQ"),
            look(dep_utc + timedelta(hours=30), "D-ABYQ", status="ARRIVED"),
        ]
        self.row, self.changes = legs.build_leg(("2026-09-28", "LH", "716"), self.obs)

    def test_truth_and_changes(self):
        self.assertEqual(self.row["truth_tail"], "D-ABYQ")
        self.assertEqual(self.row["n_changes"], 2)
        self.assertEqual([c["to_tail"] for c in self.changes], ["D-ABYR", "D-ABYQ"])

    def test_lead_bands_score_against_truth(self):
        self.assertEqual(self.row["tail_at_96h"], "D-ABYT")
        self.assertEqual(self.row["hold_96h"], 0)
        self.assertEqual(self.row["tail_at_24h"], "D-ABYQ")
        self.assertEqual(self.row["hold_24h"], 1)
        self.assertEqual(self.row["tail_at_120h"], "")   # first look was at 100h

    def test_timeline_is_collapsed_and_answers_tail_at(self):
        tl = self.row["timeline"]
        self.assertEqual([t for _, t in tl], ["D-ABYT", "D-ABYR", "D-ABYQ"])
        first = self.row["first_lead_h"]
        self.assertEqual(legs.tail_at(tl, first, 72), "D-ABYT")   # D-ABYR only from 60h
        self.assertEqual(legs.tail_at(tl, first, 50), "D-ABYR")
        self.assertEqual(legs.tail_at(tl, first, 10), "D-ABYQ")
        self.assertIsNone(legs.tail_at(tl, first, 150))

    def test_sold_layout_comes_from_the_operated_look(self):
        obs = [dict(o, seat_config="F8C80E32M244", allegris=False) for o in self.obs[:-1]]
        obs.append(dict(self.obs[-1], seat_config="C88E32M244", allegris=False))
        row, _ = legs.build_leg(("2026-09-28", "LH", "754"), obs)
        self.assertEqual((row["seat_config"], row["allegris"]), ("C88E32M244", 0))
        row, _ = legs.build_leg(("2026-09-28", "LH", "754"), obs[:-1])   # not flown yet
        self.assertEqual(row["seat_config"], "F8C80E32M244")

    def test_leg_never_found_is_skipped(self):
        o = dict(self.obs[0], found=False)
        self.assertEqual(legs.build_leg(("2026-09-28", "LH", "716"), [o]), (None, []))


class StandIns(unittest.TestCase):
    """A return leg's look names its outbound (previousFlight) and carries its
    tail: what the collector reads an outbound through since 2026-11-01."""
    DEP = datetime(2026, 9, 28, 12, 5, tzinfo=UTC)          # LH716 FRA 14:05

    def back(self, at, reg, prev_date="2026-09-28", dep="HND"):
        """LH717 HND -> FRA the next day, naming LH716 as the flight before."""
        return dict(look(at, reg), flight_date="2026-09-29", airline="LH", flight_number="717",
                    dep_airport_iata=dep, arr_airport_iata="FRA", prev_airline="LH",
                    prev_flight_number="0716", prev_flight_date=prev_date)

    def test_a_return_leg_names_its_outbound(self):
        got = legs.stand_ins([self.back(self.DEP - timedelta(hours=50), "D-ABYQ"),
                              self.back(self.DEP - timedelta(hours=40), "D-ABYQ", dep="FRA"),      # left a hub
                              dict(self.back(self.DEP - timedelta(hours=30), "D-ABYQ"), found=False)])
        self.assertEqual(list(got), [("2026-09-28", "LH", "716")])                         # zero-padding off
        (one,) = got[("2026-09-28", "LH", "716")]
        self.assertEqual((one["registration"], one["derived"], one["observed_at"]),
                         ("D-ABYQ", True, self.DEP - timedelta(hours=50)))

    def test_its_looks_fill_the_outbounds_gaps_not_its_schedule(self):
        own = [look(self.DEP - timedelta(hours=100), "D-ABYT"), look(self.DEP - timedelta(hours=5), "D-ABYQ")]
        stand = legs.stand_ins([self.back(self.DEP - timedelta(hours=h), reg) for h, reg in
                                ((98, "D-ABYR"), (60, "D-ABYR"), (30, "D-ABYQ"))])[("2026-09-28", "LH", "716")]
        row, changes = legs.build_leg(("2026-09-28", "LH", "716"), own + stand)
        # the look 2 h after its own is left out; the two in the gap count
        self.assertEqual([t for _, t in row["timeline"]], ["D-ABYT", "D-ABYR", "D-ABYQ"])
        self.assertEqual((row["n_obs"], row["n_derived"]), (2, 2))
        self.assertEqual(row["tail_at_48h"], "D-ABYR")
        self.assertEqual((row["dep_airport"], row["arr_airport"], row["latest_status"]), ("FRA", "HND", "ONTIME"))
        self.assertEqual([c["to_tail"] for c in changes], ["D-ABYR", "D-ABYQ"])

    def test_the_newest_look_is_the_published_tail_whoever_took_it(self):
        own = [look(self.DEP - timedelta(hours=100), "D-ABYT")]
        stand = legs.stand_ins([self.back(self.DEP - timedelta(hours=20), "D-ABYR")])[("2026-09-28", "LH", "716")]
        row, _ = legs.build_leg(("2026-09-28", "LH", "716"), own + stand)
        self.assertEqual((row["latest_tail"], row["latest_observed_at"]),
                         ("D-ABYR", (self.DEP - timedelta(hours=20)).isoformat()))
        self.assertEqual(legs.build_leg(("2026-09-28", "LH", "716"), stand), (None, []))   # none of its own


class FlownTail(unittest.TestCase):
    DEP = datetime(2026, 9, 28, 12, 5, tzinfo=UTC)

    def test_one_aircraft_under_its_callsign_near_departure(self):
        key = ("2026-09-28", "LH", "716")
        flights = [("DLH716", "D-ABYQ", "B748", self.DEP + timedelta(minutes=20)),
                   ("DLH716", "D-ABYN", "B748", self.DEP + timedelta(hours=26)),     # the next day's
                   ("DLH717", "D-ABYR", "B748", self.DEP)]
        self.assertEqual(legs.flown_tails([(key, "716", self.DEP)], flights), {key: ("D-ABYQ", "B748")})
        two = flights + [("DLH716", "D-ABYS", "B748", self.DEP - timedelta(hours=2))]
        self.assertEqual(legs.flown_tails([(key, "716", self.DEP)], two), {})            # two: no answer
        self.assertEqual(legs.flown_tails([(key, "0716", self.DEP)], flights[:1]), {key: ("D-ABYQ", "B748")})

    def test_it_is_the_truth_where_the_feed_has_none(self):
        obs = [look(self.DEP - timedelta(hours=30), "D-ABYR"), look(self.DEP - timedelta(hours=5), "D-ABYR")]
        row, _ = legs.build_leg(("2026-09-28", "LH", "716"), obs, flown=("D-ABYQ", "B748"))
        self.assertEqual((row["truth_tail"], row["truth_status"], row["truth_observed_at"]), ("D-ABYQ", "ADSB", ""))
        self.assertEqual((row["hold_24h"], row["reassigned"], row["fleet_type"]), (0, 1, "B748"))
        # the feed's own word wins; a cancelled flight has none
        flown = obs + [look(self.DEP + timedelta(hours=14), "D-ABYR", status="ARRIVED")]
        self.assertEqual(legs.build_leg(("2026-09-28", "LH", "716"), flown, flown=("D-ABYQ", "B748"))[0]["truth_tail"],
                         "D-ABYR")
        gone = obs + [look(self.DEP - timedelta(hours=2), "D-ABYR", status="CANCELLED")]
        self.assertEqual(legs.build_leg(("2026-09-28", "LH", "716"), gone, flown=("D-ABYQ", "B748"))[0]["truth_tail"], "")


class MultiLeg(unittest.TestCase):
    """LH569: SSG 21:00 -> LOS 22:25 (1h25), LOS 23:35 -> FRA 06:50 (6h15),
    local clocks (Malabo and Lagos UTC+1, Frankfurt UTC+2)."""

    SEGS = [["SSG", "LOS", "2026-10-04T21:00:00.000+0000", "2026-10-04T22:25:00.000+0000", "PT1H25M"],
            ["LOS", "FRA", "2026-10-04T23:35:00.000+0000", "2026-10-05T06:50:00.000+0000", "PT6H15M"]]

    def look(self, at, reg, status="ONTIME", segs=SEGS):
        r = look(at, reg, status, dep=fis(2026, 10, 4, 21, 0))
        # the columns describe the first leg only, as the collector stores them
        r.update(dep_airport_iata="SSG", arr_airport_iata="LOS",
                 arr_scheduled=fis(2026, 10, 4, 22, 25), flight_duration="PT1H25M",
                 segments=json.dumps(segs), seed_type="B789", fleet_type="B789")
        return r

    def test_itinerary_ends_stop_and_gate_to_gate_time(self):
        dep = datetime(2026, 10, 4, 20, 0, tzinfo=UTC)
        row, _ = legs.build_leg(("2026-10-04", "LH", "569"), [
            self.look(dep - timedelta(hours=30), "D-ABPO"),
            self.look(dep + timedelta(hours=14), "D-ABPO", "ARRIVED")])
        self.assertEqual((row["dep_airport"], row["stops"], row["arr_airport"]), ("SSG", "LOS", "FRA"))
        self.assertEqual(row["duration_min"], 85 + 70 + 375)
        # anchored on the Frankfurt arrival, backed off the whole itinerary
        self.assertEqual(row["dep_scheduled_utc"], dep.isoformat())
        segs = json.loads(row["segments"])
        self.assertEqual([(g["dep"], g["arr"], g["duration_min"]) for g in segs],
                         [("SSG", "LOS", 85), ("LOS", "FRA", 375)])

    def test_a_single_leg_flight_has_no_segments(self):
        dep_utc = datetime(2026, 9, 28, 12, 5, tzinfo=UTC)
        row, _ = legs.build_leg(("2026-09-28", "LH", "716"), [look(dep_utc - timedelta(hours=30), "D-ABYQ")])
        self.assertEqual((row["stops"], row["segments"]), ("", ""))
        self.assertEqual(row["duration_min"], 760)

    def test_a_diversion_ends_where_the_operated_look_says(self):
        # planned FRA-HND, operated FRA-CTS-HND (a stop added on the day)
        segs = [["FRA", "CTS", "2026-09-28T14:05:00.000+0000", "2026-09-29T08:00:00.000+0000", "PT11H55M"],
                ["CTS", "HND", "2026-09-29T10:00:00.000+0000", "2026-09-29T11:35:00.000+0000", "PT1H35M"]]
        dep_utc = datetime(2026, 9, 28, 12, 5, tzinfo=UTC)
        planned = [look(dep_utc - timedelta(hours=h), "D-ABYQ") for h in (60, 30, 6)]
        operated = look(dep_utc + timedelta(hours=20), "D-ABYQ", "ARRIVED")
        operated["segments"] = segs
        row, _ = legs.build_leg(("2026-09-28", "LH", "716"), planned + [operated])
        self.assertEqual((row["dep_airport"], row["stops"], row["arr_airport"]), ("FRA", "CTS", "HND"))
        self.assertEqual(row["dep_scheduled_utc"], dep_utc.isoformat())
        # still ahead: the itinerary most looks show
        row, _ = legs.build_leg(("2026-09-28", "LH", "716"), planned + [dict(planned[-1], segments=segs)])
        self.assertEqual((row["stops"], row["arr_airport"]), ("", "HND"))


if __name__ == "__main__":
    unittest.main()
