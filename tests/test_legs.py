"""Unit tests for flightstatus/legs.py (the shared leg builder).

    python3 -m unittest discover tests
"""
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

    def test_leg_never_found_is_skipped(self):
        o = dict(self.obs[0], found=False)
        self.assertEqual(legs.build_leg(("2026-09-28", "LH", "716"), [o]), (None, []))


if __name__ == "__main__":
    unittest.main()
