"""Unit tests for flightstatus/census.py (which flight numbers to ask the feed
for beyond the catalog) and for what the collector's schedule promises about
the census and the timetable look.

    python3 -m unittest discover tests
"""
import os
import re
import sys
import types
import unittest
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "flightstatus"))
import census  # noqa: E402

DAY = date(2026, 10, 14)


class Slice(unittest.TestCase):
    def test_parse_block(self):
        self.assertEqual(census.parse_block("400-403"), [400, 401, 402, 403])
        self.assertEqual(census.parse_block("96-97, 400-401,x,7-"), [96, 97, 400, 401])
        self.assertEqual(census.parse_block(""), [])

    def test_every_unknown_number_comes_round_once_a_cycle(self):
        block = census.parse_block("400-799")
        known = {"400", "401", 716, "LH9"}                    # as the catalog spells them, and noise
        asked = [n for d in range(6) for n in census.census_slice(block, known, DAY + timedelta(days=d), 6)]
        self.assertEqual(sorted(asked), [n for n in block if n not in (400, 401, 716)])
        self.assertEqual(len(asked), len(set(asked)))
        sizes = [len(census.census_slice(block, known, DAY + timedelta(days=d), 6)) for d in range(6)]
        self.assertLessEqual(max(sizes) - min(sizes), 2)      # an even sixth a day

    def test_the_weekday_asked_about_moves_on_each_round(self):
        days = [d for d in range(42) if 500 in census.census_slice([500], (), DAY + timedelta(days=d), 6)]
        self.assertEqual(len(days), 7)
        self.assertEqual({(DAY + timedelta(days=d)).weekday() for d in days}, set(range(7)))

    def test_the_whole_block_at_once(self):
        self.assertEqual(census.census_slice([400, 401, 402], {"401"}, DAY, 1), [400, 402])


class Schedule(unittest.TestCase):
    """What flightstatus/crontab and the image have to hold for the above to run."""

    def lines(self):
        return [ln for ln in (ROOT / "flightstatus" / "crontab").read_text().splitlines()
                if re.match(r"^\d+\s+(\d+|\*)\s+\* \* \*\s+root\s+.*run_nightly\.sh", ln)]

    def test_the_census_has_its_slot_between_two_runs_of_the_plan(self):
        slot = [ln for ln in self.lines() if "--census" in ln]
        self.assertEqual(len(slot), 1)
        minute, hour = (int(x) for x in slot[0].split()[:2])
        wait = int(re.search(r"NIGHTLY_JITTER_MAX_S=(\d+)", slot[0]).group(1)) // 60
        due = int(next(ln for ln in self.lines() if "--due" in ln).split()[0])
        self.assertEqual(hour, 8)                                             # after the morning's first open hours
        self.assertLessEqual(minute + wait + 15, 60 + due - 3)                # done before the next run's deadline

    def test_no_timetable_look_rides_on_a_run(self):
        # the plan reads every day it shows (plan.py); the look stays for runs by hand
        self.assertFalse([ln for ln in self.lines() if "FIS_TIMETABLE_OFFSETS" in ln])

    def test_the_image_copies_every_module_the_collector_imports(self):
        src = (ROOT / "flightstatus" / "fetch_flightstatus.py").read_text()
        local = {m for m in re.findall(r"^import (\w+)$", src, re.M) if (ROOT / "flightstatus" / (m + ".py")).exists()}
        copied = re.search(r"^COPY (.*) \./$", (ROOT / "flightstatus" / "Dockerfile").read_text(), re.M).group(1).split()
        self.assertEqual(local, {"legs", "census", "door", "plan"})
        self.assertTrue({m + ".py" for m in local} <= set(copied), copied)


@unittest.skipIf(sys.version_info < (3, 10), "the collector is written for the image's Python (3.10+)")
class Answers(unittest.TestCase):
    """fetch_flightstatus.parse_flight, loaded without a database or a browser."""

    @classmethod
    def setUpClass(cls):
        stubs = {"psycopg": types.ModuleType("psycopg"), "psycopg.types": types.ModuleType("psycopg.types"),
                 "psycopg.types.json": types.ModuleType("psycopg.types.json"),
                 "playwright": types.ModuleType("playwright"),
                 "playwright.sync_api": types.ModuleType("playwright.sync_api")}
        stubs["psycopg"].Connection = object
        stubs["psycopg.types.json"].Jsonb = object
        stubs["playwright.sync_api"].sync_playwright = None
        cls.saved = {k: sys.modules.get(k) for k in stubs}
        cls.env = {k: os.environ.get(k) for k in ("POSTGRES_USER", "POSTGRES_PASSWORD")}
        sys.modules.update(stubs)
        os.environ.setdefault("POSTGRES_USER", "test")
        os.environ.setdefault("POSTGRES_PASSWORD", "test")
        sys.modules.pop("fetch_flightstatus", None)
        import fetch_flightstatus
        cls.fs = fetch_flightstatus

    @classmethod
    def tearDownClass(cls):
        sys.modules.pop("fetch_flightstatus", None)
        for k, v in cls.saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
        for k, v in cls.env.items():
            if v is None:
                os.environ.pop(k, None)

    def parse(self, payload):
        return self.fs.parse_flight(payload, "716", DAY, "B748")

    def test_a_refused_lookup_leaves_no_answer(self):
        obs = self.parse(None)
        self.assertEqual((obs["found"], obs["raw"]), (False, None))

    def test_no_such_flight_is_kept_as_an_answer(self):
        for payload in ({}, {"flights": []}):                 # the JSON 404, an empty list
            obs = self.parse(payload)
            self.assertEqual((obs["found"], obs["raw"]), (False, "{}"), payload)

    def test_an_answer_of_another_shape_stays_unknown(self):
        obs = self.parse({"error": "try again later"})
        self.assertEqual((obs["found"], obs["raw"]), (False, None))

    def test_a_flight_is_found(self):
        obs = self.parse({"flights": [{"aircraftInfo": {"aircraftRegistration": "DABYN"}, "legs": [
            {"overallStatus": "ONTIME", "departure": {"departureAirport": "FRA"}, "arrival": {"arrivalAirport": "HND"}}]}]})
        self.assertEqual((obs["found"], obs["registration"], obs["dep_airport_iata"]), (True, "D-ABYN", "FRA"))
        self.assertTrue(obs["raw"].startswith("{"))


if __name__ == "__main__":
    unittest.main()
