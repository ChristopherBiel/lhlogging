"""Unit tests for dashboard/photos.py (one Wikimedia Commons photo per tail,
kept on this server). No network: Commons is a fake.

    python3 -m unittest discover tests
"""
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dashboard"))
import photos  # noqa: E402

JPEG = b"\xff\xd8\xff\xe0" + b"\0" * 20_000


def page(pid, title, width=4000, height=2667, taken="2026-03-20", license="CC BY-SA 4.0",
         mime="image/jpeg", artist='<a href="//commons.wikimedia.org/wiki/User:X">Sg1959</a>',
         description="", info=True):
    p = {"pageid": pid, "title": "File:" + title}
    if info:
        p["imageinfo"] = [{
            "descriptionurl": "https://commons.wikimedia.org/wiki/File:" + title.replace(" ", "_"),
            "url": "https://upload.wikimedia.org/o/%d.jpg" % pid,
            "thumburl": "https://upload.wikimedia.org/t/1280px-%d.jpg" % pid,
            "thumbwidth": 1280, "thumbheight": round(1280 * height / width),
            "width": width, "height": height, "mime": mime, "timestamp": "2026-04-01T10:00:00Z",
            "extmetadata": {"Artist": {"value": artist}, "LicenseShortName": {"value": license},
                            "LicenseUrl": {"value": "https://creativecommons.org/licenses/by-sa/4.0"},
                            "DateTimeOriginal": {"value": taken},
                            "ImageDescription": {"value": description}}}]
    return p


def files(*pages):
    return photos.candidates(list(pages))


class FakeCommons:
    """Answers the three queries photos.py asks: a category, a title search, a title."""

    def __init__(self, category=(), search=(), titles=()):
        self.category, self.search, self.titles = list(category), list(search), list(titles)
        self.asked = []

    def __call__(self, params):
        if params.get("generator") == "categorymembers":
            self.asked.append("category")
            return {"query": {"pages": self.category}}
        if params.get("generator") == "search":
            self.asked.append("search")
            return {"query": {"pages": self.search}}
        self.asked.append("titles")
        return {"query": {"pages": self.titles}}


class TakenTest(unittest.TestCase):
    def test_formats(self):
        self.assertEqual(photos.taken("2026-03-20 10:47:37"), "2026-03-20")
        self.assertEqual(photos.taken("2017:05:13 17:54:00"), "2017-05-13")
        self.assertEqual(photos.taken("24 November 2009"), "2009-11-24")
        self.assertEqual(photos.taken("November 24, 2009"), "2009-11-24")
        self.assertEqual(photos.taken('<time class="dtstart" datetime="2019-10">October 2019</time>'), "2019-10-01")
        self.assertEqual(photos.taken("circa 1987"), "1987-01-01")

    def test_falls_back_to_the_upload_day(self):
        self.assertEqual(photos.taken("", "2026-04-01T10:00:00Z"), "2026-04-01")


class CandidatesTest(unittest.TestCase):
    def test_a_continued_query_repeats_pages_without_their_info(self):
        c = files(page(1, "D-ABYN.jpg", info=False), page(1, "D-ABYN.jpg"), page(2, "x.jpg", info=False))
        self.assertEqual([x["title"] for x in c], ["File:D-ABYN.jpg"])

    def test_credit_is_plain_text(self):
        c = files(page(1, "D-ABYN.jpg"))[0]
        self.assertEqual(c["artist"], "Sg1959")
        self.assertEqual((c["src_width"], c["src_height"]), (1280, 853))


class RulesTest(unittest.TestCase):
    def test_names_reg(self):
        self.assertTrue(photos.names_reg("File:D-ABYN AIRCRAFT Boeing 747-830.jpg", "D-ABYN"))
        self.assertTrue(photos.names_reg("File:Lufthansa DABYN at FRA.jpg", "D-ABYN"))
        self.assertTrue(photos.names_reg("File:D-ABYN@PEK (2019).jpg", "D-ABYN"))
        self.assertFalse(photos.names_reg("File:D-ABYNX.jpg", "D-ABYN"))
        self.assertFalse(photos.names_reg("File:XD-ABYN.jpg", "D-ABYN"))

    def test_another_type_on_the_same_registration(self):
        self.assertTrue(photos.names_other("D-ABYR Boeing B747-230B Lufthansa LHR 31.1.87", "B748"))
        self.assertTrue(photos.names_other("Dornier Do 18 D-ABYM taking off 1936", "B748"))
        self.assertTrue(photos.names_other("Air Berlin DHC-8-400 Dash 8; D-ABQC@ZRH", "B789"))
        self.assertFalse(photos.names_other("D-ABYN AIRCRAFT Boeing 747-830 -1b", "B748"))
        self.assertFalse(photos.names_other("Lufthansa 747-8i D-ABYC departing Boston", "B748"))
        self.assertFalse(photos.names_other("Airbus A350-941 (c-n 340, D-AIXQ)", "A359"))
        self.assertFalse(photos.names_other("Lufthansa A321neo D-AIEA", "A21N"))
        self.assertFalse(photos.names_other("D-ABYN.jpg", "B748"))
        # its own type and another in one frame: kept, the title names the tail's
        self.assertFalse(photos.names_other("A380 and A340 at FRA, D-AIMA", "A388"))

    def test_usable(self):
        ok = lambda **kw: photos.usable(files(page(1, kw.pop("title", "D-ABYN.jpg"), **kw))[0])
        self.assertTrue(ok())
        self.assertFalse(ok(license="GFDL 1.2"))              # wants its whole text shown
        self.assertTrue(ok(license="CC0"))
        self.assertTrue(ok(license="Public domain"))
        self.assertFalse(ok(width=1000, height=667))           # soft at 2x
        self.assertFalse(ok(width=3024, height=4032))          # portrait
        self.assertFalse(ok(width=4032, height=3024))          # 4:3, mostly from a seat
        self.assertFalse(ok(mime="image/png"))
        self.assertFalse(ok(title="D-ABYN Business Class seat.jpg"))
        self.assertFalse(ok(title="D-ABYN wing view over Siberia.jpg"))
        self.assertFalse(ok(title="D-ABYN cockpit.jpg"))


class PickTest(unittest.TestCase):
    def test_the_registration_in_the_title_first(self):
        best = photos.pick(files(page(1, "Observation Point East, Frankfurt.jpg", taken="2026-09-01"),
                                 page(2, "D-ABYN at FRA.jpg", taken="2025-06-01")), "D-ABYN", "B748")
        self.assertEqual(best["title"], "File:D-ABYN at FRA.jpg")

    def test_the_current_livery_before_the_type_named(self):
        best = photos.pick(files(page(1, "D-ABYN Boeing 747-830 2015.jpg", taken="2015-05-01"),
                                 page(2, "D-ABYN.jpg", taken="2026-06-20")), "D-ABYN", "B748")
        self.assertEqual(best["title"], "File:D-ABYN.jpg")

    def test_the_newest(self):
        best = photos.pick(files(page(1, "D-ABYN a.jpg", taken="2026-03-20"),
                                 page(2, "D-ABYN b.jpg", taken="2026-06-20")), "D-ABYN", "B748")
        self.assertEqual(best["title"], "File:D-ABYN b.jpg")

    def test_old_scans_and_other_types_are_out(self):
        self.assertIsNone(photos.pick(files(page(1, "D-AIDC 21-9-90 Frankfurt.jpg", taken="1990-09-21")), "D-AIDC", "A321"))
        self.assertIsNone(photos.pick(files(page(1, "D-ABYR Boeing B747-230B Lufthansa LHR 31.1.87.jpg",
                                                 taken="2026-04-12")), "D-ABYR", "B748"))

    def test_a_found_file_must_name_registration_and_type(self):
        found = files(page(1, "D-ABQA (6192700242).jpg"),
                      page(2, "Lufthansa 787 at MUC.jpg", description="D-ABQA, Boeing 787-9"))
        self.assertIsNone(photos.pick(found, "D-ABQA", "B789", trusted=False))
        found = files(page(3, "D-ABQA (6192700242).jpg", description="Lufthansa Boeing 787-9 at MUC"))
        self.assertEqual(photos.pick(found, "D-ABQA", "B789", trusted=False)["title"], "File:D-ABQA (6192700242).jpg")


class LookUpTest(unittest.TestCase):
    def test_category_first(self):
        commons = FakeCommons(category=[page(1, "D-ABYN.jpg")], search=[page(2, "D-ABYN b.jpg")])
        self.assertEqual(photos.look_up("D-ABYN", "B748", commons)["title"], "File:D-ABYN.jpg")
        self.assertEqual(commons.asked, ["category"])

    def test_search_when_the_category_has_nothing(self):
        commons = FakeCommons(search=[page(2, "Lufthansa Boeing 747-8 D-ABYA FRA.jpg")])
        self.assertEqual(photos.look_up("D-ABYA", "B748", commons)["title"], "File:Lufthansa Boeing 747-8 D-ABYA FRA.jpg")
        self.assertEqual(commons.asked, ["category", "search"])

    def test_no_search_without_a_known_type(self):
        commons = FakeCommons(search=[page(2, "D-AXXX Global Express.jpg")])
        self.assertIsNone(photos.look_up("D-AXXX", "GLEX", commons))
        self.assertEqual(commons.asked, ["category"])

    def test_overrides(self):
        commons = FakeCommons(category=[page(1, "D-ABYN.jpg")], titles=[page(9, "D-ABYN chosen.jpg")])
        try:
            photos.OVERRIDES["D-ABYN"] = "File:D-ABYN chosen.jpg"
            self.assertEqual(photos.look_up("D-ABYN", "B748", commons)["title"], "File:D-ABYN chosen.jpg")
            photos.OVERRIDES["D-ABYN"] = None
            self.assertIsNone(photos.look_up("D-ABYN", "B748", commons))
        finally:
            photos.OVERRIDES.pop("D-ABYN", None)


class RefreshTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.store = photos.Store(self.dir.name)
        self.downloads = []

    def tearDown(self):
        self.dir.cleanup()

    def get_bytes(self, url):
        self.downloads.append(url)
        return JPEG

    def refresh(self, commons, t=1000.0):
        return photos.refresh("D-ABYN", "B748", self.store, commons, self.get_bytes, now=lambda: t)

    def test_keeps_the_copy_and_its_credit(self):
        self.refresh(FakeCommons(category=[page(1, "D-ABYN.jpg")]))
        self.assertEqual(self.store.image("D-ABYN").read_bytes(), JPEG)
        pub = self.store.public("D-ABYN")
        self.assertRegex(pub["src"], r"^/photos/D-ABYN\.jpg\?v=[0-9a-f]{10}$")
        self.assertEqual((pub["width"], pub["height"], pub["artist"], pub["license"], pub["taken"]),
                         (1280, 853, "Sg1959", "CC BY-SA 4.0", "2026-03-20"))
        self.assertEqual(pub["link"], "https://commons.wikimedia.org/wiki/File:D-ABYN.jpg")

    def test_downloads_only_when_the_pick_changes(self):
        self.refresh(FakeCommons(category=[page(1, "D-ABYN.jpg")]))
        rec = self.refresh(FakeCommons(category=[page(1, "D-ABYN.jpg")]), t=2000.0)
        self.assertEqual(len(self.downloads), 1)
        self.assertEqual(rec["checked"], 2000.0)
        self.refresh(FakeCommons(category=[page(1, "D-ABYN.jpg"), page(2, "D-ABYN new.jpg", taken="2026-10-01")]))
        self.assertEqual(len(self.downloads), 2)

    def test_none_left_drops_the_copy(self):
        self.refresh(FakeCommons(category=[page(1, "D-ABYN.jpg")]))
        rec = self.refresh(FakeCommons())
        self.assertIsNone(rec["photo"])
        self.assertIsNone(self.store.image("D-ABYN"))
        self.assertIsNone(self.store.public("D-ABYN"))
        self.assertEqual(self.store.meta("D-ABYN")["checked"], 1000.0)

    def test_refuses_what_is_not_a_jpeg(self):
        with self.assertRaises(ValueError):
            photos.refresh("D-ABYN", "B748", self.store, FakeCommons(category=[page(1, "D-ABYN.jpg")]),
                           lambda url: b"<html>rate limited</html>")
        self.assertIsNone(self.store.meta("D-ABYN"))

    def test_store_refuses_a_path(self):
        self.assertIsNone(self.store.meta("../../etc"))
        with self.assertRaises(ValueError):
            self.store.save("../x", {})


class Clock:
    def __init__(self):
        self.t = 10 * photos.RECHECK_S

    def __call__(self):
        return self.t


class KeeperTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.store = photos.Store(self.dir.name)
        self.clock = Clock()
        self.fleet = [("D-ABYA", "B748"), ("D-ABYN", "B748"), ("D-AIMA", "A388")]
        self.reads, self.done, self.fail = 0, [], set()
        self.keeper = photos.Keeper(self.store, self.tails, self.refresh, clock=self.clock)

    def tearDown(self):
        self.dir.cleanup()

    def tails(self):
        self.reads += 1
        return self.fleet

    def refresh(self, reg, ftype, store):
        if reg in self.fail:
            raise OSError("Commons did not answer")
        self.done.append(reg)
        store.save(reg, {"reg": reg, "checked": self.clock(), "photo": None})

    def test_never_seen_first_then_the_longest_ago(self):
        self.store.save("D-ABYA", {"checked": self.clock() - 8 * 86400, "photo": None})
        self.store.save("D-ABYN", {"checked": self.clock() - 9 * 86400, "photo": None})
        self.assertEqual([r for r, _ in self.keeper.due(self.fleet)], ["D-AIMA", "D-ABYN", "D-ABYA"])

    def test_a_tail_looked_at_this_week_waits(self):
        self.store.save("D-ABYA", {"checked": self.clock() - 86400, "photo": None})
        self.assertEqual([r for r, _ in self.keeper.due(self.fleet)], ["D-ABYN", "D-AIMA"])

    def test_one_fleet_read_per_round(self):
        waits = [self.keeper.step() for _ in range(4)]
        self.assertEqual(self.done, ["D-ABYA", "D-ABYN", "D-AIMA"])
        self.assertEqual(waits, [photos.PAUSE_S] * 3 + [3600])
        self.assertEqual(self.reads, 2)  # the round, then the empty one that finds nothing due

    def test_a_failing_tail_does_not_hold_up_the_walk(self):
        self.fail = {"D-ABYA"}
        self.assertEqual(self.keeper.step(), photos.RETRY_S)
        self.keeper.step()
        self.keeper.step()
        self.assertEqual(self.done, ["D-ABYN", "D-AIMA"])
        self.clock.t += 3600
        self.assertEqual(self.keeper.due(self.fleet), [])  # sits out a day
        self.clock.t += photos.SKIP_S
        self.assertEqual([r for r, _ in self.keeper.due(self.fleet)], ["D-ABYA"])

    def test_a_wanted_tail_goes_first(self):
        self.keeper.step()
        self.keeper.want("D-AXYZ", "A20N")
        self.assertTrue(self.keeper.wake.is_set())
        self.keeper.step()
        self.assertEqual(self.done, ["D-ABYA", "D-AXYZ"])


if __name__ == "__main__":
    unittest.main()
