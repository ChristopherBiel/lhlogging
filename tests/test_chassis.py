"""Unit tests for dashboard/chassis.py (the page every route is built on: the
top bar with the project's glyph and name, the footer with the signature, one
intensity) and for what it needs around it: the files it serves, the glyph,
and the Docker image that has to carry them.

    python3 -m unittest discover tests
"""
import glob
import re
import shlex
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DASH = ROOT / "dashboard"
sys.path.insert(0, str(DASH))
sys.path.insert(0, str(ROOT / "tools"))
import chassis  # noqa: E402
import _preview  # noqa: E402

GLYPH = DASH / "static" / "icons" / "lh-fleet.svg"


def path_data(svg):
    return re.findall(r'\bd="([^"]+)"', svg)


class PageTest(unittest.TestCase):
    def setUp(self):
        self.html = chassis.page("Catch a Tail & more", "/book", "<section id='lead'></section>",
                                 styles=("book.css",), scripts=("book.js",))
        self.top = self.html.split('<header class="fp-top">')[1].split("</header>")[0]
        self.foot = self.html.split('<footer class="fp-foot">')[1].split("</footer>")[0]

    def test_every_slot_is_filled(self):
        self.assertNotIn("{{", self.html)
        self.assertIn("<section id='lead'></section>", self.html)
        self.assertIn("<title>Catch a Tail &amp; more</title>", self.html)

    def test_one_intensity_for_every_page(self):
        """Declared once, in chassis.html, as a level of the dial; which one is the site's choice."""
        levels = re.findall(r'data-fp-intensity="([^"]*)"', self.html)
        self.assertEqual(len(levels), 1)
        self.assertIn(levels[0], ("01", "02", "03", "04"))
        self.assertEqual(re.findall(r'data-fp-intensity="([^"]*)"', (DASH / "chassis.html").read_text()), levels)

    def test_the_current_page_is_marked(self):
        self.assertEqual(re.findall(r'<a href="([^"]+)" aria-current="page">', self.top), ["/book"])
        self.assertEqual(re.findall(r'<a href="(/[^"]*)"', self.top.split("<nav")[1]), [p for p, _ in chassis.PAGES])

    def test_project_level(self):
        """The project's glyph and name lead; Christopher's mark is the footer's signature only."""
        self.assertIn('class="fp-top__glyph"', self.top)
        self.assertEqual(path_data(self.top), path_data(GLYPH.read_text()))
        self.assertIn("LH&middot;Fleet", self.top)
        self.assertNotIn("Christopher", self.top)
        self.assertIn('<a class="fp-signature" href="https://chris.biels.net">', self.foot)
        self.assertEqual(path_data(self.foot), path_data((DASH / "dist/assets/logo/mark.svg").read_text()))

    def test_assets_are_versioned(self):
        links = re.findall(r'(?:href|src)="(/(?:static|faceplate)/[^"]+\.(?:css|js)[^"]*)"', self.html)
        self.assertIn("/static/css/app.css", " ".join(links))
        for link in links:
            self.assertRegex(link, r"\?v=\d+$")


class PagesTest(unittest.TestCase):
    """dashboard/app.py names a spec for every route in _PAGES; both are
    literals, which is what lets the preview tools build the pages without
    Flask."""

    def setUp(self):
        self.pages = _preview.constant("_PAGES")
        self.specs = {route: _preview.constant(name) for route, name in self.pages.items()}

    def test_every_entry_of_the_top_bar_is_a_page(self):
        for path, _name in chassis.PAGES:
            self.assertIn(path, self.pages)

    def test_every_page_builds(self):
        for route, spec in self.specs.items():
            html = chassis.page(**spec)
            self.assertIn('<section class="fp-sec fp-sec--lead"', html, route)
            self.assertEqual(html.count('class="fp-display"'), 1, route)      # one display line a page

    def test_a_page_marks_its_entry_or_none(self):
        entries = {path for path, _name in chassis.PAGES}
        for route, spec in self.specs.items():
            self.assertIn(spec["current"], entries | {None}, route)
        self.assertEqual(self.specs["/fleet/<icao24>"]["current"], "/fleet")    # a page below an entry marks its parent

    def test_every_linked_file_exists(self):
        for route, spec in self.specs.items():
            for f in spec.get("styles", ()):
                self.assertTrue((DASH / "static" / "css" / f).is_file(), route + " " + f)
            for f in spec.get("scripts", ()):
                self.assertTrue((DASH / "static" / "js" / f).is_file(), route + " " + f)

    def test_no_stylesheet_or_script_is_left_over(self):
        used = {f for spec in self.specs.values() for f in (*spec.get("styles", ()), *spec.get("scripts", ()))}
        used |= {"app.css", "admin.css", "admin.js"}                            # every page; the admin, outside _PAGES
        have = {f.name for d in ("css", "js") for f in (DASH / "static" / d).iterdir()}
        self.assertEqual(sorted(have - used), [])

    def test_the_markup_names_no_level_and_no_colour(self):
        """A page spec carries structure: the intensity is chassis.html's, colours are the stylesheets'."""
        for route, spec in self.specs.items():
            self.assertNotIn("data-fp-intensity", spec["main"], route)
            self.assertIsNone(re.search(r"#[0-9a-fA-F]{6}\b|var\(--", spec["main"]), route)

    def test_a_route_resolves_through_the_preview(self):
        self.assertIn("Fleet DB", _preview.page("/fleet/3c4b21?x=1"))
        self.assertIn('data-fp-intensity="02"', _preview.page("/schedule?intensity=02"))
        self.assertIsNone(_preview.page("/nowhere"))


class AssetTest(unittest.TestCase):
    def test_serves_what_the_pages_link_and_nothing_else(self):
        for name in chassis.ASSETS:
            self.assertTrue(chassis.asset(name).is_file(), name)
        for name in ("assets/logo/mark.svg", "tokens.json", "MANIFEST.sha256", "../app.py", "fonts/ttf/Manrope-Bold.ttf", ""):
            self.assertIsNone(chassis.asset(name), name)

    def test_every_font_of_fonts_css_is_served(self):
        urls = re.findall(r'url\("\./([^"]+)"\)', (DASH / "fonts.css").read_text())
        self.assertEqual(len(urls), 4)
        for url in urls:
            self.assertIn(url, chassis.ASSETS)


class GlyphTest(unittest.TestCase):
    def test_follows_the_icon_spec(self):
        svg = GLYPH.read_text()
        self.assertIn('viewBox="0 0 24 24"', svg)
        self.assertIn('stroke="currentColor"', svg)
        self.assertEqual(set(re.findall(r'stroke-width="([^"]+)"', svg)), {"2"})
        self.assertIn('fill="none"', svg)

    def test_the_favicon_is_the_glyph(self):
        self.assertEqual(path_data((DASH / "static" / "favicon.svg").read_text()), path_data(GLYPH.read_text()))


class ImageTest(unittest.TestCase):
    """chassis.py reads its files when the dashboard starts, so a file the
    Dockerfile forgets takes the site down on the next deploy."""

    def image(self):
        """The files the Dockerfile copies, as paths inside the image."""
        files = set()
        for line in (DASH / "Dockerfile").read_text().splitlines():
            if not line.startswith("COPY "):
                continue
            *sources, dest = shlex.split(line)[1:]
            for pattern in sources:
                for src in glob.glob(str(DASH / pattern)):
                    src = Path(src)
                    if src.is_dir():
                        files.update((Path(dest) / f.relative_to(src)).as_posix() for f in src.rglob("*") if f.is_file())
                    else:
                        files.add((Path(dest) / src.name if dest.endswith("/") or dest == "." else Path(dest)).as_posix())
        return files

    def test_the_image_carries_what_the_chassis_reads(self):
        image = self.image()
        need = {"chassis.py", "chassis.html", "static/icons/lh-fleet.svg", "dist/assets/logo/mark.svg",
                "static/css/app.css", "static/favicon.svg"}
        need |= {(Path(folder) / name).as_posix() for name, folder in chassis.ASSETS.items()}
        self.assertEqual(sorted(need - image), [])

    def test_the_image_carries_every_module_the_dashboard_imports(self):
        image = self.image()
        local = {m for m in re.findall(r"^import (\w+)", (DASH / "app.py").read_text(), re.M) if (DASH / (m + ".py")).exists()}
        self.assertIn("chassis", local)
        self.assertEqual(sorted(m for m in local if m + ".py" not in image), [])


if __name__ == "__main__":
    unittest.main()
