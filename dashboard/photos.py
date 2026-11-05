"""
Airframe photos from Wikimedia Commons, kept on this server.

Planespotters' thumbnails stop at 420 px, which a 2x screen draws soft.
Commons sorts photos of an airframe into "Category:<REG> (aircraft)" under
free licences, at full size. So one background thread walks the fleet, picks
a photo per tail, fetches Wikimedia's own 1280 px copy of it and keeps it on
disk with its credit. The airframe page shows it straight away from this
server, so a visitor's browser never contacts Wikimedia. A tail with no
usable photo there keeps Planespotters behind its click (app.py).

Which photo: an exterior-looking shot (landscape, no cabin/cockpit/wing words
in its title), at least 1200 px wide, under a licence that only asks for
credit (CC0, CC BY, CC BY-SA, public domain; not GFDL, which wants its whole
text shown). The registration in the title first, then the newest, so the
livery is the current one. A tail without such a photo in its category is
searched by title. A registration is reused over the decades (D-ABYM was a
1936 Dornier, D-ABQC an Air Berlin Dash 8, and the only file in "D-ABYR
(aircraft)" is the 747-200 of 1987), so no file may name another type than
the tail's, and a found one must name the tail's own. Nothing taken before
TAKEN_FROM either: those are scans, often of the older airframe (D-AIDA and
D-AIDC were A310s in 1990). OVERRIDES fixes a pick by hand.

Pure and stdlib-only: the tests run it without Flask or a database.
"""
import hashlib
import html
import json
import os
import re
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

API = "https://commons.wikimedia.org/w/api.php"
UA = "LHFleetLogger/1.0 (+https://lhlogging.biels.net/impressum)"
WIDTH = 1280        # the copy kept: a standard Wikimedia thumbnail step
MIN_WIDTH = 1200    # the page shows it up to ~600 px wide: sharp at 2x
ASPECT = (1.4, 2.6)  # 4:3 here is mostly phone shots from a seat
RECHECK_S = 7 * 86400  # how often a tail's photos are looked at again
PAUSE_S = 2.0       # between two tails, so Commons sees a slow trickle
RETRY_S = 600       # after a failed look-up or fleet read
SKIP_S = 86400      # a tail whose look-up failed waits this long, not the walk

# reg -> "File:..." to show that file, or None for no Commons photo at all
OVERRIDES = {}

_FREE = re.compile(r"^(cc0|cc by(-sa)? \d(\.\d)?( [a-z]{2})?|public domain|no restrictions)$", re.I)
_NOT_OUTSIDE = re.compile(
    r"\b(cabin|seats?|seating|cockpit|flight ?deck|interior|inside|business class|first class"
    r"|economy|premium|galley|lavatory|toilet|menu|meal|lounge|window|wings?|wingtip|winglets?"
    r"|engines?|landing gear|tail ?fin|detail|model|ife|screen|on ?board|in-?flight"
    r"|kabine|sitze?|innen|triebwerk|tragfl\w+)\b", re.I)
# a reused registration: a found file must name the family of the tail's type
_FAMILY = [
    (("A318", "A319", "A320", "A20N", "A321", "A21N"), r"(?<!\d)(318|319|320|321)(?!\d)"),
    (("A332", "A333", "A339"), r"(?<!\d)33[0239](?!\d)"),
    (("A343", "A346"), r"(?<!\d)34[036](?!\d)"),
    (("A359", "A35K"), r"(?<!\d)35[019](?!\d)"),
    (("A388",), r"(?<!\d)38[08](?!\d)"),
    (("B744",), r"(?<!\d)747-?4|(?<!\d)744(?!\d)"),
    (("B748",), r"(?<!\d)747-?8|(?<!\d)748(?!\d)"),
    (("B788", "B789", "B78X"), r"(?<!\d)78[789](?!\d)|dreamliner"),
    (("B77W", "B77L", "B772", "B773", "B77F"), r"(?<!\d)77[7WLF23](?!\d)"),
    (("CRJ7", "CRJ9", "CRJX"), r"crj|canadair"),
    (("E190", "E195", "E75L", "E290", "E295"), r"embraer|\be?1[79]\d\b"),
    (("DH8D",), r"dash ?8|dhc-?8|q400"),
]
# type designators a title or description may name: A350-941, B747-230B, Do 18
_MODEL = re.compile(
    r"(?<![A-Z0-9])(A3[0-8]\d(?:-?\d{3})?(?:neo|N)?|B?7[0-8]7(?:-?\d{1,3}[A-Z]?)?|DC-?\d{1,2}|MD-?\d{2}"
    r"|CRJ-?\d*|DHC-?\d|Dash ?8|Q400|E-?1[79]\d|ERJ-?\d*|Do ?\d{1,3}|Ju ?\d{2}|Fokker ?\d*|ATR ?\d*)"
    r"(?![A-Z0-9])", re.I)
LIVERY_FROM = "2019"  # photos before it mostly show the old livery
TAKEN_FROM = "2005"   # before it: scans, often of an older airframe
_MONTHS = {m: i + 1 for i, m in enumerate(
    "january february march april may june july august september october november december".split())}


def _text(value):
    """Commons metadata values are HTML: the words, on one line."""
    t = html.unescape(re.sub(r"<[^>]+>", " ", value or ""))
    return re.sub(r"\s+", " ", t).strip()


def taken(value, uploaded=""):
    """When the photo was taken, as 'YYYY-MM-DD' (or the upload day): EXIF
    dates, '24 November 2009', 'November 24, 2009', 'October 2019', '2009-11',
    '2009'."""
    t = _text(value).lower()
    m = re.search(r"(\d{4})[-:](\d{2})[-:](\d{2})", t)
    if m:
        return "%s-%s-%s" % m.groups()
    m = re.search(r"(\d{1,2}) (%s),? (\d{4})" % "|".join(_MONTHS), t)
    if m:
        return "%s-%02d-%02d" % (m.group(3), _MONTHS[m.group(2)], int(m.group(1)))
    m = re.search(r"(%s) (\d{1,2}),? (\d{4})" % "|".join(_MONTHS), t)
    if m:
        return "%s-%02d-%02d" % (m.group(3), _MONTHS[m.group(1)], int(m.group(2)))
    m = re.search(r"(%s),? (\d{4})" % "|".join(_MONTHS), t)
    if m:
        return "%s-%02d-01" % (m.group(2), _MONTHS[m.group(1)])
    m = re.search(r"\b((?:19|20)\d{2})-(\d{2})\b", t)
    if m:
        return "%s-%s-01" % m.groups()
    m = re.search(r"\b((?:19|20)\d{2})\b", t)
    if m:
        return m.group(1) + "-01-01"
    return (uploaded or "")[:10]


def candidates(pages):
    """The files of a Commons query, one dict each. A continued query repeats
    a page without its imageinfo; the copy that has it wins."""
    by = {}
    for p in pages:
        if p.get("imageinfo") or p.get("pageid") not in by:
            by[p.get("pageid")] = p
    out = []
    for p in by.values():
        if not p.get("imageinfo"):
            continue
        ii = p["imageinfo"][0]
        m = ii.get("extmetadata") or {}
        val = lambda k: (m.get(k) or {}).get("value") or ""
        out.append({
            "title": p.get("title", ""), "page": ii.get("descriptionurl"),
            "mime": ii.get("mime"), "width": ii.get("width") or 0, "height": ii.get("height") or 0,
            "src": ii.get("thumburl") or ii.get("url"),
            "src_width": ii.get("thumbwidth") or ii.get("width"),
            "src_height": ii.get("thumbheight") or ii.get("height"),
            "artist": _text(val("Artist"))[:120] or None,
            "license": _text(val("LicenseShortName")), "license_url": val("LicenseUrl") or None,
            "description": _text(val("ImageDescription"))[:400],
            "taken": taken(val("DateTimeOriginal"), ii.get("timestamp", "")),
        })
    return out


def _flat(s):
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def names_reg(title, reg):
    """The registration stands in the title (D-ABYN, DABYN, D ABYN), not as
    part of a longer one."""
    r = re.escape(reg.upper()).replace(r"\-", "[- ]?")
    return re.search(r"(?<![A-Z0-9])%s(?![A-Z0-9])" % r, title.upper()) is not None


def names_family(text, ftype):
    for types, pattern in _FAMILY:
        if ftype in types:
            return re.search(pattern, text, re.I) is not None
    return False


def names_other(text, ftype):
    """The text names a type, and not the tail's own: another airframe that
    carried the registration, or another aircraft in the frame."""
    others = [m for m in _MODEL.findall(text) if not names_family(m, ftype)]
    return bool(others) and not names_family(text, ftype)


def usable(c):
    """A sharp, landscape, freely licensed JPEG whose title reads as an outside shot."""
    w, h = c["width"], c["height"]
    return (c["mime"] == "image/jpeg" and c["src"] and c["page"] and w >= MIN_WIDTH and h > 0
            and ASPECT[0] <= w / h <= ASPECT[1] and _FREE.match(c["license"] or "") is not None
            and not _NOT_OUTSIDE.search(c["title"].rsplit(".", 1)[0]))


def pick(files, reg, ftype=None, trusted=True):
    """The photo to show, or None. Files of the tail's own category are
    trusted; anything else must name the registration and the type."""
    said = lambda c: c["title"] + " " + c["description"]
    ok = [c for c in files if usable(c) and c["taken"] >= TAKEN_FROM
          and not names_other(said(c), ftype)]
    if not trusted:
        ok = [c for c in ok if names_reg(c["title"], reg) and names_family(said(c), ftype)]
    if not ok:
        return None
    return max(ok, key=lambda c: (names_reg(c["title"], reg), c["taken"] >= LIVERY_FROM,
                                  names_family(said(c), ftype), c["taken"], c["width"]))


_IIPROP = {"prop": "imageinfo", "iiprop": "url|size|mime|timestamp|extmetadata",
           "iiurlwidth": str(WIDTH), "iiextmetadatalanguage": "en",
           "iiextmetadatafilter": "Artist|LicenseShortName|LicenseUrl|DateTimeOriginal|ImageDescription"}


def _query(get_json, params):
    """All pages of a query, following its continuation."""
    pages, cont = [], {}
    for _ in range(20):
        r = get_json({"action": "query", "format": "json", "formatversion": "2", **params, **_IIPROP, **cont})
        pages += (r.get("query") or {}).get("pages") or []
        if "continue" not in r:
            break
        cont = r["continue"]
    return pages


def look_up(reg, ftype, get_json):
    """The photo for one tail: OVERRIDES, else its category, else a title search."""
    if reg in OVERRIDES:
        title = OVERRIDES[reg]
        if not title:
            return None
        found = candidates(_query(get_json, {"titles": title}))
        return found[0] if found else None
    cat = candidates(_query(get_json, {
        "generator": "categorymembers", "gcmtitle": "Category:%s (aircraft)" % reg,
        "gcmtype": "file", "gcmlimit": "max"}))
    best = pick(cat, reg, ftype, trusted=True)
    if best is None and any(ftype in types for types, _ in _FAMILY):
        found = candidates(_query(get_json, {
            "generator": "search", "gsrsearch": 'intitle:"%s"' % reg,
            "gsrnamespace": "6", "gsrlimit": "50"}))
        best = pick(found, reg, ftype, trusted=False)
    return best


def http_json(params):
    req = urllib.request.Request(API + "?" + urllib.parse.urlencode(params), headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp)


def http_bytes(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read(8 * 1024 * 1024)


class Store:
    """<REG>.jpg and <REG>.json (when it was looked at, and the credit) in one
    directory. Written file-then-rename, so a reader sees old or new, whole."""

    def __init__(self, root):
        self.root = Path(root)

    def _path(self, reg, ext):
        if not re.match(r"^[A-Z0-9-]{3,8}$", reg):
            raise ValueError(reg)
        return self.root / (reg + ext)

    def drop_image(self, reg):
        self._path(reg, ".jpg").unlink(missing_ok=True)

    def image(self, reg):
        p = self._path(reg, ".jpg")
        return p if p.is_file() else None

    def meta(self, reg):
        try:
            return json.loads(self._path(reg, ".json").read_text())
        except (OSError, ValueError):
            return None

    def _write(self, path, data):
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_bytes(data)
        os.replace(tmp, path)

    def save(self, reg, rec, image=None):
        self.root.mkdir(parents=True, exist_ok=True)
        if image is not None:
            self._write(self._path(reg, ".jpg"), image)
        self._write(self._path(reg, ".json"), json.dumps(rec, sort_keys=True).encode())

    def public(self, reg):
        """What the airframe page shows: the credit and our own URL, or None."""
        rec = self.meta(reg)
        ph = (rec or {}).get("photo")
        if not ph or not self.image(reg):
            return None
        return {"src": "/photos/%s.jpg?v=%s" % (reg, ph["v"]),
                "width": ph["width"], "height": ph["height"], "link": ph["page"],
                "artist": ph["artist"], "license": ph["license"], "license_url": ph["license_url"],
                "taken": ph["taken"], "source": "Wikimedia Commons"}


def refresh(reg, ftype, store, get_json=http_json, get_bytes=http_bytes, now=time.time):
    """Look the tail up again; download only when the pick has changed."""
    best = look_up(reg, ftype, get_json)
    rec = {"reg": reg, "checked": now(), "photo": None}
    if best is None:
        store.save(reg, rec)
        store.drop_image(reg)
        return rec
    old = (store.meta(reg) or {}).get("photo") or {}
    image = None
    if old.get("src") != best["src"] or not store.image(reg):
        image = get_bytes(best["src"])
        if not image.startswith(b"\xff\xd8") or len(image) < 10_000:
            raise ValueError("not a JPEG: %s" % best["src"])
    v = hashlib.sha1(image).hexdigest()[:10] if image is not None else old["v"]
    rec["photo"] = {"title": best["title"], "page": best["page"], "src": best["src"],
                    "width": best["src_width"], "height": best["src_height"],
                    "artist": best["artist"], "license": best["license"],
                    "license_url": best["license_url"], "taken": best["taken"], "v": v}
    store.save(reg, rec, image)
    return rec


class Keeper:
    """The background walk. tails() -> [(reg, type)] is read once an hour into
    a queue of the tails due (never looked at first, then the longest ago);
    one tail is looked at per step, PAUSE_S apart. want(reg) puts a tail
    ahead of the queue and wakes the walk: an airframe page asked for one it
    has not reached."""

    def __init__(self, store, tails, refresh=refresh, clock=time.time):
        self.store, self.tails, self._refresh, self.clock = store, tails, refresh, clock
        self.wanted, self.failed = {}, {}
        self.queue, self.queued_at = [], None
        self.lock = threading.Lock()
        self.wake = threading.Event()

    def want(self, reg, ftype=None):
        with self.lock:
            self.wanted.setdefault(reg, ftype)
        self.wake.set()

    def due(self, fleet):
        """The fleet's tails to look at, in order. A tail whose look-up failed
        sits out SKIP_S, so one bad tail cannot hold up the walk."""
        now, out, seen = self.clock(), [], set()
        for reg, ftype in fleet:
            if reg in seen or now - self.failed.get(reg, -SKIP_S) < SKIP_S:
                continue
            seen.add(reg)
            rec = self.store.meta(reg)
            at = rec.get("checked", 0) if rec else -1
            if now - at >= RECHECK_S:
                out.append((at, reg, ftype))
        return [(reg, ftype) for _, reg, ftype in sorted(out, key=lambda x: x[0])]

    def step(self):
        """Look at the next tail. Returns how long to wait before the next step."""
        with self.lock:
            nxt = next(iter(self.wanted.items()), None)
            if nxt:
                del self.wanted[nxt[0]]
        if nxt is None:
            if not self.queue or self.clock() - self.queued_at >= 3600:
                try:
                    self.queue = self.due(self.tails())
                except Exception:
                    return RETRY_S
                self.queued_at = self.clock()
            if not self.queue:
                return 3600
            nxt = self.queue.pop(0)
        reg, ftype = nxt
        try:
            self._refresh(reg, ftype, self.store)
        except Exception:
            self.failed[reg] = self.clock()
            return RETRY_S
        self.failed.pop(reg, None)
        return PAUSE_S

    def start(self):
        def loop():
            while True:
                if self.wake.wait(self.step()):  # want() cuts a long wait short
                    self.wake.clear()
                    time.sleep(PAUSE_S)
        thread = threading.Thread(target=loop, name="photo-keeper", daemon=True)
        thread.start()
        return thread
