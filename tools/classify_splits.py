"""
Classify each candidate gap-split pair against raw positions, to separate
REAL detector bugs from false positives of the consensus-route heuristic.

The consensus heuristic (tools/find_gap_splits.py) flags any leg1.arr==leg2.dep
that isn't a consensus endpoint. But some of those are REAL intermediate stops
or diversions (e.g. D-AIMH actually flew Munich→Mumbai→Delhi). We tell them
apart by the physics at the split boundary (leg1.last_seen):

  REAL_STOP    sustained on-ground signature near the phantom airport
               (>=3 consecutive samples on_ground, or alt<150m & vel<40m/s).
               The aircraft genuinely landed -> detection was correct.

  CRUISE_SNAP  the boundary sample is airborne-by-physics (high vel/alt) and
               isolated (neighbours airborne); no sustained ground. A single
               spurious on_ground=true / bad-altitude sample at cruise that the
               air->ground scan turned into a false landing -> the real bug.

  GAP_DESCENT  a real coverage gap (>8min to next sample) at low-ish altitude
               with no sustained ground -> ambiguous; the aircraft may or may
               not have landed. Reported separately.

Sub-flags on CRUISE_SNAP: ONGROUND_AT_SPEED (on_ground=t with vel>60),
ALT_GLITCH (altitude implausibly high, >16000m), CONTINUOUS (no >8min gap).

Reads tmp/gap_split_pairs.csv + tmp/positions_export.csv. Pure stdlib.
"""
import collections
import csv
import math
from datetime import datetime, timedelta
from pathlib import Path

TMP = Path(__file__).resolve().parent.parent / "tmp"


def haversine_km(la1, lo1, la2, lo2):
    R = 6371.0088
    p = math.radians
    a = (math.sin((p(la2) - p(la1)) / 2) ** 2
         + math.cos(p(la1)) * math.cos(p(la2)) * math.sin((p(lo2) - p(lo1)) / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def parse_ts(s):
    s = s.strip().replace(" ", "T", 1)
    if s.endswith("+00"):
        s = s[:-3] + "+00:00"
    return datetime.fromisoformat(s)


def f(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main():
    pairs = list(csv.DictReader(open(TMP / "gap_split_pairs.csv")))
    for p in pairs:
        p["_t"] = parse_ts(p["a_last"])
    want = {p["icao24"] for p in pairs}

    # phantom airport coordinates, to test ground presence AT the phantom (not
    # merely near the boundary time — that catches the departure takeoff roll).
    apll = {}
    for r in csv.DictReader(open(TMP / "airports_export.csv")):
        try:
            apll[r["icao_code"].strip()] = (float(r["latitude"]), float(r["longitude"]))
        except (ValueError, KeyError):
            continue

    # Stream positions once; keep lean tuples only for involved aircraft,
    # only near (±90min) some candidate boundary.
    bounds = collections.defaultdict(list)
    for p in pairs:
        bounds[p["icao24"]].append(p["_t"])
    keep = collections.defaultdict(list)  # icao -> [(t, alt, vel, gnd, lat, lon)]
    with open(TMP / "positions_export.csv", newline="") as fh:
        for r in csv.DictReader(fh):
            icao = r["icao24"].strip()
            if icao not in want:
                continue
            t = parse_ts(r["captured_at"])
            if not any(abs((t - b).total_seconds()) <= 5400 for b in bounds[icao]):
                continue
            gnd = r["on_ground"].strip()
            keep[icao].append((t, f(r["altitude_m"]), f(r["velocity_ms"]),
                               True if gnd == "t" else False if gnd == "f" else None,
                               f(r["latitude"]), f(r["longitude"])))
    for icao in keep:
        keep[icao].sort()

    def is_groundish(alt, vel, gnd):
        # Genuinely on the ground: on_ground flag AND not moving at flight speed
        # (a high-speed on_ground=True is the cruise glitch, NOT ground), or
        # very low + very slow. This keeps real stops (taxi/parked) but excludes
        # the spurious-flag samples that the bug is made of.
        if gnd is True and (vel is None or vel < 40):
            return True
        if alt is not None and vel is not None and alt < 150 and vel < 40:
            return True
        return False

    def classify(p):
        seq = keep.get(p["icao24"], [])
        t0 = p["_t"]
        win = [s for s in seq if abs((s[0] - t0).total_seconds()) <= 1800]
        if not win:
            return "NO_DATA", []
        # trigger = sample closest to the recorded landing time
        trig = min(win, key=lambda s: abs((s[0] - t0).total_seconds()))
        ti, alt, vel, gnd, _, _ = trig
        flags = []
        # Longest run of PARKED/TAXI samples NEAR THE PHANTOM AIRPORT within
        # ±20min. Near-stationary (vel<15 m/s ≈ 29 kt) is something only a
        # grounded aircraft does; requiring it within 10km of the phantom
        # excludes the departure takeoff-roll (which is near the *origin*, not
        # the phantom) and approach/climb-out snaps. This is the unambiguous
        # signature of a genuine stop AT the phantom (vs a corridor glitch).
        ph = apll.get(p["phantom"])
        near = [s for s in win if abs((s[0] - t0).total_seconds()) <= 1200]
        best = run = 0
        for s in near:
            parked = (s[2] is not None and s[2] < 15) or (s[2] is None and s[3] is True)
            near_ph = (ph is not None and s[4] is not None
                       and haversine_km(s[4], s[5], ph[0], ph[1]) <= 10)
            run = run + 1 if (parked and near_ph) else 0
            best = max(best, run)
        # real coverage gap right after the boundary?
        after = [s for s in seq if s[0] > ti]
        gap_after = None
        if after:
            gap_after = (after[0][0] - ti).total_seconds() / 60.0
        continuous = gap_after is not None and gap_after <= 8

        airborne_phys = (vel is not None and vel > 80) or (alt is not None and alt > 3000)
        if gnd is True and vel is not None and vel > 60:
            flags.append("ONGROUND_AT_SPEED")
        if alt is not None and alt > 16000:
            flags.append("ALT_GLITCH")
        if continuous:
            flags.append("CONTINUOUS")
        # sustained near-stationary signature -> genuinely landed (detection correct)
        if best >= 2:
            return "REAL_STOP", flags
        # airborne-by-physics + no sustained ground -> single-sample false landing
        if airborne_phys:
            return "CRUISE_SNAP", flags
        # a real coverage gap at low-ish altitude, no sustained ground -> ambiguous
        if gap_after is not None and gap_after > 8:
            return "GAP_DESCENT", flags
        return "AMBIGUOUS", flags

    cats = collections.Counter()
    flagc = collections.Counter()
    examples = collections.defaultdict(list)
    for p in pairs:
        cat, flags = classify(p)
        cats[cat] += 1
        for fl in flags:
            flagc[fl] += 1
        if len(examples[cat]) < 4:
            examples[cat].append(
                f"{p['registration']}/{p['callsign']} {p['flight_date']} "
                f"true={p['true_dep']}->{p['true_arr']} phantom={p['phantom']} {flags}")
        p["_cat"] = cat
        p["_flags"] = "|".join(flags)

    in_window = sum(n for c, n in cats.items() if c != "NO_DATA")
    print(f"Classified {len(pairs)} candidate split pairs "
          f"({in_window} have retained positions; {cats['NO_DATA']} predate retention):\n")
    for cat, n in cats.most_common():
        pct = f"{n/in_window*100:.1f}% of in-window" if cat != "NO_DATA" and in_window else ""
        print(f"  {cat:12s} {n:4d}  {pct}")

    named = {("D-AIMH", "EGTE"), ("D-AIMA", "EDDK"), ("D-AIMH", "VABB")}
    print("\nNamed-case sanity check:")
    for p in pairs:
        if (p["registration"], p["phantom"]) in named:
            print(f"  {p['registration']}/{p['callsign']} phantom={p['phantom']} -> {p['_cat']} {p['_flags']}")
    print("\nSub-flags (on CRUISE_SNAP etc.):")
    for fl, n in flagc.most_common():
        print(f"  {fl:18s} {n}")
    print("\nExamples per category:")
    for cat in cats:
        print(f"  [{cat}]")
        for e in examples[cat]:
            print(f"     {e}")

    # write per-pair category back for the corpus builder
    cols = list(pairs[0].keys())
    cols = [c for c in cols if not c.startswith("_t")]
    with open(TMP / "gap_split_classified.csv", "w", newline="") as out:
        w = csv.DictWriter(out, fieldnames=[c for c in cols] )
        w.writeheader()
        for p in pairs:
            row = {k: v for k, v in p.items() if k in cols}
            w.writerow(row)
    print(f"\nWrote tmp/gap_split_classified.csv")


if __name__ == "__main__":
    main()
