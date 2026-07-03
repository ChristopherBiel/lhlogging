"""
Edge-case & failure-mode audit of the flight detector — the quantification
engine behind tools/EDGE_CASES.md. Read-only: consumes the tmp/ CSV exports
(tools/pull_data.sh, tools/pull_positions.sh, tools/pull_fis.sh) and the
offline replay harness; never touches the DB.

Each audit emits a labeled candidate CSV to tmp/audit_<name>.csv plus a
console summary. Every candidate is verified against raw position physics
before being labeled a bug (the classify_splits.py discipline — screening
heuristics over-count).

Audits (taxonomy IDs from the analysis plan):
  presplit  one-time: split positions_export.csv into per-aircraft slices
  replay    cache a full-fleet replay under a named config (prod / nop3)
  e3        arrival-collapse: rotation folded into one dep==arr leg
  e2        spurious single airborne samples while parked
  e7        P3 missed-departure snap attribution (config-diff prod vs nop3)
  e6        wrong-field arrival snaps (arr near, but != consensus arr)
  e8        dep=previous-arrival fallback poisoning
  e10       C6 mid-air callsign-change false closes
  census    E1 residual splits, E9 stale-close, E4 UNKN recovery
  e12       analysis-layer: consensus staleness / poisoning
  fis       cross-check detector legs vs FIS observations (external oracle)

Usage:
    PYTHONPATH=tools python3 tools/audit_edge_cases.py presplit
    PYTHONPATH=tools python3 tools/audit_edge_cases.py replay --tag prod
    PYTHONPATH=tools python3 tools/audit_edge_cases.py replay --tag nop3
    PYTHONPATH=tools python3 tools/audit_edge_cases.py e3 e2 e6 e8 e10 census e12 fis e7
"""
import argparse
import bisect
import collections
import csv
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import _lhdata as L
import detector_replay as D
from _airports import Airports, _haversine_km

TMP = Path(__file__).resolve().parent.parent / "tmp"
CACHE = Path(os.environ.get(
    "AUDIT_CACHE",
    TMP / "audit_cache",
))
DEPLOY = datetime(2026, 6, 29, tzinfo=timezone.utc)

# Production guard values (app/lhlogging/config.py defaults).
PROD_CFG = D.Config(
    onground_max_speed_ms=80.0,
    onground_max_altitude_m=6000.0,
    landing_min_consecutive=1,
    missed_departure_snap=True,
    scan_arrival_max_km=8.0,
    min_turnaround_min=0,
)
NOP3_CFG = D.Config(
    onground_max_speed_ms=80.0,
    onground_max_altitude_m=6000.0,
    landing_min_consecutive=1,
    missed_departure_snap=False,   # the only difference
    scan_arrival_max_km=8.0,
    min_turnaround_min=0,
)
CONFIGS = {"prod": PROD_CFG, "nop3": NOP3_CFG}

POS_FIELDS = ["icao24", "callsign", "captured_at", "latitude", "longitude",
              "altitude_m", "velocity_ms", "heading", "on_ground"]


def parse_ts(s):
    s = (s or "").strip()
    if not s:
        return None
    s = s.replace(" ", "T", 1)
    if s.endswith("+00"):
        s = s[:-3] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def load_airport_coords():
    """icao -> (type, lat, lon) from the airports export."""
    out = {}
    with open(TMP / "airports_export.csv", newline="") as f:
        for r in csv.DictReader(f):
            try:
                out[r["icao_code"].strip()] = (
                    (r.get("type") or "").strip(),
                    float(r["latitude"]), float(r["longitude"]),
                )
            except (ValueError, KeyError):
                continue
    return out


# --------------------------------------------------------------------------
# presplit — one pass over the 154MB export -> per-aircraft slices
# --------------------------------------------------------------------------
def cmd_presplit(args):
    outdir = CACHE / "positions_by_icao"
    outdir.mkdir(parents=True, exist_ok=True)
    writers, files = {}, {}
    n = 0
    with open(TMP / "positions_export.csv", newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            icao = r["icao24"].strip()
            if icao not in writers:
                fh = open(outdir / f"{icao}.csv", "w", newline="")
                w = csv.DictWriter(fh, fieldnames=POS_FIELDS, extrasaction="ignore")
                w.writeheader()
                writers[icao], files[icao] = w, fh
            writers[icao].writerow(r)
            n += 1
    for fh in files.values():
        fh.close()
    print(f"presplit: {n} positions -> {len(files)} aircraft under {outdir}")


def positions_for(icao):
    """Load one aircraft's positions from the presplit cache (sorted)."""
    path = CACHE / "positions_by_icao" / f"{icao}.csv"
    if not path.exists():
        return []
    seq = D.load_positions(path, icao24s={icao})
    return seq.get(icao, [])


def presplit_icaos():
    d = CACHE / "positions_by_icao"
    return sorted(p.stem for p in d.glob("*.csv")) if d.exists() else []


# --------------------------------------------------------------------------
# replay — cache a fleet replay under a named config
# --------------------------------------------------------------------------
def cmd_replay(args):
    tag = args.tag
    cfg = CONFIGS[tag]
    airports = Airports.load()
    icaos = presplit_icaos()
    if not icaos:
        raise SystemExit("run `presplit` first")
    out = CACHE / f"legs_{tag}.csv"
    cols = ["icao24", "callsign", "dep", "arr", "first_seen", "last_seen",
            "needs_review", "origin"]
    n = 0
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for i, icao in enumerate(icaos):
            seq = positions_for(icao)
            for l in D.replay_aircraft(seq, airports, cfg):
                w.writerow([l.icao24, l.callsign or "", l.departure_airport_icao or "",
                            l.arrival_airport_icao or "", l.first_seen.isoformat(),
                            l.last_seen.isoformat(), "t" if l.needs_review else "f",
                            l.origin])
                n += 1
            if (i + 1) % 50 == 0:
                print(f"  ... {i+1}/{len(icaos)} aircraft", flush=True)
    print(f"replay[{tag}]: {n} legs -> {out}")


def load_replay(tag):
    out = collections.defaultdict(list)
    with open(CACHE / f"legs_{tag}.csv", newline="") as f:
        for r in csv.DictReader(f):
            r["first_seen"] = datetime.fromisoformat(r["first_seen"])
            r["last_seen"] = datetime.fromisoformat(r["last_seen"])
            out[r["icao24"]].append(r)
    return out


# --------------------------------------------------------------------------
# shared physics helpers
# --------------------------------------------------------------------------
def window_stats(seq, t0, t1, coords, home=None):
    """Physics summary of one leg window from the raw positions."""
    inw = [p for p in seq if t0 <= p["captured_at"] <= t1]
    airborne = [p for p in inw
                if p["on_ground"] is not True
                and ((p["altitude_m"] or 0) > 300 or (p["velocity_ms"] or 0) > 100)]
    max_alt = max((p["altitude_m"] or 0) for p in inw) if inw else 0
    # longest inter-sample gap and the samples flanking it
    gap_s, gpre, gpost = 0, None, None
    for a, b in zip(inw, inw[1:]):
        d = (b["captured_at"] - a["captured_at"]).total_seconds()
        if d > gap_s:
            gap_s, gpre, gpost = d, a, b
    far_km = 0.0
    if home and home in coords and inw:
        _, hla, hlo = coords[home]
        far_km = max(_haversine_km(p["latitude"], p["longitude"], hla, hlo)
                     for p in inw if p["latitude"] is not None)
    return {"n_pos": len(inw), "airborne": len(airborne), "max_alt": max_alt,
            "gap_h": gap_s / 3600.0, "gap_pre": gpre, "gap_post": gpost,
            "far_km": far_km, "inw": inw, "airborne_pos": airborne}


def neighbors(seq, t, span_s=1200):
    """Samples within ±span of t, excluding the exact sample."""
    return [p for p in seq if p["captured_at"] != t
            and abs((p["captured_at"] - t).total_seconds()) <= span_s]


def write_csv(name, rows, cols):
    path = TMP / f"audit_{name}.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  -> {path} ({len(rows)} rows)")


# --------------------------------------------------------------------------
# E3 — arrival-collapse (dep==arr with a real airborne track)
# --------------------------------------------------------------------------
def cmd_e3(args):
    rows = L.load()
    cmap = L.build_callsign_routes(rows)
    coords = load_airport_coords()
    pos_start = datetime(2026, 6, 2, tzinfo=timezone.utc)

    cands = []
    for r in rows:
        dep, arr = L.norm_ap(r["departure_airport_icao"]), L.norm_ap(r["arrival_airport_icao"])
        t0 = parse_ts(r["first_seen"])
        if dep and dep == arr and t0 and t0 >= pos_start:
            cands.append((r, dep, t0, parse_ts(r["last_seen"])))

    out = []
    for r, ap, t0, t1 in cands:
        icao = r["icao24"].strip()
        seq = positions_for(icao)
        st = window_stats(seq, t0, t1, coords, home=ap)
        label = ("GROUND" if st["airborne"] == 0 else
                 "CRUISE" if st["max_alt"] >= 6000 else "LOW")
        cs = (r["callsign"] or "").strip()
        cons = cmap.get(cs)

        # mechanism evidence: was the recorded dep actually observed? the arr?
        inw = st["inw"]
        dep_status = arr_status = "NO_POS"
        cs_first = cs_last = ""
        if inw and ap in coords:
            _, ala, alo = coords[ap]
            f0, fN = inw[0], inw[-1]
            cs_first = (f0.get("callsign") or "").strip()
            cs_last = (fN.get("callsign") or "").strip()
            d_first = _haversine_km(f0["latitude"], f0["longitude"], ala, alo)
            d_last = _haversine_km(fN["latitude"], fN["longitude"], ala, alo)
            ground_first = f0["on_ground"] is True or (
                (f0.get("altitude_m") or 0) < 300 and (f0.get("velocity_ms") or 99) < 30)
            ground_last = fN["on_ground"] is True or (
                (fN.get("altitude_m") or 0) < 300 and (fN.get("velocity_ms") or 99) < 30)
            dep_status = ("OBSERVED" if d_first <= 8 and ground_first else
                          "AIRBORNE_NEAR" if d_first <= 50 else
                          f"NOT_OBSERVED")
            arr_status = ("OBSERVED" if d_last <= 8 and ground_last else
                          "AIRBORNE_NEAR" if d_last <= 8 else
                          "WRONG")
        mech = (
            "RETURN_LEG_FAB_DEP" if dep_status == "NOT_OBSERVED" and arr_status == "OBSERVED"
            else "ARR_SNAP_WRONG" if arr_status == "WRONG" and label != "GROUND"
            else "LOCAL_EVENT" if dep_status == "OBSERVED" and arr_status == "OBSERVED"
            else "OTHER")
        out.append({
            "icao24": icao, "registration": r["registration"], "callsign": cs,
            "airport": ap, "first_seen": r["first_seen"], "last_seen": r["last_seen"],
            "dur_min": r["duration_minutes"], "needs_review": r["needs_review"],
            "post_deploy": "t" if t0 >= DEPLOY else "f",
            "label": label, "mech": mech,
            "dep_status": dep_status, "arr_status": arr_status,
            "cs_flip": "t" if (cs_first and cs_last and cs_first != cs_last) else "f",
            "cs_first": cs_first, "cs_last": cs_last,
            "n_pos": st["n_pos"], "airborne": st["airborne"],
            "max_alt": int(st["max_alt"]), "gap_h": round(st["gap_h"], 1),
            "far_km": round(st["far_km"]),
            "consensus_route": f"{cons[0]}-{cons[1]}" if cons else "",
        })

    post = [o for o in out if o["post_deploy"] == "t"]
    print(f"E3: dep==arr legs in positions window: {len(out)} "
          f"(post-deploy {len(post)})")
    for lab in ("CRUISE", "LOW", "GROUND"):
        n = sum(1 for o in out if o["label"] == lab)
        np_ = sum(1 for o in post if o["label"] == lab)
        print(f"  {lab:6s}: {n:4d} total, {np_:4d} post-deploy")
    print("\n  mechanism x review (airborne-track legs only):")
    mc = collections.Counter((o["mech"], o["needs_review"]) for o in out
                             if o["label"] != "GROUND")
    for (m, rev), n in mc.most_common():
        print(f"    {m:20s} review={rev}: {n}")
    cf = sum(1 for o in out if o["cs_flip"] == "t")
    print(f"  callsign flip within leg window: {cf}")
    wrong = [o for o in out if o["mech"] == "ARR_SNAP_WRONG"]
    print(f"\n  ARR_SNAP_WRONG (arrival airport contradicted by physics): {len(wrong)}")
    for o in wrong[:10]:
        print(f"    {o['registration']:7s} {o['callsign']:8s} {o['airport']} "
              f"{o['first_seen'][:16]} alt={o['max_alt']:>5} far={o['far_km']:>5}km "
              f"rev={o['needs_review']} cons={o['consensus_route'] or '-'}")
    write_csv("e3_self_loops", out,
              list(out[0].keys()) if out else [])
    return out


# --------------------------------------------------------------------------
# E2 — spurious airborne samples while parked
# --------------------------------------------------------------------------
def cmd_e2(args):
    rows = L.load()
    coords = load_airport_coords()
    pos_start = datetime(2026, 6, 2, tzinfo=timezone.utc)

    out = []
    for r in rows:
        dep, arr = L.norm_ap(r["departure_airport_icao"]), L.norm_ap(r["arrival_airport_icao"])
        t0, t1 = parse_ts(r["first_seen"]), parse_ts(r["last_seen"])
        if not (dep and dep == arr and t0 and t0 >= pos_start):
            continue
        icao = r["icao24"].strip()
        seq = positions_for(icao)
        st = window_stats(seq, t0, t1, coords, home=dep)
        if not (1 <= st["airborne"] <= 2):
            continue  # E3 handles cruise tracks; ground-only handled by prune
        # plausibility of each "airborne" sample against its timeline neighbors
        # (±60 min — parked snapshots can be sparse)
        verdicts = []
        for p in st["airborne_pos"]:
            nb = neighbors(seq, p["captured_at"], span_s=3600)
            ground_nb = [q for q in nb
                         if q["on_ground"] is True or (q["velocity_ms"] or 0) < 5]
            # corroboration must be temporally adjacent — an airborne fix an hour
            # away is a different flight, not support for this sample
            corroborating = [q for q in nb
                             if abs((q["captured_at"] - p["captured_at"]).total_seconds()) <= 600
                             and q["on_ground"] is not True
                             and ((q["altitude_m"] or 0) > 300 or (q["velocity_ms"] or 0) > 100)]
            if corroborating:
                verdicts.append("CORROBORATED")
                continue
            if not ground_nb:
                verdicts.append("ISOLATED")            # nothing around to judge by
                continue
            # a lone "airborne" fix bracketed only by parked fixes: physically
            # impossible unless the bracket is wide enough for a full circuit
            before = [q for q in ground_nb if q["captured_at"] < p["captured_at"]]
            after = [q for q in ground_nb if q["captured_at"] > p["captured_at"]]
            if before and after:
                bracket_min = (min(q["captured_at"] for q in after)
                               - max(q["captured_at"] for q in before)).total_seconds() / 60
                verdicts.append("SPURIOUS" if bracket_min < 45 else "SINGLETON")
            else:
                verdicts.append("SINGLETON")
        verdict = ("ALL_SPURIOUS" if verdicts and all(v == "SPURIOUS" for v in verdicts)
                   else "SINGLETON" if verdicts and all(v in ("SPURIOUS", "SINGLETON", "ISOLATED")
                                                        for v in verdicts)
                   else "MIXED")
        out.append({
            "icao24": icao, "registration": r["registration"],
            "callsign": (r["callsign"] or "").strip(), "airport": dep,
            "first_seen": r["first_seen"], "dur_min": r["duration_minutes"],
            "post_deploy": "t" if t0 >= DEPLOY else "f",
            "n_pos": st["n_pos"], "airborne": st["airborne"],
            "max_alt": int(st["max_alt"]), "verdict": verdict,
        })

    print(f"E2: dep==arr legs whose airborne evidence is 1-2 samples: {len(out)}")
    vc = collections.Counter(o["verdict"] for o in out)
    for k, n in vc.most_common():
        print(f"    {k:14s}: {n}  (post-deploy "
              f"{sum(1 for o in out if o['verdict'] == k and o['post_deploy'] == 't')})")
    spur = [o for o in out if o["verdict"] in ("ALL_SPURIOUS", "SINGLETON")]
    print(f"    -> uncorroborated single-sample evidence "
          f"(falsely kept by the prune keep-rule): {len(spur)}")
    for o in spur[:10]:
        print(f"    {o['registration']:7s} {o['callsign']:8s} {o['airport']} "
              f"{o['first_seen'][:16]} n_pos={o['n_pos']} airborne={o['airborne']} "
              f"max_alt={o['max_alt']}")
    write_csv("e2_spurious_airborne", out, list(out[0].keys()) if out else [])
    return out


# --------------------------------------------------------------------------
# E7 — P3 missed-departure snap attribution (config diff prod vs nop3)
# --------------------------------------------------------------------------
def cmd_e7(args):
    prod, nop3 = load_replay("prod"), load_replay("nop3")
    rows = L.load()
    cmap = L.build_callsign_routes(rows)
    coords = load_airport_coords()

    out = []
    for icao, legs in prod.items():
        other = {l["first_seen"]: l for l in nop3.get(icao, [])}
        for l in legs:
            o = other.get(l["first_seen"])
            dep_a = L.norm_ap(l["dep"])
            dep_b = L.norm_ap(o["dep"]) if o else None
            if dep_a and (o is None or dep_b != dep_a):
                # this leg's dep exists only because of P3
                cs = (l["callsign"] or "").strip()
                cons = cmap.get(cs)
                seq = positions_for(icao)
                i = bisect.bisect_left([p["captured_at"] for p in seq], l["first_seen"])
                first = seq[i] if i < len(seq) else None
                nxt = seq[i + 1] if i + 1 < len(seq) else None
                trend = ""
                if first and nxt and first.get("altitude_m") is not None \
                        and nxt.get("altitude_m") is not None:
                    trend = ("CLIMB" if nxt["altitude_m"] > first["altitude_m"] + 10
                             else "DESCENT" if nxt["altitude_m"] < first["altitude_m"] - 10
                             else "FLAT")
                arr_a = L.norm_ap(l["arr"])
                wrong = cons is not None and dep_a != cons[0]
                out.append({
                    "icao24": icao, "callsign": cs, "p3_dep": dep_a,
                    "arr": arr_a or "", "first_seen": l["first_seen"].isoformat(),
                    "needs_review": l["needs_review"],
                    "first_alt": int(first["altitude_m"]) if first and first.get("altitude_m") is not None else "",
                    "trend": trend,
                    "consensus_dep": cons[0] if cons else "",
                    "verdict": ("WRONG_DEP" if wrong else
                                "SELF_LOOP" if arr_a == dep_a else
                                "OK" if cons else "NO_CONSENSUS"),
                })

    n = len(out)
    v = collections.Counter(o["verdict"] for o in out)
    print(f"E7: legs whose departure came from the P3 snap (prod vs nop3 diff): {n}")
    for k, c in v.most_common():
        print(f"    {k:12s}: {c}")
    silent = [o for o in out if o["verdict"] == "WRONG_DEP" and o["needs_review"] == "f"]
    print(f"    WRONG_DEP with needs_review=f (SILENT): {len(silent)}")
    for o in silent[:10]:
        print(f"    {o['icao24']} {o['callsign']:8s} P3dep={o['p3_dep']} "
              f"cons_dep={o['consensus_dep']} arr={o['arr']} alt={o['first_alt']} {o['trend']}")
    desc = [o for o in out if o["trend"] == "DESCENT"]
    print(f"    P3 fired on a DESCENT first-sighting: {len(desc)}")
    write_csv("e7_p3_snaps", out, list(out[0].keys()) if out else [])
    return out


# --------------------------------------------------------------------------
# E6 — wrong-field arrival snaps
# --------------------------------------------------------------------------
def cmd_e6(args):
    rows = L.load()
    cmap = L.build_callsign_routes(rows)
    coords = load_airport_coords()
    pos_start = datetime(2026, 6, 2, tzinfo=timezone.utc)

    out = []
    for r in rows:
        cs = (r["callsign"] or "").strip()
        cons = cmap.get(cs)
        if not cons:
            continue
        arr = L.norm_ap(r["arrival_airport_icao"])
        dep = L.norm_ap(r["departure_airport_icao"])
        if not arr or arr == cons[1] or arr == cons[0] or arr == dep:
            continue
        if arr not in coords or cons[1] not in coords:
            continue
        _, ala, alo = coords[arr]
        _, cla, clo = coords[cons[1]]
        dist = _haversine_km(ala, alo, cla, clo)
        if dist > 60:
            continue  # a different city entirely — diversion territory, not a snap
        t0, t1 = parse_ts(r["first_seen"]), parse_ts(r["last_seen"])
        # physics: where was the aircraft at close time?
        verdict, min_vel = "NO_DATA", ""
        if t1 and t1 >= pos_start:
            seq = positions_for(r["icao24"].strip())
            near = [p for p in seq
                    if abs((p["captured_at"] - t1).total_seconds()) <= 1800]
            vels = [p["velocity_ms"] for p in near if p["velocity_ms"] is not None]
            if near:
                last = near[-1]
                d_wrong = _haversine_km(last["latitude"], last["longitude"], ala, alo)
                d_cons = _haversine_km(last["latitude"], last["longitude"], cla, clo)
                min_vel = round(min(vels), 1) if vels else ""
                verdict = ("MIS_SNAP" if d_cons < d_wrong else
                           "REAL_AT_FIELD" if (vels and min(vels) < 15) else "UNCLEAR")
        out.append({
            "icao24": r["icao24"].strip(), "registration": r["registration"],
            "callsign": cs, "dep": dep or "", "arr": arr,
            "consensus_arr": cons[1], "dist_km": round(dist, 1),
            "arr_type": coords[arr][0],
            "first_seen": r["first_seen"], "needs_review": r["needs_review"],
            "post_deploy": "t" if (t0 and t0 >= DEPLOY) else "f",
            "verdict": verdict, "min_vel_at_close": min_vel,
        })

    print(f"E6: arrivals != consensus but within 60km of it: {len(out)}")
    v = collections.Counter(o["verdict"] for o in out)
    for k, c in v.most_common():
        print(f"    {k:14s}: {c}")
    silent = [o for o in out if o["verdict"] == "MIS_SNAP" and o["needs_review"] == "f"]
    print(f"    MIS_SNAP with needs_review=f (SILENT wrong route): {len(silent)}")
    wrongfield = collections.Counter((o["arr"], o["consensus_arr"]) for o in out)
    print("    top wrong->true pairs:")
    for (a, c), n in wrongfield.most_common(10):
        print(f"      {a} instead of {c}: {n}")
    write_csv("e6_wrong_field", out, list(out[0].keys()) if out else [])
    return out


# --------------------------------------------------------------------------
# E8 — dep=previous-arrival fallback poisoning
# --------------------------------------------------------------------------
def cmd_e8(args):
    rows = L.load()
    cmap = L.build_callsign_routes(rows)
    by_reg = L.by_registration(rows)

    out = []
    for reg, legs in by_reg.items():
        for a, b in zip(legs, legs[1:]):
            dep_b = L.norm_ap(b["departure_airport_icao"])
            arr_a = L.norm_ap(a["arrival_airport_icao"])
            if not dep_b or dep_b != arr_a:
                continue
            cs = (b["callsign"] or "").strip()
            cons = cmap.get(cs)
            if not cons or cons[0] == dep_b:
                continue
            ta, tb = parse_ts(a["last_seen"]), parse_ts(b["first_seen"])
            gap_h = (tb - ta).total_seconds() / 3600.0 if ta and tb else None
            out.append({
                "registration": reg, "icao24": b["icao24"].strip(),
                "callsign": cs, "inherited_dep": dep_b,
                "consensus_dep": cons[0], "arr": L.norm_ap(b["arrival_airport_icao"]) or "",
                "prev_callsign": (a["callsign"] or "").strip(),
                "prev_review": a["needs_review"], "review": b["needs_review"],
                "gap_h": round(gap_h, 1) if gap_h is not None else "",
                "first_seen": b["first_seen"],
                "post_deploy": "t" if (parse_ts(b["first_seen"]) or DEPLOY) >= DEPLOY else "f",
            })

    print(f"E8: legs whose dep == previous arrival but consensus disagrees: {len(out)}")
    post = [o for o in out if o["post_deploy"] == "t"]
    silent = [o for o in out if o["review"] == "f"]
    print(f"    post-deploy: {len(post)}   needs_review=f (SILENT): {len(silent)}")
    src = collections.Counter(o["inherited_dep"] for o in out)
    print("    top inherited airports:", dict(src.most_common(8)))
    write_csv("e8_inherited_dep", out, list(out[0].keys()) if out else [])
    return out


# --------------------------------------------------------------------------
# E10 — C6 mid-air callsign-change false closes
# --------------------------------------------------------------------------
def cmd_e10(args):
    rows = L.load()
    by_reg = L.by_registration(rows)
    pos_start = datetime(2026, 6, 2, tzinfo=timezone.utc)

    out = []
    for reg, legs in by_reg.items():
        for a, b in zip(legs, legs[1:]):
            arr_a = (a["arrival_airport_icao"] or "").strip()
            dep_b = (b["departure_airport_icao"] or "").strip()
            if arr_a != "UNKN" or dep_b:
                continue
            ta, tb = parse_ts(a["last_seen"]), parse_ts(b["first_seen"])
            if not ta or not tb or (tb - ta).total_seconds() > 1800:
                continue
            # verify from positions: airborne at the handover?
            alt = ""
            if tb >= pos_start:
                seq = positions_for(b["icao24"].strip())
                near = [p for p in seq
                        if abs((p["captured_at"] - tb).total_seconds()) <= 600]
                alts = [p["altitude_m"] for p in near if p["altitude_m"] is not None]
                alt = int(max(alts)) if alts else ""
            b_dep = L.norm_ap(b["departure_airport_icao"])
            b_arr = L.norm_ap(b["arrival_airport_icao"])
            outcome = ("ENRICHED_SELF_LOOP" if b_dep and b_dep == b_arr else
                       "STILL_NULL_DEP" if not b_dep else
                       "ARR_UNKN" if not b_arr else "ROUTE_FILLED")
            out.append({
                "registration": reg, "icao24": b["icao24"].strip(),
                "cs_before": (a["callsign"] or "").strip(),
                "cs_after": (b["callsign"] or "").strip(),
                "a_last": a["last_seen"], "b_first": b["first_seen"],
                "alt_at_handover": alt,
                "b_dep": b_dep or "", "b_arr": b_arr or "",
                "b_review": b["needs_review"],
                "outcome": outcome,
                "post_deploy": "t" if tb >= DEPLOY else "f",
            })

    print(f"E10: UNKN-close immediately followed by dep=None open (C6 signature): {len(out)}")
    airborne = [o for o in out if o["alt_at_handover"] and o["alt_at_handover"] > 3000]
    print(f"    verified airborne at handover (mid-flight callsign change): {len(airborne)}")
    print(f"    post-deploy: {sum(1 for o in out if o['post_deploy'] == 't')}")
    oc = collections.Counter((o["outcome"], o["b_review"]) for o in out)
    print("    successor-leg outcome (post-enrichment state) x review:")
    for (k, rev), n in oc.most_common():
        print(f"      {k:18s} review={rev}: {n}")
    for o in out[:10]:
        print(f"    {o['registration']:7s} {o['cs_before']:8s}->{o['cs_after']:8s} "
              f"@ {o['b_first'][:16]} alt={o['alt_at_handover']} -> arr {o['b_arr'] or '?'}")
    write_csv("e10_c6_closes", out, list(out[0].keys()) if out else [])
    return out


# --------------------------------------------------------------------------
# census — E1 residual splits, E9 stale-close, E4 UNKN recovery
# --------------------------------------------------------------------------
def cmd_census(args):
    rows = L.load()
    cmap = L.build_callsign_routes(rows)
    by_reg = L.by_registration(rows)

    # E1: gap-split pairs (find_gap_splits heuristic), pre vs post deploy
    pre = post = 0
    post_pairs = []
    for reg, legs in by_reg.items():
        for a, b in zip(legs, legs[1:]):
            csa, csb = (a["callsign"] or "").strip(), (b["callsign"] or "").strip()
            if not csa or csa != csb:
                continue
            arr_a, dep_b = L.norm_ap(a["arrival_airport_icao"]), L.norm_ap(b["departure_airport_icao"])
            if not arr_a or arr_a != dep_b:
                continue
            cons = cmap.get(csa)
            if not cons or arr_a in cons:
                continue
            t = parse_ts(a["first_seen"])
            if t and t >= DEPLOY:
                post += 1
                post_pairs.append((reg, csa, arr_a, a["first_seen"]))
            else:
                pre += 1
    pre_days = (DEPLOY - datetime(2026, 3, 16, tzinfo=timezone.utc)).days
    post_days = max(1, (datetime.now(timezone.utc) - DEPLOY).days)
    print(f"E1 census: consensus-flagged split pairs  "
          f"pre-deploy {pre} ({pre/pre_days:.2f}/day)   "
          f"post-deploy {post} ({post/post_days:.2f}/day)")
    for p in post_pairs[:10]:
        print(f"    post-deploy pair: {p}")

    # E9: stale-close census (needs_review rows by duration bucket)
    review = [r for r in rows if r["needs_review"] == "t"]
    buckets = collections.Counter()
    for r in review:
        try:
            d = float(r["duration_minutes"])
        except (TypeError, ValueError):
            continue
        arr = (r["arrival_airport_icao"] or "").strip()
        dep = L.norm_ap(r["departure_airport_icao"])
        b = ("<20h" if d < 1200 else ">=20h")
        kind = ("UNKN" if arr == "UNKN" else
                "self_loop" if L.norm_ap(arr) == dep else "resolved")
        buckets[(b, kind)] += 1
    print(f"\nE9 census: needs_review={len(review)} rows by duration x arrival kind:")
    for (b, kind), n in sorted(buckets.items()):
        print(f"    {b:6s} {kind:10s}: {n}")

    # E4: UNKN arrivals and their recovery potential, post-deploy
    unkn = [r for r in rows if (r["arrival_airport_icao"] or "").strip() == "UNKN"]
    post_unkn = [r for r in unkn if (parse_ts(r["first_seen"]) or DEPLOY) >= DEPLOY]
    recoverable = [r for r in post_unkn if (r["callsign"] or "").strip() in cmap]
    print(f"\nE4 census: UNKN arrivals total {len(unkn)}, post-deploy {len(post_unkn)}, "
          f"of which callsign HAS consensus (enrichment should fill): {len(recoverable)}")
    stale_cs = collections.Counter((r["callsign"] or "").strip() for r in post_unkn
                                   if (r["callsign"] or "").strip() not in cmap)
    print(f"    post-deploy UNKN with NO consensus (unrecoverable): "
          f"{sum(stale_cs.values())}; top callsigns: {dict(stale_cs.most_common(8))}")


# --------------------------------------------------------------------------
# E12 — analysis-layer: consensus staleness / poisoning
# --------------------------------------------------------------------------
def cmd_e12(args):
    rows = L.load()
    cmap = L.build_callsign_routes(rows)
    now = datetime.now(timezone.utc)
    recent_cut = now - timedelta(days=14)

    recent = collections.defaultdict(collections.Counter)
    for r in rows:
        cs = (r["callsign"] or "").strip()
        dep, arr = L.norm_ap(r["departure_airport_icao"]), L.norm_ap(r["arrival_airport_icao"])
        t = parse_ts(r["first_seen"])
        if cs and dep and arr and dep != arr and t and t >= recent_cut:
            recent[cs][(dep, arr)] += 1

    stale = []
    for cs, cnt in recent.items():
        top, n = cnt.most_common(1)[0]
        if cs in cmap and n >= 3 and cmap[cs] != top:
            stale.append((cs, cmap[cs], top, n))
    print(f"E12: callsigns whose recent (14d) modal route disagrees with all-time "
          f"consensus: {len(stale)}")
    for cs, old, new, n in stale[:15]:
        print(f"    {cs:8s} consensus={old[0]}-{old[1]}  recent={new[0]}-{new[1]} (x{n})")

    # poisoning: winning pair with weak dominance
    cons_full = collections.defaultdict(collections.Counter)
    for r in rows:
        cs = (r["callsign"] or "").strip()
        dep, arr = L.norm_ap(r["departure_airport_icao"]), L.norm_ap(r["arrival_airport_icao"])
        if cs and dep and arr and dep != arr:
            cons_full[cs][(dep, arr)] += 1
    weak = []
    for cs, cnt in cons_full.items():
        mc = cnt.most_common(2)
        if len(mc) >= 2 and mc[0][1] >= 3 and mc[1][1] >= 0.5 * mc[0][1]:
            weak.append((cs, mc[0], mc[1]))
    print(f"\n    contested consensus (runner-up >= 50% of winner): {len(weak)}")
    for cs, a, b in weak[:10]:
        print(f"    {cs:8s} {a[0][0]}-{a[0][1]} x{a[1]}  vs  {b[0][0]}-{b[0][1]} x{b[1]}")


# --------------------------------------------------------------------------
# FIS cross-check — external oracle for widebody legs
# --------------------------------------------------------------------------
IATA_ICAO = {
    "FRA": "EDDF", "MUC": "EDDM", "JFK": "KJFK", "EWR": "KEWR", "ORD": "KORD",
    "BOS": "KBOS", "IAD": "KIAD", "MIA": "KMIA", "IAH": "KIAH", "LAX": "KLAX",
    "SFO": "KSFO", "DEN": "KDEN", "SEA": "KSEA", "YVR": "CYVR", "YYZ": "CYYZ",
    "MEX": "MMMX", "GRU": "SBGR", "GIG": "SBGL", "EZE": "SAEZ", "SCL": "SCEL",
    "JNB": "FAOR", "NRT": "RJAA", "HND": "RJTT", "ICN": "RKSI", "PVG": "ZSPD",
    "PEK": "ZBAA", "HKG": "VHHH", "SIN": "WSSS", "BKK": "VTBS", "DEL": "VIDP",
    "BOM": "VABB", "BLR": "VOBL", "SGN": "VVTS", "CLT": "KCLT", "DFW": "KDFW",
    "ATL": "KATL", "PHL": "KPHL", "DTW": "KDTW", "MSP": "KMSP", "AUS": "KAUS",
    "SJC": "KSJC", "SAN": "KSAN", "LAS": "KLAS", "MCO": "KMCO", "TPA": "KTPA",
    "BER": "EDDB", "DUS": "EDDL", "HAM": "EDDH", "CDG": "LFPG", "LHR": "EGLL",
    "NBO": "HKJK", "CPT": "FACT", "DXB": "OMDB", "TLV": "LLBG", "RUH": "OERK",
    "JED": "OEJN", "BAH": "OBBI", "KWI": "OKKK", "DOH": "OTHH", "NGO": "RJGG",
    "KIX": "RJBB", "CAI": "HECA",
}


def cmd_fis(args):
    rows = L.load()
    # tail -> icao24 map + legs per registration/date
    legs_by_reg = collections.defaultdict(list)
    for r in rows:
        t0 = parse_ts(r["first_seen"])
        if t0 and t0 >= DEPLOY - timedelta(days=10):
            legs_by_reg[r["registration"].strip()].append(r)

    fis = []
    seen = set()
    with open(TMP / "fis_export.csv", newline="") as f:
        for r in csv.DictReader(f):
            if r["found"] != "t" or not r["registration"]:
                continue
            # keep only the LAST observation per (flight_date, flight_number)
            key = (r["flight_date"], r["flight_number"])
            fis.append((key, r))
    latest = {}
    for key, r in fis:
        if key not in latest or r["observed_date"] >= latest[key]["observed_date"]:
            latest[key] = r

    unmapped = collections.Counter()
    results = collections.Counter()
    misses = []
    for (fdate, fnum), r in sorted(latest.items()):
        # only past flights can be checked against detector legs
        if fdate >= datetime.now(timezone.utc).strftime("%Y-%m-%d"):
            continue
        dep_i, arr_i = IATA_ICAO.get(r["dep_airport_iata"]), IATA_ICAO.get(r["arr_airport_iata"])
        if not dep_i or not arr_i:
            unmapped[r["dep_airport_iata"] if not dep_i else r["arr_airport_iata"]] += 1
            continue
        reg = r["registration"].strip()
        cands = [l for l in legs_by_reg.get(reg, [])
                 if l["flight_date"] in (fdate,) or
                 abs((parse_ts(l["first_seen"]) or DEPLOY).date().toordinal()
                     - datetime.fromisoformat(fdate).toordinal()) <= 1]
        # match by route or by callsign number
        cs_want = f"DLH{int(fnum)}" if fnum.isdigit() else None
        hit = None
        for l in cands:
            dep, arr = L.norm_ap(l["departure_airport_icao"]), L.norm_ap(l["arrival_airport_icao"])
            if dep == dep_i and arr == arr_i:
                hit = ("MATCH", l)
                break
        if not hit and cs_want:
            for l in cands:
                if (l["callsign"] or "").strip() == cs_want:
                    dep, arr = L.norm_ap(l["departure_airport_icao"]), L.norm_ap(l["arrival_airport_icao"])
                    if arr is None or dep is None:
                        hit = ("PARTIAL_UNKN", l)
                    else:
                        hit = ("WRONG_ROUTE", l)
                    break
        if not hit:
            # was the aircraft captured by the poller at all around that date?
            icao = None
            for l in legs_by_reg.get(reg, []):
                icao = l["icao24"].strip()
                break
            if icao is None:
                for row in rows:
                    if row["registration"].strip() == reg:
                        icao = row["icao24"].strip()
                        break
            n_pos = 0
            if icao:
                day0 = datetime.fromisoformat(fdate).replace(tzinfo=timezone.utc)
                seq = positions_for(icao)
                n_pos = sum(1 for p in seq
                            if day0 <= p["captured_at"]
                            <= day0 + timedelta(hours=30))
            kind = "MISSING_NO_ADSB" if n_pos < 5 else "MISSING_HAS_DATA"
            if fdate < "2026-06-29":
                kind += "_PREPURGE"   # flagged rows before deploy were deleted 07-02
            hit = (kind, None)
        results[hit[0]] += 1
        if hit[0] in ("WRONG_ROUTE", "MISSING_HAS_DATA") and len(misses) < 40:
            got = ""
            if hit[1] is not None:
                got = (f"{hit[1]['departure_airport_icao']}->"
                       f"{hit[1]['arrival_airport_icao']} rev={hit[1]['needs_review']}")
            misses.append(f"    {fdate} LH{fnum:>4s} {reg:7s} "
                          f"want {dep_i}->{arr_i}  got {got or '-'}")

    total = sum(results.values())
    print(f"FIS cross-check (truth = latest observation per flight, past dates, "
          f"{total} checkable):")
    for k, n in results.most_common():
        print(f"    {k:13s}: {n:4d}  ({n/total:.0%})")
    print("  discrepancies:")
    print("\n".join(misses))
    if unmapped:
        print(f"    (skipped, unmapped IATA: {dict(unmapped.most_common())})")


# --------------------------------------------------------------------------
AUDITS = {
    "presplit": cmd_presplit, "replay": cmd_replay,
    "e3": cmd_e3, "e2": cmd_e2, "e7": cmd_e7, "e6": cmd_e6, "e8": cmd_e8,
    "e10": cmd_e10, "census": cmd_census, "e12": cmd_e12, "fis": cmd_fis,
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("audits", nargs="+", choices=sorted(AUDITS))
    ap.add_argument("--tag", default="prod", choices=sorted(CONFIGS),
                    help="config tag for `replay`")
    args = ap.parse_args()
    for name in args.audits:
        print(f"\n===== {name} =====")
        AUDITS[name](args)


if __name__ == "__main__":
    main()
