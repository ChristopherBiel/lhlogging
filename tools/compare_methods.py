"""
Comparison script: /flights/aircraft (current) vs /flights/all (candidate).

Tests both methods against a small set of known ICAO24s over yesterday's UTC day,
logs every X-Rate-Limit-Remaining header seen, and compares the returned flight sets.

Usage:
    cd /path/to/lhlogging
    pip install requests python-dotenv
    python3 tools/compare_methods.py
"""
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import os

ICAO24S = ["3c65ad", "3c4b2e", "3c65a1", "3c65a8", "3c4a8c"]

CLIENT_ID     = os.environ["OPENSKY_CLIENT_ID"]
CLIENT_SECRET = os.environ["OPENSKY_CLIENT_SECRET"]
BASE_URL      = os.environ.get("OPENSKY_BASE_URL", "https://opensky-network.org/api")
TOKEN_URL     = os.environ.get(
    "OPENSKY_TOKEN_URL",
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
)

# Yesterday UTC: 00:00 → 23:59:59
today     = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
yesterday = today - timedelta(days=1)
WIN_BEGIN = int(yesterday.timestamp())
WIN_END   = int(today.timestamp()) - 1

print(f"Time window: {yesterday.date()} 00:00 UTC → {yesterday.date()} 23:59:59 UTC")
print(f"  begin={WIN_BEGIN}  end={WIN_END}\n")

# ---------------------------------------------------------------------------
# Credit tracking
# ---------------------------------------------------------------------------

class CreditTracker:
    def __init__(self):
        self.calls: list[dict] = []  # {label, status, remaining, retry_after}

    def record(self, resp: requests.Response, label: str) -> None:
        remaining   = resp.headers.get("X-Rate-Limit-Remaining")
        retry_after = resp.headers.get("X-Rate-Limit-Retry-After-Seconds")

        self.calls.append({
            "label":       label,
            "status":      resp.status_code,
            "remaining":   int(remaining)   if remaining   is not None else None,
            "retry_after": int(retry_after) if retry_after is not None else None,
        })

        parts = [f"  [{label}] HTTP {resp.status_code}"]
        parts.append(f"credits_remaining={remaining if remaining is not None else '<absent>'}")
        if retry_after is not None:
            reset_at = datetime.now(timezone.utc) + timedelta(seconds=int(retry_after))
            parts.append(f"retry_after={retry_after}s (resets ~{reset_at.strftime('%Y-%m-%d %H:%M UTC')})")
        print("  ".join(parts))

    def first_429_reset(self) -> datetime | None:
        for c in self.calls:
            if c["status"] == 429 and c["retry_after"] is not None:
                return datetime.now(timezone.utc) + timedelta(seconds=c["retry_after"])
        return None

    def credits_used(self, method_calls: int) -> str:
        """
        Estimate credits consumed from the delta in X-Rate-Limit-Remaining,
        or fall back to a note explaining it's unavailable.
        """
        remainders = [c["remaining"] for c in self.calls if c["remaining"] is not None]
        if len(remainders) >= 2:
            used = remainders[0] - remainders[-1]
            return f"{used} credits used ({remainders[0]} → {remainders[-1]} remaining)"
        elif len(remainders) == 1:
            return f"remaining after run: {remainders[0]} (no start baseline)"
        else:
            return f"<X-Rate-Limit-Remaining absent — cannot measure directly> ({method_calls} calls made)"

    def summary(self, label: str, call_count: int) -> None:
        ok  = sum(1 for c in self.calls if c["status"] < 400)
        err = sum(1 for c in self.calls if c["status"] >= 400)
        print(f"  {label}: {call_count} calls  ({ok} ok, {err} errors)  {self.credits_used(call_count)}")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def fetch_token() -> str:
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type":    "client_credentials",
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def make_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {token}"
    s.headers["User-Agent"]    = "LHLogging/compare-methods"
    return s


# ---------------------------------------------------------------------------
# Preflight: probe a single cheap call to check if we're rate-limited
# ---------------------------------------------------------------------------

def preflight_check(session: requests.Session) -> None:
    """One quick probe call. Exits with a clear message if we're still rate-limited."""
    resp = session.get(
        f"{BASE_URL}/flights/aircraft",
        params={"icao24": ICAO24S[0], "begin": WIN_BEGIN, "end": WIN_END},
        timeout=30,
    )
    if resp.status_code == 429:
        retry_after = resp.headers.get("X-Rate-Limit-Retry-After-Seconds")
        if retry_after:
            reset_at = datetime.now(timezone.utc) + timedelta(seconds=int(retry_after))
            print(f"Rate-limited. Credits reset at {reset_at.strftime('%Y-%m-%d %H:%M UTC')} "
                  f"(in {int(retry_after) // 3600}h {(int(retry_after) % 3600) // 60}m).")
        else:
            print("Rate-limited. No retry-after header present.")
        sys.exit(1)
    print(f"Preflight OK (HTTP {resp.status_code}). Credits are available.\n")


# ---------------------------------------------------------------------------
# Method A: /flights/aircraft  (current — one call per aircraft)
# ---------------------------------------------------------------------------

def method_a(session: requests.Session) -> tuple[dict[str, list[dict]], CreditTracker]:
    print("=" * 60)
    print("METHOD A: /flights/aircraft  (one call per ICAO24)")
    print("=" * 60)
    tracker = CreditTracker()
    results: dict[str, list[dict]] = {}

    for icao24 in ICAO24S:
        time.sleep(2)
        resp = session.get(
            f"{BASE_URL}/flights/aircraft",
            params={"icao24": icao24, "begin": WIN_BEGIN, "end": WIN_END},
            timeout=30,
        )
        tracker.record(resp, f"aircraft/{icao24}")
        if resp.status_code == 404:
            results[icao24] = []
        elif resp.ok:
            raw = resp.json()
            results[icao24] = raw if raw else []
        else:
            results[icao24] = []

    total = sum(len(v) for v in results.values())
    print(f"\nMethod A: {len(ICAO24S)} calls → {total} flights total\n")
    return results, tracker


# ---------------------------------------------------------------------------
# Method B: /flights/all  (one call per 2-hour chunk, filter client-side)
# ---------------------------------------------------------------------------

def method_b(session: requests.Session) -> tuple[dict[str, list[dict]], CreditTracker]:
    print("=" * 60)
    print("METHOD B: /flights/all  (2-hour windows, filter client-side)")
    print("=" * 60)
    tracker   = CreditTracker()
    fleet_set = set(ICAO24S)
    results: dict[str, list[dict]] = {icao24: [] for icao24 in ICAO24S}

    window_size = 2 * 3600
    chunk_start = WIN_BEGIN
    call_count  = 0

    while chunk_start < WIN_END:
        chunk_end = min(chunk_start + window_size, WIN_END)
        time.sleep(2)
        resp = session.get(
            f"{BASE_URL}/flights/all",
            params={"begin": chunk_start, "end": chunk_end},
            timeout=30,
        )
        label = (
            f"all/{datetime.fromtimestamp(chunk_start, tz=timezone.utc).strftime('%H:%M')}"
            f"–{datetime.fromtimestamp(chunk_end, tz=timezone.utc).strftime('%H:%M')}"
        )
        tracker.record(resp, label)
        call_count += 1

        if resp.ok:
            raw = resp.json()
            if raw:
                for flight in raw:
                    icao = (flight.get("icao24") or "").strip().lower()
                    if icao in fleet_set:
                        results[icao].append(flight)
        else:
            pass  # already logged by tracker.record

        chunk_start += window_size

    total = sum(len(v) for v in results.values())
    print(f"\nMethod B: {call_count} calls → {total} flights matched for our fleet\n")
    return results, tracker


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------

def flight_key(f: dict) -> tuple:
    return (
        (f.get("icao24") or "").strip().lower(),
        f.get("firstSeen"),
        f.get("lastSeen"),
        (f.get("estDepartureAirport") or "").strip().upper(),
        (f.get("estArrivalAirport") or "").strip().upper(),
    )


def compare(a_results: dict, b_results: dict) -> None:
    print("=" * 60)
    print("COMPARISON")
    print("=" * 60)
    all_match = True
    for icao24 in ICAO24S:
        a_keys = {flight_key(f) for f in a_results[icao24]}
        b_keys = {flight_key(f) for f in b_results[icao24]}
        only_in_a = a_keys - b_keys
        only_in_b = b_keys - a_keys
        status = "OK" if not only_in_a and not only_in_b else "MISMATCH"
        if status != "OK":
            all_match = False
        print(f"  {icao24}: A={len(a_keys)} flights  B={len(b_keys)} flights  [{status}]")
        if only_in_a:
            print(f"    Only in A: {only_in_a}")
        if only_in_b:
            print(f"    Only in B: {only_in_b}")
    print()
    if all_match:
        print("Result: both methods return identical flight sets.")
    else:
        print("Result: DISCREPANCY detected — review above.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Fetching OAuth2 token...")
    token = fetch_token()
    print("Token obtained.\n")

    session = make_session(token)

    print("Preflight check...")
    preflight_check(session)

    a_results, a_tracker = method_a(session)
    b_results, b_tracker = method_b(session)
    compare(a_results, b_results)

    print("=" * 60)
    print("CREDIT SUMMARY")
    print("=" * 60)
    a_tracker.summary("Method A", len(ICAO24S))
    b_tracker.summary("Method B", 12)
    print()
    print(f"  Method A extrapolated to 120 aircraft:")
    a_calls_full = [c for c in a_tracker.calls if c["remaining"] is not None]
    if len(a_calls_full) >= 2:
        cost_per_call = (a_calls_full[0]["remaining"] - a_calls_full[-1]["remaining"]) / len(a_calls_full)
        print(f"    ~{cost_per_call:.1f} credits/call × 120 aircraft = ~{cost_per_call * 120:.0f} credits/run")
    else:
        print("    (cannot extrapolate — X-Rate-Limit-Remaining header absent)")
