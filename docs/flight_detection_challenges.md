# Flight Detection Challenges

Known edge cases and challenges for ADS-B based flight detection. Reference this
when making changes to `flight_detector.py` to avoid regressions.

## 1. Missed ground-to-air transition

ADS-B coverage at the departure airport is poor, or the poller misses the
on-ground state between 2-minute polling intervals. The first airborne position
is already climbing.

**Mitigation:** `_infer_missed_departures` creates a pending flight using the
aircraft's last arrival airport as the departure.

## 2. Missed air-to-ground transition

ADS-B coverage at the arrival airport is sparse. The last position shows low
altitude and high velocity, then positions stop. The aircraft never appears
"on_ground."

**Mitigation:** `_close_pending_flights` has a proximity-based landing fallback
that triggers when positions are stale (>10 min), altitude is low (<500 m), and
the aircraft is within 8 km of an airport.

## 3. Mid-flight coverage gaps

Long-haul routes cross regions with no ADS-B receivers (oceans, Arctic).
Positions disappear at cruise altitude and resume on descent. Must not be
misinterpreted as a landing.

**Why our checks are safe:** The proximity fallback requires altitude <500 m.
Cruise altitude is 10,000 m+, so the fallback does not trigger. When positions
resume on descent, `_close_pending_flights` picks up normally.

## 4. Go-arounds

Aircraft descends near an airport to low altitude, then climbs again.
Proximity-based landing detection must not trigger while positions are still
flowing.

**Why our checks are safe:** The proximity fallback requires positions to be
stale (>10 min old). During a go-around, fresh positions keep arriving, so the
staleness check prevents a false positive.

## 5. Short turnarounds / multi-sector days

A320s can fly 3-4 sectors in the time a widebody does one long-haul. A large
`MISSED_DEPARTURE_MAX_GAP_H` value risks attributing a departure to a much
earlier arrival at a different airport.

**Current setting:** 48 hours. This works for widebodies but may need tuning if
narrowbody tracking is added.

## 6. dep == arr false positives

Maintenance flights, training flights, or missed real routes where the detection
thinks the aircraft returned to the same airport.

**Mitigation:** These flights are flagged with `needs_review = TRUE` for manual
inspection.

## 7. OpenSky on_ground flag unreliability

The `on_ground` flag is sometimes wrong — says airborne when aircraft is taxiing,
or vice versa. The velocity+altitude fallback (`_is_on_ground`) compensates but
has its own thresholds (velocity <30 m/s, altitude <300 m).

## 8. Duplicate/split flights

If a coverage gap causes a stale flight to be closed with UNKN and then
positions resume, `_infer_missed_departures` creates a second flight for the
same real-world flight. This results in two flight records where there should be
one.

**Mitigation:** Both flights are flagged for review. No automated merging exists
yet.
