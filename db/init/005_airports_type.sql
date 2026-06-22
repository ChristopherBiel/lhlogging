-- ============================================================
-- Airport size class (source: OurAirports `type`)
-- ============================================================
-- Lets nearest-airport lookup prefer a major hub over a nearby GA/medium
-- field (e.g. an airliner at Frankfurt EDDF must not snap to the Egelsbach
-- GA strip EDFE, ~7 km away). Populated by tools/load_airports.py.
-- Existing rows stay NULL until the loader is re-run; the lookup treats a
-- NULL/blank type as "no hub preference" so behaviour is unchanged until then.
ALTER TABLE airports ADD COLUMN IF NOT EXISTS type VARCHAR(20);
