-- ============================================================
-- Manufacturer serial number (MSN) per airframe
-- ============================================================
-- Shown on the /airframe profile. Filled by the weekly fleet_refresh from the
-- OpenSky aircraft database's `serialnumber` column (present for ~70 of the
-- 80 LH widebodies; the database's build/first-flight dates are almost always
-- empty, so the airframe's age is not shown). COALESCE-only, like the other
-- fleet_refresh fields: never overwrites a value already set.
ALTER TABLE aircraft ADD COLUMN IF NOT EXISTS serial_number VARCHAR(20);
