-- ============================================================
-- The cabin a flight is SOLD with (fis_legs)
-- ============================================================
-- FIS's seatConfig describes the flight, not just the airframe: the same
-- 747-8 is published F8C80E32M244 to Boston and C88E32M244 to Bengaluru
-- (a route without First: no First cabin published), and Allegris A350s drop from F4C38E24M201 to
-- C42E24M201 on some flights. For an award booking the flight's own layout is
-- what counts, so each leg keeps the layout of its operated look (else its
-- latest). The airframe's physical cabin is the layout it is most often
-- published with (dashboard _cabin_configs).
--
-- Additive and nullable: the collector and dashboard check for the columns and
-- work without them. After applying, rebuild so history gets the values:
--   docker compose exec flightstatus python fetch_flightstatus.py --rebuild-legs
ALTER TABLE fis_legs ADD COLUMN IF NOT EXISTS seat_config VARCHAR(40);
ALTER TABLE fis_legs ADD COLUMN IF NOT EXISTS allegris BOOLEAN;
