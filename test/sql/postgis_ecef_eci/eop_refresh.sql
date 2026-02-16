-- Test: EOP refresh, sync, and staleness detection
-- Validates the production-ready EOP auto-refresh pipeline (§8.3)

\set ON_ERROR_STOP 1

\ir include/setup.sql
\ir ../../../../sql/postgis_ecef_eci/eop.sql

-- =============================================================================
-- Test 1: sync_eop_to_postgis with missing postgis_eop table (graceful skip)
-- =============================================================================
-- When PostGIS ECEF/ECI extension is not installed, sync should return -1

SELECT ecef_eci.sync_eop_to_postgis() AS sync_no_table;

-- =============================================================================
-- Test 2: sync_eop_to_postgis with populated data
-- =============================================================================

-- Create a mock postgis_eop table (simulates PostGIS extension)
CREATE TABLE IF NOT EXISTS public.postgis_eop (
    mjd   FLOAT8 PRIMARY KEY,
    xp    FLOAT8,
    yp    FLOAT8,
    dut1  FLOAT8,
    dx    FLOAT8,
    dy    FLOAT8
);

-- Insert synthetic EOP data
INSERT INTO ecef_eci.eop_data (mjd, date, xp, yp, dut1, lod, dx, dy, data_type)
VALUES
    (60676.0, '2025-01-01', 0.1234, 0.2345, -0.0456, 0.8, 0.05, -0.03, 'I'),
    (60677.0, '2025-01-02', 0.1240, 0.2350, -0.0460, 0.9, 0.06, -0.02, 'I'),
    (60678.0, '2025-01-03', 0.1245, 0.2360, -0.0465, 0.7, 0.04, -0.04, 'I'),
    (60679.0, '2025-01-04', 0.1250, 0.2370, -0.0470, 0.8, 0.05, -0.03, 'P'),
    (60680.0, '2025-01-05', 0.1255, 0.2380, -0.0475, 0.9, 0.06, -0.02, 'P');

-- Sync to mock postgis_eop
SELECT ecef_eci.sync_eop_to_postgis() AS sync_count;

-- Verify rows match
SELECT count(*) AS postgis_eop_count FROM public.postgis_eop;

-- Verify a specific row
SELECT mjd, round(xp::numeric, 4) AS xp, round(dut1::numeric, 4) AS dut1
FROM public.postgis_eop
WHERE mjd = 60676.0;

-- =============================================================================
-- Test 3: sync_eop_to_postgis — upsert behavior
-- =============================================================================

-- Update source data
UPDATE ecef_eci.eop_data SET xp = 0.9999 WHERE mjd = 60676.0;

-- Re-sync
SELECT ecef_eci.sync_eop_to_postgis() AS sync_upsert_count;

-- Verify update propagated
SELECT round(xp::numeric, 4) AS xp_updated
FROM public.postgis_eop
WHERE mjd = 60676.0;

-- =============================================================================
-- Test 4: eop_staleness with populated data (data from 2025 is stale)
-- =============================================================================

SELECT
    latest_date,
    gap_days > 0 AS has_gap,
    is_stale,
    load_age_hours >= 0 AS age_valid
FROM ecef_eci.eop_staleness();

-- With a very large threshold, data should not be stale
SELECT is_stale AS stale_with_large_threshold
FROM ecef_eci.eop_staleness(99999);

-- =============================================================================
-- Test 5: eop_staleness with empty table
-- =============================================================================

TRUNCATE ecef_eci.eop_data;

SELECT
    latest_date IS NULL AS no_date,
    gap_days = -1 AS gap_sentinel,
    is_stale AS empty_is_stale
FROM ecef_eci.eop_staleness();

-- =============================================================================
-- Test 6: refresh_eop with missing config
-- =============================================================================

-- Should emit WARNING, not EXCEPTION (job stays active)
DO $$
BEGIN
    CALL ecef_eci.refresh_eop(0, '{}'::jsonb);
END;
$$;

-- =============================================================================
-- Test 7: refresh_eop with nonexistent file
-- =============================================================================

DO $$
BEGIN
    CALL ecef_eci.refresh_eop(0, '{"eop_file_path": "/nonexistent/finals2000A.all"}'::jsonb);
END;
$$;

-- =============================================================================
-- Test 8: End-to-end with synthetic finals2000A data
-- =============================================================================

-- Synthetic finals2000A lines (columns match IERS fixed-width spec)
-- Cols: 1-2=year, 3-4=month, 5-6=day, 7=space, 8-15=MJD, 16=space, 17=I/P flag,
--       18=space, 19-27=PM-x, ... 38-46=PM-y, ... 59-68=UT1-UTC
-- Each line is at least 68 chars

SELECT ecef_eci.load_eop_finals2000a(
-- Line 1: MJD=60676.000, I flag, xp=0.123400, yp=0.234500, dut1=-0.045600
'25 1 1 60676.000 I  0.123400                    0.234500              -0.0456000' || E'\n' ||
-- Line 2: MJD=60677.000, I flag, xp=0.124000, yp=0.235000, dut1=-0.046000
'25 1 2 60677.000 I  0.124000                    0.235000              -0.0460000' || E'\n' ||
-- Line 3: MJD=60678.000, P flag, xp=0.124500, yp=0.236000, dut1=-0.046500
'25 1 3 60678.000 P  0.124500                    0.236000              -0.0465000'
) AS e2e_loaded;

-- Verify loaded data
SELECT
    count(*) AS loaded_rows,
    round(min(mjd)::numeric, 0) AS min_mjd,
    round(max(mjd)::numeric, 0) AS max_mjd
FROM ecef_eci.eop_data;

-- Verify one row's values
SELECT
    round(xp::numeric, 6) AS xp,
    round(yp::numeric, 6) AS yp,
    round(dut1::numeric, 7) AS dut1,
    data_type
FROM ecef_eci.eop_data
WHERE mjd = 60676.0;

-- Sync to PostGIS and verify
SELECT ecef_eci.sync_eop_to_postgis() AS e2e_synced;

SELECT count(*) AS postgis_rows FROM public.postgis_eop;

-- Check EOP status
SELECT total_entries, earliest_date, latest_date
FROM ecef_eci.eop_status();

-- Staleness check (data from 2025 is stale relative to current date)
SELECT is_stale AS e2e_is_stale FROM ecef_eci.eop_staleness();

-- Cleanup
DROP TABLE IF EXISTS public.postgis_eop;
DROP SCHEMA ecef_eci CASCADE;
