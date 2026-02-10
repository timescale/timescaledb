-- Test: EOP data loading and interpolation
-- Validates the Earth Orientation Parameters management system

\set ON_ERROR_STOP 1

\ir include/setup.sql
\ir ../../../../sql/postgis_ecef_eci/eop.sql

-- =============================================================================
-- Test 1: MJD <-> Timestamp conversion
-- =============================================================================

-- MJD 51544.0 = 2000-01-01 00:00:00 UTC
SELECT ecef_eci.mjd_to_timestamp(51544.0)::date AS mjd_epoch;

-- Round-trip
SELECT ecef_eci.timestamp_to_mjd('2000-01-01 00:00:00+00'::timestamptz) AS roundtrip_mjd;

-- 2025-01-01 = MJD 60676.0
SELECT round(ecef_eci.timestamp_to_mjd('2025-01-01 00:00:00+00'::timestamptz)::numeric, 1) AS mjd_2025;

-- =============================================================================
-- Test 2: EOP data loading with synthetic data
-- =============================================================================

-- Insert synthetic EOP entries directly (bypassing parser for unit test)
INSERT INTO ecef_eci.eop_data (mjd, date, xp, yp, dut1, lod, dx, dy, data_type)
VALUES
    (60676.0, '2025-01-01', 0.1234, 0.2345, -0.0456, 0.8, 0.05, -0.03, 'I'),
    (60677.0, '2025-01-02', 0.1240, 0.2350, -0.0460, 0.9, 0.06, -0.02, 'I'),
    (60678.0, '2025-01-03', 0.1245, 0.2360, -0.0465, 0.7, 0.04, -0.04, 'I'),
    (60679.0, '2025-01-04', 0.1250, 0.2370, -0.0470, 0.8, 0.05, -0.03, 'P'),
    (60680.0, '2025-01-05', 0.1255, 0.2380, -0.0475, 0.9, 0.06, -0.02, 'P');

-- Verify
SELECT * FROM ecef_eci.eop_status();

-- =============================================================================
-- Test 3: EOP interpolation
-- =============================================================================

-- At an exact entry (should return values close to the entry itself)
SELECT
    round(xp::numeric, 4) AS xp,
    round(yp::numeric, 4) AS yp,
    round(dut1::numeric, 4) AS dut1
FROM ecef_eci.eop_at_epoch('2025-01-02 00:00:00+00'::timestamptz);

-- At midpoint between 2025-01-02 and 2025-01-03 (should average the two)
SELECT
    round(xp::numeric, 4) AS xp_mid,
    round(yp::numeric, 4) AS yp_mid,
    round(dut1::numeric, 4) AS dut1_mid
FROM ecef_eci.eop_at_epoch('2025-01-02 12:00:00+00'::timestamptz);

-- At 25% between day 1 and day 2
SELECT
    round(xp::numeric, 4) AS xp_quarter,
    round(dut1::numeric, 4) AS dut1_quarter
FROM ecef_eci.eop_at_epoch('2025-01-01 06:00:00+00'::timestamptz);

-- =============================================================================
-- Test 4: EOP status summary
-- =============================================================================

SELECT * FROM ecef_eci.eop_status();

-- =============================================================================
-- Test 5: Upsert behavior (re-loading same data with updates)
-- =============================================================================

-- Update one entry's values
INSERT INTO ecef_eci.eop_data (mjd, date, xp, yp, dut1, lod, dx, dy, data_type)
VALUES (60677.0, '2025-01-02', 0.9999, 0.9999, -0.9999, 0.1, 0.01, -0.01, 'B')
ON CONFLICT (mjd) DO UPDATE SET
    xp = EXCLUDED.xp, yp = EXCLUDED.yp, dut1 = EXCLUDED.dut1,
    data_type = EXCLUDED.data_type, loaded_at = now();

-- Verify the update took effect
SELECT mjd, xp, yp, data_type
FROM ecef_eci.eop_data
WHERE mjd = 60677.0;

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
