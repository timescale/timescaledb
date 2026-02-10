-- Unit Tests: EOP functions — edge cases and boundary conditions

\set ON_ERROR_STOP 1

\ir include/setup.sql
\ir ../../../../sql/postgis_ecef_eci/eop.sql

-- =============================================================================
-- MJD <-> Timestamp: known values
-- =============================================================================

-- J2000 epoch: MJD 51544.5 = 2000-01-01 12:00:00 TT (approx)
SELECT ecef_eci.mjd_to_timestamp(51544.0)::text AS mjd_epoch;
SELECT ecef_eci.mjd_to_timestamp(51544.5)::text AS mjd_epoch_noon;

-- Known dates
SELECT ecef_eci.mjd_to_timestamp(59580.0)::date AS mjd_known_date;  -- ~2022-01-01
SELECT round(ecef_eci.timestamp_to_mjd('2000-01-01 00:00:00+00')::numeric, 1) AS ts_to_mjd_epoch;

-- Round-trip precision
SELECT
    abs(ecef_eci.timestamp_to_mjd(
        ecef_eci.mjd_to_timestamp(51544.123456)
    ) - 51544.123456) < 1e-9 AS roundtrip_precise;

-- =============================================================================
-- EOP interpolation: edge cases
-- =============================================================================

-- Empty table — should return NULL (no bracket found)
SELECT * FROM ecef_eci.eop_at_epoch('2025-01-01 00:00:00+00');

-- Single entry — cannot interpolate (no bracket)
INSERT INTO ecef_eci.eop_data (mjd, date, xp, yp, dut1, lod, dx, dy, data_type)
VALUES (60676.0, '2025-01-01', 0.1234, 0.2345, -0.0456, 0.8, 0.05, -0.03, 'I');

-- Query at exactly the single entry — should still return NULL (no "after" bracket)
SELECT xp IS NULL AS single_entry_null FROM ecef_eci.eop_at_epoch('2025-01-01 00:00:00+00');

-- Add second entry to enable interpolation
INSERT INTO ecef_eci.eop_data (mjd, date, xp, yp, dut1, lod, dx, dy, data_type)
VALUES (60677.0, '2025-01-02', 0.1240, 0.2350, -0.0460, 0.9, 0.06, -0.02, 'I');

-- At exact first entry (should interpolate at t=0, returning values close to first entry)
SELECT
    round(xp::numeric, 4) AS xp_at_first
FROM ecef_eci.eop_at_epoch('2025-01-01 00:00:00+00');

-- Midpoint (should average)
SELECT
    round(xp::numeric, 4) AS xp_midpoint,
    round(yp::numeric, 4) AS yp_midpoint,
    round(dut1::numeric, 4) AS dut1_midpoint
FROM ecef_eci.eop_at_epoch('2025-01-01 12:00:00+00');

-- At 25% (should be 0.1234 + 0.25*(0.1240-0.1234) = 0.12355)
SELECT
    round(xp::numeric, 5) AS xp_quarter
FROM ecef_eci.eop_at_epoch('2025-01-01 06:00:00+00');

-- =============================================================================
-- EOP interpolation: 5-day dataset
-- =============================================================================

INSERT INTO ecef_eci.eop_data (mjd, date, xp, yp, dut1, lod, dx, dy, data_type) VALUES
    (60678.0, '2025-01-03', 0.1245, 0.2360, -0.0465, 0.7, 0.04, -0.04, 'I'),
    (60679.0, '2025-01-04', 0.1250, 0.2370, -0.0470, 0.8, 0.05, -0.03, 'P'),
    (60680.0, '2025-01-05', 0.1255, 0.2380, -0.0475, 0.9, 0.06, -0.02, 'P');

-- Interpolation should use nearest bracket, not necessarily first/last
SELECT
    round(xp::numeric, 4) AS xp_day3_noon
FROM ecef_eci.eop_at_epoch('2025-01-03 12:00:00+00');

-- =============================================================================
-- EOP status: verify counts
-- =============================================================================

SELECT
    total_entries,
    earliest_date,
    latest_date,
    observed_count,
    predicted_count
FROM ecef_eci.eop_status();

-- =============================================================================
-- load_eop_finals2000a: malformed input handling
-- =============================================================================

-- Empty string
SELECT ecef_eci.load_eop_finals2000a('') AS empty_load;

-- Short lines (should be skipped)
SELECT ecef_eci.load_eop_finals2000a('short line') AS short_load;

-- Multiple short lines
SELECT ecef_eci.load_eop_finals2000a(E'line1\nline2\nline3') AS multi_short_load;

-- =============================================================================
-- Upsert: update existing entry
-- =============================================================================

-- Get current xp for day 1
SELECT xp FROM ecef_eci.eop_data WHERE mjd = 60676.0;

-- Update via upsert
INSERT INTO ecef_eci.eop_data (mjd, date, xp, yp, dut1, data_type)
VALUES (60676.0, '2025-01-01', 0.9999, 0.9999, -0.9999, 'B')
ON CONFLICT (mjd) DO UPDATE SET
    xp = EXCLUDED.xp, yp = EXCLUDED.yp, dut1 = EXCLUDED.dut1,
    data_type = EXCLUDED.data_type, loaded_at = now();

-- Verify update
SELECT xp, data_type FROM ecef_eci.eop_data WHERE mjd = 60676.0;

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
