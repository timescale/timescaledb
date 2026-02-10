-- Test: Altitude-band partitioning function
-- Validates bucket assignment for all orbital regimes

\set ON_ERROR_STOP 1

\ir include/setup.sql

-- =============================================================================
-- Test 1: Bucket assignments for known altitudes
-- =============================================================================

-- Earth surface (radius ~6371 km = 6371000 m)
SELECT ecef_eci.altitude_band_bucket(6371000.0, 0.0, 0.0) AS surface_bucket;

-- LEO: ISS at ~400 km (radius ~6771 km)
SELECT ecef_eci.altitude_band_bucket(6771000.0, 0.0, 0.0) AS iss_bucket;

-- LEO: 800 km (radius ~7171 km)
SELECT ecef_eci.altitude_band_bucket(7171000.0, 0.0, 0.0) AS leo_800_bucket;

-- LEO: 1200 km
SELECT ecef_eci.altitude_band_bucket(7571000.0, 0.0, 0.0) AS leo_1200_bucket;

-- LEO: 1800 km
SELECT ecef_eci.altitude_band_bucket(8171000.0, 0.0, 0.0) AS leo_1800_bucket;

-- MEO: GPS at ~20200 km (radius ~26571 km)
SELECT ecef_eci.altitude_band_bucket(26571000.0, 0.0, 0.0) AS gps_bucket;

-- GEO: ~35786 km (radius ~42157 km) — positive X axis (0 degrees longitude)
SELECT ecef_eci.altitude_band_bucket(42157000.0, 0.0, 0.0) AS geo_q1_bucket;

-- GEO: positive Y axis (90E longitude)
SELECT ecef_eci.altitude_band_bucket(0.0, 42157000.0, 0.0) AS geo_q2_bucket;

-- GEO: negative X axis (180 longitude)
SELECT ecef_eci.altitude_band_bucket(-42157000.0, 0.0, 0.0) AS geo_q3_bucket;

-- GEO: negative Y axis (90W longitude)
SELECT ecef_eci.altitude_band_bucket(0.0, -42157000.0, 0.0) AS geo_q4_bucket;

-- HEO: 45000 km
SELECT ecef_eci.altitude_band_bucket(51371000.0, 0.0, 0.0) AS heo_45k_bucket;

-- Sub-orbital / inside Earth
SELECT ecef_eci.altitude_band_bucket(6000000.0, 0.0, 0.0) AS sub_orbital_bucket;

-- =============================================================================
-- Test 2: Bucket labels
-- =============================================================================

SELECT bucket, ecef_eci.altitude_band_label(bucket)
FROM generate_series(0, 15) AS bucket
ORDER BY bucket;

-- =============================================================================
-- Test 3: Altitude computation
-- =============================================================================

-- ISS altitude
SELECT round(ecef_eci.ecef_altitude_km(6771000.0, 0.0, 0.0)::numeric, 1) AS iss_alt_km;

-- GPS altitude
SELECT round(ecef_eci.ecef_altitude_km(26571000.0, 0.0, 0.0)::numeric, 1) AS gps_alt_km;

-- Off-axis point: altitude should be sqrt(x^2+y^2+z^2)/1000 - 6371
SELECT round(ecef_eci.ecef_altitude_km(4000000.0, 4000000.0, 4000000.0)::numeric, 1) AS offaxis_alt_km;

-- =============================================================================
-- Test 4: Partitioning function is IMMUTABLE
-- =============================================================================
-- Verify it can be used in an index expression

CREATE TABLE test_immutable_check (x FLOAT8, y FLOAT8, z FLOAT8);
CREATE INDEX ON test_immutable_check (ecef_eci.altitude_band_bucket(x, y, z));
DROP TABLE test_immutable_check;

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
