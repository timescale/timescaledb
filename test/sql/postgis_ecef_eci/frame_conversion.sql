-- Test: Frame conversion stubs
-- Validates ECEF<->ECI conversion logic and round-trip accuracy

\set ON_ERROR_STOP 1

\ir include/setup.sql

-- =============================================================================
-- Test 1: ECEF->ECI conversion at J2000 epoch
-- =============================================================================
-- At J2000 epoch, GMST ≈ 0, so ECEF ≈ ECI

SELECT
    round(eci_x::numeric, 0) AS eci_x,
    round(eci_y::numeric, 0) AS eci_y,
    round(eci_z::numeric, 0) AS eci_z
FROM ecef_eci.stub_ecef_to_eci(
    6771000.0, 0.0, 0.0,
    '2000-01-01 11:58:55.816+00'::timestamptz
);

-- =============================================================================
-- Test 2: Round-trip accuracy
-- =============================================================================

-- At various epochs, ECEF->ECI->ECEF should recover original coordinates
SELECT ecef_eci.test_roundtrip(6771000.0, 0.0, 0.0, '2025-01-01 00:00:00+00') AS rt_xaxis;
SELECT ecef_eci.test_roundtrip(0.0, 6771000.0, 0.0, '2025-01-01 00:00:00+00') AS rt_yaxis;
SELECT ecef_eci.test_roundtrip(0.0, 0.0, 6771000.0, '2025-01-01 00:00:00+00') AS rt_zaxis;
SELECT ecef_eci.test_roundtrip(4000000.0, 4000000.0, 4000000.0, '2025-06-15 12:00:00+00') AS rt_offaxis;

-- =============================================================================
-- Test 3: Conversion varies with time (Earth rotates)
-- =============================================================================
-- Same ECEF point at different times should produce different ECI coordinates

SELECT
    'T+0h' AS label,
    round(eci_x::numeric, 0) AS eci_x,
    round(eci_y::numeric, 0) AS eci_y
FROM ecef_eci.stub_ecef_to_eci(6771000.0, 0.0, 0.0, '2025-01-01 00:00:00+00')
UNION ALL
SELECT
    'T+6h' AS label,
    round(eci_x::numeric, 0) AS eci_x,
    round(eci_y::numeric, 0) AS eci_y
FROM ecef_eci.stub_ecef_to_eci(6771000.0, 0.0, 0.0, '2025-01-01 06:00:00+00')
UNION ALL
SELECT
    'T+12h' AS label,
    round(eci_x::numeric, 0) AS eci_x,
    round(eci_y::numeric, 0) AS eci_y
FROM ecef_eci.stub_ecef_to_eci(6771000.0, 0.0, 0.0, '2025-01-01 12:00:00+00');

-- =============================================================================
-- Test 4: Z-axis invariance
-- =============================================================================
-- ECEF<->ECI rotation is around Z axis, so Z should be unchanged

SELECT
    round(eci_z::numeric, 0) AS eci_z_unchanged
FROM ecef_eci.stub_ecef_to_eci(0.0, 0.0, 6771000.0, '2025-01-01 00:00:00+00');

-- =============================================================================
-- Test 5: Conversion on trajectory data
-- =============================================================================
-- Insert some data, then convert a trajectory to ECI and verify it's smooth

INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_circular_orbit(
    25544, 0::SMALLINT, 400.0, 51.6, 0.0,
    '2025-01-01 00:00:00+00', '10 minutes', '1 minute'
);

-- Convert trajectory to ECI — verify we get results for each row
SELECT
    t.time,
    round(eci.eci_x::numeric, 0) AS eci_x,
    round(eci.eci_y::numeric, 0) AS eci_y,
    round(eci.eci_z::numeric, 0) AS eci_z
FROM ecef_eci.trajectories t
CROSS JOIN LATERAL ecef_eci.stub_ecef_to_eci(t.x, t.y, t.z, t.time) AS eci
WHERE t.object_id = 25544
ORDER BY t.time
LIMIT 5;

-- =============================================================================
-- Test 6: Verify frame conversion is STABLE (not IMMUTABLE)
-- =============================================================================
-- Should NOT be usable in an index expression (depends on epoch parameter)

\set ON_ERROR_STOP 0
CREATE TABLE test_volatility_check (x FLOAT8, y FLOAT8, z FLOAT8, t TIMESTAMPTZ);
-- This should fail because the function is STABLE, not IMMUTABLE
CREATE INDEX ON test_volatility_check (
    (ecef_eci.stub_ecef_to_eci(x, y, z, t))
);
DROP TABLE IF EXISTS test_volatility_check;
\set ON_ERROR_STOP 1

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
