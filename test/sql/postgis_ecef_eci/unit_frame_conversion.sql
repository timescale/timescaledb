-- Unit Tests: Frame conversion stubs — comprehensive boundary coverage

\set ON_ERROR_STOP 1

\ir include/setup.sql

-- =============================================================================
-- stub_ecef_to_eci: J2000 epoch (GMST ≈ 0, ECEF ≈ ECI)
-- =============================================================================

-- At J2000 epoch, rotation angle should be near zero
-- ECEF and ECI should be nearly identical
SELECT
    round(eci_x::numeric, 0) AS eci_x,
    round(eci_y::numeric, 0) AS eci_y,
    round(eci_z::numeric, 0) AS eci_z
FROM ecef_eci.stub_ecef_to_eci(
    6771000.0, 0.0, 0.0,
    '2000-01-01 11:58:55.816+00'::timestamptz
);

-- =============================================================================
-- stub_ecef_to_eci: Z-axis invariance
-- =============================================================================
-- Rotation is around Z axis, so Z component should be unchanged for any epoch

SELECT
    round(eci_z::numeric, 0) AS z_unchanged_1
FROM ecef_eci.stub_ecef_to_eci(0.0, 0.0, 6771000.0, '2025-01-01 00:00:00+00');

SELECT
    round(eci_z::numeric, 0) AS z_unchanged_2
FROM ecef_eci.stub_ecef_to_eci(0.0, 0.0, -6771000.0, '2025-06-15 12:00:00+00');

SELECT
    round(eci_z::numeric, 0) AS z_unchanged_3
FROM ecef_eci.stub_ecef_to_eci(0.0, 0.0, 12345678.0, '2030-12-31 23:59:59+00');

-- =============================================================================
-- stub_ecef_to_eci: Radius preservation
-- =============================================================================
-- Rotation should preserve vector magnitude: |ECI| = |ECEF|

SELECT
    abs(
        sqrt(eci_x*eci_x + eci_y*eci_y + eci_z*eci_z) -
        sqrt(6771000.0*6771000.0 + 1000000.0*1000000.0 + 3000000.0*3000000.0)
    ) < 0.001 AS radius_preserved
FROM ecef_eci.stub_ecef_to_eci(6771000.0, 1000000.0, 3000000.0, '2025-01-01 00:00:00+00');

-- =============================================================================
-- Round-trip: various positions and epochs
-- =============================================================================

-- Along each axis
SELECT ecef_eci.test_roundtrip(6771000.0, 0.0, 0.0, '2025-01-01 00:00:00+00') AS rt_x;
SELECT ecef_eci.test_roundtrip(0.0, 6771000.0, 0.0, '2025-01-01 00:00:00+00') AS rt_y;
SELECT ecef_eci.test_roundtrip(0.0, 0.0, 6771000.0, '2025-01-01 00:00:00+00') AS rt_z;

-- Off-axis
SELECT ecef_eci.test_roundtrip(4000000.0, 4000000.0, 4000000.0, '2025-01-01 00:00:00+00') AS rt_offaxis;

-- Different epochs spanning a full day
SELECT ecef_eci.test_roundtrip(6771000.0, 1000000.0, 3000000.0, '2025-01-01 00:00:00+00') AS rt_0h;
SELECT ecef_eci.test_roundtrip(6771000.0, 1000000.0, 3000000.0, '2025-01-01 06:00:00+00') AS rt_6h;
SELECT ecef_eci.test_roundtrip(6771000.0, 1000000.0, 3000000.0, '2025-01-01 12:00:00+00') AS rt_12h;
SELECT ecef_eci.test_roundtrip(6771000.0, 1000000.0, 3000000.0, '2025-01-01 18:00:00+00') AS rt_18h;

-- Far future epoch
SELECT ecef_eci.test_roundtrip(6771000.0, 1000000.0, 3000000.0, '2050-06-15 12:00:00+00') AS rt_future;

-- =============================================================================
-- stub_ecef_to_eci: Time dependence (Earth rotates ~15 deg/hr)
-- =============================================================================
-- Same ECEF point at 6-hour intervals should produce very different ECI

SELECT
    'T+0h' AS label,
    round(eci_x::numeric, 0) AS x,
    round(eci_y::numeric, 0) AS y
FROM ecef_eci.stub_ecef_to_eci(6771000.0, 0.0, 0.0, '2025-01-01 00:00:00+00')
UNION ALL
SELECT
    'T+6h',
    round(eci_x::numeric, 0),
    round(eci_y::numeric, 0)
FROM ecef_eci.stub_ecef_to_eci(6771000.0, 0.0, 0.0, '2025-01-01 06:00:00+00')
UNION ALL
SELECT
    'T+12h',
    round(eci_x::numeric, 0),
    round(eci_y::numeric, 0)
FROM ecef_eci.stub_ecef_to_eci(6771000.0, 0.0, 0.0, '2025-01-01 12:00:00+00')
UNION ALL
SELECT
    'T+18h',
    round(eci_x::numeric, 0),
    round(eci_y::numeric, 0)
FROM ecef_eci.stub_ecef_to_eci(6771000.0, 0.0, 0.0, '2025-01-01 18:00:00+00')
ORDER BY label;

-- =============================================================================
-- Edge: zero vector
-- =============================================================================

SELECT
    round(eci_x::numeric, 0) AS zero_x,
    round(eci_y::numeric, 0) AS zero_y,
    round(eci_z::numeric, 0) AS zero_z
FROM ecef_eci.stub_ecef_to_eci(0.0, 0.0, 0.0, '2025-01-01 00:00:00+00');

-- =============================================================================
-- test_roundtrip: tolerance edge case
-- =============================================================================

-- Very tight tolerance (should still pass for our stubs)
SELECT ecef_eci.test_roundtrip(6771000.0, 0.0, 0.0, '2025-01-01 00:00:00+00', 0.000001) AS rt_tight;

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
