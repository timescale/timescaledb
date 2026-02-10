-- PostGIS ECEF/ECI Integration for TimescaleDB
-- Stub frame conversion functions for testing
--
-- These are simplified PL/pgSQL implementations that approximate ECEF<->ECI
-- conversion using Earth's rotation angle only (no precession/nutation/polar motion).
-- They exist so TimescaleDB integration tests can run without the PostGIS fork.
--
-- The PostGIS fork will replace these with C implementations using full IAU 2006
-- precession-nutation model and IERS Earth Orientation Parameters.

-- =============================================================================
-- Constants
-- =============================================================================

-- Earth's rotation rate (rad/s) — WGS84
-- omega_e = 7.2921150e-5 rad/s
-- Seconds per sidereal day ≈ 86164.0905

-- =============================================================================
-- Stub: ECEF to ECI (J2000) — simplified rotation only
-- =============================================================================
-- Applies a Z-axis rotation by the Greenwich Mean Sidereal Time angle.
-- Accuracy: ~1 degree over 1 day (no precession/nutation).
-- Sufficient for testing partitioning, compression, and cagg behavior.

CREATE OR REPLACE FUNCTION ecef_eci.stub_ecef_to_eci(
    ecef_x FLOAT8,
    ecef_y FLOAT8,
    ecef_z FLOAT8,
    epoch  TIMESTAMPTZ
) RETURNS TABLE(eci_x FLOAT8, eci_y FLOAT8, eci_z FLOAT8)
LANGUAGE SQL
STABLE
PARALLEL SAFE
AS $$
    WITH params AS (
        SELECT
            -- Seconds since J2000 epoch (2000-01-12T12:00:00 TT ≈ 2000-01-01T11:58:55.816 UTC)
            EXTRACT(EPOCH FROM epoch - '2000-01-01 11:58:55.816+00'::timestamptz) AS t_sec,
            -- Earth rotation rate (rad/s)
            7.2921150e-5 AS omega_e
    ),
    rotation AS (
        SELECT
            -- Greenwich Mean Sidereal Time angle (simplified: just rotation rate * time)
            (p.omega_e * p.t_sec) AS gmst_rad
        FROM params p
    )
    SELECT
        ecef_x * cos(r.gmst_rad) - ecef_y * sin(r.gmst_rad) AS eci_x,
        ecef_x * sin(r.gmst_rad) + ecef_y * cos(r.gmst_rad) AS eci_y,
        ecef_z                                                 AS eci_z
    FROM rotation r
$$;

COMMENT ON FUNCTION ecef_eci.stub_ecef_to_eci(FLOAT8, FLOAT8, FLOAT8, TIMESTAMPTZ)
IS 'Stub ECEF->ECI conversion using simplified Earth rotation only. For testing — will be replaced by PostGIS fork C implementation.';

-- =============================================================================
-- Stub: ECI (J2000) to ECEF — inverse rotation
-- =============================================================================

CREATE OR REPLACE FUNCTION ecef_eci.stub_eci_to_ecef(
    eci_x  FLOAT8,
    eci_y  FLOAT8,
    eci_z  FLOAT8,
    epoch  TIMESTAMPTZ
) RETURNS TABLE(ecef_x FLOAT8, ecef_y FLOAT8, ecef_z FLOAT8)
LANGUAGE SQL
STABLE
PARALLEL SAFE
AS $$
    WITH params AS (
        SELECT
            EXTRACT(EPOCH FROM epoch - '2000-01-01 11:58:55.816+00'::timestamptz) AS t_sec,
            7.2921150e-5 AS omega_e
    ),
    rotation AS (
        SELECT (p.omega_e * p.t_sec) AS gmst_rad
        FROM params p
    )
    SELECT
        eci_x * cos(r.gmst_rad) + eci_y * sin(r.gmst_rad) AS ecef_x,
       -eci_x * sin(r.gmst_rad) + eci_y * cos(r.gmst_rad) AS ecef_y,
        eci_z                                                AS ecef_z
    FROM rotation r
$$;

COMMENT ON FUNCTION ecef_eci.stub_eci_to_ecef(FLOAT8, FLOAT8, FLOAT8, TIMESTAMPTZ)
IS 'Stub ECI->ECEF conversion using simplified Earth rotation only. For testing — will be replaced by PostGIS fork C implementation.';

-- =============================================================================
-- Stub: Round-trip validation
-- =============================================================================
-- Convert ECEF->ECI->ECEF and verify coordinates match within tolerance.

CREATE OR REPLACE FUNCTION ecef_eci.test_roundtrip(
    x FLOAT8,
    y FLOAT8,
    z FLOAT8,
    epoch TIMESTAMPTZ,
    tolerance_m FLOAT8 DEFAULT 0.001
) RETURNS BOOLEAN
LANGUAGE SQL
STABLE
PARALLEL SAFE
AS $$
    SELECT
        abs(rt.ecef_x - x) < tolerance_m AND
        abs(rt.ecef_y - y) < tolerance_m AND
        abs(rt.ecef_z - z) < tolerance_m
    FROM ecef_eci.stub_ecef_to_eci(x, y, z, epoch) AS fwd
    CROSS JOIN LATERAL ecef_eci.stub_eci_to_ecef(fwd.eci_x, fwd.eci_y, fwd.eci_z, epoch) AS rt
$$;

COMMENT ON FUNCTION ecef_eci.test_roundtrip(FLOAT8, FLOAT8, FLOAT8, TIMESTAMPTZ, FLOAT8)
IS 'Validates ECEF->ECI->ECEF round-trip within tolerance (meters). Returns true if coordinates match.';
