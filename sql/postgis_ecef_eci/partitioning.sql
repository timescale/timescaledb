-- PostGIS ECEF/ECI Integration for TimescaleDB
-- Spatial partitioning functions for ECEF coordinate data
--
-- These functions map 3D ECEF coordinates to 1D partition buckets
-- for use as a TimescaleDB closed dimension.

-- =============================================================================
-- Option A: Altitude-Band Bucketing
-- =============================================================================
-- Partitions by orbital regime / altitude above WGS84 ellipsoid.
-- 16 buckets covering sub-orbital through HEO.
--
-- Bucket Layout:
--   0-3:   LEO    (0-2000 km)      4 sub-bands by 500 km
--   4-7:   MEO    (2000-35786 km)  4 sub-bands
--   8-11:  GEO    (35786 +/- 200 km) 4 sub-bands by longitude quadrant
--   12-14: HEO    (>35986 km)      3 sub-bands
--   15:    Sub-orbital / terrestrial / invalid
-- =============================================================================

CREATE OR REPLACE FUNCTION ecef_eci.altitude_band_bucket(
    x FLOAT8,
    y FLOAT8,
    z FLOAT8
) RETURNS INT
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        -- Sub-orbital / terrestrial / inside Earth
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 0
            THEN 15

        -- LEO: 0-500 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 500
            THEN 0
        -- LEO: 500-1000 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 1000
            THEN 1
        -- LEO: 1000-1500 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 1500
            THEN 2
        -- LEO: 1500-2000 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 2000
            THEN 3

        -- MEO: 2000-10000 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 10000
            THEN 4
        -- MEO: 10000-20000 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 20000
            THEN 5
        -- MEO: 20000-30000 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 30000
            THEN 6
        -- MEO: 30000-35786 km (below GEO)
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 35786
            THEN 7

        -- GEO belt: 35786 +/- 200 km, split by longitude quadrant
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 35986
            THEN 8 + (CASE
                WHEN atan2(y, x) >= 0 AND atan2(y, x) < pi()/2 THEN 0
                WHEN atan2(y, x) >= pi()/2 THEN 1
                WHEN atan2(y, x) >= -pi()/2 AND atan2(y, x) < 0 THEN 2
                ELSE 3
            END)

        -- HEO: 35986-50000 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 50000
            THEN 12
        -- HEO: 50000-100000 km
        WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 100000
            THEN 13
        -- HEO: >100000 km
        ELSE 14
    END
$$;

COMMENT ON FUNCTION ecef_eci.altitude_band_bucket(FLOAT8, FLOAT8, FLOAT8)
IS 'Maps ECEF coordinates (meters) to a spatial partition bucket (0-15) based on altitude band above WGS84 ellipsoid';

-- =============================================================================
-- Helper: Compute altitude in km from ECEF coordinates in meters
-- =============================================================================

CREATE OR REPLACE FUNCTION ecef_eci.ecef_altitude_km(
    x FLOAT8,
    y FLOAT8,
    z FLOAT8
) RETURNS FLOAT8
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0
$$;

COMMENT ON FUNCTION ecef_eci.ecef_altitude_km(FLOAT8, FLOAT8, FLOAT8)
IS 'Computes approximate altitude (km) above WGS84 mean radius from ECEF coordinates (meters)';

-- =============================================================================
-- Helper: Altitude band label for human-readable output
-- =============================================================================

CREATE OR REPLACE FUNCTION ecef_eci.altitude_band_label(bucket INT)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE bucket
        WHEN 0  THEN 'LEO 0-500km'
        WHEN 1  THEN 'LEO 500-1000km'
        WHEN 2  THEN 'LEO 1000-1500km'
        WHEN 3  THEN 'LEO 1500-2000km'
        WHEN 4  THEN 'MEO 2000-10000km'
        WHEN 5  THEN 'MEO 10000-20000km'
        WHEN 6  THEN 'MEO 20000-30000km'
        WHEN 7  THEN 'MEO 30000-GEO'
        WHEN 8  THEN 'GEO Q1 (0-90E)'
        WHEN 9  THEN 'GEO Q2 (90E-180E)'
        WHEN 10 THEN 'GEO Q3 (180W-90W)'
        WHEN 11 THEN 'GEO Q4 (90W-0)'
        WHEN 12 THEN 'HEO 36000-50000km'
        WHEN 13 THEN 'HEO 50000-100000km'
        WHEN 14 THEN 'HEO >100000km'
        WHEN 15 THEN 'Sub-orbital/Terrestrial'
        ELSE 'Unknown'
    END
$$;

COMMENT ON FUNCTION ecef_eci.altitude_band_label(INT)
IS 'Returns human-readable label for a spatial partition bucket number';
