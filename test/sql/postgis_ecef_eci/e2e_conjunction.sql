-- End-to-End Test: Conjunction screening workflow
-- Simulates: detect objects in same spatial region at same time,
-- then narrow down with distance check

\set ON_ERROR_STOP 1

\ir include/setup.sql

-- =============================================================================
-- Step 1: Generate crossing orbits
-- =============================================================================

-- Two objects in similar LEO orbits but different inclinations
-- They will occasionally be in the same spatial bucket
INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_circular_orbit(
    10001, 0::SMALLINT, 400.0, 51.6, 0.0,
    '2025-01-01 00:00:00+00', '2 hours', '10 seconds'
);

INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_circular_orbit(
    10002, 0::SMALLINT, 410.0, 97.4, 45.0,
    '2025-01-01 00:00:00+00', '2 hours', '10 seconds'
);

-- A GEO object (should never be in conjunction with LEO)
INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_circular_orbit(
    20001, 0::SMALLINT, 35786.0, 0.1, 0.0,
    '2025-01-01 00:00:00+00', '2 hours', '10 seconds'
);

-- =============================================================================
-- Step 2: Create conjunction pre-filter (15-min buckets)
-- =============================================================================

CREATE MATERIALIZED VIEW ecef_eci.conjunction_prefilter
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS bucket,
    spatial_bucket,
    min(x) AS min_x, max(x) AS max_x,
    min(y) AS min_y, max(y) AS max_y,
    min(z) AS min_z, max(z) AS max_z,
    count(DISTINCT object_id) AS n_objects
FROM ecef_eci.trajectories
GROUP BY bucket, spatial_bucket
WITH NO DATA;

CALL refresh_continuous_aggregate('ecef_eci.conjunction_prefilter', NULL, NULL);

-- =============================================================================
-- Step 3: Screen for potential conjunctions
-- =============================================================================

-- Buckets with multiple objects = potential conjunctions
SELECT bucket, spatial_bucket,
       ecef_eci.altitude_band_label(spatial_bucket) AS band,
       n_objects
FROM ecef_eci.conjunction_prefilter
WHERE n_objects > 1
ORDER BY bucket
LIMIT 20;

-- GEO objects should never share a bucket with LEO objects
SELECT bucket, spatial_bucket, n_objects
FROM ecef_eci.conjunction_prefilter
WHERE spatial_bucket IN (8, 9, 10, 11)  -- GEO quadrants
  AND n_objects > 1;

-- =============================================================================
-- Step 4: Detailed distance check for flagged time windows
-- =============================================================================

-- For each time window with >1 object, compute actual pairwise distances
-- Use the first flagged window
WITH flagged AS (
    SELECT bucket, spatial_bucket
    FROM ecef_eci.conjunction_prefilter
    WHERE n_objects > 1
    ORDER BY bucket
    LIMIT 1
)
SELECT
    a.time,
    a.object_id AS obj_a,
    b.object_id AS obj_b,
    round(sqrt(
        (a.x - b.x)*(a.x - b.x) +
        (a.y - b.y)*(a.y - b.y) +
        (a.z - b.z)*(a.z - b.z)
    )::numeric / 1000.0, 1) AS distance_km
FROM flagged f
JOIN ecef_eci.trajectories a ON a.spatial_bucket = f.spatial_bucket
    AND a.time >= f.bucket AND a.time < f.bucket + INTERVAL '15 minutes'
JOIN ecef_eci.trajectories b ON b.spatial_bucket = f.spatial_bucket
    AND b.time = a.time AND b.object_id > a.object_id
ORDER BY distance_km ASC
LIMIT 10;

-- =============================================================================
-- Step 5: Verify the pipeline correctly separates orbital regimes
-- =============================================================================

-- LEO objects should never appear in GEO-belt buckets (8-11)
SELECT count(*) AS leo_in_geo_bucket
FROM ecef_eci.trajectories
WHERE object_id IN (10001, 10002)
  AND spatial_bucket BETWEEN 8 AND 11;

-- GEO object should always be in GEO-belt buckets
SELECT spatial_bucket, count(*) AS n
FROM ecef_eci.trajectories
WHERE object_id = 20001
GROUP BY spatial_bucket
ORDER BY spatial_bucket;

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
