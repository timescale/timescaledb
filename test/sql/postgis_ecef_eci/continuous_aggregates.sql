-- Test: Continuous aggregates on trajectory data
-- Validates cagg creation, refresh, and querying for spatiotemporal rollups

\set ON_ERROR_STOP 1

\ir include/setup.sql

-- =============================================================================
-- Setup: Load test data
-- =============================================================================

INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_test_dataset(
    '2025-01-01 00:00:00+00',
    '2 hours',
    '10 seconds'
);

SELECT count(*) AS total_rows FROM ecef_eci.trajectories;

-- Verify distribution across spatial buckets
SELECT spatial_bucket,
       ecef_eci.altitude_band_label(spatial_bucket) AS band,
       count(DISTINCT object_id) AS n_objects,
       count(*) AS n_rows
FROM ecef_eci.trajectories
GROUP BY spatial_bucket
ORDER BY spatial_bucket;

-- =============================================================================
-- Test 1: Per-object trajectory summary (hourly) — Template 1
-- =============================================================================

CREATE MATERIALIZED VIEW ecef_eci.traj_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    object_id,
    object_type,
    avg(x) AS avg_x,
    avg(y) AS avg_y,
    avg(z) AS avg_z,
    avg(vx) AS avg_vx,
    avg(vy) AS avg_vy,
    avg(vz) AS avg_vz,
    min(altitude_km) AS min_alt_km,
    max(altitude_km) AS max_alt_km,
    avg(altitude_km) AS avg_alt_km,
    count(*) AS n_obs,
    min(time) AS first_obs,
    max(time) AS last_obs
FROM ecef_eci.trajectories
GROUP BY bucket, object_id, object_type
WITH NO DATA;

-- Refresh for all data
CALL refresh_continuous_aggregate('ecef_eci.traj_hourly', NULL, NULL);

-- Verify materialized data
SELECT object_id, bucket, n_obs,
       round(avg_alt_km::numeric, 1) AS avg_alt_km
FROM ecef_eci.traj_hourly
WHERE object_id IN (25544, 28000, 30000)
ORDER BY object_id, bucket;

-- =============================================================================
-- Test 2: Spatial density (hourly) — Template 2
-- =============================================================================

CREATE MATERIALIZED VIEW ecef_eci.spatial_density_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    spatial_bucket,
    count(DISTINCT object_id) AS n_objects,
    count(*) AS n_observations
FROM ecef_eci.trajectories
GROUP BY bucket, spatial_bucket
WITH NO DATA;

CALL refresh_continuous_aggregate('ecef_eci.spatial_density_hourly', NULL, NULL);

-- Verify: LEO buckets should have the most objects
SELECT bucket, spatial_bucket,
       ecef_eci.altitude_band_label(spatial_bucket) AS band,
       n_objects, n_observations
FROM ecef_eci.spatial_density_hourly
WHERE bucket = '2025-01-01 00:00:00+00'
ORDER BY spatial_bucket;

-- =============================================================================
-- Test 3: Conjunction screening pre-filter (15-min) — Template 3
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

-- Buckets with multiple objects are conjunction candidates
SELECT bucket, spatial_bucket,
       ecef_eci.altitude_band_label(spatial_bucket) AS band,
       n_objects
FROM ecef_eci.conjunction_prefilter
WHERE n_objects > 1
ORDER BY n_objects DESC, bucket
LIMIT 10;

-- =============================================================================
-- Test 4: Incremental refresh after new data
-- =============================================================================

-- Add more data in a new time range
INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_circular_orbit(
    99999, 0::SMALLINT, 400.0, 28.5, 180.0,
    '2025-01-01 02:00:00+00', '1 hour', '10 seconds'
);

-- Refresh only the new time range
CALL refresh_continuous_aggregate('ecef_eci.traj_hourly',
    '2025-01-01 02:00:00+00'::timestamptz,
    '2025-01-01 04:00:00+00'::timestamptz);

-- Verify new object appears
SELECT object_id, bucket, n_obs
FROM ecef_eci.traj_hourly
WHERE object_id = 99999
ORDER BY bucket;

-- =============================================================================
-- Test 5: Query pattern — trajectory retrieval from cagg
-- =============================================================================

-- Reconstruct approximate position from hourly aggregate
SELECT bucket,
       round(avg_x::numeric, 0) AS x,
       round(avg_y::numeric, 0) AS y,
       round(avg_z::numeric, 0) AS z,
       round(avg_alt_km::numeric, 1) AS alt_km
FROM ecef_eci.traj_hourly
WHERE object_id = 25544
ORDER BY bucket;

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
