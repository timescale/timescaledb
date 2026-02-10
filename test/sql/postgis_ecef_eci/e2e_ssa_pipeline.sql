-- End-to-End Test: Full SSA (Space Situational Awareness) ingest pipeline
-- Simulates a realistic workflow: generate objects → insert → compress →
-- create continuous aggregates → query dashboard views → retention

\set ON_ERROR_STOP 1

\ir include/setup.sql

-- =============================================================================
-- Step 1: Generate and ingest multi-regime dataset
-- =============================================================================

INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_test_dataset(
    '2025-01-01 00:00:00+00', '3 hours', '10 seconds'
);

-- Verify: 10 objects × 3h × 6/min = 10 × 1080 = 10800 rows
SELECT count(*) AS total_rows FROM ecef_eci.trajectories;

-- Verify: objects distributed across spatial buckets
SELECT spatial_bucket,
       ecef_eci.altitude_band_label(spatial_bucket) AS band,
       count(DISTINCT object_id) AS objects,
       count(*) AS observations
FROM ecef_eci.trajectories
GROUP BY spatial_bucket
ORDER BY spatial_bucket;

-- =============================================================================
-- Step 2: Compress old chunks (keep latest hour uncompressed)
-- =============================================================================

-- Compress chunks older than 2 hours
SELECT compress_chunk(c)
FROM show_chunks('ecef_eci.trajectories', older_than => INTERVAL '2 hours') c;

-- Verify: some chunks compressed, some not
SELECT is_compressed, count(*) AS n_chunks
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'ecef_eci' AND hypertable_name = 'trajectories'
GROUP BY is_compressed
ORDER BY is_compressed;

-- Verify: data accessible through compressed chunks
SELECT count(*) AS total_after_compress FROM ecef_eci.trajectories;

-- =============================================================================
-- Step 3: Create continuous aggregates (dashboard views)
-- =============================================================================

-- Template 1: Hourly trajectory summary
CREATE MATERIALIZED VIEW ecef_eci.traj_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    object_id,
    avg(x) AS avg_x, avg(y) AS avg_y, avg(z) AS avg_z,
    min(altitude_km) AS min_alt_km,
    max(altitude_km) AS max_alt_km,
    count(*) AS n_obs,
    min(time) AS first_obs,
    max(time) AS last_obs
FROM ecef_eci.trajectories
GROUP BY bucket, object_id
WITH NO DATA;

-- Template 2: Spatial density
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

-- Refresh all
CALL refresh_continuous_aggregate('ecef_eci.traj_hourly', NULL, NULL);
CALL refresh_continuous_aggregate('ecef_eci.spatial_density_hourly', NULL, NULL);

-- =============================================================================
-- Step 4: Dashboard queries
-- =============================================================================

-- Query 1: Object trajectory at hourly resolution
SELECT bucket, object_id,
       round(avg_x::numeric, 0) AS x,
       round(avg_y::numeric, 0) AS y,
       round(avg_z::numeric, 0) AS z,
       n_obs
FROM ecef_eci.traj_hourly
WHERE object_id = 25544
ORDER BY bucket;

-- Query 2: Spatial density — how many objects per orbital regime per hour?
SELECT bucket,
       ecef_eci.altitude_band_label(spatial_bucket) AS band,
       n_objects
FROM ecef_eci.spatial_density_hourly
WHERE bucket = '2025-01-01 00:00:00+00'
ORDER BY spatial_bucket;

-- Query 3: Raw trajectory for one object in one hour
SELECT time, round(x::numeric, 0) AS x, round(y::numeric, 0) AS y, round(z::numeric, 0) AS z
FROM ecef_eci.trajectories
WHERE object_id = 25544
  AND time >= '2025-01-01 00:00:00+00'
  AND time <  '2025-01-01 00:01:00+00'
ORDER BY time;

-- =============================================================================
-- Step 5: Frame conversion on trajectory data (using stubs)
-- =============================================================================

\ir ../../../../sql/postgis_ecef_eci/frame_conversion_stubs.sql

-- Convert ISS trajectory to ECI for one hour
SELECT
    t.time,
    round(eci.eci_x::numeric, 0) AS eci_x,
    round(eci.eci_y::numeric, 0) AS eci_y,
    round(eci.eci_z::numeric, 0) AS eci_z
FROM ecef_eci.trajectories t
CROSS JOIN LATERAL ecef_eci.stub_ecef_to_eci(t.x, t.y, t.z, t.time) AS eci
WHERE t.object_id = 25544
  AND t.time >= '2025-01-01 00:00:00+00'
  AND t.time <  '2025-01-01 00:01:00+00'
ORDER BY t.time;

-- =============================================================================
-- Step 6: Incremental update (new data arrives)
-- =============================================================================

-- Add a new object (space debris)
INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_circular_orbit(
    99999, 1::SMALLINT, 450.0, 28.5, 180.0,
    '2025-01-01 03:00:00+00', '1 hour', '10 seconds'
);

-- Incremental refresh — only new time range
CALL refresh_continuous_aggregate('ecef_eci.traj_hourly',
    '2025-01-01 03:00:00+00', '2025-01-01 05:00:00+00');

-- Verify new object appears in cagg
SELECT object_id, bucket, n_obs
FROM ecef_eci.traj_hourly
WHERE object_id = 99999
ORDER BY bucket;

-- =============================================================================
-- Step 7: Verify data integrity after full pipeline
-- =============================================================================

-- Total row count should be original + new object
SELECT count(*) AS final_total FROM ecef_eci.trajectories;

-- All rows should have valid spatial buckets
SELECT count(*) AS invalid_buckets
FROM ecef_eci.trajectories
WHERE spatial_bucket < 0 OR spatial_bucket > 15;

-- All rows should have valid altitude
SELECT count(*) AS negative_altitude
FROM ecef_eci.trajectories
WHERE altitude_km < -100;  -- allow small negative for rounding

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
