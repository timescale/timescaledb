-- Test: Schema creation and hypertable setup
-- Validates the full trajectory table, hypertable, compression, and basic queries

\set ON_ERROR_STOP 1

\ir include/setup.sql

-- =============================================================================
-- Test 1: Schema and table exist
-- =============================================================================

SELECT schemaname, tablename
FROM pg_tables
WHERE schemaname = 'ecef_eci' AND tablename = 'trajectories';

-- Verify it's a hypertable
SELECT hypertable_schema, hypertable_name, num_dimensions
FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'ecef_eci' AND hypertable_name = 'trajectories';

-- Verify dimensions
SELECT column_name, dimension_type
FROM timescaledb_information.dimensions
WHERE hypertable_schema = 'ecef_eci' AND hypertable_name = 'trajectories'
ORDER BY dimension_number;

-- =============================================================================
-- Test 2: Insert with trigger-computed derived fields
-- =============================================================================

-- Insert ISS-like position (400 km altitude on X axis)
INSERT INTO ecef_eci.trajectories (time, object_id, object_type, frame, x, y, z, vx, vy, vz, altitude_km, spatial_bucket)
VALUES ('2025-01-01 00:00:00+00', 25544, 0, 0, 6771000.0, 0.0, 0.0, 0.0, 7660.0, 0.0,
        ecef_eci.ecef_altitude_km(6771000.0, 0.0, 0.0),
        ecef_eci.altitude_band_bucket(6771000.0, 0.0, 0.0));

-- Insert GPS-like position (20200 km altitude)
INSERT INTO ecef_eci.trajectories (time, object_id, object_type, frame, x, y, z, vx, vy, vz, altitude_km, spatial_bucket)
VALUES ('2025-01-01 00:00:00+00', 28000, 0, 0, 26571000.0, 0.0, 0.0, 0.0, 3870.0, 0.0,
        ecef_eci.ecef_altitude_km(26571000.0, 0.0, 0.0),
        ecef_eci.altitude_band_bucket(26571000.0, 0.0, 0.0));

-- Verify data
SELECT object_id, round(altitude_km::numeric, 1) AS alt_km, spatial_bucket,
       ecef_eci.altitude_band_label(spatial_bucket) AS band
FROM ecef_eci.trajectories
ORDER BY object_id;

-- =============================================================================
-- Test 3: Chunk creation — verify spatial partitioning works
-- =============================================================================

SELECT chunk_schema, chunk_name,
       range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'ecef_eci' AND hypertable_name = 'trajectories'
ORDER BY chunk_name;

-- =============================================================================
-- Test 4: Bulk insert with generated data
-- =============================================================================

INSERT INTO ecef_eci.trajectories
SELECT * FROM ecef_eci.generate_circular_orbit(
    99001, 0::SMALLINT, 400.0, 51.6, 0.0,
    '2025-01-01 00:00:00+00', '30 minutes', '10 seconds'
);

-- Count inserted rows
SELECT count(*) AS orbit_rows FROM ecef_eci.trajectories WHERE object_id = 99001;

-- Verify all went into bucket 0 (LEO 0-500km)
SELECT spatial_bucket, count(*) AS n
FROM ecef_eci.trajectories
WHERE object_id = 99001
GROUP BY spatial_bucket
ORDER BY spatial_bucket;

-- =============================================================================
-- Test 5: Compression round-trip
-- =============================================================================

-- Compress all chunks
SELECT compress_chunk(c) FROM show_chunks('ecef_eci.trajectories') c;

-- Query compressed data — verify it matches
SELECT count(*) AS compressed_rows FROM ecef_eci.trajectories WHERE object_id = 99001;

-- Verify values survive compression (spot check first row)
SELECT object_id, round(x::numeric, 1) AS x, round(altitude_km::numeric, 1) AS alt_km, spatial_bucket
FROM ecef_eci.trajectories
WHERE object_id = 99001
ORDER BY time
LIMIT 1;

-- Decompress for further tests
SELECT decompress_chunk(c) FROM show_chunks('ecef_eci.trajectories') c;

-- =============================================================================
-- Test 6: Index usage verification
-- =============================================================================

-- Object + time index should be used for this query
EXPLAIN (COSTS OFF)
SELECT * FROM ecef_eci.trajectories
WHERE object_id = 99001
  AND time >= '2025-01-01 00:00:00+00'
  AND time <  '2025-01-01 00:10:00+00'
ORDER BY time DESC
LIMIT 10;

-- Spatial bucket index should be used
EXPLAIN (COSTS OFF)
SELECT * FROM ecef_eci.trajectories
WHERE spatial_bucket = 0
  AND time >= '2025-01-01 00:00:00+00'
  AND time <  '2025-01-01 00:10:00+00'
ORDER BY time DESC;

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
