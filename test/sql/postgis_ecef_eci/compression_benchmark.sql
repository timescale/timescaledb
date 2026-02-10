-- Test: Compression benchmark across schema approaches
-- Compares Approach A (geometry + floats), B (floats only), C (geometry only)

\set ON_ERROR_STOP 1

\ir include/setup.sql
\ir ../../../../sql/postgis_ecef_eci/schema_variants.sql

-- =============================================================================
-- Test 1: Load data into all three approaches
-- =============================================================================

SELECT * FROM ecef_eci.load_benchmark_data(
    '2025-01-01 00:00:00+00',
    '2 hours',
    '10 seconds'
);

-- =============================================================================
-- Test 2: Verify row counts match
-- =============================================================================

SELECT 'A' AS approach, count(*) AS rows FROM ecef_eci.trajectories
UNION ALL
SELECT 'B', count(*) FROM ecef_eci.trajectories_b
UNION ALL
SELECT 'C', count(*) FROM ecef_eci.trajectories_c
ORDER BY approach;

-- =============================================================================
-- Test 3: Run compression benchmark
-- =============================================================================

SELECT * FROM ecef_eci.compression_benchmark();

-- =============================================================================
-- Test 4: Query equivalence — same object, same time range
-- =============================================================================

-- Approach A: direct
SELECT count(*) AS a_rows, round(avg(x)::numeric, 0) AS a_avg_x
FROM ecef_eci.trajectories
WHERE object_id = 25544
  AND time >= '2025-01-01 00:00:00+00'
  AND time <  '2025-01-01 01:00:00+00';

-- Approach B: identical (same columns)
SELECT count(*) AS b_rows, round(avg(x)::numeric, 0) AS b_avg_x
FROM ecef_eci.trajectories_b
WHERE object_id = 25544
  AND time >= '2025-01-01 00:00:00+00'
  AND time <  '2025-01-01 01:00:00+00';

-- Approach C: extract from geometry
SELECT count(*) AS c_rows, round(avg(ST_X(pos_ecef))::numeric, 0) AS c_avg_x
FROM ecef_eci.trajectories_c
WHERE object_id = 25544
  AND time >= '2025-01-01 00:00:00+00'
  AND time <  '2025-01-01 01:00:00+00';

-- =============================================================================
-- Test 5: Spatial query — only Approach C has a GiST index
-- =============================================================================

-- Approach C: native spatial query using GiST 3D index
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM ecef_eci.trajectories_c
WHERE pos_ecef &&& ST_MakeEnvelope(6700000, -100000, 6800000, 100000)::geometry
  AND time >= '2025-01-01 00:00:00+00'
  AND time <  '2025-01-01 01:00:00+00';

-- Approach B: must reconstruct geometry (no index benefit)
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM ecef_eci.trajectories_b
WHERE x BETWEEN 6700000 AND 6800000
  AND y BETWEEN -100000 AND 100000
  AND time >= '2025-01-01 00:00:00+00'
  AND time <  '2025-01-01 01:00:00+00';

-- Cleanup
DROP SCHEMA ecef_eci CASCADE;
