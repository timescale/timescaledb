-- PostGIS ECEF/ECI Integration for TimescaleDB
-- Alternative schema variants for compression benchmarking
--
-- Approach A (default in schema.sql): Redundant geometry + decomposed floats
-- Approach B (this file):            Floats only, reconstruct geometry at query time
-- Approach C (this file):            Geometry only, extract floats at query time
--
-- Run all three with the same test dataset and compare:
--   - Compression ratio (pg_total_relation_size before/after)
--   - Ingest throughput (rows/sec)
--   - Query latency for common patterns
--   - Storage footprint

-- =============================================================================
-- Approach B: Floats Only (No Geometry Column)
-- =============================================================================
-- Pros: Best compression (Gorilla on all float columns, no WKB blob overhead)
-- Cons: Must call ST_MakePoint(x,y,z) for any PostGIS spatial function
--
-- Expected compression: ~10:1 on position data

CREATE TABLE IF NOT EXISTS ecef_eci.trajectories_b (
    time            TIMESTAMPTZ     NOT NULL,
    object_id       INT             NOT NULL,
    object_type     SMALLINT        NOT NULL DEFAULT 5,
    frame           SMALLINT        NOT NULL DEFAULT 0,

    -- ECEF coordinates (meters from Earth center)
    x               FLOAT8          NOT NULL,
    y               FLOAT8          NOT NULL,
    z               FLOAT8          NOT NULL,

    -- Velocity (m/s)
    vx              FLOAT8,
    vy              FLOAT8,
    vz              FLOAT8,

    -- Derived fields
    altitude_km     FLOAT8          NOT NULL,
    spatial_bucket  SMALLINT        NOT NULL
);

SELECT create_hypertable(
    'ecef_eci.trajectories_b', 'time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

SELECT add_dimension(
    'ecef_eci.trajectories_b', 'spatial_bucket',
    number_partitions => 16, if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_traj_b_object_time
    ON ecef_eci.trajectories_b (object_id, time DESC);

ALTER TABLE ecef_eci.trajectories_b SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'object_id',
    timescaledb.compress_orderby = 'time ASC'
);

-- Geometry reconstruction helper for Approach B
CREATE OR REPLACE FUNCTION ecef_eci.make_ecef_point(x FLOAT8, y FLOAT8, z FLOAT8)
RETURNS geometry
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    -- Requires PostGIS. Returns geometry(PointZ, 4978).
    SELECT ST_SetSRID(ST_MakePoint(x, y, z), 4978)
$$;

-- =============================================================================
-- Approach C: Geometry Only (No Decomposed Floats)
-- =============================================================================
-- Pros: No redundancy, standard PostGIS workflow
-- Cons: Poor compression (WKB blob), must extract x/y/z for math
--
-- Expected compression: ~1.4:1 on geometry column (TOAST/LZ only)

CREATE TABLE IF NOT EXISTS ecef_eci.trajectories_c (
    time            TIMESTAMPTZ     NOT NULL,
    object_id       INT             NOT NULL,
    object_type     SMALLINT        NOT NULL DEFAULT 5,
    frame           SMALLINT        NOT NULL DEFAULT 0,

    -- PostGIS ECEF geometry — the only position representation
    pos_ecef        geometry(PointZ, 4978) NOT NULL,

    -- Velocity stored as a second geometry or as floats
    -- (geometry velocity is unusual; keep as floats for now)
    vx              FLOAT8,
    vy              FLOAT8,
    vz              FLOAT8,

    -- Derived fields — computed from geometry at insert
    altitude_km     FLOAT8          NOT NULL,
    spatial_bucket  SMALLINT        NOT NULL
);

SELECT create_hypertable(
    'ecef_eci.trajectories_c', 'time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

SELECT add_dimension(
    'ecef_eci.trajectories_c', 'spatial_bucket',
    number_partitions => 16, if_not_exists => TRUE
);

-- GiST 3D index — the advantage of storing geometry
CREATE INDEX IF NOT EXISTS idx_traj_c_pos_gist
    ON ecef_eci.trajectories_c USING gist (pos_ecef gist_geometry_ops_nd);

CREATE INDEX IF NOT EXISTS idx_traj_c_object_time
    ON ecef_eci.trajectories_c (object_id, time DESC);

ALTER TABLE ecef_eci.trajectories_c SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'object_id',
    timescaledb.compress_orderby = 'time ASC'
);

-- =============================================================================
-- Benchmark Helper: Load same data into all three approaches
-- =============================================================================

CREATE OR REPLACE FUNCTION ecef_eci.load_benchmark_data(
    p_start_time   TIMESTAMPTZ DEFAULT '2025-01-01 00:00:00+00',
    p_duration     INTERVAL DEFAULT '2 hours',
    p_interval     INTERVAL DEFAULT '10 seconds'
) RETURNS TABLE(approach TEXT, row_count BIGINT)
LANGUAGE plpgsql
AS $$
DECLARE
    n_a BIGINT;
    n_b BIGINT;
    n_c BIGINT;
BEGIN
    -- Approach A (default trajectories table — must already exist from schema.sql)
    INSERT INTO ecef_eci.trajectories
    SELECT * FROM ecef_eci.generate_test_dataset(p_start_time, p_duration, p_interval);
    GET DIAGNOSTICS n_a = ROW_COUNT;

    -- Approach B (floats only)
    INSERT INTO ecef_eci.trajectories_b
    SELECT * FROM ecef_eci.generate_test_dataset(p_start_time, p_duration, p_interval);
    GET DIAGNOSTICS n_b = ROW_COUNT;

    -- Approach C (geometry only)
    INSERT INTO ecef_eci.trajectories_c (time, object_id, object_type, frame,
        pos_ecef, vx, vy, vz, altitude_km, spatial_bucket)
    SELECT time, object_id, object_type, frame,
        ST_SetSRID(ST_MakePoint(x, y, z), 4978),
        vx, vy, vz, altitude_km, spatial_bucket
    FROM ecef_eci.generate_test_dataset(p_start_time, p_duration, p_interval);
    GET DIAGNOSTICS n_c = ROW_COUNT;

    RETURN QUERY VALUES
        ('A: geometry + floats', n_a),
        ('B: floats only',       n_b),
        ('C: geometry only',     n_c);
END;
$$;

-- =============================================================================
-- Benchmark Helper: Measure compression ratios
-- =============================================================================

CREATE OR REPLACE FUNCTION ecef_eci.compression_benchmark()
RETURNS TABLE (
    approach          TEXT,
    uncompressed_bytes BIGINT,
    compressed_bytes   BIGINT,
    ratio             NUMERIC
)
LANGUAGE plpgsql
AS $$
DECLARE
    size_before_a BIGINT; size_after_a BIGINT;
    size_before_b BIGINT; size_after_b BIGINT;
    size_before_c BIGINT; size_after_c BIGINT;
BEGIN
    -- Measure uncompressed sizes
    SELECT pg_total_relation_size('ecef_eci.trajectories') INTO size_before_a;
    SELECT pg_total_relation_size('ecef_eci.trajectories_b') INTO size_before_b;
    SELECT pg_total_relation_size('ecef_eci.trajectories_c') INTO size_before_c;

    -- Compress all
    PERFORM compress_chunk(c) FROM show_chunks('ecef_eci.trajectories') c;
    PERFORM compress_chunk(c) FROM show_chunks('ecef_eci.trajectories_b') c;
    PERFORM compress_chunk(c) FROM show_chunks('ecef_eci.trajectories_c') c;

    -- Measure compressed sizes
    SELECT pg_total_relation_size('ecef_eci.trajectories') INTO size_after_a;
    SELECT pg_total_relation_size('ecef_eci.trajectories_b') INTO size_after_b;
    SELECT pg_total_relation_size('ecef_eci.trajectories_c') INTO size_after_c;

    RETURN QUERY VALUES
        ('A: geometry + floats', size_before_a, size_after_a,
         round(size_before_a::numeric / NULLIF(size_after_a, 0), 2)),
        ('B: floats only',       size_before_b, size_after_b,
         round(size_before_b::numeric / NULLIF(size_after_b, 0), 2)),
        ('C: geometry only',     size_before_c, size_after_c,
         round(size_before_c::numeric / NULLIF(size_after_c, 0), 2));
END;
$$;

COMMENT ON FUNCTION ecef_eci.compression_benchmark()
IS 'Compresses all three schema variants and reports before/after sizes and ratio. Call load_benchmark_data() first.';
