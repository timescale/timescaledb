# Isolation test: Insert during compression
# Verifies that new inserts to a hypertable succeed while an older chunk
# is being compressed.

setup
{
    CREATE SCHEMA IF NOT EXISTS ecef_eci;

    CREATE TABLE ecef_eci.trajectories (
        time TIMESTAMPTZ NOT NULL,
        object_id INT NOT NULL,
        x FLOAT8 NOT NULL, y FLOAT8 NOT NULL, z FLOAT8 NOT NULL,
        altitude_km FLOAT8 NOT NULL,
        spatial_bucket SMALLINT NOT NULL
    );

    SELECT create_hypertable('ecef_eci.trajectories', 'time',
        chunk_time_interval => INTERVAL '1 hour');
    SELECT add_dimension('ecef_eci.trajectories', 'spatial_bucket',
        number_partitions => 4);

    ALTER TABLE ecef_eci.trajectories SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'object_id',
        timescaledb.compress_orderby = 'time ASC'
    );

    -- Insert data into an old chunk (hour 0)
    INSERT INTO ecef_eci.trajectories (time, object_id, x, y, z, altitude_km, spatial_bucket)
    SELECT '2025-01-01 00:00:00+00'::timestamptz + (s * INTERVAL '10 seconds'),
           1, 6771000.0, 0.0, 0.0, 400.0, 0
    FROM generate_series(0, 59) s;
}

teardown { DROP SCHEMA ecef_eci CASCADE; }

session "compressor"
setup { BEGIN; SET LOCAL lock_timeout = '2s'; }
step "compress" {
    SELECT compress_chunk(c) FROM show_chunks('ecef_eci.trajectories') c;
}
step "compressor_commit" { COMMIT; }

session "inserter"
setup { BEGIN; SET LOCAL lock_timeout = '2s'; }
step "insert_new_chunk" {
    -- Insert into a DIFFERENT time range (hour 1) while hour 0 is being compressed
    INSERT INTO ecef_eci.trajectories (time, object_id, x, y, z, altitude_km, spatial_bucket)
    VALUES ('2025-01-01 01:00:00+00', 2, 6771000.0, 100.0, 0.0, 400.0, 0);
}
step "insert_same_chunk" {
    -- Insert into the SAME chunk that is being compressed
    INSERT INTO ecef_eci.trajectories (time, object_id, x, y, z, altitude_km, spatial_bucket)
    VALUES ('2025-01-01 00:30:00+00', 3, 6771000.0, 200.0, 0.0, 400.0, 0);
}
step "inserter_commit" { COMMIT; }

# Compress old chunk while inserting into new chunk (should not block)
permutation "compress" "insert_new_chunk" "compressor_commit" "inserter_commit"

# Insert into chunk being compressed (may block until compression completes)
permutation "compress" "insert_same_chunk" "compressor_commit" "inserter_commit"
