# Isolation test: Concurrent trajectory inserts into same and different spatial buckets
# Verifies that two sessions can insert trajectory data simultaneously without
# deadlocks or data corruption.

setup
{
    CREATE SCHEMA IF NOT EXISTS ecef_eci;

    CREATE OR REPLACE FUNCTION ecef_eci.altitude_band_bucket(x FLOAT8, y FLOAT8, z FLOAT8)
    RETURNS INT LANGUAGE SQL IMMUTABLE PARALLEL SAFE
    SET search_path = ecef_eci, pg_catalog, public
    AS $$ SELECT CASE WHEN sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 < 500 THEN 0 ELSE 1 END $$;

    CREATE OR REPLACE FUNCTION ecef_eci.ecef_altitude_km(x FLOAT8, y FLOAT8, z FLOAT8)
    RETURNS FLOAT8 LANGUAGE SQL IMMUTABLE PARALLEL SAFE
    SET search_path = ecef_eci, pg_catalog, public
    AS $$ SELECT sqrt(x*x + y*y + z*z) / 1000.0 - 6371.0 $$;

    CREATE TABLE ecef_eci.trajectories (
        time TIMESTAMPTZ NOT NULL,
        object_id INT NOT NULL,
        object_type SMALLINT NOT NULL DEFAULT 0,
        frame SMALLINT NOT NULL DEFAULT 0,
        x FLOAT8 NOT NULL, y FLOAT8 NOT NULL, z FLOAT8 NOT NULL,
        vx FLOAT8, vy FLOAT8, vz FLOAT8,
        altitude_km FLOAT8 NOT NULL,
        spatial_bucket SMALLINT NOT NULL
    );

    SELECT create_hypertable('ecef_eci.trajectories', 'time',
        chunk_time_interval => INTERVAL '1 hour');
    SELECT add_dimension('ecef_eci.trajectories', 'spatial_bucket',
        number_partitions => 4);
}

teardown { DROP SCHEMA ecef_eci CASCADE; }

session "s1"
setup { BEGIN; SET LOCAL lock_timeout = '500ms'; SET LOCAL deadlock_timeout = '10ms'; }
step "s1_insert_leo" {
    INSERT INTO ecef_eci.trajectories (time, object_id, x, y, z, altitude_km, spatial_bucket)
    VALUES ('2025-01-01 00:00:00+00', 1, 6771000.0, 0.0, 0.0, 400.0, 0);
}
step "s1_insert_meo" {
    INSERT INTO ecef_eci.trajectories (time, object_id, x, y, z, altitude_km, spatial_bucket)
    VALUES ('2025-01-01 00:00:00+00', 2, 26571000.0, 0.0, 0.0, 20200.0, 1);
}
step "s1_commit" { COMMIT; }

session "s2"
setup { BEGIN; SET LOCAL lock_timeout = '500ms'; SET LOCAL deadlock_timeout = '10ms'; }
step "s2_insert_leo" {
    INSERT INTO ecef_eci.trajectories (time, object_id, x, y, z, altitude_km, spatial_bucket)
    VALUES ('2025-01-01 00:00:00+00', 3, 6771000.0, 100.0, 0.0, 400.0, 0);
}
step "s2_insert_meo" {
    INSERT INTO ecef_eci.trajectories (time, object_id, x, y, z, altitude_km, spatial_bucket)
    VALUES ('2025-01-01 00:00:00+00', 4, 26571000.0, 100.0, 0.0, 20200.0, 1);
}
step "s2_commit" { COMMIT; }

# Both insert to same spatial bucket (LEO)
permutation "s1_insert_leo" "s2_insert_leo" "s1_commit" "s2_commit"

# Both insert to different spatial buckets
permutation "s1_insert_leo" "s2_insert_meo" "s1_commit" "s2_commit"

# Interleaved: s1 inserts LEO, s2 inserts LEO, then both insert MEO
permutation "s1_insert_leo" "s2_insert_leo" "s1_insert_meo" "s2_insert_meo" "s1_commit" "s2_commit"
