# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test that concurrent add_continuous_aggregate_column calls are properly
# serialized via AccessExclusiveLock on the continuous aggregate relation.

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE temperature (
        time TIMESTAMPTZ NOT NULL,
        device INT,
        temperature FLOAT,
        humidity FLOAT,
        pressure FLOAT
    ) WITH (tsdb.hypertable);

    CREATE MATERIALIZED VIEW cagg_test
        WITH (timescaledb.continuous, timescaledb.materialized_only = true)
    AS SELECT time_bucket('1 day', time) AS bucket, device,
              avg(temperature) AS avg_temp
    FROM temperature
    GROUP BY 1, 2 WITH NO DATA;
}

teardown
{
    DROP TABLE temperature CASCADE;
}

# Session that holds an AccessExclusiveLock on the cagg to simulate
# a concurrent add_continuous_aggregate_column in progress.
session "L1"
setup
{
    SET lock_timeout = '5s';
}
step "l1_lock"
{
    BEGIN;
    LOCK TABLE cagg_test IN ACCESS SHARE MODE;
}
step "l1_unlock"
{
    ROLLBACK;
}

# Session 1: add sum(humidity)
session "S1"
setup
{
    SET lock_timeout = '5s';
}
step "s1_add_column"
{
    BEGIN;
    SELECT add_continuous_aggregate_column('cagg_test', 'sum(humidity) AS sum_humidity');
}
step "s1_add_column_commit"
{
    COMMIT;
}

# Session 2: add max(pressure)
session "S2"
setup
{
    SET lock_timeout = '5s';
}
step "s2_add_column"
{
    BEGIN;
    SELECT add_continuous_aggregate_column('cagg_test', 'max(pressure) AS max_pressure');
}
step "s2_add_column_commit"
{
    COMMIT;
}

# Session to verify the final state
session "S3"
step "s3_select"
{
    SELECT * FROM cagg_test ORDER BY bucket, device;
}

# Test 1: add_continuous_aggregate_column blocks when cagg is locked.
# L1 holds AccessExclusiveLock, S1 blocks waiting for it, then proceeds
# after L1 releases the lock.
permutation "l1_lock" "s1_add_column" "l1_unlock" "s1_add_column_commit" "s3_select"

# Test 2: Two concurrent add_continuous_aggregate_column calls serialize.
# S1 takes AccessExclusiveLock, S2 blocks until S1 completes.
# Both columns should be present in the final view.
permutation "s1_add_column" "s2_add_column" "s1_add_column_commit" "s2_add_column_commit" "s3_select"
