# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test concurrent CAgg refreshes with overlapping invalidation entry cutting.
# This test verifies that when two non-overlapping refreshes attempt to cut a single invalidation entry, the final state is sequentially consistent. i.e. Only a single refresh cuts the original entry
# This is the test setup:
# A single invalidation entry [-100, 100] exists. Two refreshes with
# non-overlapping windows [20, 40) and [0, 15) run concurrently. We verify that:
#   1. Only one refresh processes the original entry at a time
#      (the other blocks on ShareUpdateExclusiveLock)
#   2. The second refresh processes the "cut" ranges left by the first
#   3. Final invalidation log contains the expected cut ranges
#
# We also test with a single invalidation entry [-100, 99] to check bucket alignment during refresh registration works correctly

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time int, temp float);
    SELECT create_hypertable('conditions', 'time', chunk_time_interval => 50);

    INSERT INTO conditions
    SELECT t, abs(t) % 40
    FROM generate_series(-200, 200, 1) t;

    CREATE OR REPLACE FUNCTION cond_now()
    RETURNS int LANGUAGE SQL STABLE AS
    $$
      SELECT 200
    $$;

    SELECT set_integer_now_func('conditions', 'cond_now');

    CREATE MATERIALIZED VIEW cond_5
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket(5, time) AS bucket, avg(temp) AS avg_temp
      FROM conditions
      GROUP BY 1 WITH NO DATA;

    CREATE MATERIALIZED VIEW cond_5_seq
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket(5, time) AS bucket, avg(temp) AS avg_temp
      FROM conditions
      GROUP BY 1 WITH NO DATA;

    CREATE OR REPLACE FUNCTION cagg_bucket_count(cagg regclass)
    RETURNS int AS
    $$
    DECLARE
      cagg_schema name;
      cagg_name name;
      cagg_hyper_schema name;
      cagg_hyper_name name;
      result int;
    BEGIN
      SELECT nspname, relname
      INTO cagg_schema, cagg_name
      FROM pg_class c, pg_namespace n
      WHERE c.oid = cagg
      AND c.relnamespace = n.oid;

      SELECT h.schema_name, h.table_name
      INTO cagg_hyper_schema, cagg_hyper_name
      FROM _timescaledb_catalog.continuous_agg ca, _timescaledb_catalog.hypertable h
      WHERE ca.user_view_name = cagg_name
      AND ca.user_view_schema = cagg_schema
      AND ca.mat_hypertable_id = h.id;

      EXECUTE format('SELECT count(*) FROM %I.%I',
                quote_ident(cagg_hyper_schema),
                quote_ident(cagg_hyper_name))
      INTO result;

      RETURN result;
    END
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE VIEW cagg_inval_log AS
    SELECT mil.lowest_modified_value, mil.greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log mil
    JOIN _timescaledb_catalog.continuous_agg ca
      ON mil.materialization_id = ca.mat_hypertable_id
    WHERE ca.user_view_name = 'cond_5'
    ORDER BY mil.lowest_modified_value;
}

# Initial refresh to set the invalidation threshold and materialize all data
setup
{
    CALL refresh_continuous_aggregate('cond_5', -200, 200);
}
setup
{
    CALL refresh_continuous_aggregate('cond_5_seq', -200, 200);
}

teardown
{
    DROP TABLE conditions CASCADE;
}

# Waitpoint: pauses after invalidation processing but before SPI_commit
session "WP"
step "wp_enable"
{
    SELECT debug_waitpoint_enable('before_process_cagg_invalidations_for_refresh_lock');
}
step "wp_release"
{
    SELECT debug_waitpoint_release('before_process_cagg_invalidations_for_refresh_lock');
}

# Session R1: refresh [20, 40)
session "R1"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "r1_refresh"
{
    CALL refresh_continuous_aggregate('cond_5', 20, 40);
}

# Session R2: refresh [0, 15)
session "R2"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "r2_refresh"
{
    CALL refresh_continuous_aggregate('cond_5', 0, 15);
}

# Session to insert invalidation entries before each permutation.
# Inserts matching entries into both cond_5 (concurrent) and cond_5_seq (sequential).
session "I1"
step "i1_insert_inval_100"
{
    INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
        (materialization_id, lowest_modified_value, greatest_modified_value)
    SELECT mat_hypertable_id, -100, 100
    FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name IN ('cond_5', 'cond_5_seq');
}
step "i1_insert_inval_99"
{
    INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
        (materialization_id, lowest_modified_value, greatest_modified_value)
    SELECT mat_hypertable_id, -100, 99
    FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name IN ('cond_5', 'cond_5_seq');
}

# Verification session
session "S1"
step "s1_check_inval_log"
{
    SELECT * FROM cagg_inval_log;
}

# Permutation 1: Invalidation [-100,100], R1 goes first
permutation "i1_insert_inval_100" "wp_enable" "r1_refresh"("wp_enable") "r2_refresh" "s1_check_inval_log" "wp_release" "s1_check_inval_log"

# Permutation 2: Invalidation [-100,99], R2 goes first
permutation "i1_insert_inval_99" "wp_enable" "r2_refresh"("wp_enable") "r1_refresh" "s1_check_inval_log" "wp_release" "s1_check_inval_log"
