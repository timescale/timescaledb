# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# Setup prior to every permutation.
#
# We define a function 'cagg_bucket_count' to get the number of
# buckets in a continuous aggregate.  We use it to verify that there
# aren't any duplicate buckets/rows inserted into the materialization
# hypertable after concurrent refreshes. Duplicate buckets are
# possible since there is no unique constraint on the GROUP BY keys in
# the materialized hypertable.
#
setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time int, temp float);
    SELECT create_hypertable('conditions', 'time', chunk_time_interval => 20);

    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(1, 100, 1) t;

    CREATE OR REPLACE FUNCTION cond_now()
    RETURNS int LANGUAGE SQL STABLE AS
    $$
      SELECT coalesce(max(time), 0)
      FROM conditions
    $$;

    SELECT set_integer_now_func('conditions', 'cond_now');

    CREATE MATERIALIZED VIEW cond_10
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket(10, time) AS bucket, avg(temp) AS avg_temp
      FROM conditions
      GROUP BY 1 WITH NO DATA;

    CREATE MATERIALIZED VIEW cond_20
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket(20, time) AS bucket, avg(temp) AS avg_temp
      FROM conditions
      GROUP BY 1 WITH NO DATA;

    CREATE TABLE conditions2(time int, temp float);

    SELECT create_hypertable('conditions2', 'time', chunk_time_interval => 20);

    INSERT INTO conditions2
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(1, 100, 1) t;

    CREATE OR REPLACE FUNCTION cond2_now()
    RETURNS int LANGUAGE SQL STABLE AS
    $$
      SELECT coalesce(max(time), 0)
      FROM conditions2
    $$;

    SELECT set_integer_now_func('conditions2', 'cond2_now');

    CREATE MATERIALIZED VIEW cond2_10
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket(10, time) AS bucket, avg(temp) AS avg_temp
      FROM conditions2
      GROUP BY 1 WITH NO DATA;

    CREATE OR REPLACE FUNCTION cagg_bucket_count(cagg regclass)
    RETURNS int AS
    $$
    DECLARE
      cagg_schema name;
      cagg_name name;
      cagg_hyper_schema name;
      cagg_hyper_name name;
      cagg_hyper_relid regclass;
      result int;
    BEGIN
      SELECT nspname, relname
      INTO cagg_schema, cagg_name
      FROM pg_class c, pg_namespace n
      WHERE c.oid = cagg
      AND c.relnamespace = n.oid;

      SELECT format('%I.%I', h.schema_name, h.table_name)::regclass, h.schema_name, h.table_name
      INTO cagg_hyper_relid, cagg_hyper_schema, cagg_hyper_name
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

    CREATE TABLE cancelpid (
        pid INTEGER NOT NULL PRIMARY KEY
    );
    CREATE OR REPLACE PROCEDURE cancelpids() AS
    $$
    DECLARE
        max_attempts INT := 100; -- 2 seconds total (100 * 20ms), enough for 500ms lock_timeout + buffer
        attempts INT := 0;
        remaining_pids INT;
    BEGIN
        PERFORM pg_cancel_backend(pid) FROM cancelpid;
        WHILE EXISTS (SELECT FROM pg_stat_activity WHERE pid IN (SELECT pid FROM cancelpid) AND state = 'active') AND attempts < max_attempts
        LOOP
            PERFORM pg_sleep(0.02);
            attempts := attempts + 1;
        END LOOP;
        -- Check if any processes are still active after timeout
        SELECT COUNT(*) INTO remaining_pids
        FROM pg_stat_activity
        WHERE pid IN (SELECT pid FROM cancelpid) AND state = 'active';
        IF remaining_pids > 0 THEN
            RAISE EXCEPTION 'Timeout waiting for % process(es) to become inactive after cancellation', remaining_pids;
        END IF;
        DELETE FROM cancelpid;
    END;
    $$ LANGUAGE plpgsql;

    CREATE OR REPLACE VIEW pending_materialization_ranges AS
    SELECT
        c.user_view_name,
        m.lowest_modified_value,
        m.greatest_modified_value
    FROM
        _timescaledb_catalog.continuous_aggs_materialization_ranges m
        LEFT JOIN _timescaledb_catalog.continuous_agg c on c.mat_hypertable_id = m.materialization_id
    ORDER BY
        1, 2, 3;

}

# Move the invalidation threshold so that we can generate some
# invalidations. This must be done in its own setup block since
# refreshing can't be done in a transaction block.
setup
{
    CALL refresh_continuous_aggregate('cond_10', 0, 30);
}

# Generate some invalidations. Must be done in separate transcations
# or otherwise there will be only one invalidation.
setup
{
    BEGIN;
    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(1, 10, 1) t;
    COMMIT;
    BEGIN;
    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(10, 20, 1) t;
    COMMIT;
    BEGIN;
    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(15, 40, 1) t;
    COMMIT;
}

teardown {
    DROP TABLE conditions CASCADE;
    DROP TABLE conditions2 CASCADE;
    DROP TABLE cancelpid;
}

# Waitpoint for cagg invalidation logs
session "WP_after"
step "WP_after_enable"
{
    SELECT debug_waitpoint_enable('after_process_cagg_invalidations_for_refresh_lock');
}
step "WP_after_release"
{
    SELECT debug_waitpoint_release('after_process_cagg_invalidations_for_refresh_lock');
}

session "WP_before"
step "WP_before_enable"
{
    SELECT debug_waitpoint_enable('before_process_cagg_invalidations_for_refresh_lock');
}
step "WP_before_release"
{
    SELECT debug_waitpoint_release('before_process_cagg_invalidations_for_refresh_lock');
}

session "WP_after_materialization"
step "WP_after_materialization_enable"
{
    SELECT debug_waitpoint_enable('after_process_cagg_materializations');
}
step "WP_after_materialization_release"
{
    SELECT debug_waitpoint_release('after_process_cagg_materializations');
}

# Session to refresh the cond_10 continuous aggregate
session "R1"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
    INSERT INTO cancelpid VALUES (pg_backend_pid())
    ON CONFLICT (pid) DO NOTHING;
}
step "R1_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 25, 70);
}
step "R1_refresh2"
{
    CALL refresh_continuous_aggregate('cond_10', 30, 120);
}
step "R1_refresh3"
{
    CALL refresh_continuous_aggregate('cond_10', 60, 70);
}
step "R1_drop"
{
    DROP MATERIALIZED VIEW cond_10;
}

## generate invalidation that has overlaps for R1_refresh and R2_refresh
session "RI2"
step "RI2_invalidation"
{
    INSERT INTO conditions VALUES  (20, 1000), (30, 1000), (40, 1000), (50, 1000), (60, 1000), (70, 1000), (79, 1000);
}

session "R12"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "R12_refresh"
{
    CALL refresh_continuous_aggregate('cond2_10', 25, 70);
}

session "R13"
step "R13_refresh1"
{
    -- the window start 65 is far from the pending range start 30
    -- so in this case the left behind pending range will NOT be processed
    CALL refresh_continuous_aggregate('cond_10', 65, 100);
}
step "R13_refresh2"
{
    -- the window start 40 is one bucket before the pending range start 30
    -- so in this case the left behind pending range will be processed
    CALL refresh_continuous_aggregate('cond_10', 40, 100);
}
step "R13_refresh3"
{
    -- the window end 100 is far from the pending range end 120
    -- so in this case the left behind pending range will NOT be processed
    CALL refresh_continuous_aggregate('cond_10', 40, 100);
}
step "R13_refresh4"
{
    -- the window end 110 is one bucket after the pending range end 120
    -- so in this case the left behind pending range will be processed
    CALL refresh_continuous_aggregate('cond_10', 40, 110);
}
step "R13_refresh5"
{
    -- the window start and end are far in more than one bucket from
    -- pending range, so in this case the left behind pending range
    -- will NOT be processed
    CALL refresh_continuous_aggregate('cond_10', 50, 100);
}

# Refresh that overlaps with R1
session "R2"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "R2_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 35, 62);
}
step "R2_refresh_exact"
{
    CALL refresh_continuous_aggregate('cond_10', 25, 70);
}
step "R2_refresh_left"
{
    CALL refresh_continuous_aggregate('cond_10', 15, 55);
}
step "R2_refresh_superset"
{
    CALL refresh_continuous_aggregate('cond_10', 15, 85);
}

# Refresh on same aggregate (cond_10) that doesn't overlap with R1 and R2
session "R3"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "R3_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 70, 107);
}
step "R3_refresh_adjacent"
{
    CALL refresh_continuous_aggregate('cond_10', 50, 60);
}

# Refresh on same aggregate (cond_10) that doesn't overlap with R1 and R2
# with DEBUG1 enabled
session "R5"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
    SET SESSION client_min_messages = 'DEBUG1';
}
step "R5_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 70, 107);
}

# Check for pending materialization ranges
session "R6"
step "R6_pending_materialization_ranges"
{
    SELECT * FROM pending_materialization_ranges WHERE user_view_name = 'cond_10';
}
step "R6_pending_materialization_ranges_orphan"
{
    SELECT * FROM pending_materialization_ranges WHERE user_view_name IS NULL;
}
step "R6_materialization_logs" {
    SELECT ca.user_view_name AS cagg,
        lowest_modified_value,
        greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log l
    JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = l.materialization_id
    WHERE ca.user_view_name = 'cond_10'
    ORDER BY 1, 2, 3;
}

# Define a number of lock sessions to simulate concurrent refreshes
# by selectively grabbing the locks we use to handle concurrency.

# The "L2" session takes an access share lock on the invalidation
# threshold table. This simulates a reader, which has not yet finished
# (e.g., and insert into the hypertable, or a refresh that has not yet
# grabbed the exclusive lock).
session "L2"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "L2_read_lock_threshold_table"
{
    BEGIN;
    LOCK _timescaledb_catalog.continuous_aggs_invalidation_threshold
    IN ACCESS SHARE MODE;
}
step "L2_read_unlock_threshold_table"
{
    ROLLBACK;
}

# The "L4" session locks the materialization invalidation table.
session "L4"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "L4_lock_mat_invals"
{
    BEGIN;
    LOCK _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    IN ACCESS EXCLUSIVE MODE;
}
step "L4_unlock_mat_invals"
{
    ROLLBACK;
}

# Session to view the contents of a cagg after materialization. It
# also prints the bucket count (number of rows in the materialization
# hypertable) and the invalidation threshold. The bucket count should
# match the number of rows in the query if there are no duplicate
# buckets/rows.
session "S1"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "S1_select"
{
    SELECT bucket, avg_temp
    FROM cond_10
    ORDER BY 1;

    SELECT * FROM cagg_bucket_count('cond_10');
    SELECT h.table_name AS hypertable, it.watermark AS threshold
    FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold it,
    _timescaledb_catalog.hypertable h
    WHERE it.hypertable_id = h.id
    ORDER BY 1;
}

session "K1"
step "K1_cancelpid"
{
    CALL cancelpids();
}

####################################################################
#
# Tests for concurrent updates to the invalidation threshold (first
# transaction of a refresh).
#
####################################################################

# Run single transaction refresh to get some reference output.  The
# result of a query on the aggregate should always look like this
# example (when refreshed with the same window).
permutation "R1_refresh" "S1_select" "R3_refresh" "S1_select"

# A threshold reader (insert) should block a refresh if the threshold
# does not exist yet (insert of new threshold)
permutation "L2_read_lock_threshold_table" "R3_refresh" "L2_read_unlock_threshold_table" "S1_select"

# A threshold reader (insert) should block a refresh if the threshold
# needs an update
permutation "R1_refresh" "L2_read_lock_threshold_table" "R3_refresh" "L2_read_unlock_threshold_table" "S1_select"

# A threshold reader (insert) blocks a refresh even if the threshold
# doesn't need an update (could be improved)
permutation "R3_refresh" "L2_read_lock_threshold_table" "R1_refresh" "L2_read_unlock_threshold_table"  "S1_select"

##################################################################
#
# Tests for concurrent refreshes of continuous aggregates (second
# and third transactions of a refresh).
#
##################################################################

# Concurrent refresh of caggs on different hypertables should not
# block each other
permutation "R1_refresh" "R12_refresh"

# CAgg invalidation logs processing in a separated transaction and the materialization
# transaction can be executed concurrently
# TODO: pending materialization ranges not populated yet
#permutation "WP_after_enable" "R1_refresh"("WP_after_enable") "R6_pending_materialization_ranges" "R5_refresh"("WP_after_enable") "R6_pending_materialization_ranges" "WP_after_release" "R6_pending_materialization_ranges" "S1_select"

# CAgg materialization phase (third trasaction of the refresh procedure) terminated by another session and then
# refreshing again and make sure the pending ranges will be processed
# TODO: pending materialization ranges not populated yet
#permutation "WP_after_enable" "R6_pending_materialization_ranges" "R1_refresh"("WP_after_enable") "R3_refresh"("WP_after_enable") "K1_cancelpid"("R1_refresh") "R6_pending_materialization_ranges" "WP_after_release" "R13_refresh1"("K1_cancelpid") "R6_pending_materialization_ranges" "R13_refresh2" "R6_pending_materialization_ranges"

# TODO: pending materialization ranges not populated yet
#permutation "WP_after_enable" "R6_pending_materialization_ranges" "R1_refresh2"("WP_after_enable") "R3_refresh"("WP_after_enable") "K1_cancelpid"("R1_refresh2") "R6_pending_materialization_ranges" "WP_after_release" "R13_refresh3"("K1_cancelpid") "R6_pending_materialization_ranges" "R13_refresh5" "R6_pending_materialization_ranges" "R13_refresh4" "R6_pending_materialization_ranges"

# When dropping a CAgg pending ranges left behind should be removed
# TODO: pending materialization ranges not populated yet
#permutation "WP_after_enable" "R6_pending_materialization_ranges" "R1_refresh"("WP_after_enable") "K1_cancelpid"("R1_refresh") "R6_pending_materialization_ranges" "WP_after_release" "R1_drop" "R6_pending_materialization_ranges_orphan"

# R3 should wait for R1 to finish because there are cagg invalidation rows locked
permutation "WP_before_enable" "R1_refresh"("WP_before_enable") "R3_refresh" "WP_before_release"

# Concurrent refresh of caggs on non-overlapping ranges should not
# block each other in the third transaction (materialization)
permutation "WP_after_materialization_enable" "R1_refresh"("WP_after_materialization_enable") "WP_after_materialization_release" "R3_refresh" 

# Concurrent refresh on same cagg that generate overlapping  materialization range will error out. Only 1 can proceed
# R1 and R2 have overlap refresh and  we add invalidations. So R2 materialization range will overlap with R1
## R1 will process invalidation first, add cagg ranges, then wait. R2 should fail as it attempts to process an
## overlapping range
# TODO: overlapping ranges now wait on lock instead of erroring
#permutation "WP_before_enable" "R1_refresh"("WP_before_enable") "RI2_invalidation" "WP_after_enable" "WP_before_release" "R2_refresh" "WP_after_release"

# Exact match overlap: R2 materializes [30, 70) which exactly matches R1's [30, 70)
# TODO: overlapping ranges now wait on lock instead of erroring
#permutation "WP_before_enable" "R1_refresh"("WP_before_enable") "RI2_invalidation" "WP_after_enable" "WP_before_release" "R2_refresh_exact" "WP_after_release"

# Left overlap: R2 materializes [20, 50) which overlaps R1's [30, 70) from the left
# TODO: overlapping ranges now wait on lock instead of erroring
#permutation "WP_before_enable" "R1_refresh"("WP_before_enable") "RI2_invalidation" "WP_after_enable" "WP_before_release" "R2_refresh_left" "WP_after_release"

# Superset overlap: R2 materializes [20, 80) which fully contains R1's [30, 70)
# TODO: overlapping ranges now wait on lock instead of erroring
#permutation "WP_before_enable" "R1_refresh"("WP_before_enable") "RI2_invalidation" "WP_after_enable" "WP_before_release" "R2_refresh_superset" "WP_after_release"

# Refresh [60, 70) waits at the beginning of Txn3, before doing any materialization.
# New rows are inserted and another refresh for [15, 55) generates new materialization logs.
# Refresh [60, 70) is expected to materialize its own range without touching new materialization logs for the same range.
permutation "WP_after_enable" "R6_materialization_logs" "R1_refresh3" "R6_materialization_logs" "RI2_invalidation" "R2_refresh_left" "R6_materialization_logs" "WP_after_release" "R6_materialization_logs"

# Adjacent ranges [50, 60) and [60, 70) can proceed concurrently without blocking each other
permutation "WP_before_enable" "R6_materialization_logs" "R1_refresh3" "R3_refresh_adjacent" "R6_materialization_logs" "WP_before_release" "S1_select" "R6_materialization_logs"

# Block R1 refresh [60, 70) at Txn3 and R2 refresh [15, 55) at Txn2, right before both tries to read materialization invalidations of the same CAgg
# They should run concurrently after releasing the lock on continuous_aggs_materialization_invalidation_log table
permutation "WP_after_enable" "R1_refresh3" "L4_lock_mat_invals" "R2_refresh_left" "WP_after_release" "L4_unlock_mat_invals" "S1_select"
