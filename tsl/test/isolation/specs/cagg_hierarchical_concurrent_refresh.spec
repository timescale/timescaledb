# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# Test concurrent refresh on hierarchical continuous aggregates.
#
# Setup:
#   hypertable (conditions) -> L1 CAgg (cagg_6h) -> L2 CAgg (cagg_1d)
#
# We test:
# 1. Concurrent refreshes of L1 and L2 with both overlapping and non-overlapping windows
# 2. Invalidation propagation from hypertable -> L1 mat hypertable -> L2
# 3. Data correctness after all refreshes complete
#

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    SET timezone TO 'UTC';

    CREATE TABLE conditions (
        time timestamptz NOT NULL,
        temp float
    );

    SELECT create_hypertable('conditions', 'time', chunk_time_interval => INTERVAL '1 day');

    -- Insert 3 days of data at 15-minute intervals
    INSERT INTO conditions (time, temp)
    SELECT ts, 20.0 + extract(hour from ts)
    FROM generate_series('2026-01-01 00:00:00+00'::timestamptz,
                         '2026-01-03 23:45:00+00', '15 minutes') ts;

    -- L1: 6-hour aggregation on the hypertable
    CREATE MATERIALIZED VIEW cagg_6h
    WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
    SELECT time_bucket('6 hours', time) AS bucket,
           avg(temp) AS avg_temp,
           min(temp) AS min_temp,
           max(temp) AS max_temp,
           count(*)  AS row_count
    FROM conditions
    GROUP BY 1
    WITH NO DATA;

    -- L2: daily aggregation on L1 (hierarchical)
    CREATE MATERIALIZED VIEW cagg_1d
    WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
    SELECT time_bucket('1 day', bucket) AS bucket,
           avg(avg_temp) AS avg_temp,
           min(min_temp) AS min_temp,
           max(max_temp) AS max_temp,
           sum(row_count) AS row_count
    FROM cagg_6h
    GROUP BY 1
    WITH NO DATA;
}

# Initial refreshes to move the invalidation threshold
setup
{
    CALL refresh_continuous_aggregate('cagg_6h', '2026-01-01', '2026-01-04');
}

setup
{
    CALL refresh_continuous_aggregate('cagg_1d', '2026-01-01', '2026-01-05');
}

# Insert new data that will generate invalidations
# We use separate setup blocks so each generates its own invalidation entry
setup
{
    BEGIN;
    INSERT INTO conditions (time, temp)
    SELECT ts, 10.0
    FROM generate_series('2026-01-02 00:00:00+00'::timestamptz,
                         '2026-01-02 11:45:00+00', '15 minutes') ts;
    COMMIT;
    BEGIN;
    INSERT INTO conditions (time, temp)
    SELECT ts, 20.0
    FROM generate_series('2026-01-02 12:00:00+00'::timestamptz,
                         '2026-01-02 23:45:00+00', '15 minutes') ts;
    COMMIT;
    BEGIN;
    INSERT INTO conditions (time, temp)
    SELECT ts, 30.0
    FROM generate_series('2026-01-01 00:00:00+00'::timestamptz,
                         '2026-01-01 23:45:00+00', '15 minutes') ts;
    COMMIT;
    BEGIN;
    INSERT INTO conditions (time, temp)
    SELECT ts, 40.0
    FROM generate_series('2026-01-03 00:00:00+00'::timestamptz,
                         '2026-01-03 23:45:00+00', '15 minutes') ts;
    COMMIT;
}

teardown {
    DROP MATERIALIZED VIEW IF EXISTS cagg_1d CASCADE;
    DROP MATERIALIZED VIEW IF EXISTS cagg_6h CASCADE;
    DROP TABLE IF EXISTS conditions CASCADE;
}

# Waitpoints to control refresh ordering
session "WP_before"
step "WP_before_enable"
{
    SELECT debug_waitpoint_enable('before_process_cagg_invalidations_for_refresh_lock');
}
step "WP_before_release"
{
    SELECT debug_waitpoint_release('before_process_cagg_invalidations_for_refresh_lock');
}

session "WP_after"
step "WP_after_enable"
{
    SELECT debug_waitpoint_enable('after_process_cagg_invalidations_for_refresh_lock');
}
step "WP_after_release"
{
    SELECT debug_waitpoint_release('after_process_cagg_invalidations_for_refresh_lock');
}

# Session to refresh L1 (6-hour)
session "L1"
setup
{
    SET SESSION deadlock_timeout = '500ms';
    SET timezone TO 'UTC';
}
step "L1_refresh_jan1"
{
    CALL refresh_continuous_aggregate('cagg_6h', '2026-01-01 00:00', '2026-01-02 00:00');
}
step "L1_refresh_full"
{
    CALL refresh_continuous_aggregate('cagg_6h', '2026-01-01', '2026-01-04');
}

# Second session to refresh L1 concurrently
session "L1b"
setup
{
    SET SESSION deadlock_timeout = '500ms';
    SET timezone TO 'UTC';
}
step "L1b_refresh_jan1_2"
{
    CALL refresh_continuous_aggregate('cagg_6h', '2026-01-01 00:00', '2026-01-03 00:00');
}
step "L1b_refresh_jan3"
{
    CALL refresh_continuous_aggregate('cagg_6h', '2026-01-03 00:00', '2026-01-04 00:00');
}

# Session to refresh L2 (daily)
session "L2"
setup
{
    SET SESSION deadlock_timeout = '500ms';
    SET timezone TO 'UTC';
}
step "L2_refresh_full"
{
    CALL refresh_continuous_aggregate('cagg_1d', '2026-01-01', '2026-01-05');
}
step "L2_refresh_jan1"
{
    CALL refresh_continuous_aggregate('cagg_1d', '2026-01-01', '2026-01-02');
}

# Second session to refresh L2 concurrently
session "L2b"
setup
{
    SET SESSION deadlock_timeout = '500ms';
    SET timezone TO 'UTC';
}
step "L2b_refresh_jan1_2"
{
    CALL refresh_continuous_aggregate('cagg_1d', '2026-01-01', '2026-01-03');
}
step "L2b_refresh_jan3"
{
    CALL refresh_continuous_aggregate('cagg_1d', '2026-01-03', '2026-01-04');
}

# Session to handle locks.
# It can block L2 refreshes by locking cagg_1d's continuous_agg catalog row.
# Unlike waitpoints (which block all refreshes), locking L2's catalog row
# only blocks L2 refreshes while L1 refreshes can run freely.
session "LOCK"
setup
{
    SET timezone TO 'UTC';
}
step "lock_L2_source"
{
    BEGIN;
    DO $$
    BEGIN
        PERFORM 1 FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_name = 'cagg_1d'
        FOR UPDATE;
    END;
    $$;
}
step "lock_mat_invals"
{
    BEGIN;
    LOCK _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    IN ACCESS EXCLUSIVE MODE;
}
step "unlock"
{
    ROLLBACK;
}

# Check invalidation state, data correctness etc.
session "CHK"
setup
{
    SET timezone TO 'UTC';
}
step "chk_hyper_invals"
{
    SELECT COALESCE(ca.user_view_name, h.table_name) AS source,
           _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest,
           _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest
    FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log l
    JOIN _timescaledb_catalog.hypertable h ON h.id = l.hypertable_id
    LEFT JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = l.hypertable_id
    ORDER BY 1, 2, 3;
}
step "chk_mat_invals"
{
    SELECT ca.user_view_name AS cagg,
           _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest,
           _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest
    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log l
    JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = l.materialization_id
    ORDER BY 1, 2, 3;
}
step "chk_cagg_6h"
{
    SELECT bucket, avg_temp, max_temp, row_count
    FROM cagg_6h
    ORDER BY 1;
}
step "chk_cagg_1d"
{
    SELECT bucket, avg_temp, min_temp, max_temp, row_count
    FROM cagg_1d
    ORDER BY 1;
}
step "chk_6h_consistency"
{
    -- Verify L1 (6-hour) matches re-aggregation from raw hypertable
    SELECT c.bucket,
           c.avg_temp AS cagg_avg,
           r.reagg_avg AS raw_avg,
           c.row_count AS cagg_count,
           r.reagg_count AS raw_count,
           (c.avg_temp = r.reagg_avg) AS avg_match,
           (c.row_count = r.reagg_count) AS count_match
    FROM cagg_6h c
    JOIN (
        SELECT time_bucket('6 hours', time) AS bucket,
               avg(temp) AS reagg_avg,
               count(*) AS reagg_count
        FROM conditions GROUP BY 1
    ) r ON r.bucket = c.bucket
    ORDER BY 1;
}
step "chk_1d_consistency"
{
    -- Verify L2 (daily) matches re-aggregation from L1 (6-hour)
    SELECT d.bucket,
           d.avg_temp AS daily_avg,
           h.reagg_avg AS from_6h_avg,
           d.row_count AS daily_count,
           h.reagg_count AS from_6h_count,
           (d.avg_temp = h.reagg_avg) AS avg_match,
           (d.row_count = h.reagg_count) AS count_match
    FROM cagg_1d d
    JOIN (
        SELECT time_bucket('1 day', bucket) AS day,
               avg(avg_temp) AS reagg_avg,
               sum(row_count) AS reagg_count
        FROM cagg_6h GROUP BY 1
    ) h ON h.day = d.bucket
    ORDER BY 1;
}
step "insert_ht"
{
    INSERT INTO conditions (time, temp)
    SELECT ts, 50.0
    FROM generate_series('2026-01-01 00:00:00+00'::timestamptz,
                         '2026-01-04 23:45:00+00', '15 minutes') ts;
}

# Sequential refreshes. First refresh L1 CAgg, then L2.
permutation "chk_hyper_invals" "WP_before_enable" "L1_refresh_jan1" "WP_before_release" "chk_hyper_invals" "WP_before_enable" "L2_refresh_jan1"  "WP_before_release" "chk_cagg_6h" "chk_cagg_1d" "chk_6h_consistency" "chk_1d_consistency"

# Concurrent refreshes on L1 CAgg with overlapping ranges. One refresh should fail due to overlap.
# Verify that only the successful refresh's range is updated; remaining ranges stay invalidated.
# L2 refresh should succeed and be consistent.
permutation "WP_before_enable" "chk_hyper_invals" "L1_refresh_jan1" "insert_ht" "chk_mat_invals" "L1b_refresh_jan1_2" "chk_mat_invals" "WP_before_release" "chk_mat_invals" "L2_refresh_full" "chk_6h_consistency" "chk_1d_consistency"

# Same as above, reverse refresh order.
permutation "WP_before_enable" "chk_hyper_invals" "L1b_refresh_jan1_2" "insert_ht" "chk_mat_invals" "L1_refresh_jan1" "chk_mat_invals" "WP_before_release" "chk_mat_invals" "L2_refresh_full" "chk_6h_consistency" "chk_1d_consistency"

# Two concurrent refreshes on L2 CAgg with overlapping ranges. One should fail due to overlap.
# L1 is refreshed between the two L2 refreshes so that the second L2 refresh encounters an overlapping materialization range and fails.
permutation "L1_refresh_full" "chk_1d_consistency" "WP_before_enable" "chk_hyper_invals" "lock_L2_source" "L2_refresh_jan1" "insert_ht" "L1_refresh_full" "L2b_refresh_jan1_2" "WP_before_release" "unlock" "chk_cagg_1d" "chk_1d_consistency"

# Same as above, reverse refresh order.
permutation "L1_refresh_full" "chk_1d_consistency" "WP_before_enable" "chk_hyper_invals" "lock_L2_source" "L2b_refresh_jan1_2" "insert_ht" "L1_refresh_full" "L2_refresh_jan1" "WP_before_release" "unlock" "chk_cagg_1d" "chk_1d_consistency"

# Non-overlapping concurrent refreshes on L1. Both refreshes should succeed. Both are blocked before Txn 3.
# Refreshing Jan 3 adds cagg invalidations that are partially overlapping with the one created by refreshing Jan 1.
# Those invalidations are left behind, but both Jan 1 and Jan 3 are refreshed successfully.
permutation "WP_before_enable" "chk_hyper_invals" "L1_refresh_jan1" "insert_ht" "chk_mat_invals" "L1b_refresh_jan3" "chk_mat_invals" "WP_before_release" "chk_mat_invals" "L2_refresh_full" "chk_6h_consistency" "chk_1d_consistency"

# Non-overlapping concurrent refresh on L2. Both refreshes should succeed. Both are blocked before Txn 3.
# Refreshing Jan 3 adds cagg invalidations that are partially overlapping with the one created by refreshing Jan 1.
# Those invalidations are left behind, but both Jan 1 and Jan 3 are refreshed successfully.
permutation "L1_refresh_full" "chk_1d_consistency" "WP_before_enable" "chk_hyper_invals" "lock_L2_source" "L2_refresh_jan1" "insert_ht" "L1_refresh_full" "L2b_refresh_jan3" "WP_before_release" "unlock" "chk_cagg_1d" "chk_1d_consistency"

# L1 (txn3, deleting/inserting mat_inval entries) and L2 (txn2, processing mat_inval entries) should not block each other.
# Lock materialization invalidation table to make both refreshes wait before processing entries, then release simultaneously.
permutation "insert_ht" "WP_after_enable"  "chk_6h_consistency" "L1_refresh_jan1" "lock_mat_invals" "WP_after_release" "L2_refresh_full" "unlock" "chk_mat_invals" "chk_6h_consistency" "chk_1d_consistency"
