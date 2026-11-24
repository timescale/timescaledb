# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    SELECT _timescaledb_functions.stop_background_workers();
    SET timezone TO 'UTC';
    SET timescaledb.current_timestamp_mock TO '2025-02-01 00:00:00+00';
    CREATE TABLE batch_readings(time timestamptz NOT NULL, tenant text, value integer);
    SELECT create_hypertable('batch_readings', 'time', chunk_time_interval => INTERVAL '7 days');
    ALTER TABLE batch_readings SET (
        timescaledb.cagg_enable_granular_refresh = true,
        timescaledb.cagg_granular_refresh_column = 'tenant',
        timescaledb.cagg_granular_refresh_start_offset = '30 days',
        timescaledb.cagg_granular_refresh_end_offset = '1 day'
    );
    INSERT INTO batch_readings VALUES ('2025-01-19 12:00:00+00', 'seed', 10);
    CREATE MATERIALIZED VIEW batch_daily
    WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
    SELECT time_bucket('1 day', time) AS bucket, tenant, sum(value) AS total
    FROM batch_readings GROUP BY 1, 2 WITH NO DATA;
    ALTER MATERIALIZED VIEW batch_daily SET (timescaledb.enable_granular_refresh = true);

    CREATE VIEW batch_ids AS
    SELECT raw_hypertable_id AS raw_id, mat_hypertable_id AS mat_id
    FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'batch_daily';
    CREATE VIEW batch_threshold AS
    SELECT _timescaledb_functions.to_timestamp(watermark) AT TIME ZONE 'UTC' AS threshold
    FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
    WHERE hypertable_id = (SELECT raw_id FROM batch_ids);
}

teardown
{
    DROP VIEW batch_threshold;
    DROP VIEW batch_ids;
    DROP MATERIALIZED VIEW batch_daily;
    DROP TABLE batch_readings;
}

session "W"
setup
{
    SET timezone TO 'UTC';
    SET timescaledb.current_timestamp_mock TO '2025-02-01 00:00:00+00';
}
step "Wprime_low_refresh"
{
    CALL refresh_continuous_aggregate('batch_daily', '2025-01-19 00:00:00+00',
        '2025-01-20 00:00:00+00', options => jsonb_build_object('buckets_per_batch', 0));
}
step "Wprime_high_refresh"
{
    CALL refresh_continuous_aggregate('batch_daily', '2025-01-19 00:00:00+00',
        '2025-01-25 00:00:00+00', options => jsonb_build_object('buckets_per_batch', 0));
}
step "Winsert_old" { INSERT INTO batch_readings VALUES ('2025-01-20 12:00:00+00', 'old_control', 1); }
step "Winsert_new" { INSERT INTO batch_readings VALUES ('2025-01-23 12:00:00+00', 'new_control', 2); }
step "Winsert_before_flush" { INSERT INTO batch_readings VALUES ('2025-01-21 04:00:00+00', 'before_flush', 3); }
step "Winsert_after_flush" { INSERT INTO batch_readings VALUES ('2025-01-21 12:00:00+00', 'after_flush', 4); }
step "Winsert_ahead_of_threshold" { INSERT INTO batch_readings VALUES ('2025-01-22 12:00:00+00', 'ahead_of_threshold', 5); }

session "WP"
step "WPpause_first" { SELECT debug_waitpoint_enable('cagg_policy_batch_1_after_txn_1_wait'); }
step "WPrelease_first" { SELECT debug_waitpoint_release('cagg_policy_batch_1_after_txn_1_wait'); }
step "WPpause_flushed" { SELECT debug_waitpoint_enable('after_process_cagg_invalidations_for_refresh_lock'); }
step "WPrelease_flushed" { SELECT debug_waitpoint_release('after_process_cagg_invalidations_for_refresh_lock'); }
step "WPpause_second" { SELECT debug_waitpoint_enable('cagg_policy_batch_2_after_txn_1_wait'); }
step "WPrelease_second" { SELECT debug_waitpoint_release('cagg_policy_batch_2_after_txn_1_wait'); }

session "R"
setup
{
    SET timezone TO 'UTC';
    SET timescaledb.current_timestamp_mock TO '2025-02-01 00:00:00+00';
    SET timescaledb.cagg_refresh_stats_level TO summary;
}
step "Rrefresh_newest"
{
    CALL refresh_continuous_aggregate('batch_daily', '2025-01-20 00:00:00+00',
        '2025-01-24 00:00:00+00',
        options => jsonb_build_object('buckets_per_batch', 2, 'refresh_newest_first', true));
}
step "Rrefresh_oldest"
{
    CALL refresh_continuous_aggregate('batch_daily', '2025-01-20 00:00:00+00',
        '2025-01-24 00:00:00+00',
        options => jsonb_build_object('buckets_per_batch', 2, 'refresh_newest_first', false));
}

session "V"
setup
{
    SET timezone TO 'UTC';
    SET timescaledb.current_timestamp_mock TO '2025-02-01 00:00:00+00';
}
step "Vpending_invalidations"
{
    SELECT 'hypertable invalidation log' AS checking,
           _timescaledb_functions.to_timestamp(lowest_modified_value) AT TIME ZONE 'UTC' AS start,
           seqnum
    FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log h
    WHERE hypertable_id = (SELECT raw_id FROM batch_ids)
    ORDER BY lowest_modified_value;

    SELECT 'tenant tracking catalog' AS checking, tenant_id, seqnum
    FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
    WHERE hypertable_id = (SELECT raw_id FROM batch_ids)
      AND tenant_id IN ('before_flush', 'after_flush')
    ORDER BY tenant_id, seqnum;

    SELECT 'cagg log entries with an unflushed seqnum' AS checking,
           count(*) AS unflushed_cagg_invalidations
    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log c
    WHERE materialization_id = (SELECT mat_id FROM batch_ids) AND seqnum > 0
      AND NOT EXISTS (SELECT FROM _timescaledb_catalog.continuous_aggs_tenant_tracking t
                      WHERE t.hypertable_id = (SELECT raw_id FROM batch_ids) AND t.seqnum = c.seqnum);

    SELECT 'materialized rows' AS checking, tenant, total
    FROM batch_daily
    WHERE bucket >= '2025-01-20 00:00:00+00' AND bucket < '2025-01-24 00:00:00+00'
    ORDER BY tenant;
}
step "Vcheck_complete"
{
    SELECT bucket AT TIME ZONE 'UTC' AS bucket, tenant, total FROM batch_daily
    WHERE bucket >= '2025-01-20 00:00:00+00' AND bucket < '2025-01-24 00:00:00+00'
    ORDER BY bucket, tenant;
}
step "Vthreshold" { SELECT * FROM batch_threshold; }

# Test 1: A batched refresh moves hypertable invalidations and flushes the
# tenant tracker only once, at the start. Writes made after that, before or
# after the flush, are not processed by the refresh.
# The priming refresh sets the threshold to Jan 25.
# The newest-first refresh runs two batches:
# [Jan 22, Jan 24) with new_control, then [Jan 20, Jan 22) with old_control.
# Batch 1 flushes the tracker, so the Jan 21 writes made during batch 1 are not
# seen by this refresh: before_flush (between the log move
# and the flush) and after_flush (after the flush).
# Vpending_invalidations must show both left in the hypertable log,
# before_flush insert tracker is in the tracking table and after_flush insert is in memory
permutation "Wprime_high_refresh" "Winsert_old" "Winsert_new" "WPpause_first" "WPpause_flushed" "Rrefresh_newest" "Winsert_before_flush" "WPrelease_first" "Winsert_after_flush" "WPrelease_flushed" "Vpending_invalidations"

# Test 2: With oldest-first batching, each batch raises the invalidation
# threshold only to its own end. A row inserted into the second batch's range
# while the first batch runs is materialized by the second batch.
# The priming refresh sets the threshold to Jan 20.
# The oldest-first refresh runs two batches:
# [Jan 20, Jan 22) with old_control, then [Jan 22, Jan 24) with new_control.
# Batch 1 raises the threshold to Jan 22, so ahead_of_threshold (Jan 22),
# inserted while batch 1 runs, is not below the threshold and is not logged.
# Batch 2 raises the threshold to Jan 24 and materializes ahead_of_threshold.
# Vthreshold must show Jan 20, Jan 22, then Jan 24; Vcheck_complete must show
# all three rows.
permutation "Wprime_low_refresh" "Winsert_old" "Winsert_new" "Vthreshold" "WPpause_first" "WPpause_second" "Rrefresh_oldest" "Vthreshold" "Winsert_ahead_of_threshold" "WPrelease_first" "Vthreshold" "WPrelease_second" "Vcheck_complete"
