# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# ===========================================================================
# Interleavings that used to lose data: the end-of-refresh cleanup deleted
# tracking rows whose invalidations no refresh had materialized yet.  The
# garbage collector replaced that cleanup, so every permutation below must now
# leave the cagg agreeing with the hypertable.
# ===========================================================================

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
    SELECT create_hypertable('conditions', 'time');
    ALTER TABLE conditions SET (
        timescaledb.granular_refresh_column = 'sensor_id',
        timescaledb.granular_refresh_start_offset = '2 years',
        timescaledb.granular_refresh_end_offset = '1 day'
    );

    CREATE MATERIALIZED VIEW cond_daily
      WITH (timescaledb.continuous) AS
      SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
      FROM conditions
      GROUP BY bucket, sensor_id
      WITH NO DATA;
    ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);

    -- Render an internal int8 time readably, mapping the +/-infinity sentinels
    -- instead of erroring on them (the priming refresh leaves such residuals).
    CREATE OR REPLACE FUNCTION invlog_ts(bigint) RETURNS timestamptz
    LANGUAGE sql IMMUTABLE AS $$
      SELECT CASE
        WHEN $1 <= _timescaledb_functions.get_internal_time_min('timestamptz'::regtype)
          THEN '-infinity'::timestamptz
        WHEN $1 >= _timescaledb_functions.get_internal_time_max('timestamptz'::regtype)
          THEN 'infinity'::timestamptz
        ELSE _timescaledb_functions.to_timestamp($1) END
    $$;

    -- Both invalidation logs side by side.  Which log an entry sits in says
    -- whether a refresh's Txn1 has picked it up yet.
    CREATE OR REPLACE VIEW invlog_view AS
    SELECT 'hypertable' AS log_name,
           invlog_ts(lowest_modified_value) AS range_start,
           invlog_ts(greatest_modified_value) AS range_end,
           seqnum
    FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
    UNION ALL
    SELECT 'materialization',
           invlog_ts(lowest_modified_value),
           invlog_ts(greatest_modified_value),
           seqnum
    FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
}

teardown
{
    DROP VIEW IF EXISTS invlog_view;
    DROP FUNCTION IF EXISTS invlog_ts(bigint);
    DROP MATERIALIZED VIEW cond_daily;
    DROP TABLE conditions;
}

# Priming: move the invalidation threshold above the 2020-01..03 test data so
# later inserts are late-arriving.
session "P"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';
}
step "p_prime_insert" {
    INSERT INTO conditions VALUES ('2020-06-01 00:00+00', 'sensor_prime', 9);
}
step "p_prime_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01', options => jsonb_build_object('buckets_per_batch', 0));
}

session "S1"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';
}
step "s1_insert_a" {
    INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'sensor_a', 1);
}

# Waitpoint 1: After Txn1 (invalidation log moved and
# committed) and before Txn2 (tenant flush).
session "WP_after_txn1"
step "wp_after_txn1_enable"  {
    SELECT debug_waitpoint_enable('cagg_policy_batch_1_after_txn_1_wait');
}
step "wp_after_txn1_release" {
    SELECT debug_waitpoint_release('cagg_policy_batch_1_after_txn_1_wait');
}

# Waitpoint 2: after a refresh has flushed its tenants and materialized its
# window.  It sits inside the "invalidations != NULL" branch, so R2 -- whose
# February window holds no invalidations -- runs straight past it instead of
# parking too.
session "WP_before_tracking_cleanup"
step "wp_before_tracking_cleanup_enable"  {
    SELECT debug_waitpoint_enable('cagg_refresh_before_tenant_tracking_cleanup');
}
step "wp_before_tracking_cleanup_release" {
    SELECT debug_waitpoint_release('cagg_refresh_before_tenant_tracking_cleanup');
}

# The refresh the writers and R2 interleave with.
session "R"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';
}
step "r_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-11', options => jsonb_build_object('buckets_per_batch', 0));
}

# The writers whose trackings get written during the gap, while R is parked.
session "I"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';
}
step "i_insert_t2" {
    INSERT INTO conditions VALUES ('2020-01-05 00:00+00', 'sensor_t2', 2);
}
# makes sensor_t3's box for this generation start before
# sensor_t2's bucket.
step "i_insert_t3_early" {
    INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'sensor_t3', 5);
}
# one transaction touching two tenants either side of R's
# window end. This writes a single [Jan 09, Jan 14] invalidation
# while the tracker keeps 1 range per tenant.
step "i_insert_wide_txn" {
    INSERT INTO conditions VALUES ('2020-01-09 00:00+00', 'sensor_w1', 6);
    INSERT INTO conditions VALUES ('2020-01-14 00:00+00', 'sensor_w2', 7);
}
step "i_insert_t3_jan" {
    INSERT INTO conditions VALUES ('2020-01-08 00:00+00', 'sensor_t3', 3);
}
step "i_insert_t3_mar" {
    INSERT INTO conditions VALUES ('2020-03-01 00:00+00', 'sensor_t3', 4);
}

# R2 refresh a non-overlapping February window, whose Txn2 flush persists the new
# tracking rows while R is parked.
session "R2"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';
}
step "r2_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2020-02-01', '2020-02-11', options => jsonb_build_object('buckets_per_batch', 0));
}

# Observation, verification + recovery.
session "V"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';
}
step "v_tracking" {
    SELECT tenant_id,
           _timescaledb_functions.to_timestamp(min_timestamp) AS min_timestamp,
           _timescaledb_functions.to_timestamp(max_timestamp) AS max_timestamp,
           seqnum
    FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
    WHERE hypertable_id = (
        SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_name = 'cond_daily')
    ORDER BY tenant_id, min_timestamp;
}
# Entries still in the hypertable log have not been picked up by any refresh's
# Txn1 yet, so the refresh cannot materialize them -- even though its Txn2 flush
# has already written their tracking rows.
step "v_invalidations" {
    SELECT * FROM invlog_view ORDER BY log_name, range_start, range_end, seqnum;
}
# Materializes the invalidations left behind
step "v_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-04-01', options => jsonb_build_object('buckets_per_batch', 0));
}
step "v_check_cagg" {
    SELECT sensor_id, bucket, avg FROM cond_daily ORDER BY sensor_id, bucket;
}
# Ground truth from the hypertable, to compare against the cagg above.
step "v_check_truth" {
    SELECT sensor_id, time_bucket('1 day', time) AS bucket, avg(value)
    FROM conditions GROUP BY sensor_id, bucket ORDER BY sensor_id, bucket;
}
# A force refresh recomputes every bucket, bypassing the tenant filter, so it
# says what a non-granular refresh would have produced.
step "v_force" {
    CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-04-01', force => true, options => jsonb_build_object('buckets_per_batch', 0));
}
# ===========================================================================
# Every permutation forces a write to the tenant-tracking table by someone other
# than R to land (
# Shape 1: writes not yet visible to R's Txn1;
# Shape 2: a concurrent refresh's flush for an unrelated window/generation).

# Shape 1 -- Txn1-move / Txn2-flush gap.  R parks after Txn1; the writers commit
# into the generation R is about to flush, so R's flush writes their tracking
# rows while their invalidations are still sitting in the hypertable log,
# unmaterialized by R.
#
# Permutation 1a: each writer commits one row on its own, so every invalidation
# covers a single bucket.
#
# R parks twice: first after its Txn1, so the writers can commit into the gap,
# then again after it has materialized, so the tracking rows its own flush just
# wrote can be seen.

permutation "p_prime_insert" "p_prime_refresh" "s1_insert_a" "wp_after_txn1_enable" "wp_before_tracking_cleanup_enable" "r_refresh" "i_insert_t2" "i_insert_t3_jan" "i_insert_t3_mar" "v_invalidations" "wp_after_txn1_release" "v_tracking" "wp_before_tracking_cleanup_release" "v_tracking" "v_invalidations" "v_refresh" "v_check_cagg" "v_check_truth" "v_force" "v_check_cagg"

# Permutation 1b: While refresh R parks after its Txn1, i_insert_wide_txn commits
# one transaction touching two tenants (sensor_w1 on Jan 9, sensor_w2 on Jan 14)
# that straddle R's window boundary (Jan 11).
# The invalidation log records one wide range [Jan 09, Jan 14]
# under a single transaction, but the tracker still records two separate per-tenant
# boxes when it flushes (v_tracking shows sensor_w1 and sensor_w2 as distinct
# single-point rows). R then flushes these 2 tracking rows, while not seeing their
# associated invalidation (because R has gone pass its Txn1 which moves invalidations
# from hypertable invalidation log to materialization invalidation log.
# One of these "phantom" tenant trackings falls into the refresh
# window of R (sensor_w1), while the other one (sensor_w2) is outside.
# Previously, when the refresh R used to
# delete trackings within its refresh windows in Txn3, the in-window phantom tracking
# was deleted, but the outside one stayed. Later, when another refresh (v_refresh)
# refreshed the invalidation [[Jan 09, Jan 14]], it saw the outside tracking (sensor_w2)
# that is associated with that invalidation, so it still applied granular refresh but only
# refreshed sensor_w2, because sensor_w1 tracking had been deleted, causing missing data
# in the cagg.
# With garbage collection,refresh no longer delete trackings at the end of its Txn3,
# so both the tenant trackings
# rows are retained past refresh R until later cleaned up TOGETHER by GC, and the cagg's
# data after v_refresh is correct.

permutation "p_prime_insert" "p_prime_refresh" "s1_insert_a" "wp_after_txn1_enable" "r_refresh" "i_insert_wide_txn" "v_invalidations" "wp_after_txn1_release" "v_tracking" "v_invalidations" "v_refresh" "v_check_cagg" "v_check_truth" "v_force" "v_check_cagg"

# Shape 2 -- concurrent refresh's flush.  R parks after materializing January 1-11.
# While parked, the three writer inserts land (still just sensor_a/sensor_prime in tracking
# at that point). R2 then starts, hits its own wp_after_txn1 and parks: at this viewing
# checkpoint the writers' invalidations have already moved into the materialization log
# (seqnum 3) but no seqnum-3 tracking rows exist yet — R2 hasn't flushed. Releasing R2 lets
# it flush its tracker, and the next v_tracking shows sensor_t2/sensor_t3 appear purely
# from R2's Txn2, a generation R never observed. Finally releasing R's parked waitpoint
# lets R return — and since the tracking delete is gone, R's return does nothing to R2's
# freshly-written rows. Result: tracking for sensor_t2/sensor_t3 survives R's completion,
# the January and March invalidations are still queued, and the plain v_refresh at the end
# materializes them correctly — this is the direct reproduction of "R2's
# concurrent flush lands in the gap where R's old cleanup used to fire."

#
# Permutation 2a: sensor_t3's tracking range starts after sensor_t2's bucket.
#
# R2 parks after its own Txn1 as well, which separates the two halves of its
# work: at that point the writers' invalidations have moved to the mat log but
# no seqnum-3 tracking rows exist yet, so the next peek shows them appearing
# purely from R2's Txn2 flush.

permutation "p_prime_insert" "p_prime_refresh" "s1_insert_a" "wp_before_tracking_cleanup_enable" "r_refresh" "v_tracking" "i_insert_t2" "i_insert_t3_jan" "i_insert_t3_mar" "wp_after_txn1_enable" "r2_refresh" "v_tracking" "v_invalidations" "wp_after_txn1_release" "v_tracking" "wp_before_tracking_cleanup_release" "v_tracking" "v_invalidations" "v_refresh" "v_check_cagg" "v_check_truth" "v_force" "v_check_cagg"

# Permutation 2b: Same shape as 2a, but with an earlier sensor_t3 write that makes
# that tracking range span sensor_t2's invalidation range. Before the GC, R's cleanup
# deleted sensor_t2's row; sensor_t3's box is not contained in R's window,
# so it survives while still covering Jan 05. The bucket looked tracked, the refresh
# stayed granular, and sensor_t2 is dropped. After the GC fix, the trackings (both t2 and t3)
# do not get deleted by R, so granular refresh runs correctly and hence verification steps show
# no different in the cagg results compared with forced refresh and a query on the hypertable.

permutation "p_prime_insert" "p_prime_refresh" "s1_insert_a" "wp_before_tracking_cleanup_enable" "r_refresh" "i_insert_t3_early" "i_insert_t2" "i_insert_t3_jan" "i_insert_t3_mar" "wp_after_txn1_enable" "r2_refresh" "wp_after_txn1_release" "wp_before_tracking_cleanup_release" "v_tracking" "v_invalidations" "v_refresh" "v_check_cagg" "v_check_truth" "v_force" "v_check_cagg"
