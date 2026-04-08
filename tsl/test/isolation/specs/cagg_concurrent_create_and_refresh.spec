# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# Test concurrent CAgg creation WITH DATA and manual refresh.
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
}

teardown {
    DROP TABLE conditions CASCADE;
}

# Waitpoint that fires BEFORE the first SPI_commit_and_chain in
# continuous_agg_refresh_internal. At this point during CREATE WITH
# DATA, the CAgg catalog entries are NOT yet committed/visible to
# other sessions.
session "WP_pre"
step "WP_pre_enable"
{
    SELECT debug_waitpoint_enable('cagg_refresh_before_first_txn_commit');
}
step "WP_pre_release"
{
    SELECT debug_waitpoint_release('cagg_refresh_before_first_txn_commit');
}

# Waitpoint that fires AFTER the first SPI_commit_and_chain (during
# the invalidation threshold update in Txn 2). At this point the CAgg
# catalog entries and refresh window registration are committed, so
# the CAgg is visible and the overlap check will detect the
# in-progress refresh.
session "WP_post"
step "WP_post_enable"
{
    SELECT debug_waitpoint_enable('invalidation_threshold_scan_update_enter');
}
step "WP_post_release"
{
    SELECT debug_waitpoint_release('invalidation_threshold_scan_update_enter');
}

# Session that creates the CAgg WITH DATA (triggers internal refresh)
session "C1"
step "C1_create_cagg"
{
    CREATE MATERIALIZED VIEW cond_10
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket(10, time) AS bucket, avg(temp) AS avg_temp
      FROM conditions
      GROUP BY 1;
}

# Session that attempts a concurrent manual refresh
session "R1"
step "R1_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 0, 100);
}

# Refresh before CAgg is visible in the catalog tables
#
# The CREATE WITH DATA blocks before the transaction that registers ranges has committed.
# The CAgg catalog entries have not been committed yet, so the concurrent refresh should fail because the relation does not exist.
#
permutation "WP_pre_enable" "C1_create_cagg"("WP_pre_enable") "R1_refresh" "WP_pre_release"

# Refresh after CAgg is visible, fails due to overlapping range.
#
# The refresh initiated by CREATE WITH DATA registers the ranges and commits. This also makes the CAgg visible in the catalog tables.
# Hence a concurrent refresh will find the CAgg but fails because it overlaps with a registered range.
#
permutation "WP_post_enable" "C1_create_cagg"("WP_post_enable") "R1_refresh" "WP_post_release"
