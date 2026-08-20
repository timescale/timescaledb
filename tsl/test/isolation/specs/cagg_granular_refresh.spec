# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Concurrency tests for the per-tenant invalidation tracker (granular refresh).
#
# Two scenarios, one shared hypertable + cagg:
#
#   1. First-touch race -- two backends first-touch the SAME brand-new
#      hypertable at once.  The tracker is created in get_or_attach ->
#      dshash_find_or_insert (serialized on the dshash partition lock): exactly
#      one backend creates it, the other finds it, both commit, both tenants end
#      up in the one shared tracker.  Pinned with "tenant_tracker_drain_before_attach"
#      (fires just before get_or_attach in the commit drain).
#
#   2. Insert during a refresh's generation flip -- a refresh flushes the active
#      generation and flips active_gen, then parks at
#      "before_process_cagg_invalidations_for_refresh_lock" (after flush, before
#      commit).  A concurrent INSERT must follow the flip and write the NEW
#      generation, so its tenant is neither lost nor double-counted: the parked
#      refresh materializes the flushed tenant, and a later refresh drains the new
#      generation and materializes the concurrent tenant.
#
# Honesty caveat: scenario 2 exercises the active_gen flip and writer-follows-flip
# across two flushes at SQL-statement granularity.  It does NOT pin the flush's
# num_writers quiescence wait (that wait is a CPU busy-wait, invisible to
# isolationtester; see the note in ts_tenant_tracker_flush).  The lock-free
# barrier/CAS ordering is exercised only by the PERF_GRAN pgbench stress.
#
# Tenants are tracked only for late-arriving data (older than now - 1 day), so
# all data uses fixed 2020 timestamps.

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
    SELECT create_hypertable('conditions', 'time');
    ALTER TABLE conditions SET (
        timescaledb.granular_refresh_column = 'sensor_id',
        timescaledb.granular_refresh_start_offset = '36500 days',
        timescaledb.granular_refresh_end_offset = '1 day'
    );

    CREATE MATERIALIZED VIEW cond_daily
      WITH (timescaledb.continuous) AS
      SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
      FROM conditions
      GROUP BY bucket, sensor_id
      WITH NO DATA;
    ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);
}

teardown
{
    DROP MATERIALIZED VIEW cond_daily;
    DROP TABLE conditions;
}

# Waitpoints for the two scenarios.
session "WP"
step "wp_attach_enable"  { SELECT debug_waitpoint_enable('tenant_tracker_drain_before_attach'); }
step "wp_attach_release" { SELECT debug_waitpoint_release('tenant_tracker_drain_before_attach'); }
step "wp_flush_enable"   { SELECT debug_waitpoint_enable('before_process_cagg_invalidations_for_refresh_lock'); }
step "wp_flush_release"  { SELECT debug_waitpoint_release('before_process_cagg_invalidations_for_refresh_lock'); }

# Backend inserting sensor_a.  In scenario 1 this is one of the racing
# first-touchers; in scenario 2 it is the seed that creates the tracker.
session "S1"
step "s1_insert" { INSERT INTO conditions VALUES ('2020-01-01 00:00+00', 'sensor_a', 1); }

# Backend inserting sensor_b (the other first-toucher / the concurrent writer).
session "S2"
step "s2_insert" { INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'sensor_b', 2); }

# Refresh that flushes + flips, then parks (scenario 2).
session "R"
step "r_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01'); }

# Verification: a final granular refresh must materialize every tenant correctly.
session "V"
setup { SET timezone TO 'UTC'; }
step "v_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01'); }
step "v_check"
{
    SELECT sensor_id, bucket, avg
    FROM cond_daily
    ORDER BY sensor_id, bucket;
}

# Scenario 1: two backends first-touch the SAME brand-new hypertable at once.  
# The tracker is created in get_or_attach -> one session creates it
# and other should find it.
# both inserts wait due to the waitpoint and the refresh materializes both
# sensors.
permutation "wp_attach_enable" "s1_insert" "s2_insert" "wp_attach_release" "v_refresh" "v_check"

# Scenario 2 : insert during the generation flip: s1 inits the tracker; r_refresh
# flushes sensor_a and flips generation, then parks; s2 writes sensor_b into the new
# generation; r_refresh is released and materializes sensor_a; v_refresh drains the
# new generation and materializes sensor_b.
permutation "s1_insert" "wp_flush_enable" "r_refresh" "s2_insert" "wp_flush_release" "v_refresh" "v_check"
