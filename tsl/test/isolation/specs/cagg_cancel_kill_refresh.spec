# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# Test that registered refresh ranges in continuous_aggs_jobs_refresh_ranges
# are properly cleaned up when a CAgg refresh is cancelled or killed
# at each point in the refresh.
#
#
# NOTE: pg_terminate_backend kills the session connection, which cannot be reconnected between permutations.
# Hence each test using terminate has to use a new session to prevent connection errors
#

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time int NOT NULL, temp float);
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

    -- Table to track PIDs for cancellation/termination
    CREATE TABLE cancelpid (
        pid INTEGER NOT NULL PRIMARY KEY
    );

    -- Signal cancel to registered backends. The isolation tester's
    -- (blocked_step) annotation synchronizes step completion, so no polling
    -- on pg_stat_activity is required here.
    CREATE OR REPLACE PROCEDURE cancelpids() AS
    $$
    BEGIN
        PERFORM pg_cancel_backend(pid) FROM cancelpid;
        DELETE FROM cancelpid;
    END;
    $$ LANGUAGE plpgsql;

    -- Signal terminate to registered backends. Same rationale as cancelpids().
    CREATE OR REPLACE PROCEDURE terminatepids() AS
    $$
    BEGIN
        PERFORM pg_terminate_backend(pid) FROM cancelpid;
        DELETE FROM cancelpid;
    END;
    $$ LANGUAGE plpgsql;
}

# Initial refresh to set the invalidation threshold
setup
{
    CALL refresh_continuous_aggregate('cond_10', 0, 50);
}

# Generate invalidations
setup
{
    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(1, 50, 1) t;
}

teardown
{
    DROP TABLE conditions CASCADE;
    DROP TABLE cancelpid;
}

# Waitpoint after txn 0 (registration committed, before invalidation threshold)
session "WP0"
step "wp0_enable"
{
    SELECT debug_waitpoint_enable('cagg_refresh_after_register');
}
step "wp0_release"
{
    SELECT debug_waitpoint_release('cagg_refresh_after_register');
}

# Waitpoint after txn 1 (hypertable invalidations processed)
session "WP1"
step "wp1_enable"
{
    SELECT debug_waitpoint_enable('cagg_policy_batch_0_after_txn_1_wait');
}
step "wp1_release"
{
    SELECT debug_waitpoint_release('cagg_policy_batch_0_after_txn_1_wait');
}

# Waitpoint after txn 2 (cagg invalidations processed)
session "WP2"
step "wp2_enable"
{
    SELECT debug_waitpoint_enable('after_process_cagg_invalidations_for_refresh_lock');
}
step "wp2_release"
{
    SELECT debug_waitpoint_release('after_process_cagg_invalidations_for_refresh_lock');
}

# Waitpoint after txn 3 (materialization done, before cleanup)
session "WP3"
step "wp3_enable"
{
    SELECT debug_waitpoint_enable('after_process_cagg_materializations');
}
step "wp3_release"
{
    SELECT debug_waitpoint_release('after_process_cagg_materializations');
}

# Session for cancel tests
session "R1"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "r1_register_pid"
{
    INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING;
}
step "r1_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 0, 50);
}

# Sessions for terminate tests.
# Each test needs a separate session since they can't be reused in permutations
session "TR1"
step "tr1_register_pid"
{
    INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING;
}
step "tr1_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 0, 50);
}

session "TR2"
step "tr2_register_pid"
{
    INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING;
}
step "tr2_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 0, 50);
}

session "TR3"
step "tr3_register_pid"
{
    INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING;
}
step "tr3_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 0, 50);
}

session "TR4"
step "tr4_register_pid"
{
    INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING;
}
step "tr4_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 0, 50);
}

session "TR5"
step "tr5_register_pid"
{
    INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING;
}
step "tr5_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 0, 50);
}

# Session for cancelling via pg_cancel_backend
session "K1"
step "k1_cancel"
{
    CALL cancelpids();
}

# Session for killing via pg_terminate_backend
session "T1"
step "t1_terminate"
{
    CALL terminatepids();
}

# Session to view registered ranges
session "S1"
step "s1_registered_ranges"
{
    SELECT ca.user_view_name AS cagg_name, r.start_range, r.end_range
    FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges r
    JOIN _timescaledb_catalog.continuous_agg ca ON r.materialization_id = ca.mat_hypertable_id
    ORDER BY ca.user_view_name, r.start_range;
}

# Session for follow-up refresh
session "R2"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
}
step "r2_refresh"
{
    CALL refresh_continuous_aggregate('cond_10', 0, 50);
}
step "r2_refresh_nonoverlap"
{
    CALL refresh_continuous_aggregate('cond_10', 50, 100);
}

#
# pg_cancel_backend tests
# Registered ranges should be cleared due to PG_CATCH blocks causing cleanup on exit
#

# Cancel after registration
permutation "wp0_enable" "r1_register_pid" "r1_refresh"("wp0_enable") "s1_registered_ranges" "k1_cancel"("r1_refresh") "wp0_release" "s1_registered_ranges"

# Cancel after txn 1
permutation "wp1_enable" "r1_register_pid" "r1_refresh"("wp1_enable") "s1_registered_ranges" "k1_cancel"("r1_refresh") "wp1_release" "s1_registered_ranges"

# Cancel after txn 2
permutation "wp2_enable" "r1_register_pid" "r1_refresh"("wp2_enable") "s1_registered_ranges" "k1_cancel"("r1_refresh") "wp2_release" "s1_registered_ranges"

# Cancel after txn 3
permutation "wp3_enable" "r1_register_pid" "r1_refresh"("wp3_enable") "s1_registered_ranges" "k1_cancel"("r1_refresh") "wp3_release" "s1_registered_ranges"

#
# pg_terminate_backend — ranges are NOT cleared since the PID is immediately killed. A follow-up refresh should detect and clean up orphaned entries.
#

# Terminate after registration (txn 0). Follow-up refresh cleans registered ranges
permutation "wp0_enable" "tr1_register_pid" "tr1_refresh"("wp0_enable") "s1_registered_ranges" "t1_terminate"("tr1_refresh") "wp0_release" "s1_registered_ranges" "r2_refresh" "s1_registered_ranges"

# Terminate after txn 1. Follow-up refresh cleans registered ranges
permutation "wp1_enable" "tr2_register_pid" "tr2_refresh"("wp1_enable") "s1_registered_ranges" "t1_terminate"("tr2_refresh") "wp1_release" "s1_registered_ranges" "r2_refresh" "s1_registered_ranges"

# Terminate after txn 2. Follow-up refresh cleans registered ranges
permutation "wp2_enable" "tr3_register_pid" "tr3_refresh"("wp2_enable") "s1_registered_ranges" "t1_terminate"("tr3_refresh") "wp2_release" "s1_registered_ranges" "r2_refresh" "s1_registered_ranges"

# Terminate after txn 3. Follow-up refresh cleans registered ranges
permutation "wp3_enable" "tr4_register_pid" "tr4_refresh"("wp3_enable") "s1_registered_ranges" "t1_terminate"("tr4_refresh") "wp3_release" "s1_registered_ranges" "r2_refresh" "s1_registered_ranges"

# A non-overlapping follow-up refresh should also clean up orphaned registered ranges
#
permutation "wp2_enable" "tr5_register_pid" "tr5_refresh"("wp2_enable") "s1_registered_ranges" "t1_terminate"("tr5_refresh") "wp2_release" "s1_registered_ranges" "r2_refresh_nonoverlap" "s1_registered_ranges"
