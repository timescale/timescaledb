# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.
#
# Test concurrent refresh while an incremental policy is mid-batch.
#
# An incremental policy refreshes 1 bucket at a time. After batch 1
# completes, a second run_job attempts to refresh the same cagg.
# We verify whether the second run is blocked or proceeds.
#

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time int, device_id int, temperature float);
    SELECT create_hypertable('conditions', 'time', chunk_time_interval => 10);

    CREATE OR REPLACE FUNCTION cond_now()
    RETURNS int LANGUAGE SQL STABLE AS
    $$
        SELECT coalesce(max(time), 0)
        FROM conditions
    $$;

    SELECT set_integer_now_func('conditions', 'cond_now');

    -- Insert data spanning time 1..60
    INSERT INTO conditions
    SELECT t, d, 10
    FROM generate_series(1, 60, 1) AS t,
         generate_series(1, 3) AS d;

    -- CAgg with bucket width 10
    CREATE MATERIALIZED VIEW cond_10
    WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
    SELECT
        time_bucket(10, time) AS bucket,
        device_id,
        count(*),
        avg(temperature)
    FROM conditions
    GROUP BY 1, 2
    WITH NO DATA;
}

# Initial refresh to set up the invalidation threshold
setup
{
    CALL refresh_continuous_aggregate('cond_10', 1, 60);
}

# Add an incremental policy (1 bucket per batch) and generate invalidations
setup
{
    SELECT add_continuous_aggregate_policy(
        'cond_10',
        start_offset => NULL,
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h',
        buckets_per_batch => 1
    );

    -- Generate invalidations
    INSERT INTO conditions
    SELECT t, d, 20
    FROM generate_series(1, 60, 1) AS t,
         generate_series(1, 3) AS d;

}

teardown
{
    DROP TABLE conditions CASCADE;
}

# Session to control the waitpoint — pauses after batch 1's txn 1 commits
session "WP"
step "wp_enable"
{
    SELECT debug_waitpoint_enable('cagg_policy_batch_2_after_txn_1_wait');
}
step "wp_release"
{
    SELECT debug_waitpoint_release('cagg_policy_batch_2_after_txn_1_wait');
}

# Session for run 1 — runs the policy job which refreshes 1 bucket at a time.
# Will pause after batch 1 at the waitpoint.
session "R1"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
    SET SESSION client_min_messages = 'LOG';
}
step "r1_run"
{
    DO $$
    DECLARE
        jid int;
    BEGIN
        SELECT job_id INTO jid FROM timescaledb_information.jobs
        WHERE hypertable_name = 'cond_10'
        AND proc_name = 'policy_refresh_continuous_aggregate';
        CALL run_job(jid);
    END;
    $$;
}

# Session for run 2 — runs the same policy job again
session "R2"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
    SET SESSION client_min_messages = 'LOG';
}
step "r2_refresh"
{
    -- Non-overlapping with R1's registered batch range [50, 60)
    CALL refresh_continuous_aggregate('cond_10', 1, 50);
}
step "r2_refresh_overlap"
{
    -- Overlapping with R1's registered batch range [50, 60)
    CALL refresh_continuous_aggregate('cond_10', 40, 60);
}

# Session to insert data while run 1 is paused
session "I1"
step "i1_insert"
{
    INSERT INTO conditions
    SELECT t, d, 30
    FROM generate_series(61, 90, 1) AS t,
         generate_series(1, 3) AS d;
}

# Session to check state
session "S1"
step "s1_count"
{
    SELECT count(*) AS row_count FROM cond_10;
}
step "s1_check_duplicates"
{
    -- Check for duplicate (bucket, device_id) rows in the materialization hypertable.
    -- If duplicates exist, the same range was materialized twice without proper cleanup.
    SELECT bucket, device_id, count(*) AS copies
    FROM cond_10
    GROUP BY bucket, device_id
    HAVING count(*) > 1
    ORDER BY bucket, device_id;
}
step "s1_refresh_ranges"
{
    SELECT ca.user_view_name AS cagg_name, r.start_range, r.end_range
    FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges r
    JOIN _timescaledb_catalog.continuous_agg ca ON r.materialization_id = ca.mat_hypertable_id
    ORDER BY ca.user_view_name;
}

# Test 1: Run 1 pauses after batch 1. Manual refresh on overlapping range [1,50).
# After both complete, check for duplicate rows.
permutation "wp_enable" "r1_run"("wp_enable") "s1_refresh_ranges" "s1_count" "r2_refresh" "wp_release" "s1_count" "s1_check_duplicates"

# Test 2: Insert data while run 1 is paused, then manual refresh on overlapping range.
permutation "wp_enable" "r1_run"("wp_enable") "i1_insert" "s1_refresh_ranges" "r2_refresh" "s1_refresh_ranges" "s1_count" "wp_release" "s1_count" "s1_check_duplicates"

# Test 3: Manual refresh that overlaps with R1's registered batch range [50, 60).
# Should be blocked.
permutation "wp_enable" "r1_run"("wp_enable") "s1_refresh_ranges" "r2_refresh_overlap" "wp_release" "s1_count" "s1_check_duplicates"
