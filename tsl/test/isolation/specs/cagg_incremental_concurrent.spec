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

    SET timescaledb.current_timestamp_mock TO '2026-04-01 0:30:00+00';

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

    CREATE TABLE sensor_data (time TIMESTAMPTZ NOT NULL, value FLOAT);
    SELECT create_hypertable('sensor_data', 'time', chunk_time_interval => INTERVAL '1 day');

    -- 10h of data anchored to mock time
    INSERT INTO sensor_data
    SELECT t, random()
    FROM generate_series(
        '2026-04-01 00:30:00+00'::timestamptz - INTERVAL '10 hours',
        '2026-04-01 00:30:00+00'::timestamptz,
        INTERVAL '1 minute'
    ) t;

    -- Hourly CAgg
    CREATE MATERIALIZED VIEW sensor_hourly
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 hour', time) AS bucket, avg(value), count(*)
    FROM sensor_data
    GROUP BY 1
    WITH NO DATA;

    -- Another cagg so we can test the "refresh oldest first" incremental refresh policy
    CREATE MATERIALIZED VIEW sensor_hourly_oldest_first
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('1 hour', time) AS bucket, avg(value), count(*)
    FROM sensor_data
    GROUP BY 1
    WITH NO DATA;
}

# Initial refresh to set up the invalidation threshold
setup
{
    CALL refresh_continuous_aggregate('cond_10', 1, 60);
}

setup
{
    CALL refresh_continuous_aggregate('sensor_hourly', NULL, NULL);
}

setup
{
    CALL refresh_continuous_aggregate('sensor_hourly_oldest_first', NULL, NULL);
}

# Add an incremental policy (1 bucket per batch) and generate invalidations
# Add 2 adjacent policies for sensor_hourly
# Add 2 adjacent policies for sensor_hourly_oldest_first
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


    -- Policy A: the "last" policy (most-recent end_offset).
    SELECT add_continuous_aggregate_policy('sensor_hourly',
        start_offset      => INTERVAL '3 hours',
        end_offset        => INTERVAL '5 minutes',
        schedule_interval => INTERVAL '15 minutes',
        buckets_per_batch => 1
    );

    -- Policy B: Because policy A has a more-recent end_offset,
    -- check_if_last_policy() returns false for B, so extend_last_bucket=true is
    -- set and applied to the boundary batch
    SELECT add_continuous_aggregate_policy('sensor_hourly',
        start_offset      => INTERVAL '12 hours',
        end_offset        => INTERVAL '3 hours',
        schedule_interval => INTERVAL '1 hour',
        buckets_per_batch => 1
    );

    --Same pair of policies as above, but with refresh_newest_first = false for policy B
    -- Policy A: the "last" policy (most-recent end_offset).
    SELECT add_continuous_aggregate_policy('sensor_hourly_oldest_first',
        start_offset      => INTERVAL '3 hours',
        end_offset        => INTERVAL '5 minutes',
        schedule_interval => INTERVAL '15 minutes',
        buckets_per_batch => 1
    );

    -- Policy B: Because policy A has a more-recent end_offset,
    -- check_if_last_policy() returns false for B, so extend_last_bucket=true is
    -- set and applied to the boundary batch
    SELECT add_continuous_aggregate_policy('sensor_hourly_oldest_first',
        start_offset      => INTERVAL '12 hours',
        end_offset        => INTERVAL '3 hours',
        schedule_interval => INTERVAL '1 hour',
        buckets_per_batch => 1,
        refresh_newest_first  => false   -- oldest first
    );


    -- Generate invalidations anchored to mock time
    INSERT INTO sensor_data
    SELECT t, random()
    FROM generate_series(
        '2026-04-01 00:30:00+00'::timestamptz - INTERVAL '10 hours',
        '2026-04-01 00:30:00+00'::timestamptz,
        INTERVAL '1 minute'
    ) t;
}

teardown
{
    DROP TABLE conditions CASCADE;
    DROP TABLE sensor_data CASCADE;
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

# Session to run refresh policy for sensor_hourly,
# Will be paused after first batch is done
session "R3"
setup
{
    SET SESSION lock_timeout = '500ms';
    SET SESSION deadlock_timeout = '500ms';
    SET SESSION client_min_messages = 'LOG';
    SET timescaledb.current_timestamp_mock TO '2026-04-01 00:30:00+00';
}
step "r3_run"
{
    DO $$
    DECLARE
        jid int;
    BEGIN
        SELECT job_id INTO jid FROM timescaledb_information.jobs
        WHERE hypertable_name = 'sensor_hourly'
        AND proc_name = 'policy_refresh_continuous_aggregate'
        ORDER BY job_id DESC
        LIMIT 1; --get the job with highest id (i.e, policy B)
        CALL run_job(jid);
    END;
    $$;
}

step "r3_run_oldest_first"
{
    DO $$
    DECLARE
        jid int;
    BEGIN
        SELECT job_id INTO jid FROM timescaledb_information.jobs
        WHERE hypertable_name = 'sensor_hourly_oldest_first'
        AND proc_name = 'policy_refresh_continuous_aggregate'
        ORDER BY job_id DESC
        LIMIT 1; --get the job with highest id (i.e, policy B)
        CALL run_job(jid);
    END;
    $$;
}

#insert data to create invalidations when refresh is stopped after batch 1
session "I2"
step "i2_insert"
{
    INSERT INTO sensor_data
    SELECT t, random()
    FROM generate_series(
        '2026-04-01 00:30:00+00'::timestamptz - INTERVAL '10 hours',
        '2026-04-01 00:30:00+00'::timestamptz,
        INTERVAL '10 minutes'
    ) t;
}


# Test 1: Run 1 pauses after batch 1. Manual refresh on overlapping range [1,50).
# After both complete, check for duplicate rows.
permutation "wp_enable" "r1_run"("wp_enable") "s1_refresh_ranges" "s1_count" "r2_refresh" "wp_release" "s1_count" "s1_check_duplicates"

# Test 2: Insert data while run 1 is paused, then manual refresh on overlapping range.
permutation "wp_enable" "r1_run"("wp_enable") "i1_insert" "s1_refresh_ranges" "r2_refresh" "s1_refresh_ranges" "s1_count" "wp_release" "s1_count" "s1_check_duplicates"

# Test 3: Manual refresh that overlaps with R1's registered batch range [50, 60).
# Should be blocked.
permutation "wp_enable" "r1_run"("wp_enable") "s1_refresh_ranges" "r2_refresh_overlap" "wp_release" "s1_count" "s1_check_duplicates"

# Test 4: Refresh on sensor_hourly pauses after batch 2 window has been determined . Then some new data arrive on batch 2 window
# refresh on batch 3 should be only 1 bucket and not overlap with batch 1
permutation "wp_enable" "r3_run"("wp_enable") "i2_insert" "wp_release"

# Test 5: Same as test 5, but the policy refreshes oldest batch first
permutation "wp_enable" "r3_run_oldest_first"("wp_enable") "i2_insert" "wp_release"