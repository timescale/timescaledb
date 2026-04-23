# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions (
        time TIMESTAMPTZ NOT NULL,
        value FLOAT);
    SELECT create_hypertable('conditions', 'time', chunk_time_interval => INTERVAL '1 week');

    INSERT INTO conditions
    SELECT ts, extract(epoch from ts)::int % 10
    FROM generate_series('2026-01-01'::timestamptz, '2026-04-10'::timestamptz, INTERVAL '1 day') ts;

    -- Table used to pass R1's backend PID to the terminator session
    CREATE TABLE cancelpid (pid int);

    CREATE MATERIALIZED VIEW cond_daily
    WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
    SELECT time_bucket('1 day', time) AS bucket, avg(value) AS avg_val
    FROM conditions
    GROUP BY 1
    WITH NO DATA;

    CREATE USER cagg_user;
    GRANT ALL ON conditions TO cagg_user;
    GRANT ALL ON cond_daily TO cagg_user;
    GRANT ALL ON cancelpid TO cagg_user;
    ALTER MATERIALIZED VIEW cond_daily OWNER TO cagg_user;
}

setup
{
    CALL refresh_continuous_aggregate('cond_daily', '2026-01-01', '2026-04-01');
}

setup
{
    BEGIN; INSERT INTO conditions VALUES ('2026-01-10', 111), ('2026-01-20', 111), ('2026-01-30', 111); COMMIT;
    BEGIN; INSERT INTO conditions VALUES ('2026-02-10', 222), ('2026-02-20', 222); COMMIT;
    BEGIN; INSERT INTO conditions VALUES ('2026-03-01', 333), ('2026-03-10', 333), ('2026-03-15', 333); COMMIT;
    BEGIN; INSERT INTO conditions VALUES ('2026-03-20', 444), ('2026-03-25', 444), ('2026-03-30', 444); COMMIT;
}

teardown {
    DROP TABLE conditions CASCADE;
    DROP TABLE IF EXISTS cancelpid;
    DROP USER cagg_user;
}

# Session R1: stores its PID then runs a refresh that will left PID behind.
session "R1"
setup {
    SET ROLE cagg_user;
    SET SESSION lock_timeout = '2s';
    INSERT INTO cancelpid SELECT pg_backend_pid();
}
step "R1_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2026-01-05', '2026-03-15');
}

# Session WP: enables / disables waitpoints
session "WP"
step "WP_before_txn2_commit_enable"  { SELECT debug_waitpoint_enable('before_process_cagg_invalidations_for_refresh_lock'); }
step "WP_before_txn2_commit_disable"  { SELECT debug_waitpoint_release('before_process_cagg_invalidations_for_refresh_lock'); }
step "WP_mat_enable"  { SELECT debug_waitpoint_enable('after_process_cagg_materializations'); }
step "WP_mat_disable"  { SELECT debug_waitpoint_release('after_process_cagg_materializations'); }
step "WP_before_txn3_start_enable"  { SELECT debug_waitpoint_enable('after_process_cagg_invalidations_for_refresh_lock'); }
step "WP_before_txn3_start_disable"  { SELECT debug_waitpoint_release('after_process_cagg_invalidations_for_refresh_lock'); }
step "WP_before_txn2_start_enable"  { SELECT debug_waitpoint_enable('cagg_refresh_after_register'); }
step "WP_before_txn2_start_disable"  { SELECT debug_waitpoint_release('cagg_refresh_after_register'); }
step "WP_after_register_enable"  { SELECT debug_waitpoint_enable('cagg_refresh_after_register'); }
step "WP_after_register_disable"  { SELECT debug_waitpoint_release('cagg_refresh_after_register'); }

# Session K1: terminate R1's backend so its PID becomes dead in the
# registration table, then wait until the process is gone.
session "K1"
step "K1_terminate" {
    DO $$
    DECLARE
        target_pid int;
    BEGIN
        SELECT pid INTO target_pid FROM cancelpid;
        PERFORM pg_terminate_backend(target_pid);
        LOOP
            EXIT WHEN NOT EXISTS (
                SELECT 1 FROM pg_stat_activity WHERE pid = target_pid
            );
            PERFORM pg_sleep(0.05);
        END LOOP;
    END;
    $$;
}

# Refresh sessions
session "R2"
setup {
    SET ROLE cagg_user;
    SET SESSION lock_timeout = '2s';
}
step "R2_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2026-01-05', '2026-02-15');
}

session "R3"
setup {
    SET ROLE cagg_user;
    SET SESSION lock_timeout = '2s';
}
step "R3_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2026-02-15', '2026-03-15');
}

session "R4"
setup {
    SET ROLE cagg_user;
    SET SESSION lock_timeout = '2s';
}
step "R4_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2026-03-15', '2026-03-30');
}
step "R4_overlapping_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2026-02-15', '2026-03-05');
}

session "A1"
step "A1_revoke_perm" {
  REVOKE SELECT on conditions FROM cagg_user;
}
step "A1_revoke_mat_perm" {
  DO $$
  DECLARE
      mat_ht text;
  BEGIN
      SELECT format('%I.%I', h.schema_name, h.table_name)
        INTO mat_ht
        FROM _timescaledb_catalog.continuous_agg ca
        JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
       WHERE ca.user_view_name = 'cond_daily';
      EXECUTE format('REVOKE SELECT ON %s FROM cagg_user', mat_ht);
  END;
  $$;
}

# Check session for jobs and locks
session "CHECK"
step "check_jobs" {
    SELECT ca.user_view_name,
           _timescaledb_functions.to_timestamp(r.start_range) AS start_time,
           _timescaledb_functions.to_timestamp(r.end_range) AS end_time
    FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges r
    JOIN _timescaledb_catalog.continuous_agg ca
        ON r.materialization_id = ca.mat_hypertable_id
    ORDER BY ca.user_view_name, r.start_range;
}
step "check_locks" {
    SELECT l.mode, l.granted
    FROM pg_locks l
    JOIN pg_class c ON c.oid = l.relation
    WHERE c.relname = 'continuous_aggs_jobs_refresh_ranges'
    ORDER BY l.mode;
}
step "L1_lock" {
    BEGIN;
    LOCK TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges
        IN ACCESS EXCLUSIVE MODE;
}
step "L1_unlock" {
    COMMIT;
}
step "check_jobs_metadata_manual" {
    SELECT ca.user_view_name,
           r.job_id = 0 AS has_zero_job_id,
           r.created_at IS NOT NULL AS has_created_at
    FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges r
    JOIN _timescaledb_catalog.continuous_agg ca
        ON r.materialization_id = ca.mat_hypertable_id
    ORDER BY ca.user_view_name, r.start_range;
}

step "check_jobs_metadata_policy" {
    SELECT ca.user_view_name,
           r.job_id != 0 AS has_non_zero_job_id,
           r.created_at IS NOT NULL AS has_created_at
    FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges r
    JOIN _timescaledb_catalog.continuous_agg ca
        ON r.materialization_id = ca.mat_hypertable_id
    ORDER BY ca.user_view_name, r.start_range;
}

# Session P1: runs a policy refresh via run_job
session "P1"
setup {
    SET SESSION lock_timeout = '2s';
    SET SESSION deadlock_timeout = '500ms';
}
step "P1_add_policy" {
    SELECT 1 AS policy_created FROM add_continuous_aggregate_policy(
        'cond_daily',
        start_offset => NULL,
        end_offset => NULL,
        schedule_interval => INTERVAL '1 h'
    );
}
step "P1_run_policy" {
    DO $$
    DECLARE
        jid int;
    BEGIN
        SELECT job_id INTO jid FROM timescaledb_information.jobs
        WHERE hypertable_name = 'cond_daily'
        AND proc_name = 'policy_refresh_continuous_aggregate';
        CALL run_job(jid);
    END;
    $$;
}

# Two refreshes wait for registration, one waits for cleanup before exiting. All blocked on an AccessExclusiveLock on continuous_aggs_jobs_refresh_ranges.
# None of those refreshes overlaps, so all should succeed.
permutation "WP_mat_enable" "R2_refresh" "L1_lock" "WP_mat_disable" "R3_refresh" "R4_refresh" "check_locks" "check_jobs" "L1_unlock" "check_locks" "check_jobs"

# Two refreshes wait for registration, one waits for cleanup before exiting. All blocked on an AccessExclusiveLock on continuous_aggs_jobs_refresh_ranges.
# Refreshes waiting for registration overlap with each other, so one should fail.
permutation "WP_mat_enable" "R2_refresh" "L1_lock" "WP_mat_disable" "R3_refresh" "R4_overlapping_refresh" "check_locks" "check_jobs" "L1_unlock" "check_locks" "check_jobs"

## Refresh registers . But fails in txn2. Gets into catch block. Cleanup should succeed
permutation "WP_before_txn2_start_enable" "R3_refresh" "check_jobs" "A1_revoke_perm" "WP_before_txn2_start_disable"("A1_revoke_perm") "check_jobs"

## Refresh registers . But fails in txn3. Gets into catch block. Cleanup should succeed
permutation "WP_before_txn3_start_enable" "R3_refresh" "check_jobs" "A1_revoke_mat_perm" "WP_before_txn3_start_disable"("A1_revoke_mat_perm") "check_jobs"

# Manual refresh registers with job_id=0 and a non-null created_at.
permutation "WP_after_register_enable" "R2_refresh" "check_jobs_metadata_manual" "WP_after_register_disable"

# Policy refresh registers with the correct job_id and a non-null created_at.
permutation "P1_add_policy" "WP_after_register_enable" "P1_run_policy" "check_jobs_metadata_policy" "WP_after_register_disable"

# Stale registration cleanup by concurrent refreshes.
# Kill a backend during refresh to end up with a pid left behind. Later two concurrent refreshes run, only one removes the stale pid.
# backend R1 is killed, we can no longer use this for later permutations
permutation "WP_before_txn2_commit_enable" "R1_refresh" "check_jobs" "K1_terminate"("check_jobs") "WP_before_txn2_commit_disable" "L1_lock" "R2_refresh"("L1_lock") "R3_refresh"("R2_refresh") "L1_unlock"(R2_refresh, R3_refresh) "check_jobs"
