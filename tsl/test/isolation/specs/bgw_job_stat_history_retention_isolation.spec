# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# This test verifies the behavior of the job stat history retention policy
# when there are concurrent inserts into the job stat history table.
#

setup {
    INSERT INTO _timescaledb_internal.bgw_job_stat_history(job_id, pid, succeeded, execution_start, execution_finish)
        SELECT 100 as job_id, 12345 as pid, true as succeeded, ts as execution_start, ts + interval '5 minutes' as execution_finish
        FROM generate_series(now() - interval '2 months', now(), interval '1 week') as ts;

    -- Verify # of rows that should be kept
    SELECT count(*) as expected_count FROM _timescaledb_internal.bgw_job_stat_history WHERE execution_finish > now() - interval '1 month';
}

teardown {
    TRUNCATE _timescaledb_internal.bgw_job_stat_history;
}

session "s1"
step "s1_retention" {
    CALL run_job(3);
    SELECT count(*) as s1_count FROM _timescaledb_internal.bgw_job_stat_history;
}

session "s2"
step "s2_insert" {
    INSERT INTO _timescaledb_internal.bgw_job_stat_history
    (job_id, pid, succeeded, execution_start, execution_finish)
    VALUES
    (201, 2001, true, now() - interval '1 week', now() - interval '1 week' + interval '5 minutes');
}

step "s2_begin" {
    BEGIN;
}

step "s2_commit" {
    COMMIT;
}


step "s2_count" {
    SELECT count(*) as s2_count FROM _timescaledb_internal.bgw_job_stat_history;
}

step "s2_lock_table" {
    BEGIN;
    LOCK TABLE _timescaledb_internal.bgw_job_stat_history IN ACCESS SHARE MODE;
}

step "s2_unlock_table" {
    ROLLBACK;
}

# Retention policy blocked by insert, inserted data shuold not be deleted
permutation "s2_begin" "s2_insert" "s2_count" "s1_retention" "s2_commit"

# Retention policy blocked by table lock
permutation "s2_lock_table" "s1_retention" "s2_unlock_table"
