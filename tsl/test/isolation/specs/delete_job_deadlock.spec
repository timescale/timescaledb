# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test that delete_job does not deadlock when a job is concurrently running
# (issue #6152).
#
# Before the fix, both run_job and delete_job used advisory locks on the
# job ID. When a job was running (holding a SHARE advisory lock via
# find_job), delete_job would block trying to acquire an EXCLUSIVE
# advisory lock. Combined with catalog table locks taken during deletion
# and job status updates, this caused deadlocks between delete_job and
# running background workers.
#
# After the fix, advisory locks are no longer used for job synchronization.
# delete_job simply deletes the catalog row and sends SIGINT to the worker.
#
# Test flow:
# 1. Session s1 locks a synchronization table
# 2. Session s2 starts run_job, the job proc blocks reading the locked table
# 3. Session s3 calls delete_job - should complete without blocking
# 4. Session s1 releases the table lock
# 5. Session s2's run_job completes
#
# The key assertion is that s3_delete_job does NOT show <waiting ...> in
# the expected output, proving it completes immediately while the job is
# still running.
###

setup {
    CREATE TABLE delete_job_gate();

    CREATE OR REPLACE FUNCTION test_job_proc_delete(job_id int, config jsonb)
    RETURNS void LANGUAGE plpgsql AS $$
    BEGIN
        -- This will block when another session holds ACCESS EXCLUSIVE on the table
        PERFORM * FROM delete_job_gate;
    END;
    $$;

    SELECT 1 FROM add_job('test_job_proc_delete', '1 hour', initial_start => '2100-01-01'::timestamptz);

    CREATE OR REPLACE FUNCTION get_delete_test_job_id() RETURNS int AS $$
        SELECT job_id FROM timescaledb_information.jobs WHERE proc_name = 'test_job_proc_delete';
    $$ LANGUAGE SQL;
}

teardown {
    SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name = 'test_job_proc_delete';
    DROP FUNCTION IF EXISTS test_job_proc_delete(int, jsonb);
    DROP FUNCTION IF EXISTS get_delete_test_job_id();
    DROP TABLE IF EXISTS delete_job_gate;
}

session "s1"

step "s1_lock" {
    BEGIN;
    LOCK TABLE delete_job_gate IN ACCESS EXCLUSIVE MODE;
}

step "s1_unlock" {
    COMMIT;
}

session "s2"

step "s2_run_job" {
    CALL run_job(get_delete_test_job_id());
}

session "s3"

step "s3_delete_job" {
    SELECT delete_job(get_delete_test_job_id());
}

permutation "s1_lock" "s2_run_job" "s3_delete_job" "s1_unlock"
