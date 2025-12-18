# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# Test for the race condition that can cause duplicate job rows to be visible
# during concurrent job scanning and updating (issue #4863).
#
# The bug occurs when:
# 1. One transaction scans the bgw_job table using SnapshotSelf
# 2. Another transaction updates the same job row concurrently (bypassing advisory locks)
# 3. Due to SnapshotSelf visibility semantics, both old and new tuple versions
#    may become visible, triggering the assertion at job.c:
#    Assert(num_found == 0)
#
# This test demonstrates the vulnerability when direct table updates bypass the
# advisory lock mechanism used by the job API functions. The test is expected to
# crash with an assertion failure if SnapshotSelf is used, proving the race
# condition exists. With the fix to use a transaction snapshot, the crash
# does not occur.
#
# Test flow:
# 1. Session s3 enables the debug waitpoint inside the scan loop
# 2. Session s1 starts alter_job which pauses at the waitpoint DURING scanning
#    (after reading the first tuple but before the loop continues)
# 3. Session s2 directly updates the job row (bypassing advisory locks)
# 4. Session s3 releases the waitpoint
# 5. Session s1 continues scanning and sees the updated tuple as a duplicate,
#    triggering the assertion failure
###

setup {
    CREATE OR REPLACE FUNCTION test_job_proc_race(job_id int, config jsonb)
    RETURNS void LANGUAGE plpgsql AS $$
    BEGIN
        -- no-op job for testing
        PERFORM pg_sleep(0.1);
    END;
    $$;

    -- Create a test job with a far-future initial_start so it doesn't auto-run
    -- Do not output job_id in order to avoid flakes
    SELECT 1 FROM add_job('test_job_proc_race', '1 hour', initial_start => '2100-01-01'::timestamptz);
}

teardown {
    -- Clean up all test jobs
    SELECT delete_job(job_id) FROM timescaledb_information.jobs WHERE proc_name = 'test_job_proc_race';
    DROP FUNCTION IF EXISTS test_job_proc_race(int, jsonb);
}

# Session s1 performs alter_job which scans the job table
session "s1"
setup {
    SET client_min_messages = WARNING;
}

# alter_job internally calls ts_bgw_job_find which uses the scanner with SnapshotSelf
# This will hit the waitpoint during the job lookup scan (after reading first tuple)
step "s1_alter_job" {
    SELECT count(*) FROM (
        SELECT alter_job(job_id, scheduled => false)
        FROM timescaledb_information.jobs
        WHERE proc_name = 'test_job_proc_race'
    ) x;
}

# Session s2 performs concurrent modifications bypassing advisory locks
session "s2"
setup {
    SET client_min_messages = WARNING;
}

# Directly update the job row - this bypasses the advisory lock mechanism
# that alter_job uses, creating the race condition
step "s2_direct_update" {
    UPDATE _timescaledb_catalog.bgw_job
    SET scheduled = NOT scheduled
    WHERE proc_name = 'test_job_proc_race';
}

# Session s3 controls the debug waitpoint
session "s3"
setup {
    SET client_min_messages = WARNING;
}

step "s3_enable_waitpoint" {
    SELECT debug_waitpoint_enable('bgw_job_find_during_scan');
}

step "s3_release_waitpoint" {
    SELECT debug_waitpoint_release('bgw_job_find_during_scan');
}

# This permutation triggers the race condition:
# s1 pauses mid-scan, s2 updates the row, s1 resumes and sees duplicate tuple
# Expected result: server crash due to assertion failure
permutation "s3_enable_waitpoint" "s1_alter_job" "s2_direct_update" "s3_release_waitpoint"
