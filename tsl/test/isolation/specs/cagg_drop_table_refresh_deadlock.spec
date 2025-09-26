# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# Setup prior to every permutation.
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

    CREATE MATERIALIZED VIEW cond_10
    WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
    AS
      SELECT time_bucket(10, time) AS bucket, avg(temp) AS avg_temp
      FROM conditions
      GROUP BY 1 WITH NO DATA;

    CREATE VIEW my_locks AS
    SELECT pid, locktype, relation::regclass AS relname, relation as relation_id,
           classid::regclass AS objtyp,
           objid::regclass,
           mode, granted, application_name
    FROM pg_locks JOIN pg_stat_activity USING (pid)
    WHERE database = (SELECT oid FROM pg_database WHERE current_database() = datname)
    ORDER BY locktype, application_name, objtyp, objid;

    CREATE TABLE refresh_pid (
        pid INTEGER NOT NULL PRIMARY KEY
    );

    --Add a retention policy, or any kind of scheduled job associated with the cagg or table
    SELECT add_retention_policy('conditions', 50);  -- Keep only the most recent 50 time units

    DO $$
    DECLARE
        job_id_param INTEGER;
    BEGIN
        SELECT add_continuous_aggregate_policy('cond_10', null, null, '1h'::interval) INTO job_id_param;
        INSERT INTO refresh_pid VALUES (job_id_param);
    END
    $$;


}

# Move the invalidation threshold so that we can generate some
# invalidations. This must be done in its own setup block since
# refreshing can't be done in a transaction block.
setup
{
    CALL refresh_continuous_aggregate('cond_10', 0, 30);
}

# Generate some invalidations. Must be done in separate transactions
# or otherwise there will be only one invalidation.
setup
{
    BEGIN;
    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(1, 10, 1) t;
    COMMIT;
    BEGIN;
    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(10, 20, 1) t;
    COMMIT;
    BEGIN;
    INSERT INTO conditions
    SELECT t, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
    FROM generate_series(15, 40, 1) t;
    COMMIT;
}

teardown {
    DROP TABLE conditions CASCADE;
    DROP VIEW my_locks;
    DROP TABLE refresh_pid;
}

# Waitpoint for drop table job, when it is dropping the jobs on the table cagg
session "WP"
step "WP_enable_beforeAccessExclusiveLockOnJob"
{
    SELECT debug_waitpoint_enable('debug_waitpoint_before_get_job_lock_for_delete');
}
step "WP_release_beforeAccessExclusiveLockOnJob"
{
    SELECT debug_waitpoint_release('debug_waitpoint_before_get_job_lock_for_delete');
}

step "WP_enable_AfterAdvisoryLock"
{
    SELECT debug_waitpoint_enable('after_taking_job_id_advisory_lock');
}
step "WP_release_AfterAdvisoryLock"
{
    SELECT debug_waitpoint_release('after_taking_job_id_advisory_lock');
}

step "WP_enable_beforeKillingJob"
{
    SELECT debug_waitpoint_enable('before_killing_job');
}
step "WP_release_BeforeKillingJob"
{
    SELECT debug_waitpoint_release('before_killing_job');
}

step "WP_enable_AfterDeleteJob"
{
    SELECT debug_waitpoint_enable('after_delete_a_job');
}
step "WP_release_AfterDeleteJob"
{
    SELECT debug_waitpoint_release('after_delete_a_job');
}

# Session to drop the table conditions
session "S1"
setup
{
    SET SESSION lock_timeout = '5s';
    SET SESSION deadlock_timeout = '2s';
}
step "S1_drop_table"
{
    DROP TABLE conditions CASCADE;
}

session "S2"
setup
{
    SET SESSION lock_timeout = '5s';
    SET SESSION deadlock_timeout = '2s';
}
step "S2_refresh"
{
    DO $$
    DECLARE
        job_pid INTEGER;
    BEGIN
        SELECT pid INTO job_pid FROM refresh_pid LIMIT 1;
        CALL run_job(job_pid);
    END
    $$;
}

# Session to view current locks being hold
session "S3"
setup
{
    SET SESSION lock_timeout = '1s';
    SET SESSION deadlock_timeout = '2s';
}
step "S3_lock_show"
{
    SELECT pid, locktype, mode, relation_id, relname, granted, application_name, objtyp, objid
    FROM my_locks
    WHERE locktype = 'advisory' OR relname IN ('conditions', '_timescaledb_internal.bgw_job_stat');
}


####################################################################
#
# Two deadlock situations, both related to drop table/cagg cascade
# while one of the relation's jobs is running
#
####################################################################

# First situation: drop table while one of its cagg policies is running
# The refresh job (P1) started to and acquired RowShareLock on the refresh's job_id (advisory lock)
# Drop table cascade (P2) acquired AccessExclusiveLock on the table, then proceeded to drop
# the dependent jobs, including the refresh one. It tried to acquire AccessExclusiveLock
# on the refresh's job_id (advisory lock) before deleting the job, but is blocked by the
# RowShareLock on the job_id (advisory lock) that P1 held. P1 proceeded and
# later tried to acquire AccessShareLock on the table, but is blocked by the AccessExclusiveLock
# that P2 held.

permutation "WP_enable_AfterAdvisoryLock"
            "S2_refresh"
            "S3_lock_show"
            "WP_enable_beforeAccessExclusiveLockOnJob"
            "S1_drop_table"
            "S3_lock_show"("WP_enable_beforeAccessExclusiveLockOnJob")
            "WP_release_beforeAccessExclusiveLockOnJob"
            "S3_lock_show"
            "WP_release_AfterAdvisoryLock"
            "S3_lock_show"


# Second situation: drop table/cagg with at least 2 jobs to be drop,
# while one of its policy is running, and the one running is not the first one dropped
#
# The drop process (P2) started its cascade drop, and after dropping the first job,
# it is holding ShareRowExclusiveLock on bgw_job_stat, the refresh job (P1),which has not
# been dropped yet, acquired RowShareLock on the refresh's job_id (advisory lock), then proceeded
# and asked for AccessShareLock on bgw_job_stat, but is blocked by the ShareRowExclusiveLock
# that P2 held. P2 continued to drop the refresh job and tried to acquire AccessExclusiveLock
# on the refresh's job_id (advisory lock), but is blocked by the RowShareLock that P1 held.

permutation "WP_enable_AfterDeleteJob"
            "S1_drop_table"
            "S3_lock_show"
            "WP_enable_AfterAdvisoryLock"
            "S2_refresh"
            "S3_lock_show"
            "WP_enable_beforeKillingJob"
            "WP_release_AfterDeleteJob"
            "S3_lock_show"
            "WP_release_BeforeKillingJob"
            "WP_release_AfterAdvisoryLock"
            "S3_lock_show"
