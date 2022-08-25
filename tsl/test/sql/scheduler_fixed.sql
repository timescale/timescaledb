-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
-- Setup for testing bgw jobs ---
--
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE FUNCTION wait_for_job_to_run(job_param_id INTEGER, expected_runs INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    r RECORD;
BEGIN
    FOR i in 1..spins
    LOOP
    SELECT total_successes, total_failures FROM _timescaledb_internal.bgw_job_stat WHERE job_id=job_param_id INTO r;
    IF (r.total_failures > 0) THEN
        RAISE INFO 'wait_for_job_to_run: job execution failed';
        RETURN false;
    ELSEIF (r.total_successes = expected_runs) THEN
        RETURN true;
    ELSEIF (r.total_successes > expected_runs) THEN
        RAISE 'num_runs > expected';
    ELSE
        PERFORM pg_sleep(0.1);
    END IF;
    END LOOP;
    RAISE INFO 'wait_for_job_to_run: timeout after % tries', spins;
    RETURN false;
END
$BODY$;

set client_min_messages TO NOTICE;
-- follow exactly cagg_bgw_drop_chunks

-- Remove any default jobs, e.g., telemetry
DELETE FROM _timescaledb_config.bgw_job;
TRUNCATE _timescaledb_internal.bgw_job_stat;

create or replace procedure job_20(jobid int, config jsonb) language plpgsql as $$
begin
perform pg_sleep(20);
end
$$;

create or replace procedure job_5(jobid int, config jsonb) language plpgsql as $$
begin
perform pg_sleep(5);
end
$$;

select * from _timescaledb_internal.bgw_job_stat;

-- add job that has a runtime well under the schedule interval
select now() as initial_start \gset

select add_job('job_5', schedule_interval => INTERVAL '15 seconds', initial_start => :'initial_start'::timestamptz) as short_job_fixed \gset
select add_job('job_5', schedule_interval => INTERVAL '15 seconds', initial_start => :'initial_start'::timestamptz, fixed_schedule => false) as short_job_drifting \gset

SELECT _timescaledb_internal.start_background_workers();

select initial_start as initial_start_given from timescaledb_information.jobs where job_id = :short_job_fixed \gset

select * from timescaledb_information.jobs;
select * from timescaledb_information.job_stats;

-- wait for the job to run
SELECT wait_for_job_to_run(:short_job_fixed, 1);
SELECT wait_for_job_to_run(:short_job_drifting, 1);
-- select ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(15000);

select next_start as next_start_short from timescaledb_information.job_stats where job_id = :short_job_fixed \gset 
-- verify the next_start is 15 seconds after the initial_start for the fixed schedule job
select :'next_start_short'::timestamptz - :'initial_start_given'::timestamptz as schedule_diff;
-- test job that runs for longer than the schedule interval
select add_job('job_20', schedule_interval => INTERVAL '15 seconds', initial_start => now()) as long_job_fixed \gset
select wait_for_job_to_run(:long_job_fixed, 1);

select initial_start as initial_start_long from timescaledb_information.jobs where job_id = :long_job_fixed \gset
select next_start as next_start_long from timescaledb_information.job_stats where job_id = :long_job_fixed \gset
select :'next_start_long'::timestamptz - :'initial_start_long'::timestamptz as schedule_diff;
