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

CREATE OR REPLACE FUNCTION ts_test_next_scheduled_execution_slot(schedule_interval INTERVAL, finish_time TIMESTAMPTZ, initial_start TIMESTAMPTZ, timezone TEXT = NULL)
RETURNS TIMESTAMPTZ AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

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

-- test some possible schedule_interval, finish_time, initial_start combinations
SET timezone = 'UTC';
-- want to execute on the 15th of the month
select ts_test_next_scheduled_execution_slot('1 month', '2022-09-16 13:21:34+00'::timestamptz, '2022-09-15 19:00:00+00'::timestamptz);
-- ..or the first of the month
select ts_test_next_scheduled_execution_slot('1 month', '2022-09-01 19:21:34+00'::timestamptz, '2022-09-01 19:00:00+00'::timestamptz);
-- want to execute on Sundays (2022-09-04 is a Sunday)
select extract(dow from date '2022-09-04') = 0;
select ts_test_next_scheduled_execution_slot('1 week', '2022-09-22 13:21:34+00'::timestamptz, '2022-09-04 19:00:00+00'::timestamptz);
-- want to execute at 6am every day
select ts_test_next_scheduled_execution_slot('1d', '2022-09-21 13:21:34+00'::timestamptz, '2022-09-21 06:00:00+00'::timestamptz);
-- test what happens across DST
-- go from +1 to +2
set timezone to 'Europe/Berlin';
-- DST switch on March 27th 2022
select ts_test_next_scheduled_execution_slot('1 week', '2022-03-23 09:21:34 CET'::timestamptz, '2022-03-23 09:00:00 CET'::timestamptz) as t1 \gset
select ts_test_next_scheduled_execution_slot('1 week', '2022-03-23 09:21:34 CET'::timestamptz, '2022-03-23 09:00:00 CET'::timestamptz, timezone => 'Europe/Berlin')
as t1_tz \gset
select :'t1' as without_timezone, :'t1_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot('1 week', :'t1'::timestamptz, '2022-03-23 09:00:00+01'::timestamptz) as t2 \gset
select ts_test_next_scheduled_execution_slot('1 week', :'t1_tz'::timestamptz, '2022-03-23 09:00:00+01'::timestamptz, timezone => 'Europe/Berlin') as t2_tz \gset
select :'t2' as without_timezone, :'t2_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot('1 week', :'t2'::timestamptz, '2022-03-23 09:00:00+01'::timestamptz) as t3 \gset
select ts_test_next_scheduled_execution_slot('1 week', :'t2_tz'::timestamptz, '2022-03-23 09:00:00+01'::timestamptz, timezone => 'Europe/Berlin') as t3_tz \gset
select :'t3' as without_timezone, :'t3_tz' as with_timezone;

-- go from +2 to +1
-- DST switch on October 30th 2022
select ts_test_next_scheduled_execution_slot('1 week', '2022-10-29 09:21:34+02'::timestamptz, '2022-10-29 09:00:00+02'::timestamptz) as t1 \gset
select ts_test_next_scheduled_execution_slot('1 week', '2022-10-29 09:21:34+02'::timestamptz, '2022-10-29 09:00:00+02'::timestamptz, 'Europe/Berlin')
as t1_tz \gset
select :'t1' as without_timezone, :'t1_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot('1 week', :'t1'::timestamptz, '2022-10-29 09:00:00+02'::timestamptz) as t2 \gset
select ts_test_next_scheduled_execution_slot('1 week', :'t1_tz'::timestamptz, '2022-10-29 09:00:00+02'::timestamptz, 'Europe/Berlin') as t2_tz \gset
select :'t2' as without_timezone, :'t2_tz' as with_timezone;
select ts_test_next_scheduled_execution_slot('1 week', :'t2'::timestamptz, '2022-10-29 09:00:00+02'::timestamptz) as t3 \gset
select ts_test_next_scheduled_execution_slot('1 week', :'t2_tz'::timestamptz, '2022-10-29 09:00:00+02'::timestamptz, 'Europe/Berlin') as t3_tz \gset
select :'t3' as without_timezone, :'t3_tz' as with_timezone;

\set ON_ERROR_STOP 0
-- test some unacceptable values for schedule interval
select add_job('job_5', schedule_interval => interval '1 month 1week', initial_start => :'initial_start'::timestamptz);
