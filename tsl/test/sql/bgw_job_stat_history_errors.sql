-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

\set client_min_messages TO NOTICE;
create or replace procedure job_fail(jobid int, config jsonb) language plpgsql as $$
begin
perform pg_sleep(2);
raise exception 'raising an exception';
end
$$;

-- very simple case: job that raises an exception
select add_job('job_fail', '4 minutes') as jobf_id \gset

-- test jobs that try to update concurrently
CREATE TABLE custom_log (
    a int,
    b int,
    msg text
);

insert into custom_log values (0, 0, 'msg0');

ALTER SYSTEM SET DEFAULT_TRANSACTION_ISOLATION TO 'serializable';
SELECT pg_reload_conf();

-- Reconnect to make sure the GUC is set
\c :TEST_DBNAME :ROLE_SUPERUSER

-- test a concurrent update
CREATE OR REPLACE PROCEDURE custom_proc1(jobid int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  UPDATE custom_log set msg = 'msg1' where msg = 'msg0';
  perform pg_sleep(5);
  COMMIT;
END
$$;

CREATE OR REPLACE PROCEDURE custom_proc2(jobid int, config jsonb) LANGUAGE PLPGSQL AS
$$
BEGIN
  UPDATE custom_log set msg = 'msg2' where msg = 'msg0';
  COMMIT;
END
$$;

select add_job('custom_proc1', '2 min', initial_start => now());
-- to make sure custom_log is first updated by custom_proc_1
select add_job('custom_proc2', '2 min', initial_start => now() + interval '2 seconds');

SELECT _timescaledb_functions.start_background_workers();
-- enough time to for job_fail to fail
select pg_sleep(5);
select job_id, data->'job'->>'proc_name' as proc_name, data->'error_data'->>'message' as err_message, data->'error_data'->>'sqlerrcode' as sqlerrcode
from _timescaledb_internal.bgw_job_stat_history where job_id = :jobf_id and succeeded is false;

select delete_job(:jobf_id);

select pg_sleep(5);
-- exclude internal jobs
select job_id, data->'job'->>'proc_name' as proc_name, data->'error_data'->>'message' as err_message, data->'error_data'->>'sqlerrcode' as sqlerrcode
from _timescaledb_internal.bgw_job_stat_history WHERE job_id >= 1000 and succeeded is false;

ALTER SYSTEM RESET DEFAULT_TRANSACTION_ISOLATION;
SELECT pg_reload_conf();

-- Reconnect to make sure the GUC is set
\c :TEST_DBNAME :ROLE_SUPERUSER

-- test the retention job
SELECT next_start FROM alter_job(3, next_start => '2060-01-01 00:00:00+00'::timestamptz);
DELETE FROM _timescaledb_internal.bgw_job_stat_history;
INSERT INTO _timescaledb_internal.bgw_job_stat_history(job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES (123, 12345, false, '2000-01-01 00:00:00+00'::timestamptz, '2000-01-01 00:00:10+00'::timestamptz, '{}'),
(456, 45678, false, '2000-01-01 00:00:20+00'::timestamptz, '2000-01-01 00:00:40+00'::timestamptz, '{}'),
-- not older than a month
(123, 23456, false, '2050-01-01 00:00:00+00'::timestamptz, '2050-01-01 00:00:10+00'::timestamptz, '{}');
-- 3 rows in the table before policy runs
SELECT job_id, pid, succeeded, execution_start, execution_finish, data
FROM _timescaledb_internal.bgw_job_stat_history
WHERE succeeded IS FALSE;
-- drop all job_stats for the retention job
DELETE FROM _timescaledb_internal.bgw_job_stat WHERE job_id = 3;
SELECT FROM alter_job(3, next_start => now());
SELECT _timescaledb_functions.restart_background_workers();
SELECT test.wait_for_job_to_run(3, 1);
-- only the last row remains
SELECT job_id, pid, succeeded, execution_start, execution_finish, data
FROM _timescaledb_internal.bgw_job_stat_history
WHERE succeeded IS FALSE;

-- test failure when starting jobs
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_functions.stop_background_workers();

-- Job didn't finish yet and Crash detected
DELETE FROM _timescaledb_internal.bgw_job_stat_history;
INSERT INTO _timescaledb_internal.bgw_job_stat_history(job_id, pid, succeeded, execution_start, execution_finish, data)
VALUES (1, NULL, NULL, '2000-01-01 00:00:00+00'::timestamptz, NULL, '{}'), -- Crash server detected
(2, 2222, false, '2000-01-01 00:00:00+00'::timestamptz, NULL, '{}'), -- Didn't finished yet
(3, 3333, false, '2000-01-01 00:00:00+00'::timestamptz, '2000-01-01 01:00:00+00'::timestamptz, '{}'), -- Finish with ERROR
(4, 4444, true, '2000-01-01 00:00:00+00'::timestamptz, '2000-01-01 01:00:00+00'::timestamptz, '{}'); -- Finish with SUCCESS

SELECT job_id, pid, succeeded, start_time, finish_time, config, err_message
FROM timescaledb_information.job_history
ORDER BY job_id;

SELECT job_id, pid, start_time, finish_time, err_message
FROM timescaledb_information.job_errors
ORDER BY job_id;

DELETE FROM _timescaledb_internal.bgw_job_stat;
DELETE FROM _timescaledb_internal.bgw_job_stat_history;
DELETE FROM _timescaledb_catalog.bgw_job CASCADE;

SELECT _timescaledb_functions.start_background_workers();

\set VERBOSITY default
-- Setup Jobs
DO
$TEST$
DECLARE
  stmt TEXT;
  njobs INT := 26;
BEGIN
  RAISE INFO 'Creating % jobs', njobs;
  FOR stmt IN
    SELECT format('CREATE PROCEDURE custom_job%s(job_id int, config jsonb) LANGUAGE PLPGSQL AS $$ BEGIN PERFORM pg_sleep(1); END; $$', i) FROM generate_series(1, njobs) AS i
  LOOP
    EXECUTE stmt;
  END LOOP;

  RAISE INFO 'Scheduling % jobs', njobs;
  PERFORM add_job(format('custom_job%s', i)::regproc, schedule_interval => interval '1 hour', initial_start := now())
  FROM generate_series(1, njobs) AS i;
END;
$TEST$;

SELECT _timescaledb_functions.restart_background_workers();

-- Wait for jobs to run
DO
$TEST$
DECLARE
  njobs INT := 26;
BEGIN
  RAISE INFO 'Waiting for the % jobs to run', njobs;
  SET LOCAL client_min_messages TO WARNING;
  PERFORM test.wait_for_job_to_run_or_fail(id) FROM _timescaledb_catalog.bgw_job WHERE id >= 1000;
END;
$TEST$;

SELECT count(*) > 0 FROM timescaledb_information.job_history WHERE succeeded IS FALSE AND err_message ~ 'failed to start job';
SELECT count(*) > 0 FROM timescaledb_information.job_errors WHERE err_message ~ 'failed to start job';
\set VERBOSITY terse

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_functions.stop_background_workers();
