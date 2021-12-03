-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
-- Setup
--
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(timeout INT = -1, mock_start_time INT = 0) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_run(timeout INT = -1, mock_start_time INT = 0) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_wait_for_scheduler_finish() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION ts_bgw_params_destroy() RETURNS VOID AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
CREATE OR REPLACE FUNCTION ts_bgw_test_job_sleep(job_id INT, config JSONB) RETURNS VOID AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_reset_time(set_time BIGINT = 0, wait BOOLEAN = false) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION insert_job(application_name NAME,job_type NAME, schedule_interval INTERVAL, max_runtime INTERVAL, retry_period INTERVAL, owner NAME DEFAULT CURRENT_ROLE, scheduled BOOL DEFAULT true) RETURNS INT LANGUAGE SQL SECURITY DEFINER AS
$$
  INSERT INTO _timescaledb_config.bgw_job(application_name,schedule_interval,max_runtime,max_retries,retry_period,proc_name,proc_schema,owner,scheduled) VALUES($1,$3,$4,5,$5,$2,'public',$6,$7) RETURNING id;
$$;

CREATE OR REPLACE FUNCTION test_toggle_scheduled(job_id INTEGER) RETURNS VOID LANGUAGE SQL SECURITY DEFINER AS
$$
  UPDATE _timescaledb_config.bgw_job SET scheduled = NOT scheduled WHERE id = $1;
$$;

\set WAIT_ON_JOB 0
\set IMMEDIATELY_SET_UNTIL 1
\set WAIT_FOR_OTHER_TO_ADVANCE 2
\set WAIT_FOR_STANDARD_WAITLATCH 3

CREATE OR REPLACE FUNCTION ts_bgw_params_mock_wait_returns_immediately(new_val INTEGER) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE FUNCTION get_application_pid(app_name TEXT) RETURNS INTEGER LANGUAGE SQL AS
$BODY$
    SELECT pid FROM pg_stat_activity WHERE application_name = app_name;
$BODY$;

CREATE FUNCTION wait_application_pid(app_name TEXT, wait_for_start BOOLEAN = true) RETURNS INTEGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    r INTEGER;
BEGIN
    --wait up to a second checking each 100ms
    FOR i in 1..10
    LOOP
        SELECT get_application_pid(app_name) INTO r;
        IF (wait_for_start AND r IS NULL) OR (NOT wait_for_start AND r IS NOT NULL) THEN
            PERFORM pg_sleep(0.1);
            PERFORM pg_stat_clear_snapshot();
        ELSE
            RETURN r;
        END IF;
    END LOOP;
    RETURN NULL;
END
$BODY$;

CREATE FUNCTION wait_for_logentry(job_id INTEGER) RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
  app_name TEXT;
  message TEXT;
BEGIN
    SELECT application_name INTO app_name FROM _timescaledb_config.bgw_job WHERE id = job_id;

    --wait up to a second checking each 100ms
    FOR i in 1..10
    LOOP
        SELECT msg INTO message FROM bgw_log WHERE application_name = app_name ORDER BY msg_no DESC LIMIT 1;
        IF FOUND THEN
          RETURN message;
        END IF;
        PERFORM pg_sleep(0.1);
        PERFORM pg_stat_clear_snapshot();
    END LOOP;
    RETURN NULL;
END
$BODY$;

-- Remove any default jobs, e.g., telemetry
DELETE FROM _timescaledb_config.bgw_job WHERE TRUE;
TRUNCATE _timescaledb_internal.bgw_job_stat;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE public.bgw_log(
    msg_no INT,
    mock_time BIGINT,
    application_name TEXT,
    msg TEXT
);

CREATE VIEW sorted_bgw_log AS
    SELECT msg_no, application_name, regexp_replace(regexp_replace(msg, 'Wait until [0-9]+, started at [0-9]+', 'Wait until (RANDOM), started at (RANDOM)'), 'background worker "[^"]+"','connection') AS msg FROM bgw_log ORDER BY mock_time, application_name COLLATE "C", msg_no;

CREATE TABLE public.bgw_dsm_handle_store(
    handle BIGINT
);

INSERT INTO public.bgw_dsm_handle_store VALUES (0);
SELECT ts_bgw_params_create();

--
-- Test running the scheduler with no jobs
--

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50);
-- empty
SELECT * FROM _timescaledb_internal.bgw_job_stat;
-- empty
SELECT * FROM sorted_bgw_log;

--
-- Test running the scheduler with a job marked as unscheduled
--

TRUNCATE bgw_log;
SELECT ts_bgw_params_reset_time();
SELECT insert_job('unscheduled', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s',scheduled:= false);
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50);
-- empty
SELECT * FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM timescaledb_information.job_stats;

SELECT test_toggle_scheduled(1000);
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50);
SELECT * FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM timescaledb_information.job_stats;
SELECT * FROM sorted_bgw_log;

SELECT delete_job(1000);

--
-- test deleting job also terminates running jobs
--

SELECT add_job('ts_bgw_test_job_sleep','1h') AS job_id \gset
SELECT ts_bgw_db_scheduler_test_run();

SELECT wait_for_logentry(:job_id);
SELECT application_name FROM pg_stat_activity WHERE application_name LIKE 'User-Defined Action%';

\x on
SELECT job_id, job_status FROM timescaledb_information.job_stats;
-- Showing non-volatile information from pg_stat_activity for
-- debugging purposes. Information schema above reads from this view.
SELECT datname, usename, application_name, state, query, wait_event_type, wait_event
  FROM pg_stat_activity WHERE application_name LIKE 'User-Defined Action%';
\x off

-- have to suppress notices here as delete_job will print pid of the running background worker processes
SET client_min_messages TO WARNING;
SELECT delete_job(:job_id);
RESET client_min_messages;
SELECT application_name FROM pg_stat_activity WHERE application_name LIKE 'User-Defined Action%';

--
-- Test running a normal job
--
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq RESTART;
SELECT ts_bgw_params_reset_time();
SELECT insert_job('test_job_1', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s');
select * from _timescaledb_config.bgw_job;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--Tests that the scheduler start a job right away if it's the first time and there is no job_stat entry for it
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

--Test that the scheduler will not run job again if not enough time has passed
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25, 25);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

--After enough time has passed the scheduler will run the job again
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(100, 50);
SELECT job_id, next_start, last_finish, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

--Now it runs it one more time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(120, 100);

SELECT job_id, next_start, last_finish, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;

SELECT * FROM sorted_bgw_log;

--
-- Test what happens when running a job that throws an error
--
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
SELECT insert_job('test_job_2', 'bgw_test_job_2_error', INTERVAL '100ms', INTERVAL '100s', INTERVAL '100ms');
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--Run the first time and error
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

SELECT last_finish, last_successful_finish, last_run_success FROM _timescaledb_internal.bgw_job_stat;

--Scheduler runs the job again, sees another error, and increases the wait time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(125);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

--The job runs and fails again a few more times increasing the wait time each time.
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(225);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(425);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

--Once the wait time reaches 500ms it stops increasion
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(525);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

-- Get status of failing job `test_job_2` to check it reached `max_retries` and
-- the new `job_status` now is `Paused`
SELECT job_id, last_run_status, job_status, total_runs, total_successes, total_failures
FROM timescaledb_information.job_stats WHERE job_id = 1001;

-- Alter job to be rescheduled and run it again
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
SELECT true FROM alter_job(1001, scheduled => true) AS discard;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(525);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat WHERE job_id = 1001;
SELECT * FROM sorted_bgw_log;

--
-- Test timeout logic
--
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
--set timeout lower than job length
SELECT insert_job('test_job_3_long', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '20ms', INTERVAL '50ms');
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT ts_bgw_params_mock_wait_returns_immediately(:IMMEDIATELY_SET_UNTIL);

--Test that the scheduler kills a job that takes too long
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(200);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

--Check that the scheduler does not kill a job with infinite timeout
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
--set timeout to 0
SELECT insert_job('test_job_3_long', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '0', INTERVAL '10ms');
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(550);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;

SELECT ts_bgw_params_mock_wait_returns_immediately(:WAIT_ON_JOB);

--
-- Test signal handling
--
--Test sending a SIGTERM to a job
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
SELECT ts_bgw_params_reset_time();
TRUNCATE _timescaledb_internal.bgw_job_stat;
DELETE FROM _timescaledb_config.bgw_job;
SELECT insert_job('test_job_3_long', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '500ms');
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--escalated priv needed for access to pg_stat_activity
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT ts_bgw_db_scheduler_test_run(300);
SELECT pg_terminate_backend(wait_application_pid('test_job_3_long'));
SELECT ts_bgw_db_scheduler_test_wait_for_scheduler_finish();

SELECT * FROM sorted_bgw_log;
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;

-- Test that the job is able to run again and succeed
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(900);

SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;

SELECT * FROM sorted_bgw_log;

--Test sending a SIGHUP to a job
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
SELECT ts_bgw_params_reset_time();
TRUNCATE _timescaledb_internal.bgw_job_stat;
DELETE FROM _timescaledb_config.bgw_job;

SELECT ts_bgw_params_mock_wait_returns_immediately(:WAIT_FOR_STANDARD_WAITLATCH);
SELECT ts_bgw_db_scheduler_test_run(-1);

SHOW timescaledb.shutdown_bgw_scheduler;

ALTER SYSTEM SET timescaledb.shutdown_bgw_scheduler TO 'on';
SELECT pg_reload_conf();
\c :TEST_DBNAME :ROLE_SUPERUSER
SHOW timescaledb.shutdown_bgw_scheduler;

SELECT ts_bgw_db_scheduler_test_wait_for_scheduler_finish();

SELECT * FROM sorted_bgw_log;

ALTER SYSTEM RESET timescaledb.shutdown_bgw_scheduler;
SELECT pg_reload_conf();
\c :TEST_DBNAME :ROLE_SUPERUSER
SHOW timescaledb.shutdown_bgw_scheduler;

SELECT ts_bgw_params_mock_wait_returns_immediately(:WAIT_ON_JOB);

--Test that sending SIGTERM to scheduler terminates the jobs as well
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
SELECT insert_job('test_job_3_long', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms');

SELECT ts_bgw_db_scheduler_test_run(500);
SELECT wait_application_pid('test_job_3_long') IS NOT NULL ;
SELECT pg_terminate_backend(wait_application_pid('DB Scheduler Test'));

SELECT ts_bgw_db_scheduler_test_wait_for_scheduler_finish();

SELECT job_id, last_finish, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;

SELECT * FROM sorted_bgw_log;

--After a SIGTERM to scheduler and jobs, the jobs are considered crashed and there is a imposed wait of 5 min before a job can be run.
--See that there is no run again because of the crash-imposed wait (not run with the 10ms retry_period)
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(500);
SELECT job_id, last_finish, next_start, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;

--But after the 5 min period the job is again run
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(400000);
SELECT job_id, last_finish, next_start, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM sorted_bgw_log;


CREATE FUNCTION wait_for_timer_to_run(started_at INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	num_runs INTEGER;
	message TEXT;
BEGIN
	select format('[TESTING] Wait until %%, started at %s', started_at) into message;
	FOR i in 1..spins
	LOOP
	SELECT COUNT(*) from bgw_log where msg LIKE message INTO num_runs;
	if (num_runs > 0) THEN
		RETURN true;
	ELSE
		PERFORM pg_sleep(0.1);
	END IF;
	END LOOP;
	RETURN false;
END
$BODY$;

CREATE FUNCTION wait_for_job_3_to_finish(runs INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	num_runs INTEGER;
BEGIN
	FOR i in 1..spins
	LOOP
	SELECT COUNT(*) from bgw_log where msg='After sleep job 3' INTO num_runs;
	if (num_runs = runs) THEN
		RETURN true;
	ELSE
		PERFORM pg_sleep(0.1);
	END IF;
	END LOOP;
	RETURN false;
END
$BODY$;

--
-- Test starting more jobs than availlable workers
--
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
SELECT ts_bgw_params_mock_wait_returns_immediately(:WAIT_FOR_OTHER_TO_ADVANCE);
--Our normal limit is 8 jobs (1 already taken up by the launcher, we don't register the test scheduler)
--so start 8 workers. Make the schedule_INTERVAL long and the retry period short so that the
--retries happen within the scheduler run time but everything only runs once.
SELECT
insert_job('test_job_3_long_1', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms'),
insert_job('test_job_3_long_2', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms'),
insert_job('test_job_3_long_3', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms'),
insert_job('test_job_3_long_4', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms'),
insert_job('test_job_3_long_5', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms'),
insert_job('test_job_3_long_6', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms'),
insert_job('test_job_3_long_7', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms'),
insert_job('test_job_3_long_8', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', INTERVAL '10ms');

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT ts_bgw_db_scheduler_test_run(25000); --quit at second 25
--the first 7 jobs will run right away, but not the last one
SELECT wait_for_timer_to_run(0);
SELECT wait_for_job_3_to_finish(7);

SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat
ORDER BY job_id;

--but after the first batch finishes and 1 second (START_RETRY_MS) passes, the last job will run.
SELECT ts_bgw_params_reset_time(1000000, true); --set to second 1
SELECT wait_for_timer_to_run(1000000);
SELECT wait_for_job_3_to_finish(8);

SELECT ts_bgw_params_reset_time(30000000, true); --set to second 30, which causes a quit.
SELECT ts_bgw_db_scheduler_test_wait_for_scheduler_finish();

--should have all 8 runs, all with success runs
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat
ORDER BY job_id;

SELECT * FROM bgw_log WHERE application_name = 'DB Scheduler' ORDER BY mock_time, application_name, msg_no;

SELECT ts_bgw_params_destroy();

--
-- Test setting next_start time within a job
--
\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
SELECT ts_bgw_params_mock_wait_returns_immediately(:WAIT_ON_JOB);
DELETE FROM _timescaledb_config.bgw_job;
SELECT insert_job('test_job_4', 'bgw_test_job_4', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s');
select * from _timescaledb_config.bgw_job;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Now run and make sure next_start is 200ms away, not 100ms
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;

SELECT * FROM sorted_bgw_log;

-- Now just make sure that the job actually runs in 200ms
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(200);
-- Print next_start and last_finish explicitly, instead of the difference, to make sure the times have changed
-- since the last run
SELECT job_id, next_start, last_finish, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;

SELECT * FROM sorted_bgw_log;

-- Test updating jobs list
TRUNCATE bgw_log;

\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.stop_background_workers();
SELECT _timescaledb_internal.restart_background_workers();
SELECT _timescaledb_internal.start_background_workers();
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_internal.stop_background_workers();

CREATE OR REPLACE FUNCTION ts_test_job_refresh() RETURNS TABLE(
id INTEGER,
application_name NAME,
schedule_interval INTERVAL,
max_runtime INTERVAL,
max_retries INT,
retry_period INTERVAL,
next_start TIMESTAMPTZ,
timeout_at TIMESTAMPTZ,
reserved_worker BOOLEAN,
may_next_mark_end BOOLEAN
)
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE FUNCTION verify_refresh_correct() RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    num_jobs INTEGER;
    num_jobs_in_list INTEGER;
BEGIN
    SELECT COUNT(*) from _timescaledb_config.bgw_job INTO num_jobs;
	select COUNT(*) from ts_test_job_refresh() JOIN _timescaledb_config.bgw_job USING (id,application_name,schedule_interval,max_runtime,max_retries,retry_period) INTO num_jobs_in_list;
	IF (num_jobs = num_jobs_in_list) THEN
		RETURN true;
	END IF;
	RETURN false;
END
$BODY$;

CREATE FUNCTION wait_for_job_1_to_run(runs INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	num_runs INTEGER;
BEGIN
	FOR i in 1..spins
	LOOP
	SELECT COUNT(*) from bgw_log where msg='Execute job 1' INTO num_runs;
	if (num_runs = runs) THEN
		RETURN true;
	ELSE
		PERFORM pg_sleep(0.1);
	END IF;
	END LOOP;
	RETURN false;
END
$BODY$;

select * from verify_refresh_correct();
-- Should return the same table
select * from verify_refresh_correct();
DELETE FROM _timescaledb_config.bgw_job;
-- Make sure jobs list is empty
select count(*) from ts_test_job_refresh();

SELECT
insert_job('test_1', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s'),
insert_job('test_2', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s'),
insert_job('test_3', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s');
select * from verify_refresh_correct();

DELETE from _timescaledb_config.bgw_job where application_name='test_2';
SELECT insert_job('test_4', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s');
select * from verify_refresh_correct();

DELETE FROM _timescaledb_config.bgw_job;
SELECT insert_job('test_10', 'test_10', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s');
select * from verify_refresh_correct();
-- Should be idempotent
select * from verify_refresh_correct();

DELETE FROM _timescaledb_config.bgw_job;
SELECT
insert_job('another', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s'),
insert_job('another1', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s'),
insert_job('another2', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s'),
insert_job('another3', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s'),
insert_job('another4', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s');
select * from verify_refresh_correct();

DELETE FROM _timescaledb_config.bgw_job where application_name='another' OR application_name='another3';
SELECT insert_job('blah', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s');
select * from verify_refresh_correct();

-- Now test a real scheduler-mock running in a loop and updating the list of jobs
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
SELECT ts_bgw_params_mock_wait_returns_immediately(:WAIT_FOR_OTHER_TO_ADVANCE);

SELECT ts_bgw_db_scheduler_test_run(500);
-- Wait for scheduler to start up
SELECT wait_for_timer_to_run(0);

SELECT insert_job('another', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', INTERVAL '1s') AS job_id \gset
-- call alter_job to trigger cache invalidation
SELECT alter_job(:job_id,scheduled:=true);

SELECT ts_bgw_params_reset_time(50000, true);
SELECT wait_for_timer_to_run(50000);
SELECT wait_for_job_1_to_run(1);
SELECT ts_bgw_params_reset_time(150000, true);
SELECT wait_for_timer_to_run(150000);
SELECT wait_for_job_1_to_run(2);

select * from _timescaledb_internal.bgw_job_stat;
SELECT delete_job(x.id) FROM (select * from _timescaledb_config.bgw_job) x;

-- test null handling in delete_job
SELECT delete_job(NULL);

SELECT ts_bgw_params_reset_time(200000, true);
SELECT wait_for_timer_to_run(200000);

-- In the next time interval, nothing should be run because scheduler should have an empty list
SELECT ts_bgw_params_reset_time(300000, true);
SELECT wait_for_timer_to_run(300000);
-- Same for this time interval
SELECT ts_bgw_params_reset_time(400000, true);
SELECT wait_for_timer_to_run(400000);

-- Now add a new job and make sure it gets run before the scheduler dies
SELECT insert_job('new_job', 'bgw_test_job_1', INTERVAL '10ms', INTERVAL '100s', INTERVAL '1s') AS job_id \gset
-- call alter_job to trigger cache invalidation
SELECT alter_job(:job_id,scheduled:=true);

SELECT ts_bgw_params_reset_time(450000, true);
-- New job should be run once, for a total of 3 runs of this job in the log
SELECT wait_for_job_1_to_run(3);

-- New job should be run again
SELECT ts_bgw_params_reset_time(480000, true);
SELECT wait_for_job_1_to_run(4);

SELECT ts_bgw_params_reset_time(500000, true);
SELECT * FROM sorted_bgw_log;
SELECT * FROM _timescaledb_internal.bgw_job_stat;

-- clean up jobs
SELECT _timescaledb_internal.stop_background_workers();

