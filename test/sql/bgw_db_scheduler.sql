--
-- Setup
--
\c single :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(timeout INT = -1, mock_start_time INT = 0) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_run(timeout INT = -1, mock_start_time INT = 0) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_wait_for_scheduler_finish() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_destroy() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_reset_time() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_mock_wait_returns_immediately(new_val BOOLEAN) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

--allow us to inject test jobs
ALTER TABLE _timescaledb_config.bgw_job DROP CONSTRAINT valid_job_type;

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

-- Remove any default jobs, e.g., telemetry
SELECT _timescaledb_internal.stop_background_workers();
DELETE FROM _timescaledb_config.bgw_job WHERE TRUE;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT _timescaledb_internal.start_background_workers();

\c single :ROLE_DEFAULT_PERM_USER

CREATE TABLE public.bgw_log(
    msg_no INT,
    mock_time BIGINT,
    application_name TEXT,
    msg TEXT
);

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
SELECT * FROM bgw_log;

--
-- Test running a normal job
--
\c single :ROLE_SUPERUSER
TRUNCATE bgw_log;
SELECT ts_bgw_params_reset_time();
INSERT INTO _timescaledb_config.bgw_job (application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
('test_job_1', 'bgw_test_job_1', INTERVAL '100ms', INTERVAL '100s', 3, INTERVAL '1s');
select * from _timescaledb_config.bgw_job;
\c single :ROLE_DEFAULT_PERM_USER

--Tests that the scheduler start a job right away if it's the first time and there is no job_stat entry for it
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

--Test that the scheduler will not run job again if not enough time has passed
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25, 25);
SELECT job_id, next_start-last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

--After enough time has passed the scheduler will run the job again
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(100, 50);
SELECT job_id, next_start, last_finish, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

--Now it runs it one more time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(120, 100);

SELECT job_id, next_start, last_finish, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;

SELECT * FROM bgw_log;

--
-- Test what happens when running a job that throws an error
--
\c single :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
INSERT INTO _timescaledb_config.bgw_job (application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
('test_job_2', 'bgw_test_job_2_error', INTERVAL '100ms', INTERVAL '100s', 3, INTERVAL '100ms');
\c single :ROLE_DEFAULT_PERM_USER

--Run the first time and error
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);
SELECT job_id, next_start-last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

--Scheduler runs the job again, sees another error, and increases the wait time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(125);
SELECT job_id, next_start-last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

--The job runs and fails again a few more times increasing the wait time each time.
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(225);
SELECT job_id, next_start-last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(425);
SELECT job_id, next_start-last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

--Once the wait time reaches 500ms it stops increasion
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(525);
SELECT job_id, next_start-last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

--
-- Test timeout logic
--
\c single :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
--set timeout lower than job length
INSERT INTO _timescaledb_config.bgw_job (application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
('test_job_3_long', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '20ms', 3, INTERVAL '50ms');
\c single :ROLE_DEFAULT_PERM_USER

SELECT ts_bgw_params_mock_wait_returns_immediately(true);

--Test that the scheduler kills a job that takes too long
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(200);
SELECT job_id, last_finish, next_start, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

--Check that the scheduler does not kill a job with infinite timeout
\c single :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
--set timeout to 0
INSERT INTO _timescaledb_config.bgw_job (application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
('test_job_3_long', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '0', 3, INTERVAL '10ms');
\c single :ROLE_DEFAULT_PERM_USER

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(550);
SELECT job_id, last_finish-next_start as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;

SELECT ts_bgw_params_mock_wait_returns_immediately(false);

--
-- Test signal handling
--
--Test sending a SIGTERM to a job
\c single :ROLE_SUPERUSER
TRUNCATE bgw_log;
SELECT ts_bgw_params_reset_time();
TRUNCATE _timescaledb_internal.bgw_job_stat;
DELETE FROM _timescaledb_config.bgw_job;
INSERT INTO _timescaledb_config.bgw_job (application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
('test_job_3_long', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '500ms');
\c single :ROLE_DEFAULT_PERM_USER

--escalated priv needed for access to pg_stat_activity
\c single :ROLE_SUPERUSER
SELECT ts_bgw_db_scheduler_test_run(300);
SELECT pg_terminate_backend(wait_application_pid('test_job_3_long'));
SELECT ts_bgw_db_scheduler_test_wait_for_scheduler_finish();

SELECT * FROM bgw_log;
SELECT job_id, next_start - last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;

-- Test that the job is able to run again and succeed
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(900);

SELECT job_id, next_start-last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
FROM _timescaledb_internal.bgw_job_stat;

SELECT * FROM bgw_log;

--Test that sending SIGTERM to scheduler terminates the jobs as well
\c single :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
INSERT INTO _timescaledb_config.bgw_job (application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
('test_job_3_long', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '10ms');
\c single :ROLE_DEFAULT_PERM_USER

--escalated priv needed for access to pg_stat_activity
\c single :ROLE_SUPERUSER

SELECT ts_bgw_db_scheduler_test_run(500);
SELECT wait_application_pid('test_job_3_long') IS NOT NULL ;
SELECT pg_terminate_backend(wait_application_pid('DB Scheduler Test'));

SELECT ts_bgw_db_scheduler_test_wait_for_scheduler_finish();

SELECT job_id, last_finish, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;

SELECT * FROM bgw_log;

--After a SIGTERM to scheduler and jobs, the jobs are considered crashed and there is a imposed wait of 5 min before a job can be run.
--See that there is no run again because of the crash-imposed wait (not run with the 10ms retry_period)
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(500);
SELECT job_id, last_finish, next_start, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;

--But after the 5 min period the job is again run
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(400000);
SELECT job_id, last_finish, next_start, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat;
SELECT * FROM bgw_log;


--
-- Test starting more jobs than availlable workers
--
\c single :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT ts_bgw_params_reset_time();
DELETE FROM _timescaledb_config.bgw_job;
--Our normal limit is 8 jobs (2 already taken up) so start 7 workers. Make the schedule_INTERVAL long and the retry period short so that the
--retries happen within the scheduler run time but everything only runs once.
INSERT INTO _timescaledb_config.bgw_job (application_name, job_type, schedule_INTERVAL, max_runtime, max_retries, retry_period) VALUES
('test_job_3_long_1', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '10ms'),
('test_job_3_long_2', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '10ms'),
('test_job_3_long_3', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '10ms'),
('test_job_3_long_4', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '10ms'),
('test_job_3_long_5', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '10ms'),
('test_job_3_long_6', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '10ms'),
('test_job_3_long_7', 'bgw_test_job_3_long', INTERVAL '5000ms', INTERVAL '100s', 3, INTERVAL '10ms');

\c single :ROLE_DEFAULT_PERM_USER

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(500);
SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes, consecutive_crashes
FROM _timescaledb_internal.bgw_job_stat
ORDER BY job_id;

SELECT * FROM bgw_log WHERE application_name = 'DB Scheduler' ORDER BY mock_time, application_name, msg_no;

SELECT ts_bgw_params_destroy();
