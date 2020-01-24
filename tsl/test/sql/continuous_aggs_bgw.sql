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

CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_destroy() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_reset_time(set_time BIGINT = 0, wait BOOLEAN = false) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

--test that this all works under the community license
ALTER DATABASE :TEST_DBNAME SET timescaledb.license_key='Community';

--create a function with no permissions to execute

CREATE FUNCTION get_constant_no_perms() RETURNS INTEGER LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT 10;
$BODY$;
REVOKE EXECUTE ON FUNCTION get_constant_no_perms() FROM PUBLIC;

\set WAIT_ON_JOB 0
\set IMMEDIATELY_SET_UNTIL 1
\set WAIT_FOR_OTHER_TO_ADVANCE 2

CREATE OR REPLACE FUNCTION ts_bgw_params_mock_wait_returns_immediately(new_val INTEGER) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

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
    SELECT * FROM bgw_log ORDER BY mock_time, application_name COLLATE "C", msg_no;

CREATE TABLE public.bgw_dsm_handle_store(
    handle BIGINT
);

INSERT INTO public.bgw_dsm_handle_store VALUES (0);
SELECT ts_bgw_params_create();

SELECT * FROM _timescaledb_config.bgw_job;
SELECT * FROM timescaledb_information.policy_stats;
SELECT * FROM _timescaledb_catalog.continuous_agg;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE test_continuous_agg_table(time int, data int);
SELECT create_hypertable('test_continuous_agg_table', 'time', chunk_time_interval => 10);
CREATE OR REPLACE FUNCTION integer_now_test() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM test_continuous_agg_table $$;
SELECT set_integer_now_func('test_continuous_agg_table', 'integer_now_test');
CREATE VIEW test_continuous_agg_view
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2', time), SUM(data) as value
        FROM test_continuous_agg_table
        GROUP BY 1;

-- even before running, stats shows something
SELECT view_name, completed_threshold, invalidation_threshold, job_status, last_run_duration
    FROM timescaledb_information.continuous_aggregate_stats;

SELECT id as raw_table_id FROM _timescaledb_catalog.hypertable WHERE table_name='test_continuous_agg_table' \gset

-- min distance from end should be 1
SELECT  mat_hypertable_id, user_view_schema, user_view_name, bucket_width, job_id, refresh_lag, ignore_invalidation_older_than FROM _timescaledb_catalog.continuous_agg;
SELECT job_id FROM _timescaledb_catalog.continuous_agg \gset

-- job was created
SELECT * FROM _timescaledb_config.bgw_job where id=:job_id;

-- create 10 time buckets
INSERT INTO test_continuous_agg_table
    SELECT i, i FROM
        (SELECT generate_series(0, 10) as i) AS j;

-- no stats
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    ORDER BY job_id;

-- no data in view
SELECT * FROM test_continuous_agg_view ORDER BY 1;

-- run first time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:job_id;

-- job ran once, successfully
SELECT job_id, next_start, last_finish, next_start-last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:job_id;

--clear log for next run of scheduler.
TRUNCATE public.bgw_log;

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

CREATE FUNCTION wait_for_job_to_run(job_param_id INTEGER, expected_runs INTEGER, spins INTEGER=:TEST_SPINWAIT_ITERS) RETURNS BOOLEAN LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	num_runs INTEGER;
BEGIN
	FOR i in 1..spins
	LOOP
	SELECT total_successes FROM _timescaledb_internal.bgw_job_stat WHERE job_id=job_param_id INTO num_runs;
	if (num_runs = expected_runs) THEN
		RETURN true;
    ELSEIF (num_runs > expected_runs) THEN
        RAISE 'num_runs > expected';
	ELSE
		PERFORM pg_sleep(0.1);
	END IF;
	END LOOP;
	RETURN false;
END
$BODY$;

--make sure there is 1 job to start with
SELECT wait_for_job_to_run(:job_id, 1);


SELECT ts_bgw_params_mock_wait_returns_immediately(:WAIT_FOR_OTHER_TO_ADVANCE);

--start the scheduler on 0 time
SELECT ts_bgw_params_reset_time(0, true);
SELECT ts_bgw_db_scheduler_test_run(extract(epoch from interval '24 hour')::int * 1000, 0);
SELECT wait_for_timer_to_run(0);

--advance to 12:00 so that it runs one more time; now we know the
--scheduler has loaded up the job with the old schedule_interval
SELECT ts_bgw_params_reset_time(extract(epoch from interval '12 hour')::bigint * 1000000, true);
SELECT wait_for_job_to_run(:job_id, 2);

--advance clock 1us to make the scheduler realize the job is done
SELECT ts_bgw_params_reset_time((extract(epoch from interval '12 hour')::bigint * 1000000)+1, true);

--alter the refresh interval and check if next_scheduled_run is altered
ALTER VIEW test_continuous_agg_view SET(timescaledb.refresh_interval= '1m');
SELECT job_id, next_start- last_finish as until_next, total_runs
FROM _timescaledb_internal.bgw_job_stat
WHERE job_id=:job_id;;

--advance to 12:02, job should have run at 12:01
SELECT ts_bgw_params_reset_time((extract(epoch from interval '12 hour')::bigint * 1000000)+(extract(epoch from interval '2 minute')::bigint * 1000000), true);
SELECT wait_for_job_to_run(:job_id, 3);

--next run in 1 minute
SELECT job_id, next_start-last_finish as until_next, total_runs
FROM _timescaledb_internal.bgw_job_stat
WHERE job_id=:job_id;

--change next run to be after 30s instead
SELECT (next_start - '30s'::interval) AS "NEW_NEXT_START"
FROM _timescaledb_internal.bgw_job_stat
WHERE job_id=:job_id \gset
SELECT alter_job_schedule(:job_id, next_start => :'NEW_NEXT_START');

SELECT ts_bgw_params_reset_time((extract(epoch from interval '12 hour')::bigint * 1000000)+(extract(epoch from interval '2 minute 30 seconds')::bigint * 1000000), true);
SELECT wait_for_job_to_run(:job_id, 4);

--advance clock to quit scheduler
SELECT ts_bgw_params_reset_time(extract(epoch from interval '25 hour')::bigint * 1000000, true);
select ts_bgw_db_scheduler_test_wait_for_scheduler_finish();


SELECT ts_bgw_params_mock_wait_returns_immediately(:WAIT_ON_JOB);
TRUNCATE public.bgw_log;

-- data before 8
SELECT * FROM test_continuous_agg_view ORDER BY 1;

-- fast restart test
SELECT ts_bgw_params_reset_time();

DROP VIEW test_continuous_agg_view CASCADE;

CREATE VIEW test_continuous_agg_view
    WITH (timescaledb.continuous,
        timescaledb.materialized_only=true,
        timescaledb.max_interval_per_job='2',
        timescaledb.refresh_lag='-2')
    AS SELECT time_bucket('2', time), SUM(data) as value
        FROM test_continuous_agg_table
        GROUP BY 1;

SELECT job_id FROM _timescaledb_catalog.continuous_agg \gset

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT * FROM sorted_bgw_log;

-- job ran once, successfully
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:job_id;

-- data at 0
SELECT * FROM test_continuous_agg_view ORDER BY 1;

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25, 25);

SELECT * FROM sorted_bgw_log;

-- job ran again, fast restart
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:job_id;

-- data at 2
SELECT * FROM test_continuous_agg_view ORDER BY 1;

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25, 25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:job_id;

-- job ran again, fast restart
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:job_id;

-- data at 4
SELECT * FROM test_continuous_agg_view ORDER BY 1;

\x on
--check the information views --
select view_name, view_owner, refresh_lag, refresh_interval, max_interval_per_job, materialization_hypertable
from timescaledb_information.continuous_aggregates
where view_name::text like '%test_continuous_agg_view';

select view_name, view_definition from timescaledb_information.continuous_aggregates
where view_name::text like '%test_continuous_agg_view';

select view_name, completed_threshold, invalidation_threshold, job_status, last_run_duration from timescaledb_information.continuous_aggregate_stats where view_name::text like '%test_continuous_agg_view';

\x off

DROP VIEW test_continuous_agg_view CASCADE;

--create a view with a function that it has no permission to execute
CREATE VIEW test_continuous_agg_view
    WITH (timescaledb.continuous,
        timescaledb.materialized_only=true,
        timescaledb.max_interval_per_job='2',
        timescaledb.refresh_lag='-2')
    AS SELECT time_bucket('2', time), SUM(data) as value, get_constant_no_perms()
        FROM test_continuous_agg_table
        GROUP BY 1;

SELECT job_id FROM _timescaledb_catalog.continuous_agg ORDER BY job_id desc limit 1 \gset

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

-- job fails
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:job_id;


--
-- Test creating continuous aggregate with a user that is the non-owner of the raw table
--
CREATE TABLE test_continuous_agg_table_w_grant(time int, data int);
SELECT create_hypertable('test_continuous_agg_table_w_grant', 'time', chunk_time_interval => 10);
CREATE OR REPLACE FUNCTION integer_now_test1() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM test_continuous_agg_table_w_grant $$;
SELECT set_integer_now_func('test_continuous_agg_table_w_grant', 'integer_now_test1');
GRANT SELECT, TRIGGER ON test_continuous_agg_table_w_grant TO public;
INSERT INTO test_continuous_agg_table_w_grant
    SELECT i, i FROM
        (SELECT generate_series(0, 10) as i) AS j;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2

-- make sure view can be created
CREATE VIEW test_continuous_agg_view_user_2
    WITH ( timescaledb.continuous,
        timescaledb.materialized_only=true,
        timescaledb.max_interval_per_job='2',
        timescaledb.refresh_lag='-2')
    AS SELECT time_bucket('2', time), SUM(data) as value
        FROM test_continuous_agg_table_w_grant
        GROUP BY 1;

SELECT job_id FROM _timescaledb_catalog.continuous_agg ORDER BY job_id desc limit 1 \gset

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:job_id;

--view is populated
SELECT * FROM test_continuous_agg_view_user_2;


\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
--revoke permissions from the continuous agg view owner to select from raw table
--no further updates to cont agg should happen
REVOKE SELECT ON test_continuous_agg_table_w_grant FROM public;

INSERT INTO test_continuous_agg_table_w_grant VALUES(1,1);

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25, 25);

--should show a failing execution because no longer has permissions (due to lack of permission on partial view owner's part)
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:job_id;

--view was NOT updated; but the old stuff is still there
SELECT * FROM test_continuous_agg_view_user_2;
