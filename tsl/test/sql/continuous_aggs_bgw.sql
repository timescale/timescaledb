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

-- Remove any default jobs, e.g., telemetry
SELECT _timescaledb_internal.stop_background_workers();
DELETE FROM _timescaledb_config.bgw_job WHERE TRUE;
TRUNCATE _timescaledb_internal.bgw_job_stat;
SELECT _timescaledb_internal.start_background_workers();

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
CREATE VIEW test_continuous_agg_view
    WITH ( timescaledb.continuous)
    AS SELECT time_bucket('2', time), SUM(data) as value
        FROM test_continuous_agg_table
        GROUP BY 1;

-- even before running, stats shows something
SELECT view_name, completed_threshold, invalidation_threshold, job_status, last_run_duration
    FROM timescaledb_information.continuous_aggregate_stats;

SELECT id as raw_table_id FROM _timescaledb_catalog.hypertable WHERE table_name='test_continuous_agg_table' \gset

-- min distance from end should be 1
SELECT  mat_hypertable_id, user_view_schema, user_view_name, bucket_width, job_id, refresh_lag FROM _timescaledb_catalog.continuous_agg;
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
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:job_id;

--alter the refresh interval and check if next_scheduled_run is altered
ALTER VIEW test_continuous_agg_view SET(timescaledb.refresh_interval= '1h');
SELECT view_name, 
case when next_scheduled_run - now() > '59 min'::interval 
      and  next_scheduled_run - now() < '60 min'::interval then 'Success'
     else 'Fail'
end 
 from 
timescaledb_information.continuous_aggregate_stats;

-- data before 8
SELECT * FROM test_continuous_agg_view ORDER BY 1;

-- fast restart test
SELECT ts_bgw_params_reset_time();

DROP VIEW test_continuous_agg_view CASCADE;

CREATE VIEW test_continuous_agg_view
    WITH (timescaledb.continuous,
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
GRANT SELECT, TRIGGER ON test_continuous_agg_table_w_grant TO public;
INSERT INTO test_continuous_agg_table_w_grant
    SELECT i, i FROM
        (SELECT generate_series(0, 10) as i) AS j;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2

-- make sure view can be created
CREATE VIEW test_continuous_agg_view_user_2
    WITH ( timescaledb.continuous,
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
