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

\x
--check the information views --
select view_name, view_owner, refresh_lag, refresh_interval, max_interval_per_job, materialization_hypertable
from timescaledb_information.continuous_aggregates
where view_name::text like '%test_continuous_agg_view';

select view_name, view_definition from timescaledb_information.continuous_aggregates
where view_name::text like '%test_continuous_agg_view';

select view_name, completed_threshold, invalidation_threshold, job_status, last_run_duration from timescaledb_information.continuous_aggregate_stats where view_name::text like '%test_continuous_agg_view';
