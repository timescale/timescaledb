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
SELECT * FROM timescaledb_information.drop_chunks_policies;
SELECT * FROM timescaledb_information.scheduled_index_policies;
SELECT * FROM timescaledb_information.policy_stats;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

---------------------------------
-- test scheduled_index policy --
---------------------------------

CREATE TABLE test_scheduled_index_table(time int, chunk_id int);
SELECT create_hypertable('test_scheduled_index_table', 'time', chunk_time_interval => 1, create_default_indexes => false);

-- no default index
SELECT * FROM test.show_indexes('test_scheduled_index_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- index only on parent
SELECT * FROM test.show_indexes('test_scheduled_index_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- These inserts should create 5 different chunks
INSERT INTO test_scheduled_index_table VALUES (1, 1);
INSERT INTO test_scheduled_index_table VALUES (2, 2);

CREATE INDEX ON test_scheduled_index_table(time) WITH (timescaledb.scheduled);

INSERT INTO test_scheduled_index_table VALUES (3, 3);
INSERT INTO test_scheduled_index_table VALUES (4, 4);
INSERT INTO test_scheduled_index_table VALUES (5, 5);


SELECT COUNT(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_scheduled_index_table';
SELECT * FROM _timescaledb_catalog.optional_index_info;

-- policy was created
SELECT * FROM _timescaledb_config.bgw_policy_scheduled_index;
SELECT job_id AS scheduled_index_job_id FROM _timescaledb_config.bgw_policy_scheduled_index LIMIT 1 \gset

-- job was created
SELECT * FROM _timescaledb_config.bgw_job where id=:scheduled_index_job_id;

-- no stats
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    ORDER BY job_id;

-- index only on parent
SELECT * FROM test.show_indexes('test_scheduled_index_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- run first time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:scheduled_index_job_id;

-- job ran once, successfully
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:scheduled_index_job_id;

-- first chunk scheduled_indexed
SELECT * FROM test.show_indexes('test_scheduled_index_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- second call to scheduler should immediately run scheduled_index again, due to catchup
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25, 25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:scheduled_index_job_id;

-- two runs
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:scheduled_index_job_id;

-- two chunks have indicies
SELECT * FROM test.show_indexes('test_scheduled_index_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

-- third call to scheduler should immediately run scheduled_index again, due to catchup
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50, 50);

-- job info is gone

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:scheduled_index_job_id;

SELECT *
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:scheduled_index_job_id;

-- three chunks have indicies
SELECT * FROM test.show_indexes('test_scheduled_index_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');


-- running is a nop
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(100, 100);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:scheduled_index_job_id;

SELECT *
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:scheduled_index_job_id;

-- still only 3 chunks have indicies
SELECT * FROM test.show_indexes('test_scheduled_index_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

--check that views work correctly
SELECT * FROM timescaledb_information.scheduled_index_policies;
SELECT * FROM timescaledb_information.policy_stats;

-- test deleting the policy
DROP INDEX test_scheduled_index_table_time_idx;

select * from _timescaledb_config.bgw_policy_scheduled_index where job_id=:scheduled_index_job_id;

SELECT * FROM _timescaledb_config.bgw_job where id=:scheduled_index_job_id;

SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:scheduled_index_job_id;

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(125, 125);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:scheduled_index_job_id;

-- no indexes
SELECT * FROM test.show_indexes('test_scheduled_index_table');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');

SELECT * FROM _timescaledb_catalog.optional_index_info;

\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
DELETE FROM _timescaledb_config.bgw_job;
SELECT ts_bgw_params_reset_time();
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
