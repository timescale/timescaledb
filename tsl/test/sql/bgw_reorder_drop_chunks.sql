-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
-- Setup
--
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(timeout INT = -1) RETURNS VOID
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
DELETE FROM _timescaledb_config.bgw_job;
TRUNCATE _timescaledb_internal.bgw_job_stat;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE public.bgw_log(
    msg_no INT,
    mock_time BIGINT,
    application_name TEXT,
    msg TEXT
);

CREATE VIEW sorted_bgw_log AS
SELECT msg_no,
  mock_time,
  application_name,
  CASE WHEN length(msg) > 80 THEN
    substring(msg, 1, 80) || '...'
  ELSE
    msg
  END AS msg
FROM bgw_log
ORDER BY mock_time,
  application_name COLLATE "C",
  msg_no;
CREATE TABLE public.bgw_dsm_handle_store(
    handle BIGINT
);

INSERT INTO public.bgw_dsm_handle_store VALUES (0);
SELECT ts_bgw_params_create();

SELECT * FROM _timescaledb_config.bgw_job;
SELECT * FROM timescaledb_information.job_stats;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

------------------------------
-- test reorder policy runs --
------------------------------

CREATE TABLE test_reorder_table(time int, chunk_id int);
SELECT create_hypertable('test_reorder_table', 'time', chunk_time_interval => 1);

-- These inserts should create 5 different chunks
INSERT INTO test_reorder_table VALUES (1, 1);
INSERT INTO test_reorder_table VALUES (2, 2);
INSERT INTO test_reorder_table VALUES (3, 3);
INSERT INTO test_reorder_table VALUES (4, 4);
INSERT INTO test_reorder_table VALUES (5, 5);

SELECT COUNT(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_reorder_table';

SELECT count(*) FROM _timescaledb_config.bgw_job WHERE proc_schema = '_timescaledb_internal' AND proc_name = 'policy_reorder';
select add_reorder_policy('test_reorder_table', 'test_reorder_table_time_idx') as reorder_job_id \gset
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE proc_schema = '_timescaledb_internal' AND proc_name = 'policy_reorder';

-- job was created
SELECT * FROM timescaledb_information.jobs WHERE job_id=:reorder_job_id;

-- no stats
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    ORDER BY job_id;

-- nothing clustered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

-- run first time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM timescaledb_information.jobs WHERE job_id=:reorder_job_id;

-- job ran once, successfully
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- first chunk reordered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

-- second call to scheduler should immediately run reorder again, due to catchup
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM timescaledb_information.jobs WHERE job_id=:reorder_job_id;

-- two runs
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- two chunks clustered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

-- third call to scheduler should immediately run reorder again, due to catchup
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50);

-- job info is gone

SELECT * FROM sorted_bgw_log;

SELECT * FROM timescaledb_information.jobs WHERE job_id=:reorder_job_id;

SELECT *
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- three chunks clustered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;


-- running is a nop
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(100);

SELECT * FROM sorted_bgw_log;

SELECT * FROM timescaledb_information.jobs WHERE job_id=:reorder_job_id;

SELECT *
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- still have 3 chunks clustered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

--check that views work correctly
SELECT * FROM timescaledb_information.job_stats;

-- test deleting the policy
SELECT remove_reorder_policy('test_reorder_table');

SELECT * FROM timescaledb_information.jobs WHERE job_id=:reorder_job_id;

SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(125);

SELECT * FROM sorted_bgw_log;

SELECT * FROM timescaledb_information.jobs WHERE job_id=:reorder_job_id;

-- still only 3 chunks clustered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;



\c :TEST_DBNAME :ROLE_SUPERUSER
TRUNCATE bgw_log;
TRUNCATE _timescaledb_internal.bgw_job_stat;
DELETE FROM _timescaledb_config.bgw_job;
SELECT ts_bgw_params_reset_time();
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-----------------------------------
-- test drop chunnks policy runs --
-----------------------------------


CREATE TABLE test_drop_chunks_table(time timestamptz, drop_order int);
SELECT create_hypertable('test_drop_chunks_table', 'time', chunk_time_interval => INTERVAL '1 week');

-- These inserts should create 5 different chunks
INSERT INTO test_drop_chunks_table VALUES (now() - INTERVAL '2 month',  4);
INSERT INTO test_drop_chunks_table VALUES (now(),                       5);
INSERT INTO test_drop_chunks_table VALUES (now() - INTERVAL '6 months', 2);
INSERT INTO test_drop_chunks_table VALUES (now() - INTERVAL '4 months', 3);
INSERT INTO test_drop_chunks_table VALUES (now() - INTERVAL '8 months', 1);


SELECT show_chunks('test_drop_chunks_table');
SELECT COUNT(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_drop_chunks_table';

SELECT count(*) FROM _timescaledb_config.bgw_job WHERE proc_schema = '_timescaledb_internal' AND proc_name = 'policy_retention';
SELECT add_retention_policy('test_drop_chunks_table', INTERVAL '4 months') as drop_chunks_job_id \gset
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE proc_schema = '_timescaledb_internal' AND proc_name = 'policy_retention';

SELECT alter_job(:drop_chunks_job_id, schedule_interval => INTERVAL '1 second');

SELECT * FROM timescaledb_information.jobs WHERE job_id=:drop_chunks_job_id;

-- no stats
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    WHERE job_id=:drop_chunks_job_id;

-- all chunks are there
SELECT show_chunks('test_drop_chunks_table');

-- run first time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM timescaledb_information.jobs WHERE job_id=:drop_chunks_job_id;

-- job ran once, successfully
SELECT job_id, time_bucket('1m',next_start) AS next_start, time_bucket('1m',last_finish) as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:drop_chunks_job_id;

-- chunks 8 and 10 dropped
SELECT show_chunks('test_drop_chunks_table');

-- job doesn't run again immediately
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM timescaledb_information.jobs WHERE job_id=:drop_chunks_job_id;

-- still only 1 run
SELECT job_id, time_bucket('1m',next_start) AS next_start, time_bucket('1m',last_finish) as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:drop_chunks_job_id;

-- same chunks
SELECT show_chunks('test_drop_chunks_table');

-- a new chunk older than the drop date will be dropped
INSERT INTO test_drop_chunks_table VALUES (now() - INTERVAL '12 months', 0);

SELECT show_chunks('test_drop_chunks_table');

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(10000);

SELECT * FROM sorted_bgw_log;

SELECT * FROM timescaledb_information.jobs WHERE job_id=:drop_chunks_job_id;

-- 2 runs
SELECT job_id, time_bucket('1m',next_start) AS next_start, time_bucket('1m',last_finish) as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:drop_chunks_job_id;

SELECT show_chunks('test_drop_chunks_table');

--test that views work
SELECT * FROM timescaledb_information.job_stats;
