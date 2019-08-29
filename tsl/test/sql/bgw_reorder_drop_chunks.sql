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
SELECT * FROM timescaledb_information.reorder_policies;
SELECT * FROM timescaledb_information.policy_stats;

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

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_reorder_policies');
select add_reorder_policy('test_reorder_table', 'test_reorder_table_time_idx') as reorder_job_id \gset
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_reorder_policies');

-- policy was created
select * from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

-- job was created
SELECT * FROM _timescaledb_config.bgw_job where id=:reorder_job_id;

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

SELECT * FROM _timescaledb_config.bgw_job where id=:reorder_job_id;

-- job ran once, successfully
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- first chunk reordered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

-- second call to scheduler should immediately run reorder again, due to catchup
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25, 25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:reorder_job_id;

-- two runs
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- two chunks clustered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

-- third call to scheduler should immediately run reorder again, due to catchup
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(50, 50);

-- job info is gone

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:reorder_job_id;

SELECT *
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- three chunks clustered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;


-- running is a nop
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(100, 100);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:reorder_job_id;

SELECT *
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- still have 3 chunks clustered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

--check that views work correctly
SELECT * FROM timescaledb_information.reorder_policies;
SELECT * FROM timescaledb_information.policy_stats;

-- test deleting the policy
SELECT remove_reorder_policy('test_reorder_table');

select * from _timescaledb_config.bgw_policy_reorder where job_id=:reorder_job_id;

SELECT * FROM _timescaledb_config.bgw_job where id=:reorder_job_id;

SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(125, 125);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:reorder_job_id;

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

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_drop_chunks_policies');
SELECT add_drop_chunks_policy('test_drop_chunks_table', INTERVAL '4 months') as drop_chunks_job_id \gset
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_drop_chunks_policies');

SELECT alter_job_schedule(:drop_chunks_job_id, schedule_interval => INTERVAL '1 second');

select * from _timescaledb_config.bgw_policy_drop_chunks where job_id=:drop_chunks_job_id;

SELECT * FROM _timescaledb_config.bgw_job where id=:drop_chunks_job_id;

-- no stats
SELECT job_id, next_start, last_finish as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    WHERE job_id=:drop_chunks_job_id;

-- all chunks are there
SELECT show_chunks('test_drop_chunks_table');

-- run first time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:drop_chunks_job_id;

-- job ran once, successfully
SELECT job_id, time_bucket('1m',next_start) AS next_start, time_bucket('1m',last_finish) as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:drop_chunks_job_id;

-- chunks 8 and 10 dropped
SELECT show_chunks('test_drop_chunks_table');

-- job doesn't run again immediately
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25, 25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:drop_chunks_job_id;

-- still only 1 run
SELECT job_id, time_bucket('1m',next_start) AS next_start, time_bucket('1m',last_finish) as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:drop_chunks_job_id;

-- same chunks
SELECT show_chunks('test_drop_chunks_table');

-- a new chunk older than the drop date will be dropped
INSERT INTO test_drop_chunks_table VALUES (now() - INTERVAL '12 months', 0);

SELECT show_chunks('test_drop_chunks_table');

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(10000, 25);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:drop_chunks_job_id;

-- 2 runs
SELECT job_id, time_bucket('1m',next_start) AS next_start, time_bucket('1m',last_finish) as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:drop_chunks_job_id;

SELECT show_chunks('test_drop_chunks_table');

--test that views work
SELECT * FROM timescaledb_information.drop_chunks_policies;
SELECT * FROM timescaledb_information.policy_stats;

-- continuous aggregate blocks drop_chunks
INSERT INTO test_drop_chunks_table VALUES (now() - INTERVAL '12 months', 0);

CREATE VIEW tdc_view
  WITH (timescaledb.continuous)
  AS SELECT time_bucket('1 hour', time), count(drop_order)
     FROM test_drop_chunks_table
     GROUP BY 1;

SELECT show_chunks('test_drop_chunks_table');

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(10000, 10000);

SELECT * FROM sorted_bgw_log;

SELECT * FROM _timescaledb_config.bgw_job where id=:drop_chunks_job_id;

-- should now have a failure
SELECT job_id, time_bucket('1m',next_start) AS next_start, time_bucket('1m',last_finish) as until_next, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:drop_chunks_job_id;

SELECT show_chunks('test_drop_chunks_table');

--drop the view to allow drop chunks to work
DROP VIEW tdc_view CASCADE;

--turn on compression and compress all chunks
ALTER TABLE test_drop_chunks_table set (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');
SELECT count(compress_chunk(chunk.schema_name|| '.' || chunk.table_name)) as count_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test_drop_chunks_table' and chunk.compressed_chunk_id IS NULL;

--make sure same # of compressed and uncompressed chunks before policy
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test_drop_chunks_table';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test_drop_chunks_table';

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(100000, 10000);

--make sure same # of compressed and uncompressed chunks after policy, reduced by 1
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test_drop_chunks_table';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test_drop_chunks_table';
