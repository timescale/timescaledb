-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
-- Setup
--
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(timeout INT = -1) RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

\set WAIT_ON_JOB 0
\set IMMEDIATELY_SET_UNTIL 1
\set WAIT_FOR_OTHER_TO_ADVANCE 2

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

-----------------------------------
-- test retention policy runs for compressed hypertables --
-----------------------------------

CREATE TABLE test_retention_table(time timestamptz, drop_order int);
SELECT create_hypertable('test_retention_table', 'time', chunk_time_interval => INTERVAL '1 week');

-- These inserts should create 5 different chunks
INSERT INTO test_retention_table VALUES (now() - INTERVAL '2 month',  4);
INSERT INTO test_retention_table VALUES (now(),                       5);
INSERT INTO test_retention_table VALUES (now() - INTERVAL '6 months', 2);
INSERT INTO test_retention_table VALUES (now() - INTERVAL '4 months', 3);
INSERT INTO test_retention_table VALUES (now() - INTERVAL '8 months', 1);

SELECT show_chunks('test_retention_table');
SELECT COUNT(*) FROM _timescaledb_catalog.chunk as c, _timescaledb_catalog.hypertable as ht where c.hypertable_id = ht.id and ht.table_name='test_retention_table';

SELECT count(*) FROM _timescaledb_config.bgw_job WHERE proc_schema = '_timescaledb_internal' AND proc_name = 'policy_retention';
SELECT add_retention_policy('test_retention_table', INTERVAL '4 months') as retention_job_id \gset
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE proc_schema = '_timescaledb_internal' AND proc_name = 'policy_retention';
SELECT alter_job(:retention_job_id, schedule_interval => INTERVAL '1 second');
SELECT * FROM _timescaledb_config.bgw_job where id=:retention_job_id;

--turn on compression and compress all chunks
ALTER TABLE test_retention_table set (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');
SELECT count(compress_chunk(chunk.schema_name|| '.' || chunk.table_name)) as count_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test_retention_table' and chunk.compressed_chunk_id IS NULL;

--make sure same # of compressed and uncompressed chunks before policy
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test_retention_table';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test_retention_table';

SELECT show_chunks('test_retention_table');

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(1000000);

SELECT show_chunks('test_retention_table');

--make sure same # of compressed and uncompressed chunks after policy, reduced by 2
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test_retention_table';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test_retention_table';

------------------------------
-- Test reorder policy runs on compressed tables. Reorder policy job must skip compressed chunks 
-- (see issue https://github.com/timescale/timescaledb/issues/1810).
-- More tests for reorder policy can be found at bgw_reorder_drop_chunks.sql
------------------------------

CREATE TABLE test_reorder_chunks_table(time int not null, chunk_id int);
CREATE INDEX test_reorder_chunks_table_time_idx ON test_reorder_chunks_table(time);
SELECT create_hypertable('test_reorder_chunks_table', 'time', chunk_time_interval => 1);

-- These inserts should create 6 different chunks
INSERT INTO test_reorder_chunks_table VALUES (1, 1);
INSERT INTO test_reorder_chunks_table VALUES (2, 2);
INSERT INTO test_reorder_chunks_table VALUES (3, 3);
INSERT INTO test_reorder_chunks_table VALUES (4, 4);
INSERT INTO test_reorder_chunks_table VALUES (5, 5);
INSERT INTO test_reorder_chunks_table VALUES (6, 6);

-- Enable compression
ALTER TABLE test_reorder_chunks_table set (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');

-- Compress 2 chunks:
SELECT compress_chunk(show_chunks('test_reorder_chunks_table', newer_than => 2, older_than => 4));

-- make sure we have total of 6 chunks:
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test_reorder_chunks_table';

-- and 2 compressed ones:
SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test_reorder_chunks_table';

-- enable reorder policy
SELECT add_reorder_policy('test_reorder_chunks_table', 'test_reorder_chunks_table_time_idx') AS reorder_job_id \gset

-- nothing is clustered yet
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

-- run first time
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- first chunk reordered
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

-- second call to scheduler
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(25);

SELECT job_id, last_run_success, total_runs, total_successes, total_failures, total_crashes
    FROM _timescaledb_internal.bgw_job_stat
    where job_id=:reorder_job_id;

-- two chunks clustered, skips the compressed chunks
SELECT indexrelid::regclass, indisclustered
    FROM pg_index
    WHERE indisclustered = true ORDER BY 1;

