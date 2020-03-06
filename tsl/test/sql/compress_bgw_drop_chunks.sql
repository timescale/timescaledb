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
-- test drop chunks policy runs for compressed hypertables --
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

SELECT show_chunks('test_drop_chunks_table');

SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(1000000);

SELECT show_chunks('test_drop_chunks_table');

--make sure same # of compressed and uncompressed chunks after policy, reduced by 2
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test_drop_chunks_table';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test_drop_chunks_table';
