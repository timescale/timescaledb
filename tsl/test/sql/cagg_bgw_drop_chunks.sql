-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
-- Setup for testing bgw jobs ---
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
-- test drop chunks policy runs for materialized hypertables created for
-- cont. aggregates
-----------------------------------
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE drop_chunks_table(time BIGINT, data INTEGER);
SELECT hypertable_id AS drop_chunks_table_nid
    FROM create_hypertable('drop_chunks_table', 'time', chunk_time_interval => 1) \gset

CREATE OR REPLACE FUNCTION integer_now_test2() returns bigint LANGUAGE SQL STABLE as $$ SELECT 40::bigint  $$;

SELECT set_integer_now_func('drop_chunks_table', 'integer_now_test2');

CREATE MATERIALIZED VIEW drop_chunks_view1 WITH (timescaledb.continuous)
AS SELECT time_bucket('5', time), max(data)
    FROM drop_chunks_table
    GROUP BY 1 WITH NO DATA;

--raw hypertable will have 40 chunks and the mat. hypertable will have 2 and 4
-- chunks respectively
SELECT set_chunk_time_interval('_timescaledb_internal._materialized_hypertable_2', 10);
\set ON_ERROR_STOP 0
INSERT INTO drop_chunks_table SELECT i, i FROM generate_series(1, 39) AS i;
\set ON_ERROR_STOP 1
CALL refresh_continuous_aggregate('drop_chunks_view1', NULL, NULL);

--TEST1  specify drop chunks policy on mat. hypertable by
-- directly does not work

\set ON_ERROR_STOP 0
SELECT add_retention_policy( '_timescaledb_internal._materialized_hypertable_2', drop_after => -50) as drop_chunks_job_id1 \gset
\set ON_ERROR_STOP 1

--TEST2  specify drop chunks policy on cont. aggregate
-- integer_now func on raw hypertable is used by the drop
-- chunks policy
SELECT hypertable_id, table_name, integer_now_func
FROM _timescaledb_catalog.dimension d,  _timescaledb_catalog.hypertable ht
WHERE ht.id = d.hypertable_id;

SELECT chunk_name, range_start_integer, range_end_integer
FROM timescaledb_information.chunks
WHERE hypertable_name = '_materialized_hypertable_2' ORDER BY range_start_integer;

SELECT add_retention_policy( 'drop_chunks_view1', drop_after => 10) as drop_chunks_job_id1 \gset
SELECT alter_job(:drop_chunks_job_id1, schedule_interval => INTERVAL '1 second');
SELECT ts_bgw_db_scheduler_test_run_and_wait_for_scheduler_finish(2000000);
SELECT count(c) from show_chunks('drop_chunks_view1') as c ;
SELECT remove_retention_policy('drop_chunks_view1');

\set ON_ERROR_STOP 0
SELECT remove_retention_policy('unknown');
SELECT remove_retention_policy('1');
\set ON_ERROR_STOP 1
