-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_internal.stop_background_workers();
SELECT _timescaledb_internal.enterprise_enabled();

CREATE OR REPLACE FUNCTION test_compress_chunks_policy(job_id INTEGER)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_auto_compress_chunks'
LANGUAGE C VOLATILE STRICT;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE conditions (
      time        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      location2    char(10)              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );
select create_hypertable( 'conditions', 'time', chunk_time_interval=> '31days'::interval);

--TEST 1--
--cannot set policy without enabling compression --
\set ON_ERROR_STOP 0
select add_compress_chunks_policy('conditions', '60d'::interval);
\set ON_ERROR_STOP 1

-- TEST2 --
--add a policy to compress chunks --
alter table conditions set (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'time');
insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 'klick', 55, 75;

select add_compress_chunks_policy('conditions', '60d'::interval);
select job_id as compressjob_id, hypertable_id, older_than from _timescaledb_config.bgw_policy_compress_chunks;
\gset
select * from _timescaledb_config.bgw_job where job_type like 'compress%';
select * from alter_job_schedule(:compressjob_id, schedule_interval=>'1s');
select * from _timescaledb_config.bgw_job where job_type like 'compress%';
insert into conditions
select now()::timestamp, 'TOK', 'sony', 55, 75;

-- TEST3 --
--only the old chunks will get compressed when policy is executed--
select test_compress_chunks_policy(:compressjob_id);
select hypertable_name, chunk_name, uncompressed_total_bytes, compressed_total_bytes from timescaledb_information.compressed_chunk_stats where compression_status like 'Compressed' order by chunk_name;

-- TEST 4 --
--cannot set another policy
\set ON_ERROR_STOP 0
select add_compress_chunks_policy('conditions', '60d'::interval, if_not_exists=>true);
select add_compress_chunks_policy('conditions', '60d'::interval);
select add_compress_chunks_policy('conditions', '30d'::interval, if_not_exists=>true);
\set ON_ERROR_STOP 1

--TEST 5 --
-- drop the policy --
select remove_compress_chunks_policy('conditions');
select job_id as compressjob_id, hypertable_id, older_than from _timescaledb_config.bgw_policy_compress_chunks;

--TEST 6 --
-- try to execute the policy after it has been dropped --
\set ON_ERROR_STOP 0
select test_compress_chunks_policy(:compressjob_id);
\set ON_ERROR_STOP 1

--TEST 7
--compress chunks policy for integer based partition hypertable
CREATE TABLE test_table_int(time bigint, val int);
SELECT create_hypertable('test_table_int', 'time', chunk_time_interval => 1);

create or replace function dummy_now() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::BIGINT';
select set_integer_now_func('test_table_int', 'dummy_now');
insert into test_table_int select generate_series(1,5), 10;
alter table test_table_int set (timescaledb.compress);
select add_compress_chunks_policy('test_table_int', 2::int);

select job_id as compressjob_id, hypertable_id, older_than from _timescaledb_config.bgw_policy_compress_chunks
where hypertable_id = (Select id from _timescaledb_catalog.hypertable where table_name like 'test_table_int');
\gset
select test_compress_chunks_policy(:compressjob_id);
select test_compress_chunks_policy(:compressjob_id);
select hypertable_name, chunk_name, uncompressed_total_bytes, compressed_total_bytes from timescaledb_information.compressed_chunk_stats
where hypertable_name::text like 'test_table_int'
and compression_status like 'Compressed'
order by chunk_name;

