-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/rand_generator.sql

CREATE TABLE test1 ("Time" timestamptz, i integer, b bigint, t text);
SELECT table_name from create_hypertable('test1', 'Time', chunk_time_interval=> INTERVAL '1 day');

INSERT INTO test1 SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') t;

ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = '"Time" DESC');

SELECT
  $$
  SELECT * FROM test1 ORDER BY "Time"
  $$ AS "QUERY" \gset

SELECT 'test1' AS "HYPERTABLE_NAME" \gset

\ir include/compression_test_hypertable.sql
