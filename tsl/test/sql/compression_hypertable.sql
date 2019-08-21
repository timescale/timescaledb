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

--add test for altered hypertable
CREATE TABLE test2 ("Time" timestamptz, i integer, b bigint, t text);
SELECT table_name from create_hypertable('test2', 'Time', chunk_time_interval=> INTERVAL '1 day');

--create some chunks with the old column numbers
INSERT INTO test2 SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-04 1:00', '1 hour') t;

ALTER TABLE test2 DROP COLUMN b;
--add default a value
ALTER TABLE test2 ADD COLUMN c INT DEFAULT -15;
--add default NULL
ALTER TABLE test2 ADD COLUMN d INT;

--write to both old chunks and new chunks with different column #s
INSERT INTO test2 SELECT t, gen_rand_minstd(), gen_rand_minstd()::text, gen_rand_minstd(), gen_rand_minstd() FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-06 1:00', '1 hour') t;

ALTER TABLE test2 set (timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'c, "Time" DESC');

SELECT
  $$
  SELECT * FROM test2 ORDER BY c, "Time"
  $$ AS "QUERY" \gset

SELECT 'test2' AS "HYPERTABLE_NAME" \gset
\ir include/compression_test_hypertable.sql
