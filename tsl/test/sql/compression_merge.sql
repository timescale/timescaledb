-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/rand_generator.sql
\c :TEST_DBNAME :ROLE_SUPERUSER
\ir include/compression_utils.sql
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE test1 ("Time" timestamptz, i integer, value integer);
SELECT table_name from create_hypertable('test1', 'Time', chunk_time_interval=> INTERVAL '1 hour');

-- This will generate 24 chunks
INSERT INTO test1 
SELECT t, i, gen_rand_minstd() 
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-03 0:59', '1 minute') t
CROSS JOIN generate_series(1, 5, 1) i;

-- Compression is set to merge those 24 chunks into 12 2 hour chunks
ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_segmentby='i', timescaledb.compress_orderby='"Time"', timescaledb.compress_chunk_time_interval='2 hours');

SELECT
  $$
  SELECT * FROM test1 ORDER BY i, "Time"
  $$ AS "QUERY" \gset

SELECT 'test1' AS "HYPERTABLE_NAME" \gset
\ir include/compression_test_merge.sql
\set TYPE timestamptz
\set ORDER_BY_COL_NAME Time
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

DROP TABLE test1;

CREATE TABLE test2 ("Time" timestamptz, i integer, loc integer, value integer);
SELECT table_name from create_hypertable('test2', 'Time', chunk_time_interval=> INTERVAL '1 hour');

-- This will generate 24 1 hour chunks.
INSERT INTO test2 
SELECT t, i, gen_rand_minstd() 
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-03 0:59', '1 minute') t
CROSS JOIN generate_series(1, 5, 1) i;

-- Compression is set to merge those 24 chunks into 3 chunks, two 10 hour chunks and a single 4 hour chunk.
ALTER TABLE test2 set (timescaledb.compress, timescaledb.compress_segmentby='i', timescaledb.compress_orderby='loc,"Time"', timescaledb.compress_chunk_time_interval='10 hours');

SELECT
  $$
  SELECT * FROM test2 ORDER BY i, "Time"
  $$ AS "QUERY" \gset

SELECT 'test2' AS "HYPERTABLE_NAME" \gset
\ir include/compression_test_merge.sql
\set TYPE integer
\set ORDER_BY_COL_NAME loc
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

DROP TABLE test2;

CREATE TABLE test3 ("Time" timestamptz, i integer, loc integer, value integer);
SELECT table_name from create_hypertable('test3', 'Time', chunk_time_interval=> INTERVAL '1 hour');
SELECT table_name from  add_dimension('test3', 'i', number_partitions=> 3);

-- This will generate 25 1 hour chunks with a closed space dimension.
INSERT INTO test3 SELECT t, 1, gen_rand_minstd(), gen_rand_minstd() FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-02 12:59', '1 minute') t;
INSERT INTO test3 SELECT t, 2, gen_rand_minstd(), gen_rand_minstd() FROM generate_series('2018-03-02 13:00'::TIMESTAMPTZ, '2018-03-03 0:59', '1 minute') t;
INSERT INTO test3 SELECT t, 3, gen_rand_minstd(), gen_rand_minstd() FROM generate_series('2018-03-02 2:00'::TIMESTAMPTZ, '2018-03-02 2:01', '1 minute') t;

-- Compression is set to merge those 25 chunks into 12 2 hour chunks and a single 1 hour chunks on a different space dimensions.
ALTER TABLE test3 set (timescaledb.compress, timescaledb.compress_orderby='loc,"Time"', timescaledb.compress_chunk_time_interval='2 hours');

SELECT
  $$
  SELECT * FROM test3 WHERE i = 1 ORDER BY "Time"
  $$ AS "QUERY" \gset

SELECT 'test3' AS "HYPERTABLE_NAME" \gset
\ir include/compression_test_merge.sql

\set TYPE integer
\set ORDER_BY_COL_NAME loc
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

DROP TABLE test3;

CREATE TABLE test4 ("Time" timestamptz, i integer, loc integer, value integer);
SELECT table_name from create_hypertable('test4', 'Time', chunk_time_interval=> INTERVAL '1 hour');
-- Setting compress_chunk_time_interval to non-multiple of chunk_time_interval should emit a warning.
ALTER TABLE test4 set (timescaledb.compress, timescaledb.compress_orderby='loc,"Time"', timescaledb.compress_chunk_time_interval='90 minutes');

DROP TABLE test4;

CREATE TABLE test5 ("Time" timestamptz, i integer, value integer);
SELECT table_name from create_hypertable('test5', 'Time', chunk_time_interval=> INTERVAL '1 hour');

-- This will generate 24 chunks. Note that we are forcing everyting into a single segment so we use table scan to when merging chunks.
INSERT INTO test5 SELECT t, 1, gen_rand_minstd() FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-03 0:59', '1 minute') t;

-- Compression is set to merge those 24 chunks into 1 24 hour chunk
ALTER TABLE test5 set (timescaledb.compress, timescaledb.compress_segmentby='i', timescaledb.compress_orderby='"Time"', timescaledb.compress_chunk_time_interval='24 hours');

SELECT compress_chunk(i) FROM show_chunks('test5') i LIMIT 1;

SELECT schemaname || '.' || indexname AS "INDEXNAME"
FROM pg_indexes i
INNER JOIN _timescaledb_catalog.chunk cc ON i.schemaname = cc.schema_name and i.tablename = cc.table_name 
INNER JOIN _timescaledb_catalog.chunk c ON (cc.id = c.compressed_chunk_id) 
LIMIT 1 \gset

DROP INDEX :INDEXNAME;

-- We dropped the index from compressed chunk thats needed to determine sequence numbers
-- during merge, merging will fallback to doing heap scans and work just fine.
SELECT
  $$
  SELECT * FROM test5 ORDER BY i, "Time"
  $$ AS "QUERY" \gset

SELECT 'test5' AS "HYPERTABLE_NAME" \gset
\ir include/compression_test_merge.sql

DROP TABLE test5;

CREATE TABLE test6 ("Time" timestamptz, i integer, value integer);
SELECT table_name from create_hypertable('test6', 'Time', chunk_time_interval=> INTERVAL '1 hour');

-- This will generate 24 chunks
INSERT INTO test6 
SELECT t, i, gen_rand_minstd() 
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-03 0:59', '1 minute') t
CROSS JOIN generate_series(1, 5, 1) i;

-- Compression is set to merge those 24 chunks into 12 2 hour chunks.
ALTER TABLE test6 set (timescaledb.compress, timescaledb.compress_segmentby='i', timescaledb.compress_orderby='"Time"', timescaledb.compress_chunk_time_interval='2 hours');

SELECT compress_chunk(i) FROM show_chunks('test6') i;
SELECT 12 as expected_number_of_chunks, count(*) as number_of_chunks FROM show_chunks('test6');
SELECT decompress_chunk(i) FROM show_chunks('test6') i;

-- Altering compress chunk time interval will cause us to create 6 chunks instead of 12.
ALTER TABLE test6 set (timescaledb.compress, timescaledb.compress_segmentby='i', timescaledb.compress_orderby='"Time"', timescaledb.compress_chunk_time_interval='4 hours');

SELECT compress_chunk(i, true) FROM show_chunks('test6') i;
SELECT 6 as expected_number_of_chunks, count(*) as number_of_chunks FROM show_chunks('test6');

DROP TABLE test6;

CREATE TABLE test7 ("Time" timestamptz, i integer, j integer, k integer, value integer);
SELECT table_name from create_hypertable('test7', 'Time', chunk_time_interval=> INTERVAL '1 hour');

-- This will generate 24 chunks
INSERT INTO test7 
SELECT t, i, gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()  
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-03 0:59', '1 minute') t
CROSS JOIN generate_series(1, 5, 1) i;

-- Compression is set to merge those 24 chunks into 12 2 hour chunks with ordering by j column before time column, causing recompression to occur during merge.
ALTER TABLE test7 set (timescaledb.compress, timescaledb.compress_segmentby='i', timescaledb.compress_orderby='j, "Time" desc', timescaledb.compress_chunk_time_interval='2 hours');

SELECT
  $$
  SELECT * FROM test7 ORDER BY i, "Time"
  $$ AS "QUERY" \gset

SELECT 'test7' AS "HYPERTABLE_NAME" \gset
\ir include/compression_test_merge.sql

DROP TABLE test7;
