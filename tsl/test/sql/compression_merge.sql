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

-- Verify we are fully recompressing unordered chunks
BEGIN;
  SELECT count(compress_chunk(chunk,  true)) FROM show_chunks('test2') chunk;
  SELECT format('%I.%I',ch.schema_name,ch.table_name) AS "CHUNK"
    FROM _timescaledb_catalog.chunk ch
    JOIN _timescaledb_catalog.hypertable ht ON ht.id=ch.hypertable_id
    JOIN _timescaledb_catalog.hypertable ht2 ON ht.id=ht2.compressed_hypertable_id AND ht2.table_name='test2' LIMIT 1 \gset

  -- Not using time as first column in orderby makes the merged chunks unordered
  -- We want to sure we are fully recompressing them which will make only
  -- a single batch per segment group
  SELECT count(*)
  FROM :CHUNK
  WHERE i = 1;
ROLLBACK;

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

SELECT
  $$
  SELECT * FROM test5 ORDER BY i, "Time"
  $$ AS "QUERY" \gset

SELECT compress_chunk(i) FROM show_chunks('test5') i LIMIT 4;

SELECT format('%I.%I',ch.schema_name,ch.table_name) AS "CHUNK"
  FROM _timescaledb_catalog.chunk ch
  JOIN _timescaledb_catalog.hypertable ht ON ht.id=ch.hypertable_id
  JOIN _timescaledb_catalog.hypertable ht2 ON ht.id=ht2.compressed_hypertable_id AND ht2.table_name='test5' \gset

SELECT schemaname || '.' || indexname AS "INDEXNAME"
FROM pg_indexes i
INNER JOIN _timescaledb_catalog.chunk cc ON i.schemaname = cc.schema_name and i.tablename = cc.table_name
INNER JOIN _timescaledb_catalog.chunk c ON (cc.id = c.compressed_chunk_id)
LIMIT 1 \gset


DROP INDEX :INDEXNAME;

-- Merging works without indexes on compressed chunks
SELECT compress_chunk(i, true) FROM show_chunks('test5') i LIMIT 5;

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
SELECT count(*) as number_of_chunks FROM show_chunks('test6');

-- This will generate another 24 chunks
INSERT INTO test6
SELECT t, i, gen_rand_minstd()
FROM generate_series('2018-03-03 1:00'::TIMESTAMPTZ, '2018-03-04 0:59', '1 minute') t
CROSS JOIN generate_series(1, 5, 1) i;
-- Altering compress chunk time interval will cause us to create 6 chunks from the additional 24 chunks.
ALTER TABLE test6 set (timescaledb.compress_chunk_time_interval='4 hours');

SELECT compress_chunk(i, true) FROM show_chunks('test6') i;
SELECT count(*) as number_of_chunks FROM show_chunks('test6');

-- This will generate another 3 chunks
INSERT INTO test6
SELECT t, i, gen_rand_minstd()
FROM generate_series('2018-03-04 1:00'::TIMESTAMPTZ, '2018-03-04 3:59', '1 minute') t
CROSS JOIN generate_series(1, 5, 1) i;
-- Altering compress chunk time interval will cause us to create 3 chunks from the additional 3 chunks.
-- Setting compressed chunk to anything less than chunk interval should disable merging chunks.
ALTER TABLE test6 set (timescaledb.compress_chunk_time_interval='30 minutes');

SELECT compress_chunk(i, true) FROM show_chunks('test6') i;
SELECT count(*) as number_of_chunks FROM show_chunks('test6');

-- This will generate another 3 chunks
INSERT INTO test6
SELECT t, i, gen_rand_minstd()
FROM generate_series('2018-03-04 4:00'::TIMESTAMPTZ, '2018-03-04 6:59', '1 minute') t
CROSS JOIN generate_series(1, 5, 1) i;
-- Altering compress chunk time interval will cause us to create 3 chunks from the additional 3 chunks.
-- Setting compressed chunk to anything less than chunk interval should disable merging chunks.
ALTER TABLE test6 set (timescaledb.compress_chunk_time_interval=0);

SELECT compress_chunk(i, true) FROM show_chunks('test6') i;
SELECT count(*) as number_of_chunks FROM show_chunks('test6');

-- Setting compress chunk time to NULL will error out.
\set ON_ERROR_STOP 0
ALTER TABLE test6 set (timescaledb.compress_chunk_time_interval=NULL);
\set ON_ERROR_STOP 1

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

--#5090
CREATE TABLE test8(time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL, series_id BIGINT NOT NULL);

SELECT create_hypertable('test8', 'time', chunk_time_interval => INTERVAL '1 h');

ALTER TABLE test8 set (timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_chunk_time_interval = '1 day');

INSERT INTO test8 (time, series_id, value) SELECT t, s, 1 FROM generate_series(NOW(), NOW()+INTERVAL'4h', INTERVAL '30s') t CROSS JOIN generate_series(0, 100, 1) s;

SELECT compress_chunk(c, true) FROM show_chunks('test8') c LIMIT 4;
SET enable_indexscan TO OFF;
SET enable_seqscan TO OFF;
SET enable_bitmapscan TO ON;

SELECT count(*) FROM test8 WHERE series_id = 1;

-- Verify rollup is prevented when compression settings differ
CREATE TABLE test9(time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL, series_id BIGINT NOT NULL);
SELECT create_hypertable('test9', 'time', chunk_time_interval => INTERVAL '1 h');

ALTER TABLE test9 set (timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_chunk_time_interval = '1 day');

-- create chunk and compress
INSERT INTO test9 (time, series_id, value) SELECT '2020-01-01 00:00:00'::TIMESTAMPTZ, 1, 1;
SELECT compress_chunk(show_chunks('test9'), true);
INSERT INTO test9 (time, series_id, value) SELECT '2020-01-01 01:00:00'::TIMESTAMPTZ, 1, 1;

-- should be 2 chunk before rollup
SELECT hypertable_name, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'test9' ORDER BY 2;
SELECT compress_chunk(show_chunks('test9'), true);
-- should be 1 chunk because of rollup
SELECT hypertable_name, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'test9' ORDER BY 2;

INSERT INTO test9 (time, series_id, value) SELECT '2020-01-01 02:00:00'::TIMESTAMPTZ, 1, 1;
-- should be 2 chunks again
SELECT hypertable_name, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'test9' ORDER BY 2;

ALTER TABLE test9 SET (timescaledb.compress_segmentby = '');
BEGIN;
  SELECT compress_chunk(show_chunks('test9'), true);
  -- should not be rolled up
  SELECT hypertable_name, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'test9' ORDER BY 2;
ROLLBACK;

ALTER TABLE test9 SET (timescaledb.compress_segmentby = 'series_id', timescaledb.compress_orderby = 'time DESC');
BEGIN;
  SELECT compress_chunk(show_chunks('test9'), true);
  -- should not be rolled up
  SELECT hypertable_name, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'test9' ORDER BY 2;
ROLLBACK;

ALTER TABLE test9 SET (timescaledb.compress_segmentby = 'series_id', timescaledb.compress_orderby = 'time NULLS FIRST');
BEGIN;
  SELECT compress_chunk(show_chunks('test9'), true);
  -- should not be rolled up
  SELECT hypertable_name, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'test9' ORDER BY 2;
ROLLBACK;

-- reset back to original settings
ALTER TABLE test9 SET (timescaledb.compress_segmentby = 'series_id', timescaledb.compress_orderby = 'time');
BEGIN;
  SELECT compress_chunk(show_chunks('test9'), true);
  -- should be rolled up
  SELECT hypertable_name, range_start, range_end FROM timescaledb_information.chunks WHERE hypertable_name = 'test9' ORDER BY 2;
ROLLBACK;

-- Test RowExclusiveLock on compressed chunk during chunk rollup using a GUC
CREATE TABLE test10 ("Time" timestamptz, i integer, value integer);
SELECT table_name from create_hypertable('test10', 'Time', chunk_time_interval=> INTERVAL '1 hour');

INSERT INTO test10
SELECT t, i, gen_rand_minstd()
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-02 3:59', '10 minutes') t
CROSS JOIN generate_series(1, 3, 1) i;

ALTER TABLE test10 set (timescaledb.compress, timescaledb.compress_segmentby='i', timescaledb.compress_orderby='"Time"', timescaledb.compress_chunk_time_interval='2 hours');
SHOW timescaledb.enable_rowlevel_compression_locking;
SET timescaledb.enable_rowlevel_compression_locking to on;

SELECT compress_chunk(show_chunks('test10'));

RESET timescaledb.enable_rowlevel_compression_locking;
DROP TABLE test10;
