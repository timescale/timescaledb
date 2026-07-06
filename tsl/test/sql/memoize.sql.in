-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_unmemoized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNMEMOIZED",
       format('%s/results/%s_results_memoized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_MEMOIZED"
\gset
SELECT format('\! diff -u --label "Unmemoized results" --label "Memoized results" %s %s', :'TEST_RESULTS_UNMEMOIZED', :'TEST_RESULTS_MEMOIZED') as "DIFF_CMD"
\gset


-- create normal hypertable with dropped columns, each chunk will have different attribute numbers
CREATE TABLE metrics(time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics','time');

INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','10m') time, generate_series(1,27) device_id;
INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id-1, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-06 0:00:00+0'::timestamptz,'2000-01-12 23:55:00+0','10m') time, generate_series(1,27) device_id;
INSERT INTO metrics(time,device_id,v0,v1,v2,v3) SELECT time, device_id, device_id, device_id + 2, device_id + 0.5, NULL FROM generate_series('2000-01-13 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','10m') time, generate_series(1,27) device_id;
VACUUM FULL ANALYZE metrics;

-- create identical hypertable with space partitioning
CREATE TABLE metrics_space(time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics_space','time','device_id',3);

INSERT INTO metrics_space SELECT * FROM metrics;

VACUUM FULL ANALYZE metrics_space;

-- create hypertable with compression
CREATE TABLE metrics_compressed(time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics_compressed','time');

INSERT INTO metrics_compressed SELECT * FROM metrics;


-- compress chunks
ALTER TABLE metrics_compressed SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(show_chunks('metrics_compressed'));
UPDATE metrics_compressed SET v3 = 42 WHERE device_id=1 AND time > '2000-01-01' AND time < '2000-01-02';

VACUUM FULL ANALYZE metrics_compressed;

-- create hypertable with space partitioning and compression
CREATE TABLE metrics_space_compressed(time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
SELECT create_hypertable('metrics_space_compressed','time','device_id',3);

INSERT INTO metrics_space_compressed SELECT * FROM metrics;

ANALYZE metrics_space_compressed;

-- compress chunks
ALTER TABLE metrics_space_compressed SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');
SELECT compress_chunk(show_chunks('metrics_space_compressed'));

VACUUM FULL ANALYZE metrics_space_compressed;


-- get EXPLAIN output for all variations
\set PREFIX 'EXPLAIN (analyze, buffers off, costs off, timing off, summary off)'

SET enable_indexonlyscan TO OFF;
SET work_mem TO '64MB';
SET enable_memoize TO on;

\set TEST_TABLE 'metrics'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_compressed'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space_compressed'
\ir :TEST_QUERY_NAME

-- get results for all the queries
-- run queries with and without memoize
\set PREFIX ''
\set ECHO none
SET client_min_messages TO error;

SET enable_memoize TO on;

\set TEST_TABLE 'metrics'
\o :TEST_RESULTS_MEMOIZED
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_compressed'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space_compressed'
\ir :TEST_QUERY_NAME
\o

SET enable_memoize TO off;

\set TEST_TABLE 'metrics'
\o :TEST_RESULTS_UNMEMOIZED
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_compressed'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space_compressed'
\ir :TEST_QUERY_NAME
\o

-- diff memoized and unmemoized results
:DIFF_CMD

RESET work_mem;
RESET enable_indexonlyscan;
