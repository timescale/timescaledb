-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET max_parallel_workers_per_gather = 0;
-- disable vectorized aggregation to prevent plan switches when running on 32-bit
SET timescaledb.enable_vectorized_aggregation = off;

\set TEST_BASE_NAME columnar_index_scan
SELECT format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff -u --label "Unoptimized results" --label "Optimized results" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') as "DIFF_CMD"
\gset

CREATE TABLE metrics(
    time timestamptz NOT NULL,
    device text,
    sensor text,
    value float,
    value2 float
) WITH (tsdb.hypertable,tsdb.orderby='time desc',tsdb.segmentby='device,sensor',tsdb.index='minmax(value)');

INSERT INTO metrics VALUES
('2025-01-01 00:00:00 PST', 'd1', 'A', 10.0, 10.1),
('2025-01-01 01:00:00 PST', 'd1', 'A', 20.0, 20.1),
('2025-01-01 02:00:00 PST', 'd1', 'A', 15.0, 15.1),
('2025-01-01 00:30:00 PST', 'd1', 'B', 4.0, 4.1),
('2025-01-01 01:30:00 PST', 'd1', 'B', 25.0, 25.1),
('2025-01-01 02:30:00 PST', 'd1', 'B', 29.0, 29.1),
('2025-01-01 00:00:00 PST', 'd2', 'A', 10.0, 10.1),
('2025-01-01 01:00:00 PST', 'd2', 'A', 20.0, 20.1),
('2025-01-01 02:00:00 PST', 'd2', 'A', 15.0, 15.1),
('2025-01-01 00:30:00 PST', 'd2', 'C', 6.0, 6.1),
('2025-01-01 01:30:00 PST', 'd2', 'C', 25.0, 25.1),
('2025-01-01 02:30:00 PST', 'd2', 'C', 31.0, 31.1);

-- Compress all chunks
SELECT compress_chunk(c) FROM show_chunks('metrics') c;

-- first run with hypertable with 1 fully compressed chunk

-- get query plans
\set PREFIX 'EXPLAIN (costs off)'
\set ECHO all
SET timescaledb.enable_columnarindexscan = on;
\ir :TEST_QUERY_NAME

\set PREFIX ''
\set ECHO errors

-- get query results with columnar index scan disabled
SET timescaledb.enable_columnarindexscan = off;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o

-- get query results with columnar index scan enabled
SET timescaledb.enable_columnarindexscan = on;
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

-- compare optimized vs non-optimized results
:DIFF_CMD

-- make initial chunk partial
INSERT INTO metrics VALUES
('2025-01-01 00:00:00 PST', 'd1', 'A', 1.0, 1.1),
('2025-01-01 00:00:00 PST', 'd3', 'A', 50.0, 50.1),
('2025-01-01 00:00:00 PST', 'd3', 'B', 52.0, 52.1);

-- second run with hypertable with 1 partial chunk

-- get query plans
\set PREFIX 'EXPLAIN (costs off)'
\set ECHO all
SET timescaledb.enable_columnarindexscan = on;
\ir :TEST_QUERY_NAME

\set PREFIX ''
\set ECHO errors

-- get query results with columnar index scan disabled
SET timescaledb.enable_columnarindexscan = off;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o

-- get query results with columnar index scan enabled
SET timescaledb.enable_columnarindexscan = on;
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

-- compare optimized vs non-optimized results
:DIFF_CMD

-- create a second chunk
INSERT INTO metrics VALUES
('2025-02-01 00:00:00 PST', 'd1', 'A', 100.0, 100.1),
('2025-02-01 01:00:00 PST', 'd1', 'B', 200.0, 200.1),
('2025-02-01 00:00:00 PST', 'd2', 'A', 150.0, 150.1),
('2025-02-01 01:00:00 PST', 'd2', 'C', 250.0, 250.1);

SELECT compress_chunk(c) FROM show_chunks('metrics') c LIMIT 1 OFFSET 1;

-- third run with hypertable with 1 partial and 1 fully compressed chunk

-- get query plans
\set PREFIX 'EXPLAIN (costs off)'
\set ECHO all
SET timescaledb.enable_columnarindexscan = on;
\ir :TEST_QUERY_NAME

\set PREFIX ''
\set ECHO errors

-- get query results with columnar index scan disabled
SET timescaledb.enable_columnarindexscan = off;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o

-- get query results with columnar index scan enabled
SET timescaledb.enable_columnarindexscan = on;
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

-- compare optimized vs non-optimized results
:DIFF_CMD

