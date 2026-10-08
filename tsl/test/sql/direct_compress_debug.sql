-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- need superuser for copy from program
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.orderby='time');

SET timescaledb.enable_direct_compress_copy = true;
SET timescaledb.enable_direct_compress_copy_client_sorted = false;
SET timescaledb.enable_direct_compress_copy_sort_batches = false;
-- simple test with compressed copy enabled and without batch sorting
BEGIN;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 + I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
ROLLBACK;

-- simple test with compressed copy enabled and reversed order and no batch sorting
BEGIN;
COPY metrics FROM PROGRAM 'seq 3000 | xargs -II date -d "2025-01-01 - I minute" +"%Y-%m-%d %H:%M:%S,d1,0.I"' WITH (FORMAT CSV);
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
ROLLBACK;

RESET timescaledb.enable_direct_compress_copy;
RESET timescaledb.enable_direct_compress_copy_sort_batches;
RESET timescaledb.enable_direct_compress_copy_client_sorted;

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
SET timescaledb.enable_direct_compress_insert_sort_batches = false;
-- simple test with compressed insert enabled and without batch sorting
BEGIN;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
-- since the chunks are new status should be COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- simple test with compressed insert enabled and reversed order and no batch sorting
BEGIN;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz - (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
-- since the chunks are new status should be COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_direct_compress_insert_sort_batches;
RESET timescaledb.enable_direct_compress_insert_client_sorted;
