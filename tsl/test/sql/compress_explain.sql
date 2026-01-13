-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Display extra statuses of compressed chunks in "explain verbose"
\set PREFIX 'EXPLAIN (verbose, analyze, buffers off, costs off, timing off, summary off)'

CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time desc', tsdb.segmentby='device');

-- uncompressed data: no explain extras
BEGIN;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

SET timescaledb.enable_direct_compress_insert = false;
BEGIN;
-- basic compressed chunks: no explain extras
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
SELECT compress_chunk(ch) FROM show_chunks('metrics') ch;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
-- Freeze/unfreeze chunks
SELECT _timescaledb_functions.freeze_chunk(ch) FROM show_chunks('metrics') AS ch;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
SELECT _timescaledb_functions.unfreeze_chunk(ch) FROM show_chunks('metrics') AS ch;
-- Partial chunk
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float FROM generate_series(0,10) i;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
-- Freeze/unfreeze chunks
SELECT _timescaledb_functions.freeze_chunk(ch) FROM show_chunks('metrics') AS ch;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
SELECT _timescaledb_functions.unfreeze_chunk(ch) FROM show_chunks('metrics') AS ch;
ROLLBACK;

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

BEGIN;
-- direct insert producing UNORDERED chunks
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
-- Freeze/unfreeze chunks
SELECT _timescaledb_functions.freeze_chunk(ch) FROM show_chunks('metrics') AS ch;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
SELECT _timescaledb_functions.unfreeze_chunk(ch) FROM show_chunks('metrics') AS ch;
-- Partial chunk
SET timescaledb.enable_direct_compress_insert = false;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float FROM generate_series(0,10) i;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
-- Freeze/unfreeze chunks
SELECT _timescaledb_functions.freeze_chunk(ch) FROM show_chunks('metrics') AS ch;
:PREFIX SELECT device FROM metrics;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
SELECT _timescaledb_functions.unfreeze_chunk(ch) FROM show_chunks('metrics') AS ch;
ROLLBACK;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_direct_compress_insert_sort_batches;
RESET timescaledb.enable_direct_compress_insert_client_sorted;

drop table metrics cascade;
