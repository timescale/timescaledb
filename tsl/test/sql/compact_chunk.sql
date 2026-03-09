-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for the compact_chunk function.
-- compact_chunk merges overlapping compressed batches within a chunk
-- without decompressing/recompressing batches that are already ordered.

CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- compact_chunk with no overlapping batches (no-op case)
-- Insert 3000 ordered rows: Jan 2 00:00 to Jan 4 02:00 PST.
-- Direct compress insert creates 3 batches: 3x1000 rows.
INSERT INTO metrics
SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(1,3000) i;

-- Status should be COMPRESSED,UNORDERED after direct compress insert
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;

-- Get the first compressed chunk for inspection
SELECT comp_ch.table_name AS "CHUNK_NAME",
       comp_ch.schema_name || '.' || comp_ch.table_name AS "CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- Show batch metadata: 3 non-overlapping batches (ordered by min time)
SELECT _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

-- Count batches before compaction
SELECT count(*) AS batch_count_before FROM :CHUNK_FULL_NAME;

-- compact_chunk on non-overlapping batches is a no-op; returns the chunk regclass
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics') chunk;

-- Batch count should be unchanged after no-op compaction
SELECT count(*) AS batch_count_after FROM :CHUNK_FULL_NAME;

-- Total row count should be unchanged
SELECT count(*) FROM metrics;

-- Status UNORDERED removed: compact_chunk clears the UNORDERED flag
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;

-- compact_chunk with overlapping batches
-- Insert data that overlaps with existing batches.
-- Jan 1 to Jan 3 data overlaps with the existing Jan 2 to Jan 4 batches
-- in the compressed chunk.
INSERT INTO metrics
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(1,3000) i;

-- Show compressed chunk metadata: overlapping batches are now visible.
-- The new batches from the Jan 1 insert interleave with the old Jan 2 batches.
SELECT _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

-- compact_chunk should identify and merge the overlapping batches.
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics') chunk;

-- compact an uncompressed chunk
-- Create a new uncompressed chunk for a different time range by
-- inserting with direct compress insert disabled.
SET timescaledb.enable_direct_compress_insert = false;
INSERT INTO metrics VALUES ('2025-12-01', 'd1', -1.0);
SET timescaledb.enable_direct_compress_insert = true;

-- The new chunk for Dec 2025 should be uncompressed (empty status array)
SELECT _timescaledb_functions.chunk_status_text(chunk) AS chunk_status,
       chunk::text AS chunk_name
FROM show_chunks('metrics') chunk
WHERE NOT (_timescaledb_functions.chunk_status_text(chunk) && ARRAY['COMPRESSED'])
ORDER BY chunk;

-- compact_chunk on an uncompressed chunk must fail
SELECT chunk AS "UNCOMPRESSED_CHUNK"
FROM show_chunks('metrics') chunk
WHERE NOT (_timescaledb_functions.chunk_status_text(chunk) && ARRAY['COMPRESSED'])
LIMIT 1 \gset

\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.compact_chunk(:'UNCOMPRESSED_CHUNK');
\set ON_ERROR_STOP 1

-- compact a partially compressed chunk
-- Insert an uncompressed row into the already-compressed Jan 2 chunk.
-- This makes chunk 1 PARTIAL (it has both compressed and uncompressed rows).
SET timescaledb.enable_direct_compress_insert = false;
INSERT INTO metrics VALUES ('2025-01-02 12:00', 'd1', -1.0);
SET timescaledb.enable_direct_compress_insert = true;

-- Chunk 1 should now show PARTIAL status
SELECT _timescaledb_functions.chunk_status_text(chunk) AS chunk_status,
       chunk::text AS chunk_name
FROM show_chunks('metrics') chunk
WHERE _timescaledb_functions.chunk_status_text(chunk) && ARRAY['PARTIAL']
ORDER BY chunk;

-- compact_chunk on a partially compressed chunk must fail
SELECT chunk AS "PARTIAL_CHUNK"
FROM show_chunks('metrics') chunk
WHERE _timescaledb_functions.chunk_status_text(chunk) && ARRAY['PARTIAL']
LIMIT 1 \gset

\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.compact_chunk(:'PARTIAL_CHUNK');
\set ON_ERROR_STOP 1

-- Create a hypertable with a segmentby column.
-- Each segment ('d1', 'd2') will have its own set of batches, and
-- compact_chunk should handle overlaps per segment independently.
CREATE TABLE metrics_seg (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert 2000 rows for each of two devices: non-overlapping within each segment
INSERT INTO metrics_seg
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,1999) i;

INSERT INTO metrics_seg
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(0,1999) i;

SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_seg') chunk;

-- Get the compressed chunk for metrics_seg
SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "SEG_CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_seg'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- 4 batches total: 2 batches per segment (1000 rows each)
SELECT count(*) AS batch_count_before FROM :SEG_CHUNK_FULL_NAME;

-- compact_chunk with no overlaps: no-op for both segments
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_seg') chunk;

-- status should be updated to COMPRESSED only
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_seg') chunk;

-- Batch count unchanged
SELECT count(*) AS batch_count_after FROM :SEG_CHUNK_FULL_NAME;

-- Total row count unchanged
SELECT count(*) FROM metrics_seg;

-- Insert overlapping data
INSERT INTO metrics_seg
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,1999) i;

INSERT INTO metrics_seg
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(0,1999) i;

-- 8 batches total: 4 batches per segment (1000 rows each)
SELECT count(*) AS batch_count_after FROM :SEG_CHUNK_FULL_NAME;

-- status should be updated to COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_seg') chunk;

-- compact_chunk with overlaps: combines the batches
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_seg') chunk;

-- status should be COMPRESSED only (UNORDERED cleared after compaction)
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_seg') chunk;

-- 8 batches total: 4 batches per segment (1000 rows each)
SELECT count(*) AS batch_count_after FROM :SEG_CHUNK_FULL_NAME;

-- compact_chunk with nullable orderby column
-- When orderby columns are nullable and batches contain NULLs,
-- compact_chunk should split batches: non-NULL rows recompressed normally,
-- NULL rows placed in a separate batch.
-- Note: 'time' is the partitioning column (implicitly NOT NULL),
-- but 'value' is nullable and used as a secondary orderby column.
CREATE TABLE metrics_nullable (time TIMESTAMPTZ, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time,value');

SET timescaledb.enable_direct_compress_insert = true;

-- Insert non-null data first
INSERT INTO metrics_nullable
SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(1,1000) i;

-- compact_chunk on nullable orderby with no nulls in data should succeed (no-op)
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_nullable') chunk;

-- Get the compressed chunk for metrics_nullable
SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "NULLABLE_CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_nullable'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

SELECT count(*) AS batch_count_no_nulls FROM :NULLABLE_CHUNK_FULL_NAME;

-- Now insert data with NULLs in the value orderby column
-- These will create batches that contain NULL values mixed with non-NULLs
INSERT INTO metrics_nullable
SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval,
       'd1',
       CASE WHEN i % 5 = 0 THEN NULL ELSE i::float END
FROM generate_series(1001,2000) i;

-- Show batch count after inserting null-containing data
SELECT count(*) AS batch_count_with_nulls FROM :NULLABLE_CHUNK_FULL_NAME;

-- compact_chunk should handle nullable orderby: split null rows into separate batches
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_nullable') chunk;

-- Verify total row count is preserved
SELECT count(*) FROM metrics_nullable;

-- Verify NULL rows are accessible
SELECT count(*) AS null_value_count FROM metrics_nullable WHERE value IS NULL;

DROP TABLE metrics_nullable;

-- compact_chunk with DESC orderby column
CREATE TABLE metrics_desc (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time DESC', tsdb.segmentby='device');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert non-overlapping data for two devices
INSERT INTO metrics_desc
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,1999) i;

INSERT INTO metrics_desc
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(0,1999) i;

SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_desc') chunk;

-- Get the compressed chunk for metrics_desc
SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "DESC_CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_desc'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- Show batch metadata: batches ordered by max time descending
SELECT _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :DESC_CHUNK_FULL_NAME
ORDER BY _ts_meta_max_1 DESC;

-- 4 batches total: 2 per segment
SELECT count(*) AS batch_count_before FROM :DESC_CHUNK_FULL_NAME;

-- No overlaps: compact_chunk is a no-op
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_desc') chunk;

SELECT count(*) AS batch_count_after FROM :DESC_CHUNK_FULL_NAME;
SELECT count(*) FROM metrics_desc;

-- Status should be COMPRESSED only after compaction
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_desc') chunk;

-- Insert overlapping data
INSERT INTO metrics_desc
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,1999) i;

INSERT INTO metrics_desc
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(0,1999) i;

-- 8 batches total after overlapping insert
SELECT count(*) AS batch_count_after FROM :DESC_CHUNK_FULL_NAME;

SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_desc') chunk;

-- compact_chunk should merge overlapping batches with DESC ordering
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_desc') chunk;

-- Status should be COMPRESSED only
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_desc') chunk;

-- 8 batches: 4 per segment (2000 rows = 2 batches of 1000 each, x2 for duplicates)
SELECT count(*) AS batch_count_after FROM :DESC_CHUNK_FULL_NAME;

-- Total row count should be preserved
SELECT count(*) FROM metrics_desc;

-- Verify data is correctly ordered (DESC) within each segment
SELECT device, time FROM metrics_desc WHERE device = 'd1' ORDER BY device, time DESC LIMIT 5;

DROP TABLE metrics_desc;
