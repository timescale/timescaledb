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
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

-- compact_chunk on non-overlapping batches is a no-op; returns the chunk regclass
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics') chunk;

-- Show batch metadata: 3 non-overlapping batches with same ctids as before
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

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
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

-- Status should contain UNORDERED flag
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;

-- compact_chunk should identify and merge the overlapping batches.
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics') chunk;

-- Show compressed chunk metadata: no overlapping batches anymore.
-- We should see new ctids of the newly merged batches.
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

-- Status should not contain UNORDERED flag
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;

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

-- 4 batches: 2 per segment, no overlaps
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :SEG_CHUNK_FULL_NAME
ORDER BY device, _ts_meta_min_1;

-- compact_chunk with no overlaps: no-op for both segments
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_seg') chunk;

-- Same ctids: nothing was rewritten
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :SEG_CHUNK_FULL_NAME
ORDER BY device, _ts_meta_min_1;

-- status should be updated to COMPRESSED only
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_seg') chunk;

-- Insert overlapping data
INSERT INTO metrics_seg
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,1999) i;

INSERT INTO metrics_seg
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(0,1999) i;

-- 8 batches: 4 per segment, overlapping
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :SEG_CHUNK_FULL_NAME
ORDER BY device, _ts_meta_min_1;

-- status should be updated to COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_seg') chunk;

-- compact_chunk with overlaps: combines the batches
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_seg') chunk;

-- New ctids: overlapping batches merged and re-sorted
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :SEG_CHUNK_FULL_NAME
ORDER BY device, _ts_meta_min_1;

-- status should be COMPRESSED only (UNORDERED cleared after compaction)
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_seg') chunk;

-- compact_chunk with nullable secondary orderby column (NULLS FIRST)
-- The second batch starts at a boundary tie on col1 (time) with a NULL
-- in col2 (value). With NULLS FIRST, (time=T, value=NULL) should sort
-- BEFORE (time=T, value=non-null), but the second batch is positioned
-- after the first in the index — an ordering violation.
CREATE TABLE metrics_nullable (time TIMESTAMPTZ, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time,value NULLS FIRST');

SET timescaledb.enable_direct_compress_insert = true;

-- Insert batch 1 (1000 rows): time [00:01..16:40], all non-null values.
-- Last row: (16:40, 1000).
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

-- 1 batch, no nulls
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :NULLABLE_CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

-- Insert batch 2 (1000 rows): starts at batch 1's max time (i=1000, time=16:40).
-- First row has NULL value: (16:40, NULL).
-- With NULLS FIRST, (16:40, NULL) should sort BEFORE (16:40, 1000) from
-- batch 1's last row. But batch 2 is after batch 1 in the index → unordered.
INSERT INTO metrics_nullable
SELECT '2025-01-02'::timestamptz + ((999 + i) || ' minute')::interval,
       'd1',
       CASE WHEN i = 1 THEN NULL ELSE (1000 + i)::float END
FROM generate_series(1,1000) i;

-- 2 batches: boundary tie on col1, NULL in col2 at the boundary
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :NULLABLE_CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

-- compact_chunk should detect the boundary-tie overlap via NULL in col2
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_nullable') chunk;

-- After compaction: batches merged and re-sorted
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :NULLABLE_CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1;

-- Verify total row count is preserved
SELECT count(*) FROM metrics_nullable;

-- Verify NULL rows are accessible
SELECT count(*) AS null_value_count FROM metrics_nullable WHERE value IS NULL;

-- Verify ordering at boundary: NULL must come before non-null with NULLS FIRST
SELECT time, value FROM metrics_nullable
WHERE time = '2025-01-02'::timestamptz + '1000 minutes'::interval
ORDER BY time, value NULLS FIRST;

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

-- 4 batches: 2 per segment, ordered by max time descending
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :DESC_CHUNK_FULL_NAME
ORDER BY device, _ts_meta_max_1 DESC;

-- No overlaps: compact_chunk is a no-op
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_desc') chunk;

-- Same ctids: nothing was rewritten
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :DESC_CHUNK_FULL_NAME
ORDER BY device, _ts_meta_max_1 DESC;

-- Status should be COMPRESSED only after compaction
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_desc') chunk;

-- Insert overlapping data
INSERT INTO metrics_desc
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,1999) i;

INSERT INTO metrics_desc
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(0,1999) i;

-- 8 batches: 4 per segment, overlapping
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :DESC_CHUNK_FULL_NAME
ORDER BY device, _ts_meta_max_1 DESC;

SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_desc') chunk;

-- compact_chunk should merge overlapping batches with DESC ordering
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_desc') chunk;

-- New ctids: overlapping batches merged
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :DESC_CHUNK_FULL_NAME
ORDER BY device, _ts_meta_max_1 DESC;

-- Status should be COMPRESSED only
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_desc') chunk;

-- Verify data is correctly ordered (DESC) within each segment
SELECT device, time FROM metrics_desc WHERE device = 'd1' ORDER BY device, time DESC LIMIT 5;

DROP TABLE metrics_desc;

-- compact_chunk with multi-column orderby
-- Tests that overlap detection works correctly when using ORDER BY device, time.
-- The bug: secondary column min/max metadata is a global aggregate across
-- all rows, not scoped to the primary column's value. This causes false
-- negatives where interleaving batches go undetected.
CREATE TABLE metrics_multi (time TIMESTAMPTZ NOT NULL, device TEXT NOT NULL, value float)
WITH (tsdb.hypertable, tsdb.orderby='device,time');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert 3 batches with different devices creating boundary ties on col1:
-- Batch 1 (500 rows): device d1..d2, time [00:01..08:20]
-- Batch 2 (500 rows): device d2..d3, time [08:21..16:40]
-- The d2 rows in both batches create a boundary tie on col1 (device).
-- Col2 (time) ranges are non-overlapping within the d2 group → no actual overlap.
INSERT INTO metrics_multi
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       CASE WHEN i <= 250 THEN 'd1' ELSE 'd2' END,
       i::float
FROM generate_series(1,500) i;

INSERT INTO metrics_multi
SELECT '2025-01-03'::timestamptz + ((500 + i) || ' minute')::interval,
       CASE WHEN i <= 250 THEN 'd2' ELSE 'd3' END,
       (500 + i)::float
FROM generate_series(1,500) i;

-- Get the compressed chunk for metrics_multi
SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "MULTI_CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_multi'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- 2 batches: boundary tie on col1 (device=d2), non-overlapping time ranges
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :MULTI_CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1, _ts_meta_min_2;

-- compact_chunk: boundary tie on col1, but no overlap on col2 (time) — should be a no-op
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_multi') chunk;

-- Same ctids: nothing was rewritten
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :MULTI_CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1, _ts_meta_min_2;

-- Now insert truly overlapping data within the d2 device group:
-- Batch 3 (500 rows): device d2, time range overlaps with both existing d2 ranges
INSERT INTO metrics_multi
SELECT '2025-01-03'::timestamptz + ((250 + i) || ' minute')::interval, 'd2', (250 + i)::float
FROM generate_series(1,500) i;

-- 3 batches: the new d2 batch overlaps with the d2 portions of existing batches
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :MULTI_CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1, _ts_meta_min_2;

-- compact_chunk: boundary tie on col1 with actual overlap on col2 — must detect and merge
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_multi') chunk;

-- New ctids: overlapping batches merged
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :MULTI_CHUNK_FULL_NAME
ORDER BY _ts_meta_min_1, _ts_meta_min_2;

-- Total row count preserved
SELECT count(*) FROM metrics_multi;

DROP TABLE metrics_multi;

-- compact_chunk with multi-column orderby, second column DESC
-- Same boundary-tie logic but the secondary column uses descending order.
-- The sort operator for col2 is now ">" instead of "<", so the boundary
-- decompression must use the correct comparator.
CREATE TABLE metrics_multi_desc (time TIMESTAMPTZ NOT NULL, device TEXT NOT NULL, value float)
WITH (tsdb.hypertable, tsdb.orderby='device,time DESC');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert 2 batches with multiple devices, DESC time ordering:
-- Batch 1 (500 rows): device d2..d3, time [08:21..16:40] DESC
-- Batch 2 (500 rows): device d1..d2, time [00:01..08:20] DESC
-- The d2 rows create a boundary tie on col1.
-- With DESC, batch 1's d2 trailing edge (min time=08:21) and batch 2's
-- d2 leading edge (max time=08:20) are non-overlapping → no actual overlap.
INSERT INTO metrics_multi_desc
SELECT '2025-01-03'::timestamptz + ((500 + i) || ' minute')::interval,
       CASE WHEN i <= 250 THEN 'd2' ELSE 'd3' END,
       (500 + i)::float
FROM generate_series(1,500) i;

INSERT INTO metrics_multi_desc
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       CASE WHEN i <= 250 THEN 'd1' ELSE 'd2' END,
       i::float
FROM generate_series(1,500) i;

-- Get the compressed chunk
SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "MULTI_DESC_CHUNK"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_multi_desc'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- 2 batches: boundary tie on col1 (device=d2), non-overlapping DESC time ranges
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :MULTI_DESC_CHUNK
ORDER BY _ts_meta_min_1, _ts_meta_max_2 DESC;

-- compact_chunk: boundary tie on col1, no overlap on col2 (DESC) — should be a no-op
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_multi_desc') chunk;

-- Same ctids: nothing was rewritten
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :MULTI_DESC_CHUNK
ORDER BY _ts_meta_min_1, _ts_meta_max_2 DESC;

-- Now insert overlapping data within the d2 device group
INSERT INTO metrics_multi_desc
SELECT '2025-01-03'::timestamptz + ((250 + i) || ' minute')::interval, 'd2', (250 + i)::float
FROM generate_series(1,500) i;

-- 3 batches: the new d2 batch overlaps with existing d2 portions
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :MULTI_DESC_CHUNK
ORDER BY _ts_meta_min_1, _ts_meta_max_2 DESC;

-- compact_chunk: boundary tie on col1, actual overlap on col2 (DESC) — must merge
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_multi_desc') chunk;

-- New ctids: overlapping batches merged
SELECT ctid, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :MULTI_DESC_CHUNK
ORDER BY _ts_meta_min_1, _ts_meta_max_2 DESC;

-- Total row count preserved
SELECT count(*) FROM metrics_multi_desc;

DROP TABLE metrics_multi_desc;

-- compact_chunk with segmentby + multi-column orderby + nullable orderby column
-- Combines all three features:
--   segmentby='device'         multiple segments processed independently
--   orderby='time,value'       multi-column overlap detection with boundary ties
--   'value' is nullable        boundary tie NULL values must respect NULL ordering
CREATE TABLE metrics_combined (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time,value', tsdb.segmentby='device');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert non-null data for two segments: 1000 rows each, non-overlapping
INSERT INTO metrics_combined
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(1,1000) i;

INSERT INTO metrics_combined
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', (i + 1000)::float
FROM generate_series(1,1000) i;

-- Get the compressed chunk
SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "COMBINED_CHUNK"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_combined'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- 2 batches: 1 per segment, no overlaps, no nulls
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :COMBINED_CHUNK
ORDER BY device, _ts_meta_min_1;

-- No overlaps, no nulls: compact_chunk is a no-op
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_combined') chunk;

-- Same ctids as before: nothing was rewritten
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :COMBINED_CHUNK
ORDER BY device, _ts_meta_min_1;

-- Status should be COMPRESSED only
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_combined') chunk;

-- Insert overlapping data with NULLs in the nullable orderby column (value)
-- for both segments. Every 5th row has NULL value.
INSERT INTO metrics_combined
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd1',
       CASE WHEN i % 5 = 0 THEN NULL ELSE (i + 2000)::float END
FROM generate_series(1,1000) i;

INSERT INTO metrics_combined
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd2',
       CASE WHEN i % 5 = 0 THEN NULL ELSE (i + 3000)::float END
FROM generate_series(1,1000) i;

-- 4 batches: original + overlapping with nulls. Both segments overlap on time col1.
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :COMBINED_CHUNK
ORDER BY device, _ts_meta_min_1;

-- Status should be UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_combined') chunk;

-- compact_chunk must:
-- 1. Detect overlaps per segment via multi-column boundary-tie logic
-- 2. Split null rows into separate batches
-- 3. Recompress non-null rows into ordered batches
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_combined') chunk;

-- After compaction: new ctids, null rows separated into their own batches
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :COMBINED_CHUNK
ORDER BY device, _ts_meta_min_1;

-- Status should be COMPRESSED only (UNORDERED cleared).
-- NULLs in secondary orderby column (value) don't require splitting since only
-- the first orderby column (time, NOT NULL) affects batch placement.
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_combined') chunk;

-- Total row count preserved
SELECT count(*) FROM metrics_combined;

-- NULL rows preserved per segment
SELECT device, count(*) AS null_value_count FROM metrics_combined WHERE value IS NULL GROUP BY device ORDER BY device;

-- Data integrity per segment
SELECT device, count(*) FROM metrics_combined GROUP BY device ORDER BY device;

-- Verify ordering is correct within segment d1
SELECT device, time, value FROM metrics_combined
WHERE device = 'd1' ORDER BY time, value LIMIT 5;

SELECT device, time, value FROM metrics_combined
WHERE device = 'd1' AND value IS NULL ORDER BY time LIMIT 5;

DROP TABLE metrics_combined;

-- compact_chunk with nullable first orderby column (NULLS LAST, default)
-- When the first orderby column is nullable and batches contain NULLs,
-- compact_chunk must split null rows into a separate batch so that
-- non-null batches remain correctly ordered in the index.
CREATE TABLE metrics_nulls_last (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='value,time', tsdb.segmentby='device');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert non-null data for two segments
INSERT INTO metrics_nulls_last
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(1,1000) i;

INSERT INTO metrics_nulls_last
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', (i + 1000)::float
FROM generate_series(1,1000) i;

SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "NULLS_LAST_CHUNK"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_nulls_last'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- 2 non-overlapping batches, no nulls
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_LAST_CHUNK
ORDER BY device, _ts_meta_min_1;

-- Insert overlapping data with NULLs in value (first orderby column)
INSERT INTO metrics_nulls_last
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd1',
       CASE WHEN i % 5 = 0 THEN NULL ELSE (i + 2000)::float END
FROM generate_series(1,1000) i;

INSERT INTO metrics_nulls_last
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd2',
       CASE WHEN i % 5 = 0 THEN NULL ELSE (i + 3000)::float END
FROM generate_series(1,1000) i;

-- 4 batches with overlaps and nulls in first orderby column
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_LAST_CHUNK
ORDER BY device, _ts_meta_min_1;

SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_nulls_last') chunk;

-- After compaction: null rows split into separate batches (NULLS LAST)
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_LAST_CHUNK
ORDER BY device, _ts_meta_min_1;

-- UNORDERED cleared: pure-null batches at the end (NULLS LAST) are correctly positioned
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_nulls_last') chunk;

-- Data integrity
SELECT count(*) FROM metrics_nulls_last;
SELECT device, count(*) FROM metrics_nulls_last GROUP BY device ORDER BY device;
SELECT device, count(*) AS null_count FROM metrics_nulls_last WHERE value IS NULL GROUP BY device ORDER BY device;

-- Now test the overlap path with NULLs: insert data that truly overlaps
-- on the value range AND contains NULLs. This exercises the overlap merge
-- path (not the mixed-null split path) with nullable first orderby column.
-- The NULL rows must survive the overlap merge and be flushed correctly.
INSERT INTO metrics_nulls_last
SELECT '2025-01-03'::timestamptz + ((2000 + i) || ' minute')::interval,
       'd1',
       CASE WHEN i % 5 = 0 THEN NULL ELSE (i + 500)::float END
FROM generate_series(1,500) i;

-- Batches before overlap compaction
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_LAST_CHUNK
WHERE device = 'd1'
ORDER BY _ts_meta_min_1 NULLS LAST;

-- This overlaps with existing d1 data (value 501..900 overlaps with 1..1000 and 2001..2999)
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_nulls_last') chunk;

-- After overlap merge: NULLs from overlapping batches preserved
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_LAST_CHUNK
WHERE device = 'd1'
ORDER BY _ts_meta_min_1 NULLS LAST;

-- Data integrity: all rows including NULLs from the overlap merge must survive
SELECT count(*) FROM metrics_nulls_last WHERE device = 'd1';
SELECT count(*) AS null_count FROM metrics_nulls_last WHERE device = 'd1' AND value IS NULL;

DROP TABLE metrics_nulls_last;

-- compact_chunk with nullable first orderby column (NULLS FIRST)
-- With NULLS FIRST, the index naturally places null-containing batches
-- at the start of each segment. No splitting is needed when the first
-- batch has nulls — it's already correctly positioned.
CREATE TABLE metrics_nulls_first (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='value NULLS FIRST,time', tsdb.segmentby='device');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert non-null data for two segments
INSERT INTO metrics_nulls_first
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(1,1000) i;

INSERT INTO metrics_nulls_first
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval, 'd2', (i + 1000)::float
FROM generate_series(1,1000) i;

SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "NULLS_FIRST_CHUNK"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_nulls_first'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- 2 non-overlapping batches, no nulls
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_FIRST_CHUNK
ORDER BY device, _ts_meta_min_1;

-- Insert overlapping data with NULLs in value (first orderby column)
INSERT INTO metrics_nulls_first
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd1',
       CASE WHEN i % 5 = 0 THEN NULL ELSE (i + 2000)::float END
FROM generate_series(1,1000) i;

INSERT INTO metrics_nulls_first
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd2',
       CASE WHEN i % 5 = 0 THEN NULL ELSE (i + 3000)::float END
FROM generate_series(1,1000) i;

-- 4 batches with overlaps and nulls in first orderby column
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_FIRST_CHUNK
ORDER BY device, _ts_meta_min_1;

SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_nulls_first') chunk;

-- After compaction: with NULLS FIRST, null batches at segment start are fine
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_FIRST_CHUNK
ORDER BY device, _ts_meta_min_1;

SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_nulls_first') chunk;

-- Data integrity
SELECT count(*) FROM metrics_nulls_first;
SELECT device, count(*) FROM metrics_nulls_first GROUP BY device ORDER BY device;
SELECT device, count(*) AS null_count FROM metrics_nulls_first WHERE value IS NULL GROUP BY device ORDER BY device;

-- Same overlap+NULLs test for NULLS FIRST
INSERT INTO metrics_nulls_first
SELECT '2025-01-03'::timestamptz + ((2000 + i) || ' minute')::interval,
       'd1',
       CASE WHEN i % 5 = 0 THEN NULL ELSE (i + 500)::float END
FROM generate_series(1,500) i;

-- Batches before overlap compaction
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_FIRST_CHUNK
WHERE device = 'd1'
ORDER BY _ts_meta_min_1 NULLS FIRST;

SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_nulls_first') chunk;

-- After overlap merge: NULLs from overlapping batches preserved
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :NULLS_FIRST_CHUNK
WHERE device = 'd1'
ORDER BY _ts_meta_min_1 NULLS FIRST;

-- Data integrity: NULLs from overlap merge must survive
SELECT count(*) FROM metrics_nulls_first WHERE device = 'd1';
SELECT count(*) AS null_count FROM metrics_nulls_first WHERE device = 'd1' AND value IS NULL;

DROP TABLE metrics_nulls_first;

-- compact_chunk with mixed-null batch in non-overlapping, non-boundary-tie position
-- Regression test: a batch containing both NULL and non-NULL values in the
-- first orderby column, where the batch is NOT at a boundary tie with its
-- neighbor, must still be split. Otherwise NULLs end up in the wrong index
-- position (e.g., before higher non-null batches with NULLS LAST).
CREATE TABLE metrics_mixed_nulls (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='value,time', tsdb.segmentby='device');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert 1800 non-null rows + 200 NULL rows for device 'd1'.
-- DCI sorts by (value NULLS LAST, time), producing:
--   Batch 1 (1000 rows): value [1..1000]       — all non-null
--   Batch 2 (1000 rows): value [1001..1800] + 200 NULLs — mixed!
-- Batch 2's index metadata: min=1001, max=1800 (NULLs invisible to index).
INSERT INTO metrics_mixed_nulls
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd1',
       CASE WHEN i > 1800 THEN NULL ELSE i::float END
FROM generate_series(1,2000) i;

-- Insert 1000 more non-null rows with values [1801..2800].
-- These form Batch 3: min=1801, max=2800.
-- Batch 3 does NOT overlap with Batch 2 (1801 > 1800) and is NOT a boundary
-- tie (1801 != 1800). Without the fix, compact_chunk skips the null check
-- on Batch 2 and incorrectly clears UNORDERED.
INSERT INTO metrics_mixed_nulls
SELECT '2025-01-03'::timestamptz + ((2000 + i) || ' minute')::interval,
       'd1',
       (1800 + i)::float
FROM generate_series(1,1000) i;

SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "MIXED_NULLS_CHUNK"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_mixed_nulls'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- Show batch metadata before compaction: 3 batches, batch 2 has mixed nulls
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :MIXED_NULLS_CHUNK
ORDER BY device, _ts_meta_min_1;

-- compact_chunk must detect the mixed-null batch and split it
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_mixed_nulls') chunk;

-- After compaction: the mixed batch should have been split.
-- NULLs should be in their own batch (meta_min IS NULL), positioned
-- at the end (NULLS LAST) rather than between 1800 and 1801.
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :MIXED_NULLS_CHUNK
ORDER BY device, _ts_meta_min_1 NULLS LAST;

-- Status should be COMPRESSED only (UNORDERED cleared correctly this time)
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_mixed_nulls') chunk;

-- Data integrity
SELECT count(*) FROM metrics_mixed_nulls;
SELECT count(*) AS null_count FROM metrics_mixed_nulls WHERE value IS NULL;

-- Verify ordering is correct: NULLs must come after all non-null values
-- This query will return wrong results if the bug is present (NULLs between 1800 and 1801)
SELECT value FROM metrics_mixed_nulls
WHERE device = 'd1'
ORDER BY value NULLS LAST, time
LIMIT 5 OFFSET 1795;

DROP TABLE metrics_mixed_nulls;

-- compact_chunk with nullable SECONDARY orderby column at boundary tie
-- Regression test: when orderby='time,value' and value is nullable,
-- two batches can tie on col1 (time) at the boundary. If the prev batch's
-- last row has NULL in col2 (value), batches_overlap_by_boundary_rows must
-- compare using NULLS FIRST/LAST semantics instead of skipping the NULL
-- and picking up a value from a different row.
CREATE TABLE metrics_secondary_null (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time,value', tsdb.segmentby='device');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert batch 1 (500 rows): time [00:01..08:20], with NULL value at the
-- last timestamp (i=500, time=08:20). With orderby='time,value NULLS LAST',
-- the last row in sorted order is (08:20, NULL).
INSERT INTO metrics_secondary_null
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd1',
       CASE WHEN i = 500 THEN NULL ELSE i::float END
FROM generate_series(1,500) i;

-- Insert batch 2 (500 rows): starts exactly at batch 1's max time (08:20).
-- This creates a boundary tie on col1 (time). All values are non-null.
-- The first row (08:20, 1001) ties with batch 1's last row (08:20, NULL).
-- With NULLS LAST, (08:20, NULL) should sort AFTER (08:20, 1001), but in
-- index order batch 1 comes before batch 2 — an ordering violation.
INSERT INTO metrics_secondary_null
SELECT '2025-01-03'::timestamptz + ((499 + i) || ' minute')::interval,
       'd1',
       (1000 + i)::float
FROM generate_series(1,500) i;

SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "SEC_NULL_CHUNK"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_secondary_null'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- Show batch metadata: col1 (time) ranges overlap at the boundary
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :SEC_NULL_CHUNK
ORDER BY device, _ts_meta_min_1;

-- compact_chunk must detect the boundary-tie overlap caused by NULL in col2
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_secondary_null') chunk;

-- After compaction: batches should be merged and re-sorted
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :SEC_NULL_CHUNK
ORDER BY device, _ts_meta_min_1;

-- Status should be COMPRESSED only
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_secondary_null') chunk;

-- Data integrity
SELECT count(*) FROM metrics_secondary_null;
SELECT count(*) AS null_count FROM metrics_secondary_null WHERE value IS NULL;

-- Verify ordering: at the boundary time (08:20 = +500 min), non-null value
-- must come before NULL with NULLS LAST ordering.
SELECT time, value FROM metrics_secondary_null
WHERE device = 'd1' AND time = '2025-01-03'::timestamptz + '500 minutes'::interval
ORDER BY time, value NULLS LAST;

DROP TABLE metrics_secondary_null;

-- compact_chunk with nullable SECONDARY orderby column at boundary tie (NULLS FIRST)
-- Mirror of the NULLS LAST test above. With NULLS FIRST, the curr batch's
-- first row has NULL in col2, which should sort BEFORE the prev batch's
-- last non-null col2 value — meaning the curr batch actually starts earlier
-- than its non-null min suggests, causing an ordering violation.
CREATE TABLE metrics_secondary_null_first (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time,value NULLS FIRST', tsdb.segmentby='device');

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Insert batch 1 (500 rows): time [00:01..08:20], all non-null values.
INSERT INTO metrics_secondary_null_first
SELECT '2025-01-03'::timestamptz + (i || ' minute')::interval,
       'd1',
       i::float
FROM generate_series(1,500) i;

-- Insert batch 2 (500 rows): starts at batch 1's max time (08:20).
-- The first row at time=08:20 has NULL value. With NULLS FIRST,
-- (08:20, NULL) should sort BEFORE (08:20, 500) from batch 1's last row.
-- But batch 2 comes after batch 1 in index order — ordering violation.
INSERT INTO metrics_secondary_null_first
SELECT '2025-01-03'::timestamptz + ((499 + i) || ' minute')::interval,
       'd1',
       CASE WHEN i = 1 THEN NULL ELSE (1000 + i)::float END
FROM generate_series(1,500) i;

SELECT comp_ch.schema_name || '.' || comp_ch.table_name AS "SEC_NF_CHUNK"
FROM _timescaledb_catalog.chunk ch1,
     _timescaledb_catalog.chunk comp_ch,
     _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
  AND ht.table_name = 'metrics_secondary_null_first'
  AND ch1.compressed_chunk_id = comp_ch.id
ORDER BY ch1.id LIMIT 1 \gset

-- Show batch metadata: boundary tie on col1 (time) at 08:20
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2
FROM :SEC_NF_CHUNK
ORDER BY device, _ts_meta_min_1;

-- compact_chunk must detect the boundary-tie overlap caused by NULL in col2
SELECT _timescaledb_functions.compact_chunk(chunk) FROM show_chunks('metrics_secondary_null_first') chunk;

-- After compaction: batches should be merged and re-sorted
SELECT ctid, device, _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1
FROM :SEC_NF_CHUNK
ORDER BY device, _ts_meta_min_1;

-- Status should be COMPRESSED only
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_secondary_null_first') chunk;

-- Data integrity
SELECT count(*) FROM metrics_secondary_null_first;
SELECT count(*) AS null_count FROM metrics_secondary_null_first WHERE value IS NULL;

-- Verify ordering: at the boundary time (08:20), NULL must come before non-null
-- with NULLS FIRST ordering.
SELECT time, value FROM metrics_secondary_null_first
WHERE device = 'd1' AND time = '2025-01-03'::timestamptz + '500 minutes'::interval
ORDER BY time, value NULLS FIRST;

DROP TABLE metrics_secondary_null_first;
