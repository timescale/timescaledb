-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Set path to check data intergrity after in-memory recompression
\set TEST_BASE_NAME recompression_intergrity_unordered
-- requires TEST_BASE_NAME, TEST_TABLE_NAME, BATCH_METADATA_QUERY, ORDER_BY_CLAUSE
\set RECOMPRESSION_INTEGRITY_CHECK_RELPATH 'include/recompression_integrity_check.sql'

-- Setup Direct Compress
-- Batches may be uneven with direct compress but should always be even after recompression.
\set BATCH_METADATA_QUERY 'SELECT _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM :COMPRESSED_CHUNK_NAME;'
\set ORDER_BY_CLAUSE ''
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Test Case 1: Unordered chunk
DROP TABLE IF EXISTS recomp_unordered CASCADE;
CREATE TABLE recomp_unordered (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');
-- Insert uneven batches
SELECT setseed(0.5);
DO $$
DECLARE
start_pos int := 0;
batch_size int;
i int;
BEGIN
FOR i IN 1..5 LOOP  -- 5 random batches
    batch_size := (random() * 200 + 50)::int;  -- Random size 50-250
    EXECUTE format('INSERT INTO recomp_unordered SELECT ''2025-01-01''::timestamptz + (i || '' minute'')::interval, ''d1'', i::float FROM generate_series(%s,%s) i',
                    start_pos, start_pos + batch_size - 1);

    start_pos := start_pos + batch_size;
END LOOP;
END $$;
-- status should be compressed, unordered
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_unordered') chunk;
\set TEST_TABLE_NAME 'recomp_unordered'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH
-- status should be compressed
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_unordered') chunk;
SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE IF EXISTS recomp_unordered CASCADE;

-- Test Case 2: Unordered chunk with segmentby
DROP TABLE IF EXISTS recomp_unordered_segmentby CASCADE;
CREATE TABLE recomp_unordered_segmentby (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device');
-- Insert uneven batches with multiple devices
SELECT setseed(0.5);
DO $$
DECLARE
start_pos int := 0;
batch_size int;
i int;
device_id int;
BEGIN
FOR i IN 1..5 LOOP  -- 5 random batches
    batch_size := (random() * 200 + 50)::int;  -- Random size 50-250
    device_id := (i % 3) + 1;  -- Rotate between 3 devices
    EXECUTE format('INSERT INTO recomp_unordered_segmentby SELECT ''2025-01-01''::timestamptz + (i || '' minute'')::interval, ''d%s'', i::float FROM generate_series(%s,%s) i',
                    device_id, start_pos, start_pos + batch_size - 1);

    start_pos := start_pos + batch_size;
END LOOP;
END $$;
-- status should be compressed, unordered
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_unordered_segmentby') chunk;
\set TEST_TABLE_NAME 'recomp_unordered_segmentby'
\set ORDER_BY_CLAUSE ' ORDER BY device, time'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH
-- status should be compressed
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_unordered_segmentby') chunk;
SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE IF EXISTS recomp_unordered_segmentby CASCADE;

-- Test Case 3: Unordered chunk with segmentby and multiple orderbys
DROP TABLE IF EXISTS recomp_unordered_multi_orderby CASCADE;
CREATE TABLE recomp_unordered_multi_orderby (time TIMESTAMPTZ NOT NULL, device TEXT, sensor_id INT, value float) WITH (tsdb.hypertable, tsdb.orderby='time,sensor_id', tsdb.segmentby='device');
-- Insert uneven batches with multiple devices and sensor_ids
SELECT setseed(0.5);
DO $$
DECLARE
start_pos int := 0;
batch_size int;
i int;
device_id int;
sensor_id int;
BEGIN
FOR i IN 1..5 LOOP  -- 5 random batches
    batch_size := (random() * 200 + 50)::int;  -- Random size 50-250
    device_id := (i % 3) + 1;  -- Rotate between 3 devices
    sensor_id := (i % 2) + 1;  -- Rotate between 2 sensors
    EXECUTE format('INSERT INTO recomp_unordered_multi_orderby SELECT ''2025-01-01''::timestamptz + (i || '' minute'')::interval, ''d%s'', %s, i::float FROM generate_series(%s,%s) i',
                    device_id, sensor_id, start_pos, start_pos + batch_size - 1);

    start_pos := start_pos + batch_size;
END LOOP;
END $$;
-- status should be compressed, unordered
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_unordered_multi_orderby') chunk;
\set TEST_TABLE_NAME 'recomp_unordered_multi_orderby'
\set ORDER_BY_CLAUSE ' ORDER BY device, time, sensor_id'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH
-- status should be compressed
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_unordered_multi_orderby') chunk;
SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE IF EXISTS recomp_unordered_multi_orderby CASCADE;

-- Test Case 4: Orderby is not necessarily increasing every insert
DROP TABLE IF EXISTS recomp_truly_unordered CASCADE;
CREATE TABLE recomp_truly_unordered (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.segmentby='device', tsdb.orderby='time');
INSERT INTO recomp_truly_unordered SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(50,200) i;
INSERT INTO recomp_truly_unordered SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float FROM generate_series(50,200) i;
INSERT INTO recomp_truly_unordered SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,100) i;
INSERT INTO recomp_truly_unordered SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float FROM generate_series(0,100) i;
INSERT INTO recomp_truly_unordered SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(150,700) i;
INSERT INTO recomp_truly_unordered SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float FROM generate_series(150,700) i;
-- status should be compressed, unordered
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_truly_unordered') chunk;
\set TEST_TABLE_NAME 'recomp_truly_unordered'
\set ORDER_BY_CLAUSE ' ORDER BY device, time'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH
-- status should be compressed
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_truly_unordered') chunk;
SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE IF EXISTS recomp_truly_unordered CASCADE;

-- Test Case 5: Should not recompress, unless recompress flag is set to true
DROP TABLE IF EXISTS recomp_without_flag CASCADE;
CREATE TABLE recomp_without_flag (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.segmentby='device', tsdb.orderby='time');
INSERT INTO recomp_without_flag SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(50,200) i;
INSERT INTO recomp_without_flag SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,100) i;
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_without_flag') chunk;
SELECT compress_chunk(ch) FROM show_chunks('recomp_without_flag') ch; -- should be a no-op
-- status should be compressed, unordered
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_without_flag') chunk;
SELECT compress_chunk(ch, recompress => true) FROM show_chunks('recomp_without_flag') ch;
-- status should be compressed
SELECT chunk, _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('recomp_without_flag') chunk;
DROP TABLE IF EXISTS recomp_without_flag CASCADE;
RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_direct_compress_insert_sort_batches;
RESET timescaledb.enable_direct_compress_insert_client_sorted;



