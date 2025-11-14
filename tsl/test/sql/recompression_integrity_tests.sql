-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Set path to check data intergrity after in-memory recompression
\set RECOMPRESSION_INTEGRITY_CHECK_RELPATH 'include/recompression_integrity_check.sql'
\set BATCH_METADATA_QUERY ''

-- Test Case 1: Basic segmentby configuration
DROP TABLE IF EXISTS recomp_segmentby_test CASCADE;
CREATE TABLE recomp_segmentby_test(
    time timestamptz NOT NULL,
    device text,
    location text,
    temperature float,
    humidity float
);
SELECT create_hypertable('recomp_segmentby_test','time') \gset

-- Set compression with single segmentby column
ALTER TABLE recomp_segmentby_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby='device',
    timescaledb.compress_orderby='time'
);

-- Insert test data across multiple time ranges
INSERT INTO recomp_segmentby_test VALUES
  ('2000-01-01 00:00:00', 'device1', 'room1', 20.5, 60.0),
  ('2000-01-01 01:00:00', 'device1', 'room1', 21.0, 61.5),
  ('2000-01-01 02:00:00', 'device2', 'room2', 19.8, 58.2),
  ('2000-01-02 00:00:00', 'device1', 'room1', 22.1, 63.0),
  ('2000-01-02 01:00:00', 'device2', 'room2', 18.9, 55.7),
  ('2001-01-01 00:00:00', 'device1', 'room1', 25.0, 70.0),
  ('2001-01-01 01:00:00', 'device2', 'room2', 23.5, 68.5);

\set TEST_TABLE_NAME 'recomp_segmentby_test'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE recomp_segmentby_test CASCADE;

-- Test Case 2: Multiple segmentby columns configuration
DROP TABLE IF EXISTS recomp_multi_segmentby_test CASCADE;
CREATE TABLE recomp_multi_segmentby_test(
    time timestamptz NOT NULL,
    device text,
    location text,
    sensor_type text,
    value float
);
SELECT create_hypertable('recomp_multi_segmentby_test','time') \gset

-- Set compression with multiple segmentby columns
ALTER TABLE recomp_multi_segmentby_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby='device,location',
    timescaledb.compress_orderby='time'
);

-- Insert test data
INSERT INTO recomp_multi_segmentby_test VALUES
  ('2000-01-01', 'device1', 'room1', 'temp', 20.5),
  ('2000-01-01', 'device1', 'room1', 'humidity', 60.0),
  ('2000-01-01', 'device1', 'room2', 'temp', 21.0),
  ('2000-01-01', 'device2', 'room1', 'temp', 19.8),
  ('2000-01-01', 'device2', 'room2', 'humidity', 58.2),
  ('2001-01-01', 'device1', 'room1', 'temp', 22.1),
  ('2001-01-01', 'device2', 'room1', 'humidity', 63.0);

\set TEST_TABLE_NAME 'recomp_multi_segmentby_test'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE recomp_multi_segmentby_test CASCADE;

-- Test Case 3: Sparse index configuration
DROP TABLE IF EXISTS recomp_index_test CASCADE;
CREATE TABLE recomp_index_test(
    x int,
    value text,
    u uuid,
    ts timestamp
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='x',
    tsdb.segment_by='',
    tsdb.order_by='x',
    tsdb.index='bloom("u"),minmax("ts")'
);

-- Insert test data with sparse UUID pattern
INSERT INTO recomp_index_test
SELECT x, md5(x::text),
    CASE WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'::uuid
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid END,
    '2021-01-01'::timestamp + (interval '1 hour') * x
FROM generate_series(1, 10000) x;

INSERT INTO recomp_index_test
SELECT x, md5(x::text),
    CASE WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'::uuid
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid END,
    '2022-01-01'::timestamp + (interval '1 hour') * x
FROM generate_series(1, 10000) x;

\set TEST_TABLE_NAME 'recomp_index_test'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE recomp_index_test CASCADE;

-- Test Case 4: Large dataset
DROP TABLE IF EXISTS recomp_large_data_test CASCADE;
CREATE TABLE recomp_large_data_test(
    x int,
    value text,
    u uuid,
    ts timestamp
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='ts',
    tsdb.segment_by='',
    tsdb.order_by='ts',
    tsdb.index=''
);

-- Insert test data with sparse UUID pattern
INSERT INTO recomp_large_data_test
SELECT x, md5(x::text),
    CASE WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'::uuid
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid END,
    '2021-01-01'::timestamp + (interval '1 second') * x
FROM generate_series(1, 10000) x;

INSERT INTO recomp_large_data_test
SELECT x, md5(x::text),
    CASE WHEN x = 7134 THEN '90ec9e8e-4501-4232-9d03-6d7cf6132815'::uuid
        ELSE '6c1d0998-05f3-452c-abd3-45afe72bbcab'::uuid END,
    '2022-01-01'::timestamp + (interval '1 second') * x
FROM generate_series(1, 10000) x;

\set TEST_TABLE_NAME 'recomp_large_data_test'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH

SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE recomp_large_data_test CASCADE;

-- Setup Direct Compress
-- Batches may be uneven with direct compress but should always be even after recompression.
\set BATCH_METADATA_QUERY 'SELECT _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM :COMPRESSED_CHUNK_NAME;'
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Test Case 5: Unordered chunk
DROP TABLE IF EXISTS recomp_unordered CASCADE;
CREATE TABLE recomp_unordered (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');
INSERT INTO recomp_unordered SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,100) i;
INSERT INTO recomp_unordered SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(101,800) i;

-- Will not use in-memory recompression due to unordered
\set TEST_TABLE_NAME 'recomp_unordered'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH
SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE IF EXISTS recomp_unordered CASCADE;

-- Test Case 5: Direct Compress Batches
SET timescaledb.enable_direct_compress_insert_client_sorted = true;

DROP TABLE IF EXISTS recomp_direct_compress CASCADE;
CREATE TABLE recomp_direct_compress (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');

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
    EXECUTE format('INSERT INTO recomp_direct_compress SELECT ''2025-01-01''::timestamptz + (i || '' minute'')::interval, ''d1'', i::float FROM generate_series(%s,%s) i',
                    start_pos, start_pos + batch_size - 1);

    start_pos := start_pos + batch_size;
END LOOP;
END $$;

-- Will use in-memory recompression since client_sorted = true
\set TEST_TABLE_NAME 'recomp_direct_compress'
\ir :RECOMPRESSION_INTEGRITY_CHECK_RELPATH
SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY relid;
DROP TABLE IF EXISTS recomp_direct_compress CASCADE;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_direct_compress_insert_sort_batches;
RESET timescaledb.enable_direct_compress_insert_client_sorted;



