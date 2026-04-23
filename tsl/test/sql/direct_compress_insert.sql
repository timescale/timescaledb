-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE metrics (time TIMESTAMPTZ NOT NULL, device TEXT, value float) WITH (tsdb.hypertable, tsdb.orderby='time');

-- first try without the GUCs
BEGIN;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' hour')::interval, 'd1', i::float FROM generate_series(0,100) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- should have no status aka normal uncompressed chunk
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- EXPLAIN with too small batch
EXPLAIN (BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) INSERT INTO metrics SELECT '2025-01-01'::timestamptz, 'd1', i::float FROM generate_series(0,5) i;
-- EXPLAIN with large enough batch
EXPLAIN (BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) INSERT INTO metrics SELECT '2025-01-01'::timestamptz, 'd1', i::float FROM generate_series(0,500) i;

-- simple test with compressed insert enabled
BEGIN;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
-- since the chunks are new status should be COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- simple test with compressed insert enabled and reversed order
BEGIN;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz - (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
-- since the chunks are new status should be COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

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

-- test compressing into uncompressed chunk
RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_direct_compress_insert_sort_batches;
RESET timescaledb.enable_direct_compress_insert_client_sorted;

BEGIN;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- since the chunks are new status should be COMPRESSED, PARTIAL
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- simple test with compressed insert enabled and reversed order
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz - (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- since the chunks are new status should be COMPRESSED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- simple test with compressed insert enabled and no presorted
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz - (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- since the chunks are new status should be COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- simple test with compressed insert enabled and no presorted and with uncompressed data
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz, 'd1', 0;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz - (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
-- since the chunks are new status should be COMPRESSED, UNORDERED, PARTIAL
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- simple test with compressed insert enabled and no presorted with partial and compressed chunks
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz, 'd1', 0;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
INSERT INTO metrics SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
SELECT format('%I.%I',schema_name,table_name) AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.chunk where compressed_chunk_id IS NULL order by 1 desc limit 1 \gset
-- should see overlapping batches
select _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 from :COMPRESSED_CHUNK order by 2;
-- since the chunks are new status should be COMPRESSED, UNORDERED, PARTIAL and COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk order by 1;
ROLLBACK;

-- simple test with compressed insert enabled and presorted with partial and compressed chunks
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz, 'd1', 0;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
INSERT INTO metrics SELECT '2025-01-05'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
SELECT format('%I.%I',schema_name,table_name) AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.chunk where compressed_chunk_id IS NULL order by 1 desc limit 1 \gset
-- should not see overlapping batches
select _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 from :COMPRESSED_CHUNK order by 2;
-- since the chunks are new status should be COMPRESSED, UNORDERED, PARTIAL and COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk order by 1;
ROLLBACK;

-- test with segmentby
BEGIN;
ALTER TABLE metrics SET (tsdb.segmentby = 'device');
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz - (i || ' minute')::interval, floor(i), i::float FROM generate_series(0.0,9.8,0.2) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT format('%I.%I',schema_name,table_name) AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.chunk where compressed_chunk_id IS NULL \gset
-- should have 10 batches
SELECT count(*) FROM :COMPRESSED_CHUNK;
-- since the chunks are new status should be COMPRESSED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- segmentby with overlapping batches
BEGIN;
ALTER TABLE metrics SET (tsdb.segmentby = 'device');
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd'||i%2, i::float FROM generate_series(0,3000) i;
INSERT INTO metrics SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd'||i%2, i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT format('%I.%I',schema_name,table_name) AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.chunk where compressed_chunk_id IS NULL order by 1 desc limit 1 \gset
-- should see overlapping batches per device
select _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, device from :COMPRESSED_CHUNK order by 4, 2;
-- since the chunks are new status should be COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- multikey orderby
BEGIN;
ALTER TABLE metrics SET (tsdb.orderby = 'device desc,time');
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd'||i%3, i::float FROM generate_series(0,3000) i;
INSERT INTO metrics SELECT '2025-01-02'::timestamptz - (i || ' minute')::interval, 'd'||i%3, i::float FROM generate_series(0,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT format('%I.%I',schema_name,table_name) AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.chunk where compressed_chunk_id IS NULL order by 1 limit 1 \gset
-- should see overlapping batches
select _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1, _ts_meta_min_2, _ts_meta_max_2 from :COMPRESSED_CHUNK order by 2, 4;
-- since the chunks are new status should be COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- test unique constraints prevent direct compress
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
ALTER TABLE metrics ADD CONSTRAINT unique_time_device UNIQUE (time, device);
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,100) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- test triggers prevent direct compress
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
CREATE OR REPLACE FUNCTION test_trigger() RETURNS TRIGGER AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql;
CREATE TRIGGER metrics_trigger BEFORE INSERT OR UPDATE ON metrics FOR EACH ROW EXECUTE FUNCTION test_trigger();
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,100) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- test caggs with direct compress
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
CREATE MATERIALIZED VIEW metrics_cagg WITH (tsdb.continuous) AS SELECT time_bucket('1 hour', time) AS bucket, device, avg(value) AS avg_value FROM metrics GROUP BY bucket, device WITH NO DATA;
INSERT INTO metrics SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(0,100) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT DISTINCT status FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL;
ROLLBACK;

-- test chunk status handling
CREATE TABLE metrics_status(time timestamptz) WITH (tsdb.hypertable,tsdb.partition_column='time');

INSERT INTO metrics_status SELECT '2025-01-01';
-- normal insert should result in chunk status 0
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;

SET timescaledb.enable_direct_compress_insert = true;

BEGIN;
INSERT INTO metrics_status SELECT '2025-01-01' FROM generate_series(1,9);
-- small insert batches should not result in compressed chunk
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

BEGIN;
INSERT INTO metrics_status SELECT '2025-01-01' FROM generate_series(1,10);
-- status should be COMPRESSED, UNORDERED, PARTIAL since we have more than 10 rows in the chunk
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

BEGIN;
-- compressed sorted copy into uncompressed chunk should result in chunk status 9 (compressed,partial)
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
INSERT INTO metrics_status SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval FROM generate_series(0,100) i;
-- status should be COMPRESSED, PARTIAL
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

TRUNCATE metrics_status;

BEGIN;
-- compressed insert into new chunk should result in chunk status 3 (compressed,unordered)
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
INSERT INTO metrics_status SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval FROM generate_series(0,100) i;
-- status should be COMPRESSED, UNORDERED
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

BEGIN;
-- compressed sorted copy into new chunk should result in chunk status 1 (compressed)
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
INSERT INTO metrics_status SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval FROM generate_series(0,100) i;
-- status should be COMPRESSED
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

SET timescaledb.enable_direct_compress_insert = false;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
INSERT INTO metrics_status SELECT '2025-01-01';
-- no status aka normal uncompressed chunk
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
SELECT compress_chunk(show_chunks('metrics_status'));
-- status should be COMPRESSED
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;

BEGIN;
-- compressed insert into fully compressed chunk should result in chunk status 3 (compressed,unordered)
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
INSERT INTO metrics_status SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval FROM generate_series(0,100) i;
-- status should be COMPRESSED, UNORDERED
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

BEGIN;
-- compressed insert new chunk should result in chunk status 1 (compressed)
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
INSERT INTO metrics_status SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval FROM generate_series(0,100) i;
-- status should be COMPRESSED
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_status') chunk;
ROLLBACK;

-- test direct compress into chunk directly
CREATE TABLE metrics_chunk(time timestamptz) WITH (tsdb.hypertable,tsdb.partition_column='time');
SET timescaledb.enable_direct_compress_insert = true;

-- create uncompressed chunk
INSERT INTO metrics_chunk SELECT '2025-01-01';
-- status should be normal uncompressed chunk since it was single tuple insert
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_chunk') chunk;
SELECT show_chunks('metrics_chunk') AS "CHUNK" \gset

EXPLAIN (costs off,summary off,timing off) INSERT INTO :CHUNK SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval FROM generate_series(0,100) i;

BEGIN;
INSERT INTO :CHUNK SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval FROM generate_series(0,100) i;
-- status should be COMPRESSED, UNORDERED, PARTIAL
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_chunk') chunk;
-- delete should propagate to compressed chunk
SELECT count(*) FROM :CHUNK;
EXPLAIN (analyze,buffers off,costs off,summary off,timing off) DELETE FROM :CHUNK WHERE time > '2025-01-01'::timestamptz;
SELECT count(*) FROM :CHUNK;
ROLLBACK;

BEGIN;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;
INSERT INTO :CHUNK SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval FROM generate_series(0,100) i;
-- status should be COMPRESSED, PARTIAL
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics_chunk') chunk;
ROLLBACK;

-- simple test with compressed insert enabled and sorting limited to 500
-- batches should be limited to that amount so we have more compressed batches
-- dataset is tweaked to fall into single chunk with exactly 3 batches
BEGIN;
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.direct_compress_insert_tuple_sort_limit = 1000;
INSERT INTO metrics SELECT '2025-01-02'::timestamptz + (i || ' minute')::interval, 'd1', i::float FROM generate_series(1,3000) i;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, SUMMARY OFF, TIMING OFF) SELECT * FROM metrics;
SELECT first(time,rn), last(time,rn) FROM (SELECT ROW_NUMBER() OVER () as rn, time FROM metrics) sub;
-- since the chunks are new status should be COMPRESSED, UNORDERED
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('metrics') chunk;
ROLLBACK;

-- test INSERT SELECT from compressed source into compressed target
-- this tests that reading from ColumnarScan correctly copies all rows
SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = true;

CREATE TABLE compress_src (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    value FLOAT NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.chunk_interval = '4 hours',
    tsdb.orderby = 'time asc',
    tsdb.segment_by = 'device_id'
);

CREATE TABLE compress_tgt (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    value FLOAT NOT NULL
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'time',
    tsdb.chunk_interval = '4 hours',
    tsdb.orderby = 'time asc',
    tsdb.segment_by = 'device_id'
);

-- Insert with device_id, time ordering to create multi-row compressed blocks
INSERT INTO compress_src (time, device_id, value)
SELECT t.time, s.device_id, random()
FROM (SELECT generate_series('2025-01-01'::timestamptz, '2025-01-02'::timestamptz, INTERVAL '1 minute') AS time) t
CROSS JOIN (SELECT generate_series(1, 5)::TEXT AS device_id) s
ORDER BY s.device_id, t.time;

SELECT count(*) FROM compress_src;

INSERT INTO compress_tgt SELECT * FROM compress_src ORDER BY device_id, time;

SELECT count(*) FROM compress_tgt;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_direct_compress_insert_client_sorted;

DROP TABLE compress_src;
DROP TABLE compress_tgt;

-- simple test to check default segmentby does not get set for direct compress
BEGIN;
RESET timescaledb.enable_direct_compress_insert;
CREATE TABLE test_segmentby_stats (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 5) i;

ANALYZE test_segmentby_stats;
SELECT compress_chunk(c) FROM show_chunks('test_segmentby_stats') c;

-- will have devide_id as default segmentby for compressed chunk;
SELECT * FROM _timescaledb_catalog.compression_settings;

SET timescaledb.enable_direct_compress_insert = true;
INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-06'::timestamptz, '2024-01-07'::timestamptz, '1 second') t,
     generate_series(1, 5) i;
ANALYZE test_segmentby_stats;
-- will have default segmentby set for a newly created direct compressed chunk (should observe a skipped chunk id);
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

BEGIN;
RESET timescaledb.enable_direct_compress_insert;
CREATE TABLE test_segmentby_stats (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress, tsdb.segmentby='device_id');

SET timescaledb.enable_direct_compress_insert = true;
INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-06'::timestamptz, '2024-01-07'::timestamptz, '1 second') t,
     generate_series(1, 5) i;
ANALYZE test_segmentby_stats;
-- will have device_id by as configured segmentby for direct compressed chunk (should not observe skipped chunk id);
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test orderby columns are not selected by DC segmentby default
BEGIN;
RESET timescaledb.enable_direct_compress_insert;
CREATE TABLE test_orderby_not_segmentby(time timestamptz NOT NULL, device_id int NOT NULL,
    value float) WITH (tsdb.hypertable);
CREATE INDEX ON test_orderby_not_segmentby(device_id);
INSERT INTO test_orderby_not_segmentby SELECT '2024-01-01'::timestamptz + (i||' min')::interval,
    (i%5)+1, random() FROM generate_series(1,500) i;
ANALYZE test_orderby_not_segmentby;
ALTER TABLE test_orderby_not_segmentby SET (timescaledb.compress);
SET timescaledb.enable_direct_compress_insert = on;
INSERT INTO test_orderby_not_segmentby SELECT '2024-06-01'::timestamptz + (i||' min')::interval,
    (i%5)+1, random() FROM generate_series(1,2000) i;
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

