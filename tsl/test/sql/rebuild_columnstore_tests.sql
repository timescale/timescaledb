-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test rebuild_columnstore function with various chunk states

-- Helper view to show chunk and compressed chunk info
CREATE VIEW chunk_status_view AS
SELECT
    h.table_name AS hypertable_name,
    uc.schema_name || '.' || uc.table_name AS chunk,
    cc.schema_name || '.' || cc.table_name AS compressed_chunk,
    _timescaledb_functions.chunk_status_text((uc.schema_name || '.' || uc.table_name)::regclass) AS status
FROM _timescaledb_catalog.chunk uc
LEFT JOIN _timescaledb_catalog.chunk cc ON uc.compressed_chunk_id = cc.id
JOIN _timescaledb_catalog.hypertable h ON uc.hypertable_id = h.id;

SET timescaledb.enable_direct_compress_insert = true;

-- Test 1: Rebuild fully compressed (ordered) chunk
SET timescaledb.enable_direct_compress_insert_client_sorted = true;

CREATE TABLE rebuild_ordered (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time');

INSERT INTO rebuild_ordered
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(101,750) i;

-- should be compressed
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_ordered';
SELECT chunk FROM show_chunks('rebuild_ordered') AS chunk LIMIT 1 \gset
CALL _timescaledb_functions.rebuild_columnstore(:'chunk'::regclass);

-- should be compressed
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_ordered';

DROP TABLE rebuild_ordered CASCADE;
RESET timescaledb.enable_direct_compress_insert_client_sorted;

-- Test 2: Rebuild unordered chunk (should order the chunk)
CREATE TABLE rebuild_unordered (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time');

INSERT INTO rebuild_unordered
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,100) i;

INSERT INTO rebuild_unordered
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(101,750) i;

-- should be compressed, unordered
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_unordered';
SELECT chunk FROM show_chunks('rebuild_unordered') AS chunk LIMIT 1 \gset
CALL _timescaledb_functions.rebuild_columnstore(:'chunk'::regclass);

-- should be compressed
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_unordered';

DROP TABLE rebuild_unordered CASCADE;

-- Test 3: Rebuild partial chunk (should preserve partial state with in-memory)
CREATE TABLE rebuild_partial (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time');

INSERT INTO rebuild_partial
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,100) i;

INSERT INTO rebuild_partial
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(101,750) i;

-- Less than 10 tuples will not undergo direct compression
INSERT INTO rebuild_partial
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(751,755) i;

-- should be compressed, unordered, partial
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_partial';
SELECT chunk FROM show_chunks('rebuild_partial') AS chunk LIMIT 1 \gset
CALL _timescaledb_functions.rebuild_columnstore(:'chunk'::regclass);

-- should be compressed, partial
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_partial';

-- Verify uncompressed row count
SELECT chunk FROM show_chunks('rebuild_partial') AS chunk LIMIT 1 \gset
SELECT COUNT(*) FROM ONLY :chunk;
SELECT COUNT(*) FROM rebuild_partial;

DROP TABLE rebuild_partial CASCADE;

-- Test 4: Rebuild with segmentby
CREATE TABLE rebuild_segmentby (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device');

-- Insert data for multiple devices creating unordered batches
INSERT INTO rebuild_segmentby
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,250) i;

INSERT INTO rebuild_segmentby
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(251,500) i;

INSERT INTO rebuild_segmentby
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(501,750) i;

-- get meta data info before rebuild
SELECT chunk AS "CHUNK_NAME", compressed_chunk AS "COMPRESSED_CHUNK_NAME"
FROM chunk_status_view
WHERE hypertable_name = 'rebuild_segmentby'
LIMIT 1 \gset
SELECT _ts_meta_count, device, _ts_meta_min_1, _ts_meta_max_1 FROM :COMPRESSED_CHUNK_NAME;

SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_segmentby';
CALL _timescaledb_functions.rebuild_columnstore(:'CHUNK_NAME'::regclass);
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_segmentby';

-- get meta data info after rebuild
SELECT compressed_chunk AS "COMPRESSED_CHUNK_NAME"
FROM chunk_status_view
WHERE chunk = :'CHUNK_NAME'
LIMIT 1 \gset
SELECT _ts_meta_count, device, _ts_meta_min_1, _ts_meta_max_1 FROM :COMPRESSED_CHUNK_NAME;

DROP TABLE rebuild_segmentby CASCADE;

-- Test 5: Change compression settings
CREATE TABLE rebuild_settings (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time', tsdb.segmentby='device');

-- Insert data for multiple devices creating unordered batches
INSERT INTO rebuild_settings
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,250) i;

INSERT INTO rebuild_settings
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(251,500) i;

INSERT INTO rebuild_settings
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(501,750) i;
INSERT INTO rebuild_settings
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd2', i::float
FROM generate_series(300,305) i;

-- get meta data info before rebuild
SELECT chunk AS "CHUNK_NAME", compressed_chunk AS "COMPRESSED_CHUNK_NAME"
FROM chunk_status_view
WHERE hypertable_name = 'rebuild_settings'
LIMIT 1 \gset
SELECT _ts_meta_count, device, _ts_meta_min_1, _ts_meta_max_1 FROM :COMPRESSED_CHUNK_NAME;
ALTER TABLE rebuild_settings SET (
    timescaledb.compress_segmentby = ''
);
-- settings changed, decompress/compress fallback
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_settings';
CALL _timescaledb_functions.rebuild_columnstore(:'CHUNK_NAME'::regclass);
-- should be compressed, partial status cleared due to full recompression
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_settings';

-- get meta data info after rebuild
SELECT compressed_chunk AS "COMPRESSED_CHUNK_NAME"
FROM chunk_status_view
WHERE chunk = :'CHUNK_NAME'
LIMIT 1 \gset
SELECT _ts_meta_count, _ts_meta_min_1, _ts_meta_max_1 FROM :COMPRESSED_CHUNK_NAME;

DROP TABLE rebuild_settings CASCADE;

-- Test 6: Edge cases handling
\set ON_ERROR_STOP 0

-- Invalid OID error handling
CALL _timescaledb_functions.rebuild_columnstore(NULL);
CALL _timescaledb_functions.rebuild_columnstore(0::OID);

-- Invalid chunk_id
CALL _timescaledb_functions.rebuild_columnstore(15000);
\set ON_ERROR_STOP 1

CREATE TABLE rebuild_error (time TIMESTAMPTZ NOT NULL, device TEXT, value float)
WITH (tsdb.hypertable, tsdb.orderby='time');
INSERT INTO rebuild_error
SELECT '2025-01-01'::timestamptz + (i || ' minute')::interval, 'd1', i::float
FROM generate_series(0,5) i;
-- uncompressed notice
SELECT chunk FROM show_chunks('rebuild_error') AS chunk LIMIT 1 \gset
CALL _timescaledb_functions.rebuild_columnstore(:'chunk'::regclass);
SELECT compress_chunk(chunk) FROM show_chunks('rebuild_error') AS chunk;
SELECT _timescaledb_functions.freeze_chunk(chunk) FROM show_chunks('rebuild_error') AS chunk;
-- frozen notice
CALL _timescaledb_functions.rebuild_columnstore(:'chunk'::regclass);
SELECT _timescaledb_functions.unfreeze_chunk(chunk) FROM show_chunks('rebuild_error') AS chunk;
SET timescaledb.enable_in_memory_recompression = false;
-- guc disabled, decompress/compress fallback
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_error';
CALL _timescaledb_functions.rebuild_columnstore(:'chunk'::regclass);
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_error';
RESET timescaledb.enable_in_memory_recompression;
DROP TABLE rebuild_error CASCADE;

CREATE TABLE rebuild_no_orderby (col1 INT NOT NULL, device INT);
SELECT create_hypertable('rebuild_no_orderby', 'col1', chunk_time_interval => 10);
ALTER TABLE rebuild_no_orderby SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'col1'
);

INSERT INTO rebuild_no_orderby VALUES (1, 2);
SELECT compress_chunk(chunk) FROM show_chunks('rebuild_no_orderby') chunk;
-- no orderby, decompress/compress fallback
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_no_orderby';
SELECT chunk FROM show_chunks('rebuild_no_orderby') AS chunk LIMIT 1 \gset
CALL _timescaledb_functions.rebuild_columnstore(:'chunk'::regclass);
SELECT * FROM chunk_status_view WHERE hypertable_name = 'rebuild_no_orderby';
DROP TABLE rebuild_no_orderby CASCADE;

DROP VIEW chunk_status_view;
RESET timescaledb.enable_direct_compress_insert;
