-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO ALL

CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";
CREATE VIEW ht_settings AS SELECT * FROM timescaledb_information.hypertable_compression_settings ORDER BY upper(hypertable::text) COLLATE "C";
CREATE VIEW chunk_settings AS SELECT * FROM timescaledb_information.chunk_compression_settings ORDER BY upper(hypertable::text) COLLATE "C", upper(chunk::text) COLLATE "C";

CREATE TABLE metrics(time timestamptz not null, device text, value float);
SELECT table_name FROM create_hypertable('metrics','time');

ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device');
SELECT * FROM settings;
SELECT * FROM ht_settings;
SELECT * FROM chunk_settings;

-- create 2 chunks
INSERT INTO metrics VALUES ('2000-01-01'), ('2001-01-01');
-- no change to settings
SELECT * FROM settings;

--Enable compression path info
SET timescaledb.debug_compression_path_info= 'on';
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET timescaledb.debug_compression_path_info;

SELECT * FROM settings;
SELECT * FROM chunk_settings;

SELECT compress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM settings;
SELECT * FROM chunk_settings;

-- dropping chunk should remove that chunks compression settings
DROP TABLE _timescaledb_internal._hyper_1_1_chunk;
SELECT * FROM settings;
SELECT * FROM chunk_settings;

-- decompress_chunk should remove settings for that chunk
SELECT decompress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM settings;
SELECT * FROM chunk_settings;

-- compress_chunk should add settings back
SELECT compress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM settings;
SELECT * FROM chunk_settings;

-- dropping hypertable should remove all settings
DROP TABLE metrics;
SELECT * FROM settings;
SELECT * FROM ht_settings;
SELECT * FROM chunk_settings;

CREATE TABLE metrics(time timestamptz not null, d1 text, d2 text, value float);
SELECT table_name FROM create_hypertable('metrics','time');

ALTER TABLE metrics SET (timescaledb.compress);
-- hypertable should have default settings now
SELECT * FROM settings;
SELECT * FROM ht_settings;

-- create chunk
INSERT INTO metrics VALUES ('2000-01-01');
ALTER TABLE metrics SET (timescaledb.compress_segmentby='d1');
-- settings should be updated
SELECT * FROM settings;
SELECT * FROM ht_settings;

SELECT compress_chunk(show_chunks('metrics'));
-- settings for compressed chunk should be present
SELECT * FROM settings;
SELECT * FROM chunk_settings;

-- changing settings should update settings for hypertable but not existing compressed chunks
ALTER TABLE metrics SET (timescaledb.compress_segmentby='d2');
SELECT * FROM settings;
SELECT * FROM ht_settings;
SELECT * FROM chunk_settings;

-- changing settings should update settings for hypertable but not existing compressed chunks
ALTER TABLE metrics SET (timescaledb.compress_segmentby='');
SELECT * FROM settings;
SELECT * FROM ht_settings;
SELECT * FROM chunk_settings;

-- create another chunk
INSERT INTO metrics VALUES ('2000-02-01');
SELECT compress_chunk(show_chunks('metrics'), true);

SELECT * FROM settings;
SELECT * FROM ht_settings;
SELECT * FROM chunk_settings;
ALTER TABLE metrics SET (timescaledb.compress_orderby='"time" desc', timescaledb.compress_segmentby='d2', timescaledb.index='bloom(value)');

SELECT format('%I.%I', schema_name, table_name) AS "CHUNK" FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL ORDER BY id LIMIT 1 OFFSET 1\gset

-- recompressing chunks should apply current hypertable settings
SELECT compress_chunk(:'CHUNK', recompress:=true);
SELECT * FROM settings;
SELECT compress_chunk(:'CHUNK', recompress:=true);
SELECT * FROM settings;

ALTER TABLE metrics SET (timescaledb.compress_orderby='"time" desc', timescaledb.compress_segmentby='d2', timescaledb.index='');

SELECT format('%I.%I', schema_name, table_name) AS "CHUNK" FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL ORDER BY id LIMIT 1 OFFSET 1\gset
SELECT compress_chunk(:'CHUNK', recompress:=true);
SELECT * FROM settings;
SELECT * FROM ht_settings;
SELECT * FROM chunk_settings;

-- test different order by flags with compression settings view
CREATE TABLE metrics2(time timestamptz not null, d1 text, d2 text, value float);
SELECT create_hypertable('metrics2','time');
SELECT * FROM ht_settings;
ALTER TABLE metrics2 SET (timescaledb.compress_orderby='d1 NULLS FIRST, d2 NULLS LAST, time, value ASC');
SELECT * FROM ht_settings;
ALTER TABLE metrics2 SET (timescaledb.compress_orderby='d1 DESC NULLS LAST, d2 ASC NULLS FIRST, value DESC, time ASC NULLS FIRST');
SELECT * FROM ht_settings;

-- test decompression uses the correct settings
ALTER TABLE metrics SET (timescaledb.compress_segmentby='');
SELECT compress_chunk(show_chunks('metrics'), recompress:=true);
ALTER TABLE metrics SET (timescaledb.compress_segmentby='d1,d2');
SELECT * FROM chunk_settings;

SELECT * FROM metrics WHERE d1 = 'foo';

SELECT * FROM settings;


-- Check that TRUNCATE <hypertable> also cleans up compression
-- settings for chunks that are dropped when truncating.
TRUNCATE metrics;
SELECT * FROM settings;
SELECT * FROM chunk_settings;

-- Recreate chunks
INSERT INTO metrics VALUES ('2000-01-01'), ('2001-01-01');
SELECT compress_chunk(ch) FROM show_chunks('metrics') ch;

SELECT * FROM settings;

-- DROP TABLE with CASCADE uses a different code path for dropping
-- hypertable so needs to be tested separately.
DROP TABLE metrics CASCADE;
SELECT * FROM settings;

-- Test column limits for orderby and segmentby
\set ON_ERROR_STOP 0
\set VERBOSITY default
-- Test CREATE TABLE syntax
CREATE TABLE test_column_limit_create (
    time TIMESTAMPTZ NOT NULL,
    device_id INT,
    col1 DOUBLE PRECISION,
    col2 DOUBLE PRECISION,
    col3 DOUBLE PRECISION,
    col4 DOUBLE PRECISION,
    col5 DOUBLE PRECISION,
    col6 DOUBLE PRECISION,
    col7 DOUBLE PRECISION,
    col8 DOUBLE PRECISION,
    col9 DOUBLE PRECISION,
    col10 DOUBLE PRECISION,
    col11 DOUBLE PRECISION,
    col12 DOUBLE PRECISION,
    col13 DOUBLE PRECISION,
    col14 DOUBLE PRECISION,
    col15 DOUBLE PRECISION
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='time',
    tsdb.segmentby='device_id',
    tsdb.orderby= 'col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15'
);

-- Test ALTER TABLE syntax
CREATE TABLE test_column_limit_alter (
    time TIMESTAMPTZ NOT NULL,
    device_id INT,
    col1 DOUBLE PRECISION,
    col2 DOUBLE PRECISION,
    col3 DOUBLE PRECISION,
    col4 DOUBLE PRECISION,
    col5 DOUBLE PRECISION,
    col6 DOUBLE PRECISION,
    col7 DOUBLE PRECISION,
    col8 DOUBLE PRECISION,
    col9 DOUBLE PRECISION,
    col10 DOUBLE PRECISION,
    col11 DOUBLE PRECISION,
    col12 DOUBLE PRECISION,
    col13 DOUBLE PRECISION,
    col14 DOUBLE PRECISION,
    col15 DOUBLE PRECISION,
    col16 DOUBLE PRECISION
);

-- Create hypertable
SELECT create_hypertable('test_column_limit_alter', 'time');

-- Enable compression with 1 segmentby and 20 orderby columns (1 + 40 = 41 keys)
ALTER TABLE test_column_limit_alter SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15'
);

\set ON_ERROR_STOP 1
\set VERBOSITY terse
