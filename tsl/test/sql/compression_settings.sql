-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO ALL

CREATE VIEW settings AS SELECT * FROM _timescaledb_catalog.compression_settings ORDER BY upper(relid::text) COLLATE "C";

CREATE TABLE metrics(time timestamptz not null, device text, value float);
SELECT table_name FROM create_hypertable('metrics','time');

ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device');
SELECT * FROM settings;

-- create 2 chunks
INSERT INTO metrics VALUES ('2000-01-01'), ('2001-01-01');
-- no change to settings
SELECT * FROM settings;

--Enable compression path info
SET timescaledb.debug_compression_path_info= 'on';
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET timescaledb.debug_compression_path_info;

SELECT * FROM _timescaledb_catalog.compression_settings;

SELECT compress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM settings;

-- dropping chunk should remove that chunks compression settings
DROP TABLE _timescaledb_internal._hyper_1_1_chunk;
SELECT * FROM settings;

-- decompress_chunk should remove settings for that chunk
SELECT decompress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM settings;

-- compress_chunk should add settings back
SELECT compress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM settings;

-- dropping hypertable should remove all settings
DROP TABLE metrics;
SELECT * FROM settings;

CREATE TABLE metrics(time timestamptz not null, d1 text, d2 text, value float);
SELECT table_name FROM create_hypertable('metrics','time');

ALTER TABLE metrics SET (timescaledb.compress);
-- hypertable should have default settings now
SELECT * FROM settings;

-- create chunk
INSERT INTO metrics VALUES ('2000-01-01');
ALTER TABLE metrics SET (timescaledb.compress_segmentby='d1');
-- settings should be updated
SELECT * FROM settings;

SELECT compress_chunk(show_chunks('metrics'));
-- settings for compressed chunk should be present
SELECT * FROM settings;

-- changing settings should update settings for hypertable but not existing compressed chunks
ALTER TABLE metrics SET (timescaledb.compress_segmentby='d2');
SELECT * FROM settings;

-- changing settings should update settings for hypertable but not existing compressed chunks
ALTER TABLE metrics SET (timescaledb.compress_segmentby='');
SELECT * FROM settings;

-- create another chunk
INSERT INTO metrics VALUES ('2000-02-01');
SELECT compress_chunk(show_chunks('metrics'), true);

SELECT * FROM settings;
ALTER TABLE metrics SET (timescaledb.compress_segmentby='d2');

SELECT format('%I.%I', schema_name, table_name) AS "CHUNK" FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL ORDER BY id LIMIT 1 OFFSET 1\gset

-- recompressing chunks should apply current hypertable settings
SELECT decompress_chunk(:'CHUNK');
SELECT * FROM settings;
SELECT compress_chunk(:'CHUNK');
SELECT * FROM settings;



