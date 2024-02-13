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
ALTER TABLE metrics SET (timescaledb.compress_segmentby='d2');

SELECT format('%I.%I', schema_name, table_name) AS "CHUNK" FROM _timescaledb_catalog.chunk WHERE compressed_chunk_id IS NOT NULL ORDER BY id LIMIT 1 OFFSET 1\gset

-- recompressing chunks should apply current hypertable settings
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

