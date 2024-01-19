-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO ALL

CREATE TABLE metrics(time timestamptz, device text, value float);
SELECT create_hypertable('metrics','time');

ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_segmentby='device');
SELECT * FROM _timescaledb_catalog.compression_settings;

-- create 2 chunks
INSERT INTO metrics VALUES ('2000-01-01'), ('2001-01-01');
-- no change to settings
SELECT * FROM _timescaledb_catalog.compression_settings;

--Enable compression path info
SET timescaledb.debug_compression_path_info= 'on';
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
RESET timescaledb.debug_compression_path_info;

SELECT * FROM _timescaledb_catalog.compression_settings;

SELECT compress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM _timescaledb_catalog.compression_settings;

-- dropping chunk should remove that chunks compression settings
DROP TABLE _timescaledb_internal._hyper_1_1_chunk;
SELECT * FROM _timescaledb_catalog.compression_settings;

-- decompress_chunk should remove settings for that chunk
SELECT decompress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM _timescaledb_catalog.compression_settings;

-- compress_chunk should add settings back
SELECT compress_chunk('_timescaledb_internal._hyper_1_2_chunk');
SELECT * FROM _timescaledb_catalog.compression_settings;

-- dropping hypertable should remove all settings
DROP TABLE metrics;
SELECT * FROM _timescaledb_catalog.compression_settings;
