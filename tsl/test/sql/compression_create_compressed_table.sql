-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- create compressed hypertable
CREATE TABLE "public"."metrics" (
    "time" timestamp with time zone NOT NULL,
    "device_id" "text",
    "val" double precision
);
SELECT create_hypertable('public.metrics', 'time');
ALTER TABLE metrics SET (timescaledb.compress, timescaledb.compress_orderby = 'time', timescaledb.compress_segmentby = 'device_id');

-- insert uncompressed row into hypertable
INSERT INTO metrics (time, device_id, val)
VALUES('2023-05-01T00:00:00Z'::timestamptz, 1, 1.0);

SELECT count(*) FROM _timescaledb_internal._hyper_1_1_chunk;

-- compress these rows, we do this to get compressed row data for the test
SELECT compress_chunk('_timescaledb_internal._hyper_1_1_chunk');

-- look up the compressed chunk relation for the chunk
SELECT compress_relid AS "COMPRESSED_CHUNK" FROM _timescaledb_catalog.compression_settings WHERE relid = '_timescaledb_internal._hyper_1_1_chunk'::regclass \gset

-- create custom compressed chunk table
CREATE TABLE _timescaledb_internal.custom_compressed_chunk( LIKE :COMPRESSED_CHUNK);

-- copy compressed row from compressed table into custom compressed chunk table
INSERT INTO "_timescaledb_internal"."custom_compressed_chunk" SELECT * FROM :COMPRESSED_CHUNK;

-- decompress the rows, moving them back to uncompressed space
SELECT decompress_chunk('"_timescaledb_internal"."_hyper_1_1_chunk"');

-- attach compressed chunk to parent chunk
SELECT _timescaledb_functions.create_compressed_chunk(
    '"_timescaledb_internal"."_hyper_1_1_chunk"'::TEXT::REGCLASS,
    '"_timescaledb_internal"."custom_compressed_chunk"'::TEXT::REGCLASS,
    8192,
    8192,
    16384,
    8192,
    8192,
    16384,
    1,
    1
);

-- select total rows in chunk (should be 2)
SELECT count(*) FROM "_timescaledb_internal"."_hyper_1_1_chunk";

-- A non-owner must not be able to attach a compressed chunk
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.create_compressed_chunk(
    '"_timescaledb_internal"."_hyper_1_1_chunk"'::TEXT::REGCLASS,
    '"_timescaledb_internal"."custom_compressed_chunk"'::TEXT::REGCLASS,
    8192,
    8192,
    16384,
    8192,
    8192,
    16384,
    1,
    1
);
\set ON_ERROR_STOP 1


\c :TEST_DBNAME :ROLE_SUPERUSER

-- test create_compressed_chunk column/column type validation
CREATE TABLE selfref(time timestamptz NOT NULL, device text, value float);
SELECT create_hypertable('selfref', 'time');
ALTER TABLE selfref SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');
INSERT INTO selfref VALUES ('2025-01-01', 'dev1', 1.0);
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.create_compressed_chunk(
    ch, ch,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
) FROM show_chunks('selfref') ch;
\set ON_ERROR_STOP 1

-- create_compressed_chunk must reject a table where a segmentby column type does not match
CREATE TABLE badtype(time timestamptz NOT NULL, device text, value float);
SELECT create_hypertable('badtype', 'time');
ALTER TABLE badtype SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');
INSERT INTO badtype VALUES ('2025-01-01', 'dev1', 1.0);
SELECT ch AS "BADTYPE_CHUNK" FROM show_chunks('badtype') ch LIMIT 1 \gset
-- segmentby column "device" has the wrong type (varchar instead of text)
CREATE TABLE _timescaledb_internal.badtype_compressed(_ts_meta_count int4, device varchar(3));
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.create_compressed_chunk(
    :'BADTYPE_CHUNK'::regclass, '_timescaledb_internal.badtype_compressed'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
\set ON_ERROR_STOP 1

-- create_compressed_chunk must reject a table where a metadata column type does not match
CREATE TABLE badcount(time timestamptz NOT NULL, device text, value float);
SELECT create_hypertable('badcount', 'time');
ALTER TABLE badcount SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');
INSERT INTO badcount VALUES ('2025-01-01', 'dev1', 1.0);
SELECT ch AS "BADCOUNT_CHUNK" FROM show_chunks('badcount') ch LIMIT 1 \gset
-- metadata column "_ts_meta_count" has the wrong type (text instead of int4)
CREATE TABLE _timescaledb_internal.badcount_compressed(_ts_meta_count text, device text);
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.create_compressed_chunk(
    :'BADCOUNT_CHUNK'::regclass, '_timescaledb_internal.badcount_compressed'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
\set ON_ERROR_STOP 1

-- create_compressed_chunk must reject relations like views and partitioned
-- tables
CREATE TABLE norelkind(time timestamptz NOT NULL, device text, value float);
SELECT create_hypertable('norelkind', 'time');
ALTER TABLE norelkind SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');
INSERT INTO norelkind VALUES ('2025-01-01', 'dev1', 1.0), ('2025-01-10', 'dev1', 1.0);
SELECT ch AS "NORELKIND_CHUNK1" FROM show_chunks('norelkind', older_than => '2025-01-05') ch \gset
SELECT ch AS "NORELKIND_CHUNK2" FROM show_chunks('norelkind', newer_than => '2025-01-05') ch \gset
SELECT compress_chunk(:'NORELKIND_CHUNK1');
SELECT compress_relid AS "NORELKIND_COMPRESSED" FROM _timescaledb_catalog.compression_settings WHERE relid = :'NORELKIND_CHUNK1'::regclass \gset
CREATE VIEW norelkind_view AS SELECT * FROM :NORELKIND_COMPRESSED WHERE false;
CREATE TABLE norelkind_partitioned (LIKE :NORELKIND_COMPRESSED) PARTITION BY RANGE (device);
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.create_compressed_chunk(
    :'NORELKIND_CHUNK2'::regclass, 'norelkind_view'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
SELECT _timescaledb_functions.create_compressed_chunk(
    :'NORELKIND_CHUNK2'::regclass, 'norelkind_partitioned'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
-- the compressed chunk of another chunk must be rejected as well, decompressing
-- either chunk would drop the compressed data of the other one
SELECT _timescaledb_functions.create_compressed_chunk(
    :'NORELKIND_CHUNK2'::regclass, :'NORELKIND_COMPRESSED'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
\set ON_ERROR_STOP 1
-- nothing was attached and the hypertable is still usable
SELECT count(*) FROM _timescaledb_catalog.compression_settings WHERE relid = :'NORELKIND_CHUNK2'::regclass;
SELECT count(*) FROM norelkind;

-- create_compressed_chunk must reject hypertables and chunks as the compressed
-- table even when they have the right columns, the relation gets dropped when
-- the chunk is decompressed later on
CREATE TABLE donor(time timestamptz NOT NULL, device text, value float);
SELECT create_hypertable('donor', 'time');
ALTER TABLE donor SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');
INSERT INTO donor VALUES ('2025-01-01', 'dev1', 1.0), ('2025-01-10', 'dev1', 2.0);
SELECT ch AS "DONOR_CHUNK1" FROM show_chunks('donor', older_than => '2025-01-05') ch \gset
SELECT ch AS "DONOR_CHUNK2" FROM show_chunks('donor', newer_than => '2025-01-05') ch \gset
SELECT compress_chunk(:'DONOR_CHUNK1');
SELECT compress_relid AS "DONOR_COMPRESSED" FROM _timescaledb_catalog.compression_settings WHERE relid = :'DONOR_CHUNK1'::regclass \gset
-- a hypertable with the columns of a compressed chunk, and a chunk of it
CREATE TABLE mimic (LIKE :DONOR_COMPRESSED);
ALTER TABLE mimic ALTER COLUMN _ts_meta_min_1 SET NOT NULL;
SELECT create_hypertable('mimic', '_ts_meta_min_1');
INSERT INTO mimic SELECT * FROM :DONOR_COMPRESSED;
SELECT ch AS "MIMIC_CHUNK" FROM show_chunks('mimic') ch \gset
\set VERBOSITY default
\set ON_ERROR_STOP 0
-- a chunk of the same hypertable
SELECT _timescaledb_functions.create_compressed_chunk(
    :'DONOR_CHUNK2'::regclass, :'DONOR_CHUNK1'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
-- an uncompressed chunk of another hypertable with the right columns
SELECT _timescaledb_functions.create_compressed_chunk(
    :'DONOR_CHUNK2'::regclass, :'MIMIC_CHUNK'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
-- a hypertable with the right columns
SELECT _timescaledb_functions.create_compressed_chunk(
    :'DONOR_CHUNK2'::regclass, 'mimic'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
\set ON_ERROR_STOP 1
\set VERBOSITY terse
-- nothing was attached and the mimic hypertable still has its chunk
SELECT count(*) FROM _timescaledb_catalog.compression_settings WHERE relid = :'DONOR_CHUNK2'::regclass;
SELECT count(*) FROM mimic;
SELECT count(*) FROM show_chunks('mimic');

-- A table with a NULL count metadata column must be rejected: the metadata of
-- every batch is checked when the table is attached
CREATE TABLE nullcount(time timestamptz NOT NULL, device text, value float);
SELECT create_hypertable('nullcount', 'time');
ALTER TABLE nullcount SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');
INSERT INTO nullcount VALUES ('2025-01-01', 'dev1', 1.0), ('2025-01-10', 'dev1', 2.0);
SELECT ch AS "NULLCOUNT_CHUNK1" FROM show_chunks('nullcount', older_than => '2025-01-05') ch \gset
SELECT ch AS "NULLCOUNT_CHUNK2" FROM show_chunks('nullcount', newer_than => '2025-01-05') ch \gset
SELECT compress_chunk(:'NULLCOUNT_CHUNK1');
SELECT compress_relid AS "NULLCOUNT_COMPRESSED" FROM _timescaledb_catalog.compression_settings WHERE relid = :'NULLCOUNT_CHUNK1'::regclass \gset
CREATE TABLE nullcount_compressed (LIKE :NULLCOUNT_COMPRESSED);
INSERT INTO nullcount_compressed SELECT * FROM :NULLCOUNT_COMPRESSED;
UPDATE nullcount_compressed SET _ts_meta_count = NULL;
\set VERBOSITY default
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.create_compressed_chunk(
    :'NULLCOUNT_CHUNK2'::regclass, 'nullcount_compressed'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
-- a count outside of the valid range
UPDATE nullcount_compressed SET _ts_meta_count = 0;
SELECT _timescaledb_functions.create_compressed_chunk(
    :'NULLCOUNT_CHUNK2'::regclass, 'nullcount_compressed'::regclass,
    8192, 8192, 16384, 8192, 8192, 16384, 1, 1
);
\set ON_ERROR_STOP 1
\set VERBOSITY terse
-- nothing was attached and the hypertable is still usable
SELECT count(*) FROM _timescaledb_catalog.compression_settings WHERE relid = :'NULLCOUNT_CHUNK2'::regclass;
SELECT * FROM nullcount ORDER BY time;
-- the same corruption inside the compressed chunk itself is reported as corrupt
-- data instead of an internal error when the chunk is read
UPDATE :NULLCOUNT_COMPRESSED SET _ts_meta_count = NULL;
\set ON_ERROR_STOP 0
SELECT * FROM nullcount ORDER BY time;
SELECT decompress_chunk(:'NULLCOUNT_CHUNK1');
\set ON_ERROR_STOP 1
