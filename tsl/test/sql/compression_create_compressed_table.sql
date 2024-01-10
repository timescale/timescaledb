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

-- create custom compressed chunk table
CREATE TABLE _timescaledb_internal.custom_compressed_chunk( LIKE _timescaledb_internal.compress_hyper_2_2_chunk);

-- copy compressed row from compressed table into custom compressed chunk table
INSERT INTO "_timescaledb_internal"."custom_compressed_chunk" SELECT * FROM "_timescaledb_internal"."compress_hyper_2_2_chunk";

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

