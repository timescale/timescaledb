-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- _timescaledb_functions.decompress_batch(record) RETURNS SETOF record
-- expands a single row of a compressed chunk into the user-visible rows it
-- was compressed from.

SET datestyle TO ISO;

CREATE TABLE metrics(time timestamptz NOT NULL, device_id int, value float)
    WITH (tsdb.hypertable, tsdb.orderby = 'time', tsdb.segmentby = 'device_id');

-- Populate with test data
INSERT INTO metrics
SELECT '2025-01-01'::timestamptz + (g || ' minute')::interval, g % 3, g::float
FROM generate_series(1, 30) g;
INSERT INTO metrics VALUES ('2025-01-01 02:00', NULL, NULL);

SELECT compress_chunk(ch) FROM show_chunks('metrics') ch;

-- Capture the compressed chunk relation name
SELECT format('%I.%I', cc.schema_name, cc.table_name) AS compressed_chunk
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk cc ON c.compressed_chunk_id = cc.id
JOIN _timescaledb_catalog.hypertable h ON c.hypertable_id = h.id
WHERE h.table_name = 'metrics' \gset

-- Verify set equality: no source row missing, no extra row introduced
SELECT count(*) AS missing FROM (
    TABLE metrics
    EXCEPT ALL
    SELECT decomp.time, decomp.device_id, decomp.value
    FROM :compressed_chunk t,
         LATERAL _timescaledb_functions.decompress_batch(t)
             AS decomp(time timestamptz, device_id int, value float)
) m;

SELECT count(*) AS extras FROM (
    SELECT decomp.time, decomp.device_id, decomp.value
    FROM :compressed_chunk t,
         LATERAL _timescaledb_functions.decompress_batch(t)
             AS decomp(time timestamptz, device_id int, value float)
    EXCEPT ALL
    TABLE metrics
) e;

-- Decompressing a single batch yields exactly that batch.
SELECT decomp.time, decomp.device_id, decomp.value
FROM (SELECT t FROM :compressed_chunk t WHERE t.device_id = 1) comp,
     LATERAL _timescaledb_functions.decompress_batch(comp.t)
         AS decomp(time timestamptz, device_id int, value float)
ORDER BY decomp.time;

DROP TABLE metrics CASCADE;

\set ON_ERROR_STOP 0

-- Calling `decompress_batch()` on a non-compressed record
SELECT decomp.*
FROM (VALUES (1, 2)) AS r(a, b),
LATERAL _timescaledb_functions.decompress_batch(r)
    AS decomp(a int, b int);

-- Calling `decompress_batch()` on a "fake" compressed record with the
-- `_ts_meta_count` column of incorrect type
SELECT decomp.*
FROM (VALUES (1::INT8, 2, 3)) AS r(_ts_meta_count, a, b),
LATERAL _timescaledb_functions.decompress_batch(r)
    AS decomp(a int, b int);

-- Calling `decompress_batch()` on a "fake" compressed record with zero
-- compressed columns
SELECT decomp.*
FROM (VALUES (1,2,3)) AS r(_ts_meta_count, a, b),
LATERAL _timescaledb_functions.decompress_batch(r)
    AS decomp(a int, b int);

-- Input and output type mismatch. Here we make sure that the error message has
-- a "user-friendly" error code (`22023`) and not the internal error code (`XX000`)
DO $$
BEGIN
    SELECT decomp.*
    FROM (VALUES (1, 2, 3::int8)) AS r(_ts_meta_count, a, b),
    LATERAL _timescaledb_functions.decompress_batch(r)
        AS decomp(a int, b int4);
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'SQLSTATE %: %', SQLSTATE, SQLERRM;
END $$;

-- Decompress a batch into an incompatible type. The compressed data here is
-- built from a single text value `test` which is being decompressed into an
-- integer
SELECT decomp.*
FROM (VALUES (1, 2, 'AQBwZ19jYXRhbG9nAHRleHQAAAEAAAABAAAABHRlc3Q='::_timescaledb_internal.compressed_data)) AS r(_ts_meta_count, a, b),
LATERAL _timescaledb_functions.decompress_batch(r)
    AS decomp(a int, b int);

-- Try decompressing a batch while `_ts_meta_count` (10) is larger than the
-- actual number of compressed values (1)
SELECT decomp.*
FROM (VALUES (10, 2, 'AQBwZ19jYXRhbG9nAHRleHQAAAEAAAABAAAABHRlc3Q='::_timescaledb_internal.compressed_data)) AS r(_ts_meta_count, a, b),
LATERAL _timescaledb_functions.decompress_batch(r)
    AS decomp(a int, b text);

-- Run `decompress_batch()` with NULL as compressed data and as a "segment by"
-- column -- both should be fine
SELECT decomp.*
FROM (VALUES (1, NULL::int, NULL::_timescaledb_internal.compressed_data)) AS r(_ts_meta_count, a, b),
LATERAL _timescaledb_functions.decompress_batch(r)
    AS decomp(a int, b int);

-- Decompressing a batch with NULL in the `_ts_meta_count` column should fail
SELECT decomp.*
FROM (VALUES (NULL, NULL, NULL::_timescaledb_internal.compressed_data)) AS r(_ts_meta_count, a, b),
LATERAL _timescaledb_functions.decompress_batch(r)
    AS decomp(a int, b text);
