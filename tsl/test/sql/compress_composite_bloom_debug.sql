-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- disable hash pushdown so we don't include the hash values in the plans
-- which makes the test stable. the hash pushdown is tested separately
set timescaledb.enable_bloom1_hash_pushdown = false;

-----------------------------------------------------------
-- Debug Function Setup
-----------------------------------------------------------

CREATE OR REPLACE FUNCTION ts_bloom1_composite_debug_hash(
    type_oids oid[],
    field_values anyarray,
    field_nulls bool[]
) RETURNS int8
AS :TSL_MODULE_PATHNAME, 'ts_bloom1_composite_debug_hash'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-----------------------------------------------------------
-- Hash Signature Stability Tests
-----------------------------------------------------------
-- Binary compatibility tests

-- INT4 + INT4
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4', 'int4']::regtype[]::oid[],
    ARRAY[1,2]::int4[],
    ARRAY[false,false]::bool[]
) AS hash_1_2;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4', 'int4']::regtype[]::oid[],
    ARRAY[2,1]::int4[],
    ARRAY[false,false]::bool[]
) AS hash_2_1;
-- Order matters

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4', 'int4']::regtype[]::oid[],
    ARRAY[1,1]::int4[],
    ARRAY[false,false]::bool[]
) AS hash_1_1;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4', 'int4']::regtype[]::oid[],
    ARRAY[100,200]::int4[],
    ARRAY[false,false]::bool[]
) AS hash_100_200;

-- NULL handling
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4', 'int4']::regtype[]::oid[],
    ARRAY[1,NULL::int4]::int4[],
    ARRAY[false,true]::bool[]
) AS hash_1_null;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4', 'int4']::regtype[]::oid[],
    ARRAY[NULL::int4,1]::int4[],
    ARRAY[true,false]::bool[]
) AS hash_null_1;
-- NULL position matters

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4', 'int4']::regtype[]::oid[],
    ARRAY[NULL::int4,NULL::int4]::int4[],
    ARRAY[true,true]::bool[]
) AS hash_null_null;
-- Deterministic

-- INT4 + TEXT
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4'::regtype, 'text'::regtype]::oid[],
    ARRAY[1,'hello']::text[],
    ARRAY[false,false]::bool[]
) AS hash_int_text;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['text'::regtype, 'int4'::regtype]::oid[],
    ARRAY['hello',1]::text[],
    ARRAY[false,false]::bool[]
) AS hash_text_int;
-- Type order matters

-- TEXT + TEXT
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['text', 'text']::regtype[]::oid[],
    ARRAY['hello','world']::text[],
    ARRAY[false,false]::bool[]
) AS hash_hello_world;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['text', 'text']::regtype[]::oid[],
    ARRAY['world','hello']::text[],
    ARRAY[false,false]::bool[]
) AS hash_world_hello;


-- INT8 + INT8
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int8', 'int8']::regtype[]::oid[],
    ARRAY[1,2]::int8[],
    ARRAY[false,false]::bool[]
) AS hash_bigint_1_2;

-- FLOAT8 + FLOAT8
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['float8', 'float8']::regtype[]::oid[],
    ARRAY[3.14,2.71]::float8[],
    ARRAY[false,false]::bool[]
) AS hash_pi_e;

-- DATE + INT4
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['date'::regtype, 'int4'::regtype]::oid[],
    ARRAY['2025-01-01'::date,123]::text[],
    ARRAY[false,false]::bool[]
) AS hash_date_int;

-- TIMESTAMPTZ + INT4
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['timestamptz'::regtype, 'int4'::regtype]::oid[],
    ARRAY['2025-01-01 12:00:00+00'::timestamptz,456]::text[],
    ARRAY[false,false]::bool[]
) AS hash_ts_int;

-- UUID + INT4
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['uuid'::regtype, 'int4'::regtype]::oid[],
    ARRAY['c9757a73-7632-462e-bcfa-d5d9659e498f'::uuid,789]::text[],
    ARRAY[false,false]::bool[]
) AS hash_uuid_int;

-- Triples
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4'::regtype, 'text'::regtype, 'float8'::regtype]::oid[],
    ARRAY[1,'test',3.14]::text[],
    ARRAY[false,false,false]::bool[]
) AS hash_triple_1;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4'::regtype, 'text'::regtype, 'float8'::regtype]::oid[],
    ARRAY[1,'test',2.71]::text[],
    ARRAY[false,false,false]::bool[]
) AS hash_triple_2;

-- Triples with NULL
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4'::regtype, 'text'::regtype, 'float8'::regtype]::oid[],
    ARRAY[1,NULL::text,3.14]::text[],
    '{f,t,f}'::bool[]
) AS hash_triple_null_middle;

-----------------------------------------------------------
-- Actual Filtering Tests - Basic Setup
-----------------------------------------------------------
-- Create table with known data distribution
CREATE TABLE composite_filter_test(
    ts timestamptz NOT NULL,
    device_id int,
    sensor_type text,
    value float,
    segmentby_col int
);

SELECT create_hypertable('composite_filter_test', by_range('ts'));

ALTER TABLE composite_filter_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'segmentby_col',
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(device_id,sensor_type)'
);

-- Insert data with DISTINCT patterns per segment
-- Segment 1: device_id=1, sensor_type='temp' ONLY
INSERT INTO composite_filter_test
SELECT '2024-01-01'::timestamptz + i * interval '1 minute',
       1, 'temp', random() * 100, 1
FROM generate_series(1, 1000) i;

-- Segment 2: device_id=2, sensor_type='humidity' ONLY
INSERT INTO composite_filter_test
SELECT '2024-01-01'::timestamptz + i * interval '1 minute',
       2, 'humidity', random() * 100, 2
FROM generate_series(1, 1000) i;

-- Segment 3: device_id=3, sensor_type='pressure' ONLY
INSERT INTO composite_filter_test
SELECT '2024-01-01'::timestamptz + i * interval '1 minute',
       3, 'pressure', random() * 100, 3
FROM generate_series(1, 1000) i;

-- Segment 4: Mixed data (multiple combinations)
INSERT INTO composite_filter_test
SELECT '2024-01-01'::timestamptz + i * interval '1 minute',
       (i % 3) + 1,
       CASE (i % 3) WHEN 0 THEN 'temp' WHEN 1 THEN 'humidity' ELSE 'pressure' END,
       random() * 100,
       4
FROM generate_series(1, 900) i;

SELECT compress_chunk(c) FROM show_chunks('composite_filter_test') c;
VACUUM ANALYZE composite_filter_test;

-- Create uncompressed reference for false negative testing
CREATE TABLE composite_filter_ref AS SELECT * FROM composite_filter_test;

-----------------------------------------------------------
-- Verify Composite Bloom Column Exists
-----------------------------------------------------------
SELECT cc.schema_name || '.' || cc.table_name AS chunk
FROM _timescaledb_catalog.chunk uc, timescaledb_information.chunks tc, _timescaledb_catalog.chunk cc
WHERE uc.table_name = tc.chunk_name
  AND cc.id = uc.compressed_chunk_id
  AND tc.hypertable_name = 'composite_filter_test'
  AND tc.is_compressed
ORDER BY cc.table_name
LIMIT 1
\gset

\echo 'Chunk: ' :chunk

SELECT attname AS composite_bloom_col
FROM pg_attribute
WHERE attrelid = :'chunk'::regclass
  AND attname LIKE '%device_id%sensor_type%'
  LIMIT 1
\gset

\echo 'Composite bloom column: ' :composite_bloom_col

-----------------------------------------------------------
-- Filtering Effectiveness Tests
-----------------------------------------------------------

-- Query for data in Segment 1 only
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM composite_filter_test WHERE device_id = 1 AND sensor_type = 'temp';

-- Expected behavior:
-- - Filter should show: bloom1_contains(composite_bloom, ROW(1, 'temp'::text))
-- - Should scan ~1000-1100 rows (segment 1 + maybe some from segment 4 due to false positives)
-- - Should NOT scan segments 2 and 3 (different device_id/sensor_type)
-- - Pruning: ~50-60% of segments

-- Query for data in Segment 2 only
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM composite_filter_test WHERE device_id = 2 AND sensor_type = 'humidity';

-- Expected: Similar pruning as Test 5.1

-- Query for non-existent combination
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM composite_filter_test WHERE device_id = 5 AND sensor_type = 'temp';

-- Expected:
-- - Should scan 0 or very few rows (bloom filter prunes all segments)
-- - Actual rows returned: 0

-- Non-existent pairing (device_id=1, sensor_type=humidity)
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM composite_filter_test WHERE device_id = 1 AND sensor_type = 'humidity';

-- Expected:
-- Segment 4 data: (i%3)+1 paired with sensor via CASE
-- Pairs: (1,temp), (2,humidity), (3,pressure)
-- Query (1,humidity) doesn't exist → should return 0 rows

-----------------------------------------------------------
-- False Negative Prevention Tests
-----------------------------------------------------------
-- Test all expected combinations and verify counts match reference

WITH test_cases AS (
    SELECT device_id, sensor_type
    FROM (VALUES
        (1, 'temp'),        -- Segment 1: ~1000 rows
        (2, 'humidity'),    -- Segment 2: ~1000 rows
        (3, 'pressure'),    -- Segment 3: ~1000 rows
        (1, 'humidity'),    -- Segment 4: ~300 rows
        (2, 'pressure'),    -- Segment 4: ~300 rows
        (3, 'temp'),        -- Segment 4: ~300 rows
        (4, 'temp'),        -- Non-existent: 0 rows
        (1, 'pressure'),    -- Segment 4: ~300 rows
        (2, 'temp')         -- Segment 4: ~300 rows
    ) AS t(device_id, sensor_type)
)
SELECT
    device_id,
    sensor_type,
    (SELECT COUNT(*) FROM composite_filter_test
     WHERE device_id = tc.device_id AND sensor_type = tc.sensor_type) AS compressed_count,
    (SELECT COUNT(*) FROM composite_filter_ref
     WHERE device_id = tc.device_id AND sensor_type = tc.sensor_type) AS reference_count,
    (SELECT COUNT(*) FROM composite_filter_test
     WHERE device_id = tc.device_id AND sensor_type = tc.sensor_type) =
    (SELECT COUNT(*) FROM composite_filter_ref
     WHERE device_id = tc.device_id AND sensor_type = tc.sensor_type) AS match
FROM test_cases tc
ORDER BY device_id, sensor_type;

-- All match values should be TRUE

-----------------------------------------------------------
-- NULL Handling Tests
-----------------------------------------------------------
CREATE TABLE null_test(
    ts int,
    a int,
    b text,
    seg int
);

SELECT create_hypertable('null_test', 'ts');

ALTER TABLE null_test SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(a,b)'
);

-- Insert various NULL combinations
INSERT INTO null_test VALUES (1, NULL, NULL, 1);
INSERT INTO null_test VALUES (2, NULL, 'test', 1);
INSERT INTO null_test VALUES (3, 1, NULL, 1);
INSERT INTO null_test VALUES (4, 2, 'test', 1);
INSERT INTO null_test VALUES (5, NULL, 'test', 1);

SELECT compress_chunk(c) FROM show_chunks('null_test') c;

-- Test NULL queries
SELECT COUNT(*) FROM null_test WHERE a IS NULL AND b IS NULL;
SELECT COUNT(*) FROM null_test WHERE a IS NULL AND b = 'test';
SELECT COUNT(*) FROM null_test WHERE a = 1 AND b IS NULL;
SELECT COUNT(*) FROM null_test WHERE a = 2 AND b = 'test';

-- Verify no false negatives
CREATE TABLE null_test_ref AS SELECT * FROM null_test;

WITH test_cases AS (
    SELECT a, b FROM (VALUES
        (NULL, NULL),
        (NULL, 'test'),
        (1, NULL),
        (2, 'test')
    ) AS t(a, b)
)
SELECT
    COALESCE(a::text, 'NULL') AS a,
    COALESCE(b, 'NULL') AS b,
    (SELECT COUNT(*) FROM null_test WHERE (a IS NOT DISTINCT FROM tc.a) AND (b IS NOT DISTINCT FROM tc.b)) AS compressed,
    (SELECT COUNT(*) FROM null_test_ref WHERE (a IS NOT DISTINCT FROM tc.a) AND (b IS NOT DISTINCT FROM tc.b)) AS reference,
    (SELECT COUNT(*) FROM null_test WHERE (a IS NOT DISTINCT FROM tc.a) AND (b IS NOT DISTINCT FROM tc.b)) =
    (SELECT COUNT(*) FROM null_test_ref WHERE (a IS NOT DISTINCT FROM tc.a) AND (b IS NOT DISTINCT FROM tc.b)) AS match
FROM test_cases tc;

-- All match values should be TRUE

-----------------------------------------------------------
-- More Type Pairs
-----------------------------------------------------------
-- INT2 + INT2 (smallint)
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int2', 'int2']::regtype[]::oid[], ARRAY[1::int2,2::int2]::int2[], ARRAY[false,false]::bool[]
);

-- INT4 + INT8 (mixed integer sizes)
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4'::regtype, 'int8'::regtype], ARRAY[1,2]::int8[], ARRAY[false,false]::bool[]
);

-- FLOAT4 + FLOAT8 (mixed float sizes)
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['float4'::regtype, 'float8'::regtype], ARRAY[1.5,2.5]::float8[], ARRAY[false,false]::bool[]
);

-- VARCHAR + TEXT (string types)
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['varchar'::regtype, 'text'::regtype], ARRAY['hello'::varchar,'world'::text]::text[], ARRAY[false,false]::bool[]
);

-- TIMESTAMP + TIMESTAMPTZ
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['timestamp'::regtype, 'timestamptz'::regtype],
    ARRAY['2025-01-01 12:00:00'::timestamp,'2025-01-01 12:00:00+00'::timestamptz]::text[],
    ARRAY[false,false]::bool[]
);

-- BOOL + INT4
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['bool'::regtype, 'int4'::regtype], ARRAY[true,1]::text[], ARRAY[false,false]::bool[]
);

-----------------------------------------------------------
-- 3 or more fields
-----------------------------------------------------------
SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4'::regtype, 'text'::regtype, 'float8'::regtype],
    ARRAY[42,'answer',3.14159]::text[],
    ARRAY[false,false,false]::bool[]
) AS hash_triple;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4','int4','int4','int4']::regtype[]::oid[],
    ARRAY[1,2,3,4]::int4[],
    ARRAY[false,false,false,false]::bool[]
) AS hash_quad;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4','int4','int4','int4','int4']::regtype[]::oid[],
    ARRAY[1,2,3,4,5]::int4[],
    ARRAY[false,false,false,false,false]::bool[]
) AS hash_quint;

SELECT ts_bloom1_composite_debug_hash(
    ARRAY['int4','int4','int4','int4','int4','int4','int4','int4']::regtype[]::oid[],
    ARRAY[1,2,3,4,5,6,7,8]::int4[],
    ARRAY[false,false,false,false,false,false,false,false]::bool[]
) AS hash_octet;

-----------------------------------------------------------
-- Triple Column Tests
-----------------------------------------------------------
CREATE TABLE test_triple(ts int, a int, b text, c float8, seg int);
SELECT create_hypertable('test_triple', 'ts');
ALTER TABLE test_triple SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(a,b,c)'
);

INSERT INTO test_triple
SELECT ts,
       ts % 5,
       CASE ts % 3 WHEN 0 THEN 'x' WHEN 1 THEN 'y' ELSE 'z' END,
       (ts % 4)::float8,
       ts % 2
FROM generate_series(1, 1000) ts;

SELECT compress_chunk(c) FROM show_chunks('test_triple') c;

-- Test various triple combinations
SELECT COUNT(*) FROM test_triple WHERE a = 1 AND b = 'x' AND c = 2.0;
SELECT COUNT(*) FROM test_triple WHERE a = 2 AND b = 'y' AND c = 1.0;

CREATE TABLE test_triple_ref AS SELECT * FROM test_triple;

-- Verify comprehensive
WITH test_cases AS (
    SELECT a, b, c FROM (VALUES
        (1, 'x', 2.0),
        (2, 'y', 1.0),
        (0, 'z', 0.0),
        (4, 'x', 3.0)
    ) AS t(a, b, c)
)
SELECT
    a, b, c,
    (SELECT COUNT(*) FROM test_triple WHERE a = tc.a AND b = tc.b AND c = tc.c) AS compressed,
    (SELECT COUNT(*) FROM test_triple_ref WHERE a = tc.a AND b = tc.b AND c = tc.c) AS reference,
    (SELECT COUNT(*) FROM test_triple WHERE a = tc.a AND b = tc.b AND c = tc.c) =
    (SELECT COUNT(*) FROM test_triple_ref WHERE a = tc.a AND b = tc.b AND c = tc.c) AS match
FROM test_cases tc;

-----------------------------------------------------------
-- Cleanup
-----------------------------------------------------------

DROP TABLE IF EXISTS composite_filter_test CASCADE;
DROP TABLE IF EXISTS composite_filter_ref CASCADE;
DROP TABLE IF EXISTS null_test CASCADE;
DROP TABLE IF EXISTS null_test_ref CASCADE;
DROP TABLE IF EXISTS test_triple CASCADE;
DROP TABLE IF EXISTS test_triple_ref CASCADE;
