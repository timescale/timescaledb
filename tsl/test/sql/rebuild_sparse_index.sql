-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test rebuild_sparse_index function

CREATE OR REPLACE FUNCTION show_compressed_columns(chunk_regclass regclass)
RETURNS TABLE(attname name, typname name) AS $$
    SELECT a.attname, t.typname
    FROM pg_attribute a
    JOIN pg_type t ON a.atttypid = t.oid
    WHERE a.attrelid = (
        SELECT format('%I.%I', cc.schema_name, cc.table_name)::regclass
        FROM _timescaledb_catalog.chunk uc
        JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I', uc.schema_name, uc.table_name)::regclass
        JOIN _timescaledb_catalog.chunk cc ON cs.compress_relid = format('%I.%I', cc.schema_name, cc.table_name)::regclass
        WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = chunk_regclass
    )
    AND a.attname LIKE '_ts_meta_%'
    AND a.attnum > 0
    AND NOT a.attisdropped
    ORDER BY a.attname;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION show_chunk_index(chunk_regclass regclass)
RETURNS jsonb AS $$
    SELECT cs.index
    FROM _timescaledb_catalog.compression_settings cs
    WHERE cs.relid = chunk_regclass;
$$ LANGUAGE sql;

CREATE TABLE rsi_test(
    time timestamptz NOT NULL,
    device int,
    sensor int,
    temp float8,
    val int
);
SELECT create_hypertable('rsi_test', 'time', chunk_time_interval => INTERVAL '1 day');

INSERT INTO rsi_test
SELECT t, (i % 5) + 1, (i % 3) + 1, random() * 100, (i * 7) % 13
FROM generate_series('2024-01-01 01:00'::timestamptz, '2024-01-01 23:59', '1 minute') t,
     generate_series(1, 3) i;

SELECT format('%I.%I', schema_name, table_name) AS chunk1
FROM _timescaledb_catalog.chunk
WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable
                        WHERE table_name = 'rsi_test')
ORDER BY id LIMIT 1 \gset

-- Test 1: initial compression with bloom + minmax
ALTER TABLE rsi_test SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'bloom("device"), minmax("temp")'
);
SELECT compress_chunk(:'chunk1');

SELECT show_chunk_index(:'chunk1'::regclass);
SELECT * FROM show_compressed_columns(:'chunk1'::regclass);

-- Test 2: settings match, should skip
SELECT _timescaledb_functions.rebuild_sparse_index(:'chunk1'::regclass);

-- Test 3: swap bloom("device") for bloom("device","sensor"), keep minmax("temp")
ALTER TABLE rsi_test SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'bloom("device","sensor"), minmax("temp")'
);

SELECT show_chunk_index(:'chunk1'::regclass);
SELECT _timescaledb_functions.rebuild_sparse_index(:'chunk1'::regclass);
SELECT * FROM show_compressed_columns(:'chunk1'::regclass);

-- Test 4: remove all sparse indexes
ALTER TABLE rsi_test SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = ''
);

SELECT _timescaledb_functions.rebuild_sparse_index(:'chunk1'::regclass);
SELECT * FROM show_compressed_columns(:'chunk1'::regclass);

-- Test 5: add back bloom + minmax + composite bloom
ALTER TABLE rsi_test SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'bloom("device"), minmax("temp"), bloom("device","val")'
);

SELECT _timescaledb_functions.rebuild_sparse_index(:'chunk1'::regclass);
SELECT * FROM show_compressed_columns(:'chunk1'::regclass);

-- Test 6: force rebuild when settings already match
SELECT _timescaledb_functions.rebuild_sparse_index(:'chunk1'::regclass, force => true);
SELECT * FROM show_compressed_columns(:'chunk1'::regclass);

-- Test 7: uncompressed chunk, should skip
SELECT decompress_chunk(:'chunk1');
SELECT _timescaledb_functions.rebuild_sparse_index(:'chunk1'::regclass);

DROP TABLE rsi_test CASCADE;

-- Data integrity tests: verify rebuilt sparse index values match the
-- original compression output (file diff) and that bloom filters
-- correctly identify present and absent values.

\set TEST_BASE_NAME rebuild_sparse_index

CREATE TABLE rsi_integrity(
    time timestamptz NOT NULL,
    device text NOT NULL,
    temp float8,
    humidity int,
    label text,
    reading uuid
);
SELECT create_hypertable('rsi_integrity', 'time', chunk_time_interval => INTERVAL '1 day');


-- NULLs at row 5000/5001 to stress firstlast NULL handling at batch boundaries
SELECT setseed(0.42);
INSERT INTO rsi_integrity
SELECT
    '2024-01-01'::timestamptz + ((row_num - 1) * interval '10 seconds'),
    'd' || ((row_num % 5) + 1),
    CASE WHEN row_num % 5000 IN (0, 1) THEN NULL ELSE random() * 100 END,
    CASE WHEN row_num % 5000 IN (0, 1) THEN NULL ELSE (random() * 200)::int END,
    CASE WHEN row_num % 5000 IN (0, 1) THEN NULL ELSE 'label_' || (row_num % 7) END,
    CASE WHEN row_num % 5000 IN (0, 1) THEN NULL ELSE gen_random_uuid() END
FROM generate_series(1, 15000) row_num;

SELECT format('%I.%I', schema_name, table_name) AS ichunk1
FROM _timescaledb_catalog.chunk
WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'rsi_integrity')
ORDER BY id LIMIT 1 \gset

SELECT format('%I.%I', schema_name, table_name) AS ichunk2
FROM _timescaledb_catalog.chunk
WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'rsi_integrity')
ORDER BY id LIMIT 1 OFFSET 1 \gset

SELECT format('%s/results/%s_original.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "ORIGINAL",
       format('%s/results/%s_rebuilt.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "REBUILT"
\gset

-- Dump _ts_meta_v2_* columns sorted by name (avoids column reorder after drop+add)
CREATE OR REPLACE FUNCTION dump_sparse_metadata(chunk_regclass regclass)
RETURNS SETOF text AS $$
DECLARE
    compressed_relid oid;
    col_list text;
BEGIN
    SELECT format('%I.%I', cc.schema_name, cc.table_name)::regclass INTO compressed_relid
    FROM _timescaledb_catalog.chunk uc
    JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I', uc.schema_name, uc.table_name)::regclass
    JOIN _timescaledb_catalog.chunk cc ON cs.compress_relid = format('%I.%I', cc.schema_name, cc.table_name)::regclass
    WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = chunk_regclass;

    SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attname) INTO col_list
    FROM pg_attribute a
    WHERE a.attrelid = compressed_relid
      AND a.attname LIKE '_ts_meta_v2_%'
      AND a.attnum > 0
      AND NOT a.attisdropped;

    RETURN QUERY EXECUTE format(
        'SELECT row(%s)::text FROM %s ORDER BY _ts_meta_min_1',
        col_list, compressed_relid::regclass);
END;
$$ LANGUAGE plpgsql;

-- Test 8: minmax integrity (compress, save, drop+rebuild, save, diff)
ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'minmax("temp"), minmax("humidity"), minmax("label"), minmax("reading")'
);
SELECT compress_chunk(:'ichunk1');
SELECT compress_chunk(:'ichunk2');

SELECT cc.schema_name AS ic_schema1, cc.table_name AS ic_table1
FROM _timescaledb_catalog.chunk uc
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I', uc.schema_name, uc.table_name)::regclass
JOIN _timescaledb_catalog.chunk cc ON cs.compress_relid = format('%I.%I', cc.schema_name, cc.table_name)::regclass
WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = :'ichunk1'::regclass \gset

SELECT cc.schema_name AS ic_schema2, cc.table_name AS ic_table2
FROM _timescaledb_catalog.chunk uc
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I', uc.schema_name, uc.table_name)::regclass
JOIN _timescaledb_catalog.chunk cc ON cs.compress_relid = format('%I.%I', cc.schema_name, cc.table_name)::regclass
WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = :'ichunk2'::regclass \gset

\o :ORIGINAL
SELECT * FROM dump_sparse_metadata(:'ichunk1'::regclass);
SELECT * FROM dump_sparse_metadata(:'ichunk2'::regclass);
\o

ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = ''
);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk1'::regclass);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk2'::regclass);

ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'minmax("temp"), minmax("humidity"), minmax("label"), minmax("reading")'
);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk1'::regclass);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk2'::regclass);

\o :REBUILT
SELECT * FROM dump_sparse_metadata(:'ichunk1'::regclass);
SELECT * FROM dump_sparse_metadata(:'ichunk2'::regclass);
\o

SELECT format('\! diff -u --label "minmax original" --label "minmax rebuilt" %s %s',
              :'ORIGINAL', :'REBUILT') AS "DIFF_CMD" \gset
:DIFF_CMD

SELECT decompress_chunk(:'ichunk1');
SELECT decompress_chunk(:'ichunk2');

-- Test 9: firstlast integrity (compress, save, drop+rebuild, save, diff)
ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'firstlast("temp"), firstlast("humidity"), firstlast("label"), firstlast("reading")'
);
SELECT compress_chunk(:'ichunk1');
SELECT compress_chunk(:'ichunk2');

-- re-fetch compressed chunk names after recompress
SELECT cc.schema_name AS ic_schema1, cc.table_name AS ic_table1
FROM _timescaledb_catalog.chunk uc
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I', uc.schema_name, uc.table_name)::regclass
JOIN _timescaledb_catalog.chunk cc ON cs.compress_relid = format('%I.%I', cc.schema_name, cc.table_name)::regclass
WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = :'ichunk1'::regclass \gset

SELECT cc.schema_name AS ic_schema2, cc.table_name AS ic_table2
FROM _timescaledb_catalog.chunk uc
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I', uc.schema_name, uc.table_name)::regclass
JOIN _timescaledb_catalog.chunk cc ON cs.compress_relid = format('%I.%I', cc.schema_name, cc.table_name)::regclass
WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = :'ichunk2'::regclass \gset

\o :ORIGINAL
SELECT * FROM dump_sparse_metadata(:'ichunk1'::regclass);
SELECT * FROM dump_sparse_metadata(:'ichunk2'::regclass);
\o

ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = ''
);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk1'::regclass);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk2'::regclass);

ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'firstlast("temp"), firstlast("humidity"), firstlast("label"), firstlast("reading")'
);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk1'::regclass);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk2'::regclass);

\o :REBUILT
SELECT * FROM dump_sparse_metadata(:'ichunk1'::regclass);
SELECT * FROM dump_sparse_metadata(:'ichunk2'::regclass);
\o

SELECT format('\! diff -u --label "firstlast original" --label "firstlast rebuilt" %s %s',
              :'ORIGINAL', :'REBUILT') AS "DIFF_CMD" \gset
:DIFF_CMD

SELECT decompress_chunk(:'ichunk1');
SELECT decompress_chunk(:'ichunk2');

-- Test 10: single-column bloom integrity (drop, rebuild, check contains)
ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'bloom("humidity"), bloom("label")'
);
SELECT compress_chunk(:'ichunk1');

SELECT cc.schema_name AS ic_schema1, cc.table_name AS ic_table1
FROM _timescaledb_catalog.chunk uc
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I', uc.schema_name, uc.table_name)::regclass
JOIN _timescaledb_catalog.chunk cc ON cs.compress_relid = format('%I.%I', cc.schema_name, cc.table_name)::regclass
WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = :'ichunk1'::regclass \gset

ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = ''
);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk1'::regclass);
SELECT * FROM show_compressed_columns(:'ichunk1'::regclass);

ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'bloom("humidity"), bloom("label")'
);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk1'::regclass);

-- look up bloom column names (prefix varies between test/production builds)
SELECT a.attname AS bloom_humidity
FROM pg_attribute a
JOIN pg_type t ON a.atttypid = t.oid
WHERE a.attrelid = (:'ic_schema1' || '.' || :'ic_table1')::regclass
  AND t.typname = 'bloom1' AND a.attname LIKE '%humidity'
  AND NOT a.attisdropped LIMIT 1 \gset

SELECT a.attname AS bloom_label
FROM pg_attribute a
JOIN pg_type t ON a.atttypid = t.oid
WHERE a.attrelid = (:'ic_schema1' || '.' || :'ic_table1')::regclass
  AND t.typname = 'bloom1' AND a.attname LIKE '%label'
  AND NOT a.attisdropped LIMIT 1 \gset

-- present values (expect true)
SELECT _timescaledb_functions.bloom1_contains(:"bloom_humidity", 42) AS val_42,
       _timescaledb_functions.bloom1_contains(:"bloom_humidity", 100) AS val_100,
       _timescaledb_functions.bloom1_contains(:"bloom_humidity", 7) AS val_7
FROM :ic_schema1.:ic_table1 WHERE device = 'd1' LIMIT 1;

-- absent values (expect false)
SELECT _timescaledb_functions.bloom1_contains(:"bloom_humidity", -999) AS val_neg999,
       _timescaledb_functions.bloom1_contains(:"bloom_humidity", 999) AS val_999,
       _timescaledb_functions.bloom1_contains(:"bloom_humidity", 12345) AS val_12345
FROM :ic_schema1.:ic_table1 WHERE device = 'd1' LIMIT 1;

-- present labels (expect true)
SELECT _timescaledb_functions.bloom1_contains(:"bloom_label", 'label_0'::text) AS label_0,
       _timescaledb_functions.bloom1_contains(:"bloom_label", 'label_3'::text) AS label_3,
       _timescaledb_functions.bloom1_contains(:"bloom_label", 'label_6'::text) AS label_6
FROM :ic_schema1.:ic_table1 WHERE device = 'd2' LIMIT 1;

-- absent labels (expect false)
SELECT _timescaledb_functions.bloom1_contains(:"bloom_label", 'nonexistent'::text) AS nope1,
       _timescaledb_functions.bloom1_contains(:"bloom_label", 'zzz_never'::text) AS nope2,
       _timescaledb_functions.bloom1_contains(:"bloom_label", 'NOPE'::text) AS nope3
FROM :ic_schema1.:ic_table1 WHERE device = 'd2' LIMIT 1;

SELECT decompress_chunk(:'ichunk1');

-- Test 11: composite bloom integrity (drop, rebuild, verify via explain)
ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'bloom("humidity","label")'
);
SELECT compress_chunk(:'ichunk1');

SELECT cc.schema_name AS ic_schema1, cc.table_name AS ic_table1
FROM _timescaledb_catalog.chunk uc
JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I', uc.schema_name, uc.table_name)::regclass
JOIN _timescaledb_catalog.chunk cc ON cs.compress_relid = format('%I.%I', cc.schema_name, cc.table_name)::regclass
WHERE format('%I.%I', uc.schema_name, uc.table_name)::regclass = :'ichunk1'::regclass \gset

ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = ''
);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk1'::regclass);
SELECT * FROM show_compressed_columns(:'ichunk1'::regclass);

ALTER TABLE rsi_integrity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device',
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_index = 'bloom("humidity","label")'
);
SELECT _timescaledb_functions.rebuild_sparse_index(:'ichunk1'::regclass);
SELECT * FROM show_compressed_columns(:'ichunk1'::regclass);

-- composite bloom should appear in the plan filter
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT * FROM rsi_integrity WHERE device = 'd1' AND humidity = 42 AND label = 'label_0';

DROP TABLE rsi_integrity CASCADE;
DROP FUNCTION show_compressed_columns(regclass);
DROP FUNCTION show_chunk_index(regclass);
DROP FUNCTION dump_sparse_metadata(regclass);
