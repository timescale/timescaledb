-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
-- Test default segmentby gets set for direct compress
BEGIN;
SET timescaledb.enable_direct_compress_insert = false;
CREATE TABLE test_segmentby_stats (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;

ANALYZE test_segmentby_stats;
SELECT compress_chunk(c) FROM show_chunks('test_segmentby_stats') c;

-- will have devide_id as default segmentby for compressed chunk;
SELECT * FROM _timescaledb_catalog.compression_settings;

SET timescaledb.enable_direct_compress_insert = true;
INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-06'::timestamptz, '2024-01-07'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;
ANALYZE test_segmentby_stats;
-- will have default segmentby set for a newly created direct compressed chunk (should observe a skipped chunk id);
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test default segmentby GUC for direct compress is off
BEGIN;
SET timescaledb.enable_direct_compress_auto_segmentby = false;
CREATE TABLE test_segmentby_stats (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.segmentby='device_id');

INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-06'::timestamptz, '2024-01-07'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;
ANALYZE test_segmentby_stats;
-- will have device_id by as configured segmentby for direct compressed chunk (should not observe skipped chunk id);
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test too few rows for analysis
BEGIN;
CREATE TABLE test_too_few_rows (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

-- below the 5000 min_rows threshold
INSERT INTO test_too_few_rows
SELECT t, (i % 3), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 3) i;

-- segmentby should be NULL — not enough rows to trigger analysis
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test no candidate columns (only orderby, float, and jsonb columns present)
BEGIN;
CREATE TABLE test_no_candidates (
    time TIMESTAMPTZ NOT NULL,
    value FLOAT,
    label JSONB
) WITH (tsdb.hypertable, tsdb.orderby='time');

INSERT INTO test_no_candidates
SELECT t, random(), to_jsonb('sensor_' || (i % 5))
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;

-- segmentby should be NULL — no candidates passed ts_accept_for_segmentby
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test text column as segmentby candidate
BEGIN;
CREATE TABLE test_text_segmentby (
    time TIMESTAMPTZ NOT NULL,
    region TEXT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

INSERT INTO test_text_segmentby
SELECT t, 'region_' || (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;

-- segmentby should be region — text column with low cardinality qualifies
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test all candidates rejected (more than 20 distinct values triggers early termination)
BEGIN;
CREATE TABLE test_all_rejected (
    time TIMESTAMPTZ NOT NULL,
    high_card_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

-- 25 distinct values for high_card_id, exceeds MAX_SEGMENTBY_DISTINCT (20)
INSERT INTO test_all_rejected
SELECT t, (i % 25), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 25) i;

-- segmentby should be NULL — only candidate has too many distinct values
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test candidate not qualified (low cardinality but one value has fewer than 500 rows)
BEGIN;
CREATE TABLE test_uneven_distribution (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

-- device_id 1-4 get lots of rows, device_id 5 gets only ~6 rows
INSERT INTO test_uneven_distribution
SELECT t, device_id, random()
FROM (
    SELECT t, (i % 4) + 1 AS device_id
    FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
         generate_series(1, 4) i
    UNION ALL
    SELECT t, 5
    FROM generate_series('2024-01-01 00:00:00'::timestamptz, '2024-01-01 00:05:00'::timestamptz, '1 minute') t
) sub;

-- segmentby should be NULL — device_id=5 has ~6 rows, below segmentby_batch_size_limit
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test multiple candidate columns (first qualifying column wins)
BEGIN;
CREATE TABLE test_multi_candidate (
    time TIMESTAMPTZ NOT NULL,
    region_id INT NOT NULL,
    status_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

-- region_id: 10 distinct values, status_id: 3 distinct values
-- Both qualify, but region_id should win (earlier column position)
INSERT INTO test_multi_candidate
SELECT t, (i % 10), (i % 3), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 10) i;

-- segmentby should be region_id (first qualifying candidate by attribute order)
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test NULL values in candidate column (should be treated as a distinct group)
BEGIN;
CREATE TABLE test_null_candidate (
    time TIMESTAMPTZ NOT NULL,
    device_id INT,
    value FLOAT
) WITH (tsdb.hypertable);

-- 4 non-null device_ids + NULLs, all with enough rows
INSERT INTO test_null_candidate
SELECT t, CASE WHEN i <= 4 THEN i ELSE NULL END, random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;
SELECT count(*) from test_null_candidate WHERE device_id = NULL;

-- segmentby should be device_id — NULLs are treated as a distinct group
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test boundary cases
CREATE TABLE test_boundary_distinct (
    time TIMESTAMPTZ NOT NULL,
    category_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);
BEGIN;
-- 20 distinct values (accepted)
INSERT INTO test_boundary_distinct
SELECT t, (i % 20), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 20) i;

-- segmentby should be category_id — exactly at the limit, not over
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

BEGIN;
-- 21 distinct values (rejected)
INSERT INTO test_boundary_distinct
SELECT t, (i % 21), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 21) i;

-- segmentby should be NULL — 21 distinct values exceeds MAX_SEGMENTBY_DISTINCT
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;
DROP TABLE test_boundary_distinct;

-- Test second insert skips analyze
BEGIN;
CREATE TABLE test_second_insert (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

INSERT INTO test_second_insert
SELECT t, (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;

SELECT * FROM _timescaledb_catalog.compression_settings;

INSERT INTO test_second_insert
SELECT t, (i % 5), random()
FROM generate_series('2024-01-02'::timestamptz, '2024-01-03'::timestamptz, '1 minute') t,
     generate_series(1, 25) i;
ROLLBACK;

-- Test first candidate rejected, second qualifies (verifies we check all candidates)
BEGIN;
CREATE TABLE test_fallback_candidate (
    time TIMESTAMPTZ NOT NULL,
    high_card_id INT NOT NULL,
    low_card_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

INSERT INTO test_fallback_candidate
SELECT t, (i % 25), (i % 3), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 25) i;

-- segmentby should be low_card_id (high_card_id rejected)
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test partial chunk (small insert below sort limit, then large insert triggers analysis)
BEGIN;
CREATE TABLE test_partial (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

-- Small insert: only 5 rows, below the sort limit so no compressed chunk is created
INSERT INTO test_partial VALUES
    ('2024-01-01 00:00:00', 1, 1.0),
    ('2024-01-01 00:01:00', 1, 2.0),
    ('2024-01-01 00:02:00', 2, 3.0),
    ('2024-01-01 00:03:00', 2, 4.0),
    ('2024-01-01 00:04:00', 3, 5.0);

SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test_partial') chunk;

-- Large insert into the same chunk range: creates compressed chunk,
-- first 5 rows become partial, analysis runs and picks device_id
INSERT INTO test_partial
SELECT t, (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;

-- segmentby should now be device_id — analysis ran on the new compressed chunk
SELECT * FROM _timescaledb_catalog.compression_settings;
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test_partial') chunk;
ROLLBACK;

-- Test MAX_SEGMENTBY_CANDIDATES cap (only first 10 eligible columns are considered)
BEGIN;
CREATE TABLE test_max_candidates (
    time TIMESTAMPTZ NOT NULL,
    c1 INT NOT NULL, c2 INT NOT NULL, c3 INT NOT NULL, c4 INT NOT NULL, c5 INT NOT NULL,
    c6 INT NOT NULL, c7 INT NOT NULL, c8 INT NOT NULL, c9 INT NOT NULL, c10 INT NOT NULL,
    good_candidate INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

INSERT INTO test_max_candidates
SELECT t,
    (i % 25), (i % 25), (i % 25), (i % 25), (i % 25),
    (i % 25), (i % 25), (i % 25), (i % 25), (i % 25),
    (i % 3),
    random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 25) i;

-- segmentby should be NULL — good_candidate is beyond the 10-column analysis cap
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test recompress preserves auto-discovered segmentby
BEGIN;
CREATE TABLE test_recompress_segmentby (
    time TIMESTAMPTZ NOT NULL,
    time2 TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.partition_column='time');

-- Large insert triggers direct compress and auto-segmentby analysis
INSERT INTO test_recompress_segmentby
SELECT t, t, (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;

-- device_id should be auto-selected as segmentby
SELECT * FROM _timescaledb_catalog.compression_settings;

-- Small insert into the same chunk range makes it partial
INSERT INTO test_recompress_segmentby VALUES
    ('2024-01-01 12:00:00', '2024-01-01 12:00:00', 1, 99.9),
    ('2024-01-01 12:01:00', '2024-01-01 12:01:00', 2, 88.8),
    ('2024-01-01 12:02:00', '2024-01-01 12:02:00', 3, 77.7);

-- Recompress should preserve the auto-discovered segmentby
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test_recompress_segmentby') chunk; -- unordered, partial
SELECT compress_chunk(ch, recompress := true) FROM show_chunks('test_recompress_segmentby') ch;
SELECT * FROM _timescaledb_catalog.compression_settings;
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test_recompress_segmentby') chunk; -- unordered
SELECT compress_chunk(ch, recompress := true) FROM show_chunks('test_recompress_segmentby') ch;
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test_recompress_segmentby') chunk; -- compressed
SELECT * FROM _timescaledb_catalog.compression_settings;

-- Recompress should overwrite settings
ALTER TABLE test_recompress_segmentby SET (tsdb.segment_by = '', tsdb.order_by = 'time2');
SELECT compress_chunk(ch, recompress := true) FROM show_chunks('test_recompress_segmentby') ch;


SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test default segmentby gets set via COPY path
BEGIN;
SET timescaledb.enable_direct_compress_insert = false;
SET timescaledb.enable_direct_compress_copy = false;
CREATE TABLE test_copy_segmentby (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable);

INSERT INTO test_copy_segmentby
SELECT t, (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 5) i;

ANALYZE test_copy_segmentby;
SELECT compress_chunk(c) FROM show_chunks('test_copy_segmentby') c;

-- should have device_id as default segmentby
SELECT * FROM _timescaledb_catalog.compression_settings;

SET timescaledb.enable_direct_compress_copy = true;
-- 7205 rows with 5 device_ids into a new chunk range via COPY
\COPY test_copy_segmentby FROM 'data/segmentby_copy_test.csv' WITH (DELIMITER ',');

ANALYZE test_copy_segmentby;
-- should have default segmentby set for the new direct compressed chunk
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

-- Test NULL values in pass-by-reference column during auto segmentby analysis
BEGIN;
CREATE TABLE test_null_text(time timestamptz NOT NULL, tag text, val float);
SELECT create_hypertable('test_null_text', 'time', chunk_time_interval => '7 days'::interval);
ALTER TABLE test_null_text SET (timescaledb.compress, timescaledb.compress_orderby = 'time');

SET timescaledb.direct_compress_insert_tuple_sort_limit = 500;
SET timescaledb.direct_compress_segmentby_min_rows = 100;
SET timescaledb.direct_compress_segmentby_batch_size_limit = 10;

-- tag is text (pass-by-reference); mix of non-NULL and NULL values
-- exercises the datumCopy guard for NULL distinct entries
INSERT INTO test_null_text
SELECT
    '2020-01-01'::timestamptz + (i || ' seconds')::interval,
    CASE WHEN i <= 200 THEN 'a' WHEN i <= 400 THEN 'b' ELSE NULL END,
    i
FROM generate_series(1, 600) i;
ROLLBACK;