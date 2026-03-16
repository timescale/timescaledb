-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;
-- simple test to check default segmentby does not get set for direct compress
BEGIN;
RESET timescaledb.enable_direct_compress_insert;
CREATE TABLE test_segmentby_stats (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 5) i;

ANALYZE test_segmentby_stats;
SELECT compress_chunk(c) FROM show_chunks('test_segmentby_stats') c;

-- will have devide_id as default segmentby for compressed chunk;
SELECT * FROM _timescaledb_catalog.compression_settings;

SET timescaledb.enable_direct_compress_insert = true;
INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-06'::timestamptz, '2024-01-07'::timestamptz, '1 second') t,
     generate_series(1, 5) i;
ANALYZE test_segmentby_stats;
-- will have default segmentby set for a newly created direct compressed chunk (should observe a skipped chunk id);
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

BEGIN;
RESET timescaledb.enable_direct_compress_insert;
CREATE TABLE test_segmentby_stats (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress, tsdb.segmentby='device_id');

SET timescaledb.enable_direct_compress_insert = true;
INSERT INTO test_segmentby_stats
SELECT t, (i % 5), random()
FROM generate_series('2024-01-06'::timestamptz, '2024-01-07'::timestamptz, '1 second') t,
     generate_series(1, 5) i;
ANALYZE test_segmentby_stats;
-- will have device_id by as configured segmentby for direct compressed chunk (should not observe skipped chunk id);
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: Too few rows — analysis skipped (tuples_to_sort < segmentby_min_rows)
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_too_few_rows (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

-- Only ~4323 rows (1441 minutes * 3 devices), under the 5000 min_rows threshold
INSERT INTO test_too_few_rows
SELECT t, (i % 3), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 minute') t,
     generate_series(1, 3) i;

-- segmentby should be NULL — not enough rows to trigger analysis
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: No candidate columns — only orderby, float, and time columns
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_no_candidates (
    time TIMESTAMPTZ NOT NULL,
    value FLOAT,
    label TEXT
) WITH (tsdb.hypertable, tsdb.compress, tsdb.orderby='time');

-- 86401 rows, enough to trigger analysis, but no eligible candidate columns:
-- time is orderby, value is float, label is text — all filtered out
INSERT INTO test_no_candidates
SELECT t, random(), 'sensor_' || (i % 5)
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 1) i;

-- segmentby should be NULL — no candidates passed ts_accept_for_segmentby
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: All candidates rejected — more than MAX_SEGMENTBY_DISTINCT (20)
-- Tests early termination path (candidates_rejected == n_candidates)
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_all_rejected (
    time TIMESTAMPTZ NOT NULL,
    high_card_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

-- 25 distinct values for high_card_id, exceeds MAX_SEGMENTBY_DISTINCT (20)
INSERT INTO test_all_rejected
SELECT t, (i % 25), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 25) i;

-- segmentby should be NULL — only candidate has too many distinct values
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: Candidate not qualified — low cardinality but one value has
-- fewer than segmentby_batch_size_limit (500) rows
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_uneven_distribution (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

-- device_id 1-4 get ~10800 rows each (86401 / 4 seconds * 4 devices = lots)
-- device_id 5 gets only a handful of rows — not enough for a full batch
INSERT INTO test_uneven_distribution
SELECT t, (i % 4) + 1, random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 4) i;

INSERT INTO test_uneven_distribution
SELECT t, 5, random()
FROM generate_series('2024-01-01 00:00:00'::timestamptz, '2024-01-01 00:05:00'::timestamptz, '1 second') t;

-- segmentby should be NULL — device_id=5 has ~301 rows, below segmentby_batch_size_limit
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: Multiple candidate columns — first qualifying column wins
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_multi_candidate (
    time TIMESTAMPTZ NOT NULL,
    region_id INT NOT NULL,
    status_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

-- region_id: 10 distinct values, status_id: 3 distinct values
-- Both qualify, but region_id should win (earlier column position)
INSERT INTO test_multi_candidate
SELECT t, (i % 10), (i % 3), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 10) i;

-- segmentby should be region_id (first qualifying candidate by attribute order)
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: NULL values in candidate column
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_null_candidate (
    time TIMESTAMPTZ NOT NULL,
    device_id INT,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

-- 4 non-null device_ids + NULLs, all with enough rows
INSERT INTO test_null_candidate
SELECT t, CASE WHEN i <= 4 THEN i ELSE NULL END, random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 5) i;

-- segmentby should be device_id — NULLs are treated as a distinct group
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: Exactly MAX_SEGMENTBY_DISTINCT (20) distinct values — boundary
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_exact_max_distinct (
    time TIMESTAMPTZ NOT NULL,
    category_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

-- Exactly 20 distinct values, each with 86401/20 * 20 = enough rows
INSERT INTO test_exact_max_distinct
SELECT t, (i % 20), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 20) i;

-- segmentby should be category_id — exactly at the limit, not over
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: 21 distinct values — just over MAX_SEGMENTBY_DISTINCT, rejected
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_over_max_distinct (
    time TIMESTAMPTZ NOT NULL,
    category_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

INSERT INTO test_over_max_distinct
SELECT t, (i % 21), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 21) i;

-- segmentby should be NULL — 21 distinct values exceeds MAX_SEGMENTBY_DISTINCT
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: Boolean candidate column
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_bool_candidate (
    time TIMESTAMPTZ NOT NULL,
    is_active BOOL NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

INSERT INTO test_bool_candidate
SELECT t, (i % 2 = 0), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 2) i;

-- segmentby should be is_active — bool is an accepted type with 2 distinct values
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;

----------------------------------------------------------------------
-- Test: Second chunk reuses existing settings — no re-analysis
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_second_chunk (
    time TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

-- First chunk: triggers analysis, picks device_id
INSERT INTO test_second_chunk
SELECT t, (i % 5), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 5) i;

SELECT * FROM _timescaledb_catalog.compression_settings;

-- Second chunk: should reuse the segmentby from the first chunk, no rebuild
INSERT INTO test_second_chunk
SELECT t, (i % 5), random()
FROM generate_series('2024-01-08'::timestamptz, '2024-01-09'::timestamptz, '1 second') t,
     generate_series(1, 5) i;

-- Settings should be unchanged — same segmentby as after first chunk
SELECT * FROM _timescaledb_catalog.compression_settings;

-- Both chunks should exist
SELECT count(*) FROM show_chunks('test_second_chunk');
ROLLBACK;

----------------------------------------------------------------------
-- Test: First candidate rejected, second qualifies
-- (ensures we don't stop at the first candidate)
----------------------------------------------------------------------
BEGIN;
CREATE TABLE test_fallback_candidate (
    time TIMESTAMPTZ NOT NULL,
    high_card_id INT NOT NULL,
    low_card_id INT NOT NULL,
    value FLOAT
) WITH (tsdb.hypertable, tsdb.compress);

-- high_card_id: 25 distinct values (rejected), low_card_id: 3 distinct values (qualifies)
INSERT INTO test_fallback_candidate
SELECT t, (row_number() OVER ()) % 25, (i % 3), random()
FROM generate_series('2024-01-01'::timestamptz, '2024-01-02'::timestamptz, '1 second') t,
     generate_series(1, 3) i;

-- segmentby should be low_card_id — high_card_id rejected, low_card_id qualifies
SELECT * FROM _timescaledb_catalog.compression_settings;
ROLLBACK;