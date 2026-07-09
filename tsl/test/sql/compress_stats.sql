-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This is the sister of the compress_observ test. The difference
-- here is that the test displays a minimal set of platform and
-- PG version independent values.

CREATE TABLE t(ts timestamptz, a int, b int, c int, d int, seg int);
SELECT create_hypertable('t', by_range('ts', interval '1 day'));

CREATE VIEW observ AS
SELECT
      compressed_batch_count,                compressed_block_count,
      compressed_batch_rows_min,             compressed_batch_rows_max,
      compressed_batch_rows_sum,             compressed_batch_rows_sqsum,
      -- op counters
      n_selects, n_inserts, n_updates, n_deletes
      -- skip the below two for test stability:
      -- first_update,
      -- last_update
FROM
(
      SELECT * FROM _timescaledb_functions.chunk_statistics()
      ORDER BY last_update ASC, compressed_relid ASC, uncompressed_relid ASC
) sub;

CREATE VIEW observ_main_view AS
SELECT
      chunk,  compressed_chunk, hypertable,
      -- compression stats
      compressed_batch_count,                compressed_block_count,
      compressed_batch_rows_min,             compressed_batch_rows_max,
      compressed_batch_rows_avg,             compressed_batch_rows_stddev,
      -- op counters
      n_selects, n_inserts, n_updates, n_deletes
      -- these will make the test flaky, so skip them:
      -- first_update,
      -- last_update
FROM
(
      SELECT *
      FROM timescaledb_information.stat_chunk_activity
      ORDER BY last_update ASC, hypertable_id ASC, chunk_id ASC
) sub;

-- Initial config: bloom(a,b)
ALTER TABLE t SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'seg',
    timescaledb.compress_orderby = 'ts',
    timescaledb.compress_index = 'bloom(a,b), bloom(d)'
);

INSERT INTO t
SELECT ts, (i % 10), (i % 5), (i % 3), i, 1
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03', interval '1 hour') ts,
     generate_series(1, 100) i;

\pset expanded on

SELECT compress_chunk(c) FROM show_chunks('t') c;

SELECT * FROM observ; -- see view definition above

SELECT COUNT(*) FROM t;
SELECT count(*),min(ts),max(ts) from t where a % 19 = 9 or b % 19 = 9 or c % 19 = 2 or seg % 19 = 2;

SELECT * FROM observ; -- see view definition above

-- Test DELETE stats
BEGIN;
DELETE FROM t WHERE a % 19 = 2;
ROLLBACK;

SELECT * FROM observ; -- see view definition above

-- Test UPDATE stats
BEGIN;
UPDATE t SET a = 42 WHERE a % 19 = 2;
ROLLBACK;

SELECT * FROM observ; -- see view definition above

-- Show the information schema view as well:
SELECT * FROM observ_main_view;

-- Reset the stats and check how we re-populate them as we select from the chunk again:
SELECT _timescaledb_functions.chunk_statistics_reset();
SELECT count(*) FROM t where a % 19 = 9;

-- Only care about compression stats here:
SELECT
      chunk,                                 compressed_chunk,
      compressed_batch_count,                compressed_block_count,
      compressed_batch_rows_min,             compressed_batch_rows_max,
      compressed_batch_rows_avg,             compressed_batch_rows_stddev
FROM observ_main_view;

-- Select more and check how things change:
SELECT count(*),min(ts),max(ts) from t where a % 19 = 9 or b % 19 = 9 or c % 19 = 2 or seg % 19 = 2;

SELECT
      chunk,                                 compressed_chunk,
      compressed_batch_count,                compressed_block_count,
      compressed_batch_rows_min,             compressed_batch_rows_max,
      compressed_batch_rows_avg,             compressed_batch_rows_stddev
FROM observ_main_view;

-- The stats should be evicted after we decompress and thus throwing away
-- the compressed chunks.
SELECT decompress_chunk(c) FROM show_chunks('t') c;

-- This should be empty after the decompression
SELECT
      chunk,                                 compressed_chunk,
      compressed_batch_count,                compressed_block_count,
      compressed_batch_rows_min,             compressed_batch_rows_max,
      compressed_batch_rows_avg,             compressed_batch_rows_stddev
FROM observ_main_view;

-- Re-compress before doing the bloom tests
SELECT compress_chunk(c) FROM show_chunks('t') c;

-- Test DELETE stats with bloom filter matches
BEGIN;
DELETE FROM t WHERE a = 7 AND b = 2;
ROLLBACK;

SELECT * FROM observ; -- see view definition above

-- Test DELETE stats with bloom filter misses
BEGIN;
DELETE FROM t WHERE d = 200;
ROLLBACK;

SELECT * FROM observ; -- see view definition above

-- Test UPDATE stats with bloom filter matches
BEGIN;
UPDATE t SET a = 42 WHERE a = 7 AND b = 2;
ROLLBACK;

SELECT * FROM observ; -- see view definition above

-- Test UPDATE stats with bloom filter misses
BEGIN;
UPDATE t SET a = 42 WHERE d = 200;
ROLLBACK;

SELECT * FROM observ; -- see view definition above


DROP VIEW observ;
DROP VIEW observ_main_view;
DROP TABLE t;
