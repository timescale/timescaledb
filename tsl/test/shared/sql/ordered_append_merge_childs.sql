-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test coverage for all MergeAppend creation paths in ChunkAppend
-- There are three distinct code paths that create MergeAppend nodes:
--
-- 1. Single-dimension hypertable, partial compression, ordered append
--    (chunk_append.c: create_group_subpath via lines 282-288, 300-306)
--
-- 2. Space-partitioned hypertable, multiple space partitions per time slice
--    (chunk_append.c: direct create_merge_append_path at line 388)
--
-- 3. Space-partitioned hypertable, partial compression per chunk
--    (same code path as #2, but triggered by partial compression)

\set PREFIX 'EXPLAIN (costs off)'

-- Disable parallel to get consistent plans
SET max_parallel_workers_per_gather = 0;

----------------------------------------------------------------------
-- Case 1: Single-dimension + partial compression + ordered append
----------------------------------------------------------------------
CREATE TABLE ht_single(time timestamptz NOT NULL, device int, value float);
SELECT table_name FROM create_hypertable('ht_single','time');

ALTER TABLE ht_single SET (
  timescaledb.compress,
  timescaledb.compress_segmentby='device',
  timescaledb.compress_orderby='time'
);

-- Note: TimescaleDB creates time index automatically

-- Insert and compress
INSERT INTO ht_single
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '4 hour') time,
     generate_series(1,2) device;

SELECT count(compress_chunk(c)) FROM show_chunks('ht_single') c;

-- Make partially compressed
INSERT INTO ht_single
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '6 hour') time,
     generate_series(1,2) device;

ANALYZE ht_single;

-- This should show ChunkAppend with nested MergeAppend for each chunk
:PREFIX SELECT * FROM ht_single ORDER BY time LIMIT 10;

-- Verify data correctness
SELECT count(*) FROM ht_single;

----------------------------------------------------------------------
-- Case 2: Space-partitioned + multiple partitions + ordered append
----------------------------------------------------------------------
CREATE TABLE ht_space(time timestamptz NOT NULL, device int, value float);
SELECT table_name FROM create_hypertable('ht_space','time');
SELECT add_dimension('ht_space', 'device', number_partitions => 2);

ALTER TABLE ht_space SET (
  timescaledb.compress,
  timescaledb.compress_segmentby='device',
  timescaledb.compress_orderby='time'
);

-- Insert data that spans multiple space partitions
INSERT INTO ht_space
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '4 hour') time,
     generate_series(1,4) device;

SELECT compress_chunk(c) FROM show_chunks('ht_space') c;

ANALYZE ht_space;

-- This should show ChunkAppend with MergeAppend grouping space partitions
:PREFIX SELECT * FROM ht_space ORDER BY time LIMIT 10;

-- Verify data correctness
SELECT count(*) FROM ht_space;

----------------------------------------------------------------------
-- Case 3: Space-partitioned + partial compression + ordered append
----------------------------------------------------------------------
CREATE TABLE ht_space_partial(time timestamptz NOT NULL, device int, value float);
SELECT table_name FROM create_hypertable('ht_space_partial','time');
SELECT add_dimension('ht_space_partial', 'device', number_partitions => 2);

ALTER TABLE ht_space_partial SET (
  timescaledb.compress,
  timescaledb.compress_segmentby='device',
  timescaledb.compress_orderby='time'
);

-- Insert and compress
INSERT INTO ht_space_partial
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '4 hour') time,
     generate_series(1,4) device;

SELECT count(compress_chunk(c)) FROM show_chunks('ht_space_partial') c;

-- Make partially compressed by inserting more data
INSERT INTO ht_space_partial
SELECT time, device, device * 0.1
FROM generate_series('2020-01-01'::timestamptz, '2020-01-14'::timestamptz, '6 hour') time,
     generate_series(1,4) device;

ANALYZE ht_space_partial;

-- This exercises both space partitioning AND partial compression
-- Each time slice may have multiple space partitions AND each chunk may have
-- both compressed and uncompressed portions
:PREFIX SELECT * FROM ht_space_partial ORDER BY time LIMIT 10;

-- Verify data correctness
SELECT count(*) FROM ht_space_partial;

----------------------------------------------------------------------
-- Cleanup
----------------------------------------------------------------------
RESET max_parallel_workers_per_gather;
DROP TABLE ht_single, ht_space, ht_space_partial;
