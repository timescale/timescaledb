-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test queries over compressed data with sort keys equivalent to other columns

\set PREFIX 'EXPLAIN (buffers off, costs off, timing off, summary off)'

-- Set up compressed hypertable with 3 segmentby and 3 orderby keys
CREATE TABLE test1 (
    s1 integer NOT NULL,
    s2 integer NOT NULL,
    s3 smallint NOT NULL,
    o1 integer NOT NULL,
    o2 integer NOT NULL,
    o3 smallint NOT NULL,
    v integer);

SELECT FROM create_hypertable('test1', 'o1');
ALTER TABLE test1 SET (timescaledb.compress, timescaledb.compress_segmentby='s1, s2, s3', timescaledb.compress_orderby = 'o1, o2 DESC, o3');

INSERT INTO test1  SELECT t1, t2, t3, t1, t4, t3 % 2, t3*10
FROM generate_series(1, 2) t1, generate_series(1, 5) t2, generate_series(1, 10) t3, generate_series(1, 4) t4;

SELECT count(compress_chunk(ch)) FROM show_chunks('test1') ch;

SET enable_seqscan = 0;
SET enable_bitmapscan = 0;
SET max_parallel_workers_per_gather = 0;

-- Equivalent segmentby columns, use compressed sort
set timescaledb.debug_require_batch_sorted_merge to 'forbid';

:PREFIX SELECT * FROM test1 WHERE s1 = s3 ORDER BY s3, s2;
:PREFIX SELECT * FROM test1 WHERE s1 = s3 ORDER BY s1, s2, o1;
:PREFIX SELECT * FROM test1 WHERE s1 = s2 ORDER BY s2, s3;
:PREFIX SELECT * FROM test1 WHERE s1 = s2 AND s2 = s3 ORDER BY s3;

-- Use batch sorted merge
set timescaledb.debug_require_batch_sorted_merge to 'force';

:PREFIX SELECT * FROM test1 WHERE s1 = s3 ORDER BY s3, o1;
:PREFIX SELECT * FROM test1 WHERE s1 = s2 ORDER BY s2, o1;

-- Equivalent segmentby and orderby columns, use compressed sort or uncompressed sort
set timescaledb.debug_require_batch_sorted_merge to 'forbid';

:PREFIX SELECT * FROM test1 WHERE s1 = o1 AND s2 = 1 AND s3 = 1 ORDER BY s1, o1;
:PREFIX SELECT * FROM test1 WHERE s1 = o1 AND s2 = 1 AND s3 = 1 ORDER BY s1, o1 DESC, o2;
:PREFIX SELECT * FROM test1 WHERE s3 = o1 AND s2 = 1 AND s1 = s3 ORDER BY s1, s2, s3, o2;
:PREFIX SELECT * FROM test1 WHERE s1 = o1 AND s2 = s3 ORDER BY s1, s3, o2 DESC;

-- Equivalent orderby columns, use compressed sort or uncomressed sort
:PREFIX SELECT * FROM test1 WHERE o1 = o2 ORDER BY s1, s2, s3, o1, o3;
:PREFIX SELECT * FROM test1 WHERE o1 = o2 ORDER BY s1, s2, s3, o1, o2, o3;
:PREFIX SELECT * FROM test1 WHERE o1 = o3 ORDER BY s1, s2, s3, o1, o2 DESC, o3;
:PREFIX SELECT * FROM test1 WHERE o1 = o3 AND o1 = o2 ORDER BY s1, s2, s3, o3 DESC;

-- Equivalent to other columns: can use compressed sort

:PREFIX SELECT * FROM test1 WHERE s1 = v ORDER BY s1, v;
:PREFIX SELECT * FROM test1 WHERE o1 = v AND s1 = 1 AND s2 = s3 ORDER BY s2, o1, v;
:PREFIX SELECT * FROM test1 WHERE o1 = v AND s1 = 1 AND s2 = s3 ORDER BY s2 DESC, o1 DESC, v DESC;

reset timescaledb.debug_require_batch_sorted_merge;

drop table test1 cascade;

-- Scenario flagged by LLM Fuzzer: we cannot skip sorting on orderby columns equivalent to columns earlier in the sort order
-- as "firstlast" index relies on the whole orderby column tuples to be sorted
SET timescaledb.compression_batch_size_limit = 5;
CREATE TABLE repro (
    a integer NOT NULL,
    b integer NOT NULL,
    c integer NOT NULL,
    val integer NOT NULL
)
WITH (tsdb.hypertable = true, tsdb.partition_column = 'b', tsdb.chunk_interval = 1000);

INSERT INTO repro (a, b, c, val) VALUES
    (1, 0, 100, 1), (1, 1, 1, 2), (1, 1, 2, 3), (1, 1, 3, 4), (1, 1, 4, 5),
    (1, 1, 5, 6), (1, 1, 6, 7), (1, 2, 1, 8), (1, 2, 2, 9), (1, 2, 3, 10);

ALTER TABLE repro SET (timescaledb.compress, timescaledb.compress_segmentby = 'a', timescaledb.compress_orderby = 'b, c');
SELECT count(compress_chunk(ch)) FROM show_chunks('repro') ch;

-- Should return correct result
SELECT a, c, val FROM repro WHERE a = b ORDER BY a, c;

drop table repro cascade;

-- Test for #10507: we should choose compressed sort over batch sorted merge
-- by picking correct equivalence class member sortkey for the query
CREATE TABLE tv_ratings (ts timestamptz NOT NULL, channel_id int NOT NULL,
    reported_channel_id int NOT NULL);
SELECT FROM create_hypertable('tv_ratings', 'ts');
INSERT INTO tv_ratings SELECT '2026-08-01'::timestamptz + interval '1 second' * g,
    g % 3, g % 3 FROM generate_series(1, 5) g;
ALTER TABLE tv_ratings SET (timescaledb.compress,
    timescaledb.compress_segmentby = 'channel_id',
    timescaledb.compress_orderby = 'reported_channel_id');

-- baseline, uncompressed: returns 5 rows
SELECT  channel_id, reported_channel_id FROM tv_ratings WHERE reported_channel_id = channel_id ORDER BY channel_id;

SELECT count(compress_chunk(x)) FROM show_chunks('tv_ratings') x;

-- the same query on the compressed chunk should return the same result
SELECT channel_id, reported_channel_id  FROM tv_ratings WHERE reported_channel_id = channel_id ORDER BY channel_id;
-- Should use compressed indexscan sort order
:PREFIX SELECT channel_id, reported_channel_id  FROM tv_ratings WHERE reported_channel_id = channel_id ORDER BY channel_id;

drop table tv_ratings cascade;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;
