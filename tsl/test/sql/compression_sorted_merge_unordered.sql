-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Increase the working memory limit slightly, otherwise the batch sorted merge
-- will be penalized for segmentby cardinalities larger than 100, where it is
-- still faster than sort.
SET work_mem to '16MB';

\set PREFIX 'EXPLAIN (analyze, verbose, buffers off, costs off, timing off, summary off)'

SET timescaledb.enable_direct_compress_insert TO ON;

CREATE TABLE test1 (
    time timestamptz NOT NULL,
    x1 integer,
    x2 integer,
    x3 integer NOT NULL,
    x4 integer NOT NULL,
    x5 integer);

SELECT FROM create_hypertable('test1', 'time');

ALTER TABLE test1 SET (timescaledb.compress, timescaledb.compress_segmentby='x1, x2, x5', timescaledb.compress_orderby = 'time DESC, x3 ASC, x4 ASC');

-- Need to insert more than 10 tuples to use direct compress
INSERT INTO test1 (time, x1, x2, x3, x4, x5) values
('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0),
('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0),
('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0),
('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0),
('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0),
('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0),
('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0),
('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0),
('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0),
('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0),
('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0),
('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0);

ANALYZE test1;

-- Show chunk is compressed unordered.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test1') chunk;

CREATE TABLE test2 (
time timestamptz NOT NULL,
    x1 integer,
    x2 integer,
    x3 integer NOT NULL,
    x4 integer NOT NULL,
    x5 integer);

SELECT FROM create_hypertable('test2', 'time');

ALTER TABLE test2 SET (timescaledb.compress, timescaledb.compress_segmentby='x1, x2, x5', timescaledb.compress_orderby = 'time ASC, x3 DESC, x4 DESC');

INSERT INTO test2 (time, x1, x2, x3, x4, x5) values
('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0),
('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0),
('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0),
('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0),
('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0),
('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0),
('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0),
('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0),
('2000-01-01 00:00:00-00', 1, 2, 1, 1, 0),
('2000-01-01 01:00:00-00', 1, 3, 2, 2, 0),
('2000-01-01 02:00:00-00', 2, 1, 3, 3, 0),
('2000-01-01 03:00:00-00', 1, 2, 4, 4, 0);

ANALYZE test2;
-- Show chunk is compressed unordered.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test2') chunk;

CREATE TABLE test_with_defined_null (
    time timestamptz NOT NULL,
    x1 integer,
    x2 integer,
    x3 integer);

SELECT FROM create_hypertable('test_with_defined_null','time');

ALTER TABLE test_with_defined_null SET (timescaledb.compress,timescaledb.compress_segmentby='x1', timescaledb.compress_orderby='x2 ASC NULLS FIRST');

INSERT INTO test_with_defined_null (time, x1, x2) values
('2000-01-01', '1', NULL),
('2000-01-01','2', NULL),
('2000-01-01','1',1),
('2000-01-01','1',2),
('2000-01-01', '1', NULL),
('2000-01-01','2', NULL),
('2000-01-01','1',1),
('2000-01-01','1',2),
('2000-01-01', '1', NULL),
('2000-01-01','2', NULL),
('2000-01-01','1',1),
('2000-01-01','1',2);

ANALYZE test_with_defined_null;
-- Show chunk is compressed unordered.
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test_with_defined_null') chunk;

-- test1 uses compress_segmentby='x1, x2, x5' and compress_orderby = 'time DESC, x3 ASC, x4 ASC'
-- test2 uses compress_segmentby='x1, x2, x5' and compress_orderby = 'time ASC, x3 DESC, x4 DESC'
-- test_with_defined_null uses compress_segmentby='x1' and compress_orderby = 'x2 ASC NULLS FIRST'

------
-- Tests based on ordering
------

set timescaledb.debug_require_batch_sorted_merge to 'force';

-- Should be optimized (implicit NULLS first)
:PREFIX
SELECT * FROM test1 ORDER BY time DESC;

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST;

-- Should be optimized (implicit NULLS last)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC;

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST;

-- Should be optimized
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST, x4 ASC NULLS LAST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST, x3 DESC NULLS FIRST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST, x3 DESC NULLS FIRST, x4 DESC NULLS FIRST;

-- Should be optimized
:PREFIX
SELECT * FROM test2 ORDER BY time ASC;

-- Should be optimized
:PREFIX
SELECT * FROM test2 ORDER BY time ASC, x3 DESC;

-- Should be optimized
:PREFIX
SELECT * FROM test2 ORDER BY time ASC, x3 DESC, x4 DESC;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS FIRST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST, x4 NULLS LAST;

-- Should be optimized as we exclude NULLs from x2
:PREFIX
SELECT * FROM test_with_defined_null WHERE x2 IS NOT NULL ORDER BY x2 ASC NULLS FIRST;

-- Should not be optimized (NULL order wrong)
set timescaledb.debug_require_batch_sorted_merge to 'forbid';
:PREFIX
SELECT * FROM test_with_defined_null WHERE x2 IS NOT NULL ORDER BY x2 ASC NULLS LAST;

set timescaledb.debug_require_batch_sorted_merge to 'force';
-- Should be optimized (backward scan) as we exclude NULLs from x2
:PREFIX
SELECT * FROM test_with_defined_null  WHERE (x2 + 1) IS NOT NULL ORDER BY x2 DESC NULLS LAST;

-- Should be optimized as we exlude NULLs from x2 via strict functions
:PREFIX
SELECT * FROM test_with_defined_null WHERE x2 > 0 ORDER BY x2 ASC NULLS FIRST;

:PREFIX
SELECT * FROM test_with_defined_null WHERE x2::float + x1 > 1.0  AND time = '2000-01-01' ORDER BY x2 ASC NULLS FIRST;

set timescaledb.debug_require_batch_sorted_merge to 'forbid';

-- Should not be optimized (wrong order for x3 in backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS LAST, x3 DESC NULLS FIRST, x4 NULLS FIRST;

-- Should not be optimized (wrong order for x3 in backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS LAST, x3 DESC NULLS LAST, x4 NULLS FIRST;

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS LAST;

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS FIRST;

-- Should not be optimized (wrong order for x4)
:PREFIX
SELECT * FROM test1 ORDER BY time DESC NULLS FIRST, x3 ASC NULLS LAST, x4 DESC NULLS FIRST;

-- Should not be optimized (wrong order for x4 in backward scan)
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS FIRST, x3 DESC NULLS LAST, x4 ASC;

-- Should not be optimized (wrong order for x3)
:PREFIX
SELECT * FROM test2 ORDER BY time ASC, x3 ASC NULLS LAST, x4 DESC;

-- Should not be optimized (wrong order for x3)
:PREFIX
SELECT * FROM test2 ORDER BY time ASC, x3 ASC NULLS FIRST, x4 DESC;

-- Should not be optimized as x2 is nullable
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 ASC;

:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS LAST;

:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS FIRST;

:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 DESC;

-- Should not be optimized as x2 is nullable with non-strict-only predicates over x2
:PREFIX
SELECT * FROM test_with_defined_null WHERE x2 IS NOT NULL OR x2 > 1 ORDER BY x2;

:PREFIX
SELECT * FROM test_with_defined_null WHERE x2 IS NULL OR x2 > 1 ORDER BY x2;

------
-- Tests based on attributes
------

set timescaledb.debug_require_batch_sorted_merge to 'force';

-- Should be optimized (some batches qualify by pushed down filter on _ts_meta_max_3)
:PREFIX
SELECT * FROM test1 WHERE x4 > 0 ORDER BY time DESC;

-- Should be optimized (no batches qualify by pushed down filter on _ts_meta_max_3)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC;

-- Should be optimized
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x4;

-- Should be optimized (duplicate order by attributes)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x3;

-- Should be optimized (duplicate order by attributes)
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x3, x4, x3, x4;

set timescaledb.debug_require_batch_sorted_merge to 'forbid';

-- Should not be optimized
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC, x4, x3;

-- Should not be optimized
:PREFIX
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time ASC, x3, x4;

------
-- Tests based on results
------

set timescaledb.debug_require_batch_sorted_merge to 'force';

-- Forward scan
SELECT * FROM test1 ORDER BY time DESC;

-- Forward scan
SELECT * FROM test2 ORDER BY time ASC;

set timescaledb.debug_require_batch_sorted_merge to 'force';

-- Backward scan
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

-- Backward scan
SELECT * FROM test2 ORDER BY time DESC NULLS FIRST;

set timescaledb.debug_require_batch_sorted_merge to 'force';

-- With selection on compressed column (value larger as max value for all batches, so no batch has to be opened)
SELECT * FROM test1 WHERE x4 > 100 ORDER BY time DESC;

-- With selection on compressed column (value smaller as max value for some batches, so batches are opened and filter has to be applied)
SELECT * FROM test1 WHERE x4 > 2 ORDER BY time DESC;

-- With selection on segment_by column
SELECT * FROM test1 WHERE time < '1980-01-01 00:00:00-00' ORDER BY time DESC;
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' ORDER BY time DESC;

-- With selection on segment_by and compressed column
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' ORDER BY time DESC;
SELECT * FROM test1 WHERE time > '1980-01-01 00:00:00-00' AND x4 > 100 ORDER BY time DESC;

-- Without projection
SELECT * FROM test1 ORDER BY time DESC;

-- With projection on time
SELECT time FROM test1 ORDER BY time DESC;

-- With projection on x3
SELECT x3 FROM test1 ORDER BY time DESC;

-- With projection on x3 and time
SELECT x3,time FROM test1 ORDER BY time DESC;

-- With projection on time and x3
SELECT time,x3 FROM test1 ORDER BY time DESC;

-- Test with projection and constants
EXPLAIN (verbose, buffers off, costs off) SELECT 1 as one, 2 as two, 3 as three, time, x2 FROM test1 ORDER BY time DESC;
SELECT 1 as one, 2 as two, 3 as three, time, x2 FROM test1 ORDER BY time DESC;

-- Test with projection and constants
EXPLAIN (verbose, buffers off, costs off) SELECT 1 as one, 2 as two, 3 as three, x2, time FROM test1 ORDER BY time DESC;
SELECT 1 as one, 2 as two, 3 as three, x2, time FROM test1 ORDER BY time DESC;

-- With projection and selection on compressed column (value smaller as max value for some batches, so batches are opened and filter has to be applied)
SELECT x4 FROM test1 WHERE x4 > 2 ORDER BY time DESC;

set timescaledb.debug_require_batch_sorted_merge to 'forbid';

-- Aggregation with count
SELECT count(*) FROM test1;

-- Test with default values
ALTER TABLE test1 ADD COLUMN c1 int;
ALTER TABLE test1 ADD COLUMN c2 int NOT NULL DEFAULT 42;
set timescaledb.debug_require_batch_sorted_merge to 'force';
SELECT * FROM test1 ORDER BY time DESC;

-- Test with a changed physical layout
SELECT * FROM test1 ORDER BY time DESC;
ALTER TABLE test1 DROP COLUMN c2;
SELECT * FROM test1 ORDER BY time DESC;

-- Test with a re-created column
ALTER TABLE test1 ADD COLUMN c2 int NOT NULL DEFAULT 43;
SELECT * FROM test1 ORDER BY time DESC;

-- Test with the recreated column
:PREFIX
SELECT * FROM test1 ORDER BY time DESC;
SELECT * FROM test1 ORDER BY time DESC;

-- Test with projection and recreated column
:PREFIX
SELECT time, x2, x1, c2 FROM test1 ORDER BY time DESC;
SELECT time, x2, x1, c2 FROM test1 ORDER BY time DESC;

-- Test with projection and recreated column
:PREFIX
SELECT x2, x1, c2, time FROM test1 ORDER BY time DESC;
SELECT x2, x1, c2, time FROM test1 ORDER BY time DESC;

-- Test with projection, constants and recreated column
:PREFIX
SELECT 1 as one, 2 as two, 3 as three, x2, x1, c2, time FROM test1 ORDER BY time DESC;
SELECT 1 as one, 2 as two, 3 as three, x2, x1, c2, time FROM test1 ORDER BY time DESC;

-- Test with null values in x2: batch sorted merge supported with firstlast indexes
set timescaledb.debug_require_batch_sorted_merge to 'force';
SELECT time, x2 FROM test_with_defined_null ORDER BY x2 ASC NULLS FIRST;
SELECT time, x2 FROM test_with_defined_null ORDER BY x2 DESC NULLS LAST;

-- Should not be optimized (NULL order wrong)
set timescaledb.debug_require_batch_sorted_merge to 'forbid';
SELECT time, x2 FROM test_with_defined_null ORDER BY x2 ASC NULLS LAST;
SELECT time, x2 FROM test_with_defined_null ORDER BY x2 DESC NULLS FIRST;

------
-- Tests based on compressed chunk state
------

-- Should be optimized
set timescaledb.debug_require_batch_sorted_merge to 'force';
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

BEGIN TRANSACTION;

INSERT INTO test1 (time, x1, x2, x3, x4, x5) values('2000-01-01 02:01:00-00', 10, 20, 30, 40, 50);

-- Should be optimized using a merge append path between the compressed and uncompressed part of the chunk
:PREFIX
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

-- The inserted value should be visible
SELECT * FROM test1 ORDER BY time ASC NULLS LAST;

ROLLBACK;

-- Test with segmentby
-- Should show that BatchSortedMerge works even with segmentby
-- Using segmentby column for ordering should not use BatchSortedMerge or compressed index ordering

CREATE TABLE test_segby (
	segby int,
	time timestamptz NOT NULL,
	val int);

SELECT FROM create_hypertable('test_segby', 'time');

ALTER TABLE test_segby SET (timescaledb.compress, timescaledb.compress_segmentby='segby', timescaledb.compress_orderby = 'time DESC');

INSERT INTO test_segby (time, segby, val) values
('2000-01-01 00:00:00-00', 1, 2),
('2000-01-01 01:00:00-00', 1, 3),
('2000-01-01 02:00:00-00', 2, 1),
('2000-01-01 03:00:00-00', 1, 2),
('2000-01-01 00:00:00-00', 1, 2),
('2000-01-01 01:00:00-00', 1, 3),
('2000-01-01 02:00:00-00', 2, 1),
('2000-01-01 03:00:00-00', 1, 2),
('2000-01-01 00:00:00-00', 1, 2),
('2000-01-01 01:00:00-00', 1, 3),
('2000-01-01 02:00:00-00', 2, 1),
('2000-01-01 03:00:00-00', 1, 2);
SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test_segby') chunk;

set timescaledb.debug_require_batch_sorted_merge to 'force';

-- Should be optimized (implicit NULLS first)
:PREFIX
SELECT * FROM test_segby ORDER BY time DESC;

-- Should be optimized
:PREFIX
SELECT * FROM test_segby ORDER BY time DESC NULLS FIRST;

-- Should be optimized (implicit NULLS last)
:PREFIX
SELECT * FROM test_segby ORDER BY time ASC;

-- Should be optimized
:PREFIX
SELECT * FROM test_segby ORDER BY time ASC NULLS LAST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test_segby ORDER BY time ASC NULLS LAST;

set timescaledb.debug_require_batch_sorted_merge to 'forbid';

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test_segby ORDER BY time DESC NULLS LAST;

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test_segby ORDER BY time ASC NULLS FIRST;

-- Should not be optimized (using segmentby)
:PREFIX
SELECT * FROM test_segby ORDER BY segby, time;

-- Tests for #9445: forbid BatchSortedMerge on nullable orderby columns with no firstlast index
CREATE TABLE t(time int NOT NULL, device int, val int);
SELECT create_hypertable('t', 'time', chunk_time_interval => 10000);
ALTER TABLE t SET (timescaledb.compress,
    timescaledb.compress_segmentby='device',
    timescaledb.compress_orderby='val DESC');

-- One batch: 200 NULLs + values 1..300. _ts_meta_max = 300.
-- In DESC order the first decompressed tuple is NULL, not 300.
SET timescaledb.enable_direct_compress_insert = false;
INSERT INTO t SELECT 1, 1, NULL FROM generate_series(1, 200);
INSERT INTO t SELECT 1, 1, g FROM generate_series(1, 300) g;
SELECT compress_chunk(show_chunks('t'));

-- Two more batches via direct compress (chunk becomes unordered).
SET timescaledb.enable_direct_compress_insert = true;
INSERT INTO t SELECT 1, 1, g FROM generate_series(500, 800) g;
INSERT INTO t SELECT 1, 1, g FROM generate_series(900, 1000) g;

-- Batches with NULLs in orderby columns are correctly sorted with firstlast:
-- Batch Sorted Merge is allowed
SET timescaledb.debug_require_batch_sorted_merge = 'force';

-- Should return 0
SELECT count(*) AS wrong_rows FROM (
    SELECT val, lag(val) OVER (ORDER BY val DESC) AS prev FROM t
) t WHERE val IS NULL AND prev IS NOT NULL;

-- Remove firstlast index from order by columns
update _timescaledb_catalog.compression_settings
set index = '[{"type": "minmax", "column": "val", "source": "orderby"}, {"type": "minmax", "column": "time", "source": "orderby"}]'
where relid = 't'::regclass;

update _timescaledb_catalog.compression_settings
set index = '[{"type": "minmax", "column": "val", "source": "orderby"}, {"type": "minmax", "column": "time", "source": "orderby"}]'
where compress_relid = (select compress_relid from _timescaledb_catalog.compression_settings
    where relid = (select relid from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 't') limit 1));

-- Use minmax index on (val DESC, time DESC) instead
select compress_relid::text comp_chunk from _timescaledb_catalog.compression_settings
    where relid = (select relid from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 't') limit 1)
\gset
create index compressed_index_minmax on :comp_chunk (device, _ts_meta_min_1 DESC, _ts_meta_max_1 DESC, _ts_meta_min_2 DESC, _ts_meta_max_2 DESC);

-- Should not use BatchSortedMerge here, should return 0
SET timescaledb.debug_require_batch_sorted_merge = 'forbid';

SELECT count(*) AS wrong_rows FROM (
    SELECT val, lag(val) OVER (ORDER BY val DESC) AS prev FROM t
) t WHERE val IS NULL AND prev IS NOT NULL;

-- Can use BatchSortedMerge if we can exclude NULLs from val
SET timescaledb.debug_require_batch_sorted_merge = 'force';
SELECT val, lag(val) OVER (ORDER BY val DESC NULLS FIRST) AS prev FROM t where val > 995;

drop table t cascade;

-- Tests for BatchSortedMerge over one-segment data
--------------------------------------

-- Should be optimized (all segmentby columns are pinned to a Const, orderby columns match)
SET timescaledb.debug_require_batch_sorted_merge = 'force';

-- Encourage compressed indexscan as query sort keys may match compressed indexscan order
SET enable_seqscan=0;
SET enable_bitmapscan=0;

:PREFIX
SELECT * FROM test_segby WHERE segby = 1 ORDER BY time DESC;

:PREFIX
SELECT * FROM test1 WHERE x1 = 1 AND x2 = 2 AND x5 = 0 ORDER BY x1, x2, x5, time DESC;

-- multikey orderby: can't use compressed indexscan order, need explicit sort
:PREFIX
SELECT * FROM test2 WHERE x1 = 1 AND x2 = 2 AND x5 = 0 ORDER BY time ASC, x3 DESC;

:PREFIX
SELECT * FROM test2 WHERE x1 = 1 AND x2 = 2 AND x5 = 0 ORDER BY x1 DESC, x2 DESC, x5 DESC, time DESC, x3 ASC;

-- We should choose Batch sorted merge with explicit sort on the leading orderby metadata column
SET enable_indexscan=0;
SET enable_bitmapscan=0;
SET enable_seqscan=1;
:PREFIX
SELECT * FROM test_segby WHERE segby = 1 ORDER BY time DESC;

:PREFIX
SELECT * FROM test1 WHERE x1 = 1 AND x2 = 2 AND x5 = 0 ORDER BY x1, x2, x5, time DESC;

:PREFIX
SELECT * FROM test2 WHERE x1 = 1 AND x2 = 2 AND x5 = 0 ORDER BY time ASC, x3 DESC;

:PREFIX
SELECT * FROM test2 WHERE x1 = 1 AND x2 = 2 AND x5 = 0 ORDER BY x1 DESC, x2 DESC, x5 DESC, time DESC, x3 ASC;

SET enable_seqscan=0;
SET enable_bitmapscan=0;
SET enable_indexscan=1;

-- No segmentby
CREATE TABLE test_nosegby (
	segby int NOT NULL,
	time timestamptz NOT NULL,
	val int);

SELECT FROM create_hypertable('test_nosegby', 'time');

ALTER TABLE test_nosegby SET (timescaledb.compress, timescaledb.compress_orderby = 'segby,time DESC');

INSERT INTO test_nosegby (time, segby, val) values
('2000-01-01 00:00:00-00', 1, 2),
('2000-01-01 01:00:00-00', 1, 3),
('2000-01-01 02:00:00-00', 2, 1),
('2000-01-01 03:00:00-00', 1, 2),
('2000-01-01 00:00:00-00', 1, 2),
('2000-01-01 01:00:00-00', 1, 3),
('2000-01-01 02:00:00-00', 2, 1),
('2000-01-01 03:00:00-00', 1, 2),
('2000-01-01 00:00:00-00', 1, 2),
('2000-01-01 01:00:00-00', 1, 3),
('2000-01-01 02:00:00-00', 2, 1),
('2000-01-01 03:00:00-00', 1, 2);

SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('test_nosegby') chunk;

-- Should be optimized (implicit NULLS first)
-- but can't use compressed indexscan order, need explicit sort: multikey orderby
-- Batch sort merge needs sort by (first_col1, first_col2)
-- while compressed input is sorted on (first_col1, last_col1, first_col2, last_col2)
:PREFIX
SELECT * FROM test_nosegby ORDER BY segby, time DESC;

-- Should be optimized but can't use compressed indexscan order: multikey orderby
:PREFIX
SELECT * FROM test_nosegby ORDER BY segby, time DESC NULLS FIRST;

-- Should be optimized but can't use compressed indexscan order (backward scan, NULLS last)
:PREFIX
SELECT * FROM test_nosegby ORDER BY segby DESC, time ASC NULLS LAST;

-- Should be optimized but can't use compressed indexscan order (backward scan)
:PREFIX
SELECT * FROM test_nosegby ORDER BY segby DESC;

-- Should be optimized and can use compressed indexscan order by (first_col1)
:PREFIX
SELECT * FROM test_nosegby ORDER BY segby;

set timescaledb.debug_require_batch_sorted_merge to 'forbid';

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test_nosegby ORDER BY segby, time DESC NULLS LAST;

-- Should not be optimized (NULL order wrong)
:PREFIX
SELECT * FROM test_nosegby ORDER BY segby DESC NULLS LAST;

-- Test for correct results on table with overlapping batches
CREATE TABLE t2(time int NOT NULL, dev int NOT NULL, v int);
SELECT table_name FROM create_hypertable('t2', 'time', chunk_time_interval => 10000);
ALTER TABLE t2 SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='dev');

INSERT INTO t2 (time, dev, v) values
(1, 1, 20),
(1, 2, 300),
(2, 3, 3000),
(3, 1, 40),
(4, 1, 10),
(4, 2, 100),
(6, 3, 2000),
(6, 1, 10),
(6, 2, 300),
(8, 1, 60),
(8, 2, 100),
(8, 3, 7000);

INSERT INTO t2 (time, dev, v) values
(3, 1, 20),
(3, 2, 300),
(3, 3, 3000),
(5, 1, 40),
(5, 2, 100),
(5, 3, 1000),
(6, 1, 2000),
(6, 1, 10),
(6, 1, 200),
(7, 1, 60),
(7, 2, 100),
(7, 3, 7000);

INSERT INTO t2 (time, dev, v) values
(5, 1, 20),
(5, 3, 3000),
(5, 2, 300),
(7, 2, 400),
(8, 1, 10),
(8, 2, 100),
(8, 3, 2000),
(10, 1, 10),
(11, 1, 300),
(14, 1, 60),
(14, 2, 100),
(14, 3, 7000);

INSERT INTO t2 (time, dev, v) values
(9, 1, 20),
(9, 3, 300),
(9, 2, 30),
(9, 2, 40),
(10, 1, 10),
(10, 2, 100),
(10, 3, 2000),
(11, 1, 10),
(12, 1, 300),
(13, 1, 60),
(13, 2, 100),
(13, 3, 7000);

SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('t2') chunk;

SET timescaledb.debug_require_batch_sorted_merge = 'force';

:PREFIX
SELECT dev, time FROM t2 WHERE dev = 1 ORDER BY time DESC;
SELECT dev, time FROM t2 WHERE dev = 1 ORDER BY time DESC;

-- Can't use compressed indexscan order if orderby direction does not match:
-- we will need to sort on "last" metadata column instead of "first" in this case.
:PREFIX
SELECT dev, time FROM t2 WHERE dev = 1 ORDER BY time;
SELECT dev, time FROM t2 WHERE dev = 1 ORDER BY time;

-- Predicates on non-segmentby columns
:PREFIX
SELECT dev, time FROM t2 WHERE time > 8 AND dev=1 ORDER BY dev, time DESC;
SELECT dev, time FROM t2 WHERE time > 8 AND dev=1 ORDER BY dev, time DESC;

:PREFIX
SELECT dev, time, v FROM t2 WHERE v = 20 AND dev=1 ORDER BY dev, time DESC;
SELECT dev, time, v FROM t2 WHERE v = 20 AND dev=1 ORDER BY dev, time DESC;

-- Rescan with lateral subquery
:PREFIX
SELECT dev, time
FROM (VALUES (1), (2), (3)) a(dv),
     LATERAL (SELECT dev, time FROM t2 WHERE dev = a.dv ORDER BY time DESC) b;
SELECT dev, time
FROM (VALUES (1), (2), (3)) a(dv),
     LATERAL (SELECT dev, time FROM t2 WHERE dev = a.dv ORDER BY time DESC) b;

-- Tests for BatchSortedMerge over multi-segment data
--------------------------------------
-- Should be optimized (segmentby + orderby matches)
:PREFIX
SELECT * FROM test_segby ORDER BY segby, time DESC;

-- matches (segmentby + orderby) order
:PREFIX
SELECT dev, time FROM t2 ORDER BY dev, time DESC;
SELECT dev, time FROM t2 ORDER BY dev, time DESC;

-- matches (segmentby + orderby) reverse order
:PREFIX
SELECT dev, time FROM t2 ORDER BY dev DESC, time;
SELECT dev, time FROM t2 ORDER BY dev DESC, time;

-- Predicates on segmentby, orderby and other columns are dealt with correctly
SELECT dev, time FROM t2 WHERE dev > 1 ORDER BY dev, time DESC;

SELECT dev, time FROM t2 WHERE time > 8 ORDER BY dev, time DESC;

SELECT dev, time, v FROM t2 WHERE v = 20 ORDER BY dev, time DESC;

drop table test1 cascade;
drop table test2 cascade;
drop table test_segby cascade;
drop table test_nosegby cascade;
drop table t2 cascade;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.debug_require_batch_sorted_merge;

RESET enable_seqscan;
RESET enable_bitmapscan;
