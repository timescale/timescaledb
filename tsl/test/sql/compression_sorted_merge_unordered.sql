-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

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
    x3 integer,
    x4 integer,
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
    x3 integer,
    x4 integer,
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

-- Should be optimized
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS FIRST;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS LAST;

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
-- Should not be optimized
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS LAST;

-- Should not be optimized
:PREFIX
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS FIRST;


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

-- Test with null values
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS FIRST;
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS LAST;

set timescaledb.debug_require_batch_sorted_merge to 'forbid';
SELECT * FROM test_with_defined_null ORDER BY x2 ASC NULLS LAST;
SELECT * FROM test_with_defined_null ORDER BY x2 DESC NULLS FIRST;

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
-- Should show that BSM works even with segmentby
-- Using segmentby column for ordering should not use BSM or compressed index ordering

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

-- Should be optimized
:PREFIX
SELECT * FROM test2 ORDER BY time ASC;

-- Should be optimized (backward scan)
:PREFIX
SELECT * FROM test2 ORDER BY time DESC NULLS FIRST;

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

