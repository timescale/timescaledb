-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This file tests combinations of quals that we can or cannot push down.
--
-- We have these sparse index/columns that we can use to push down quals
-- to the compressed chunk scans:
--
--  - segmentby columns: hold a specific attribute value so all quals that
--                      may be used for the hypertable may be pushed down
--
--  - minmax indexes: hold the min and max values of the attribute values in
--                    the chunk, so range and equality quals may be pushed down
--
--  - bloom indexes: hold the bloom filter for the attribute values in the chunk,
--                    so equality quals may be pushed down
--
--  - composite bloom indexes: hold the composite bloom filter for multiple
--                              attribute values in the chunk, so equality quals
--                             that much all columns in the composite bloom may
--                             be pushed down
--
-- Furthermore, we may be able to push down partial expressions from the full
-- qual, if the partial expression is not more restrictive that the full qual.
-- Supporting the partial expressions may happen when one qual cannot satisfy
-- the above pushdown constraints.
--
-- The rules to decide if the partial expression is not more restrictive
-- than the original are as follows with respect to conjunctions and disjunctions:
--
--  - R1: we can omit any qual from a conjunction. i.e given a qual like:
--     a = 1 AND b = 2 AND c = 3, but we cannot push down b = 2, we can push down
--     a = 1 AND c = 3.
--
--  - R2: we cannot omit any qual from a disjunction. i.e given a qual like:
--     a = 1 OR b = 2 OR c = 3, but we cannot push down b = 2, we cannot push down
--     a = 1 OR c = 3.
--
--  - R3: we can relax the qual within a disjunction. i.e given a qual like:
--     (a = 1 AND b = 2) OR (c = 3 AND d = 4), but we cannot push down b = 2, we
--     can push down (a = 1) OR (c = 3 AND d = 4).
--
--  - R4: when we relax the qual within a disjunction, we cannot eliminate all quals
--    such that the entire disjunction is eliminated. i.e given a qual like:
--     (a = 1 AND b = 2) OR (c = 3 AND d = 4), but we cannot push down b = 2, and
--     we cannot push down a = 1, we cannot push down anything.
--

-- disable hash pushdown so we don't include the hash values in the plans
-- which makes the test stable. the hash pushdown is tested separately
set timescaledb.enable_bloom1_hash_pushdown = false;

DROP TABLE IF EXISTS complex_pushdown;
CREATE TABLE complex_pushdown(
    ts int,
    segby int,
    mm1 int,
    mm2 int,
    blm1 int,
    blm2 int,
    other int
)
with (
    tsdb.hypertable,
    tsdb.compress,
    tsdb.partition_column = 'ts',
    tsdb.segmentby = 'segby',
    tsdb.orderby = 'ts, mm1, mm2',
    tsdb.index = 'bloom(blm1), bloom(blm2), bloom(blm2, other)'
);

INSERT INTO complex_pushdown
SELECT x as ts, x % 10 as segby, x as mm1, x as mm2, x as blm1, x as blm2, x as other
FROM generate_series(1, 10000) x;

VACUUM ANALYZE complex_pushdown;

select count(compress_chunk(x)) from show_chunks('complex_pushdown') x;

-- combine sparse indices
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
segby = 1 AND mm1 = 1 AND blm1 = 1;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
segby = 1 OR mm1 = 1 OR blm1 = 1;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
ts < 5 OR ts > 100;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
ts != 5 OR ts != 100;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
mm1 != 5 OR blm1 != 100;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
((segby = 1 OR mm1 = 1) AND blm1 = 1) OR blm2 = 10;

-- R1: we can omit any qual from a conjunction
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
segby = 1 AND mm1 % 10 = 1 AND blm1 = 1;

-- R1: composite bloom index and another to omit
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
mm1 % 10 = 1 AND blm2 = 10 AND other = 11 AND blm1 = 12;

-- R2: we cannot omit any qual from a disjunction, so segby cannot be pushed down
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
segby = 1 OR mm1 % 10 = 1;

-- R3: base case combining conjunctions and disjunctions
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(segby = 1 AND mm1 = 1) OR (blm1 = 1 AND blm2 = 10);

-- R3: base case with composite bloom index
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(segby = 1 AND mm1 = 1) OR (other = 1 AND blm2 = 10);

-- R3: we can relax the qual within a disjunction. segby and blm2 should be
-- pushed down, but blm1 and mm1 should not
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(segby = 1 AND mm1 % 10 = 1) OR (blm1 % 10 = 1 AND blm2 = 10);

-- R3: another variation
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(segby = 1 AND other % 10 = 1) OR (blm1 = 10 AND other % 10 = 2);

-- R4: when we relax the qual within a disjunction, we cannot eliminate all quals
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(segby = 1 AND mm1 % 10 = 1) OR (blm1 % 10 = 1);

-- simple composite bloom verification
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm2 = 10 AND other = 11;

-- another variation for the composite bloom, different order
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
other = 11 AND blm2 = 10;

-- partial composite bloom cannot be pushed down
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE other = 11;

-- composite bloom index split across OR arms
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(segby = 1 AND mm1 = 10) OR (blm2 = 10 AND other = 11);

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(segby = 1 AND blm2 = 10) OR (mm1 = 1 AND other = 11);

-- composite bloom index used in an equality qual
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(blm2 = 10) = (other = 11);

-- between qual
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
mm1 BETWEEN 5 AND 10;

-- composite IS NULL qual
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm2 IS NULL AND other IS NULL;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm2 IS NULL AND other = 12;

-- Few test cases below that involve
-- segmentby and bloom columns
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm1 = segby;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm2 = segby and blm1 = other;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm2 = segby and other in (1,2,3) and blm1 = other;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm2 = segby and other in (1,2,3);

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm2 = segby and other in (1,2,3) and blm1 = segby;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm2 = segby and other in (1,2,3) or blm1 = segby;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
(blm1 = segby or blm2 = segby) and other in (1,2,3) and blm1 = other;

-- Edge case to make sure blm1 = blm2 doesn't
-- blow up
explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm1 = blm2;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm1 = blm2 AND blm1 = segby;

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm1 = blm2 and other in (1,2,3);

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm1 = blm2 or other in (1,2,3);

explain (buffers off, costs off)
SELECT * FROM complex_pushdown WHERE
blm1 = blm2 and segby in (1,2,3);

