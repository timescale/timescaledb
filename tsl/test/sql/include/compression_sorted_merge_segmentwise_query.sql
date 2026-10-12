-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for BatchSortedMerge over multi-segment data
-----------------------------------------------------

-- canary for result diff
SELECT current_setting('timescaledb.enable_decompression_sorted_merge') AS enable_batch_sorted_merge;

-- t2_seg (segmentby='dev', orderby='time DESC) with unordered chunks

-- matches (segmentby + orderby) order
SELECT dev, time FROM t2_seg ORDER BY dev, time DESC;

-- matches (segmentby + orderby) reverse order
SELECT dev, time FROM t2_seg ORDER BY dev DESC, time;

-- Predicates on segmentby, orderby and other columns are dealt with correctly

-- index does not scan d=1
SELECT dev, time FROM t2_seg WHERE dev > 1 ORDER BY dev, time DESC;

-- filters out batches (1..8) and (3..7) for each segment
SELECT dev, time FROM t2_seg WHERE time > 8 ORDER BY dev, time DESC;

SELECT dev, time, v FROM t2_seg WHERE v = 30 ORDER BY dev, time DESC;

-- filters out dev=1
SELECT dev, time, v FROM t2_seg WHERE v = 20 OR v = 5 ORDER BY dev, time DESC;

-- Skips 1st batch of 2nd segment and goes to 3rd segment after 2nd batch of 2nd segment:
-- case flagged by LLM Fuzzer
SELECT dev, time FROM t2_seg WHERE v > 1 AND time > 8 AND dev IS NOT NULL ORDER BY dev, time DESC;

-- Rescan with lateral subquery
SELECT dev, time, v
FROM (VALUES (20), (30)) a(dv),
     LATERAL (SELECT dev, time, v FROM t2_seg WHERE v = a.dv ORDER BY dev, time DESC) b;

-- Tests with predicates for reverse order
-- index does not scan d=3
SELECT dev, time FROM t2_seg WHERE dev < 3 ORDER BY dev DESC, time;

-- filters out batch (9..13) for each segment
SELECT dev, time FROM t2_seg WHERE time < 9 ORDER BY dev DESC, time;

SELECT dev, time, v FROM t2_seg WHERE v = 30 ORDER BY dev DESC, time;

-- filters out dev=NULL and dev = 1
SELECT dev, time, v FROM t2_seg WHERE v = 20 ORDER BY dev DESC, time;

SELECT dev, time FROM t2_seg WHERE v < 30 AND time < 9 ORDER BY dev DESC, time;

-- Rescan with lateral subquery
SELECT dev, time, v
FROM (VALUES (20), (5)) a(dv),
     LATERAL (SELECT dev, time, v FROM t2_seg WHERE v = a.dv ORDER BY dev DESC, time) b;

-- t2_multi (segmentby='x1, x2', orderby='time DESC, x3') with unordered chunks

-- matches (segmentby + orderby) order, merges on segmentby key x2 + orderby keys
SELECT x2, time, x3 FROM t2_multi ORDER BY x2, time DESC, x3;

-- matches (segmentby + orderby) reverse order, merges on segmentby key x2 + orderby keys
SELECT x2, time, x3 FROM t2_multi ORDER BY x2 DESC, time, x3 DESC;

-- matches (segmentby + orderby), merges on segmentby key x2 + orderby time, can use compressed order
SELECT x1, x2, time FROM t2_multi ORDER BY x1, x2, time DESC;

-- matches (segmentby + orderby) in reverse, merges on segmentby key x2 + orderby time
SELECT x1, x2, time FROM t2_multi ORDER BY x1 DESC, x2 DESC, time;

-- Index quals over segmentby
SELECT x1, x2, time, x3 FROM t2_multi WHERE x1 > 1 ORDER BY x1, x2, time DESC, x3;
SELECT x1, x2, time, x3 FROM t2_multi WHERE x2 < '4' ORDER BY x1, x2, time DESC, x3;

-- Vector quals over orderby
SELECT x1, x2, time, x3 FROM t2_multi WHERE time < '2000-01-01 05:00:00-00' ORDER BY x1, x2, time DESC, x3;
SELECT x1, x2, time, x3 FROM t2_multi WHERE time < '2000-01-01 05:00:00-00' and x3 > 1 ORDER BY x1, x2, time DESC, x3;

-- Filter out segment (1,3)
SELECT x1, x2, time, x3, x4 FROM t2_multi WHERE x4 <> 20 ORDER BY x1, x2, time DESC, x3;
-- Filter out segment (1,NULL)
SELECT x1, x2, time, x3, x4 FROM t2_multi WHERE x4 > 10 ORDER BY x1, x2, time DESC, x3;

-- t2_multi_reg (segmentby='x1, x2', orderby='time DESC, x3') with regular chunks
-- Batch sorted merge can be done when pathkeys match orderby columns but do not fully match segmentby columns

-- merges on segmentby key x2 + orderby keys
SELECT x2, time, x3 FROM t2_multi_reg ORDER BY x2, time DESC, x3;

-- merges on segmentby key x2 + orderby keys in reverse
SELECT x2, time, x3 FROM t2_multi_reg ORDER BY x2 DESC, time, x3 DESC;

-- merges on segmentby key x2 + orderby time
SELECT x2, time FROM t2_multi_reg ORDER BY x2, time DESC;

-- merges on segmentby key x2 + orderby time in reverse
SELECT x2, time FROM t2_multi_reg ORDER BY x2, time;

-- matches orderby, merges on orderby time
SELECT time FROM t2_multi_reg ORDER BY time DESC;

-- Index quals over segmentby
-- matches (segmentby + orderby) order, merges on segmentby key x2 + orderby keys
SELECT x2, time, x3 FROM t2_multi_reg WHERE x1 > 1 ORDER BY x2, time DESC, x3;
SELECT x2, time, x3 FROM t2_multi_reg WHERE x2 < '4' ORDER BY x2, time DESC, x3;

-- Vector quals over orderby
SELECT x2, time, x3 FROM t2_multi_reg WHERE time < '2000-01-01 05:00:00-00' ORDER BY x2, time DESC, x3;
SELECT x2, time, x3 FROM t2_multi_reg WHERE time < '2000-01-01 05:00:00-00' and x3 > 1 ORDER BY x2, time DESC, x3;

-- Filter out segment (1,3)
SELECT x2, time, x3, x4 FROM t2_multi_reg WHERE x4 <> 20 ORDER BY x2, time DESC, x3;
-- Filter out segment (1,NULL)
SELECT x2, time, x3, x4 FROM t2_multi_reg WHERE x4 > 10 ORDER BY x2, time DESC, x3;
