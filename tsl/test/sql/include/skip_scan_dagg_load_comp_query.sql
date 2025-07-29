-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- canary for result diff
SELECT current_setting('timescaledb.enable_compressed_skipscan') AS enable_compressed_skipscan;

-- To avoid ambiguity in test EXPLAIN outputs due to mixing of chunk plans with no SkipScan
SET max_parallel_workers_per_gather = 0;

-- Run SkipScan queries on the provided compression layout
-- compressed index on "segmentby='dev'" has default "dev ASC, NULLS LAST" sort order

-- SkipScan used when sort order on distinct column "dev" matches "segmentby" sort order
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev ORDER BY dev DESC;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev ORDER BY dev DESC NULLS FIRST;

\qecho basic DISTINCT queries on :TABLE
-- Various distint aggs over same column is OK
:PREFIX SELECT count(DISTINCT dev), sum(DISTINCT dev), 'q1_1' FROM :TABLE;
-- Distinct agg over Const is OK
:PREFIX SELECT count(DISTINCT dev), count(DISTINCT 2), 'q1_3', NULL FROM :TABLE;
-- DISTINCT over distinct agg is OK
:PREFIX SELECT DISTINCT count(DISTINCT dev), dev, 'q1_4' FROM :TABLE GROUP BY dev ORDER BY dev;

\qecho stable expression in targetlist on :TABLE
:PREFIX SELECT count(DISTINCT dev), 'q1_5', length(md5(now()::text)) FROM :TABLE;
-- volatile expression in targetlist
:PREFIX SELECT count(DISTINCT dev), 'q1_7', length(md5(random()::text)) FROM :TABLE;

-- expressions over distinct aggregates are supported
:PREFIX SELECT count(DISTINCT dev) + 1, sum(DISTINCT dev)/count(DISTINCT dev), 'q1_15' FROM :TABLE;

-- DISTINCT aggs grouped on their args
:PREFIX SELECT count(DISTINCT dev), dev, 'q2_1' FROM :TABLE GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev, 'q2_2', NULL FROM :TABLE GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev, 'q2_3', length(md5(now()::text)) FROM :TABLE GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev, 'q2_4', length(md5(random()::text)) FROM :TABLE GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev, int_func_immutable(), 'q2_5' FROM :TABLE GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev, int_func_stable(), 'q2_6' FROM :TABLE GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev, int_func_volatile(), 'q2_7' FROM :TABLE GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev+1, 'q2_8' FROM :TABLE GROUP BY dev ORDER BY 2;
:PREFIX SELECT count(DISTINCT dev), dev+1, dev+2, 'q2_9' FROM :TABLE GROUP BY dev, dev;

\qecho LIMIT queries on :TABLE
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev LIMIT 3;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev ORDER BY dev DESC LIMIT 3;

\qecho range queries on :TABLE
set enable_seqscan to off;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE time BETWEEN 100 AND 300 GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE time < 200 GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE time > 800 GROUP BY dev;
reset enable_seqscan;

-- Various tests for "segmentby = 'dev'"
---------------------------------------

\qecho SUBSELECTS on :TABLE
:PREFIX SELECT c1, c2, 'q4_1' FROM (SELECT count(DISTINCT dev) as c1, sum(DISTINCT dev) as c2 FROM :TABLE) a;
:PREFIX SELECT NULL, dev, NULL, 'q4_2' FROM (SELECT count(DISTINCT dev) as dev FROM :TABLE) a;
:PREFIX SELECT NULL, dev, NULL, c1, 2, c3, 'q4_3' FROM (SELECT count(DISTINCT dev) as c1, dev, 1 as c3 FROM :TABLE GROUP BY dev) a;

\qecho ORDER BY
:PREFIX SELECT c, dev, 'q5_1' FROM (SELECT count(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev ORDER BY dev) a;
:PREFIX SELECT c, dev, 'q5_2' FROM (SELECT count(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev ORDER BY dev DESC) a;


-- Force not choosing SeqScan under DecompressChunks before SkipScan can evaluate it,
-- as cost of scanning and sorting a small dataset can be less than scanning an index, including startup cost.
-- After the fix in #8056 we take startup costs into account,
-- but even startup costs for scanning and sorting can be smaller for seqscan if we also have a filter on the index columns.
-- So for below queries with filters on "dev" we still need to disable SeqScan to choose IndexScan+SkipScan.
SET enable_seqscan = false;

\qecho WHERE CLAUSES
:PREFIX SELECT c, dev, 'q6_1' FROM (SELECT count(DISTINCT dev) as c, dev FROM :TABLE WHERE dev > 5 GROUP BY dev) a;
:PREFIX SELECT c, dev, 'q6_2' FROM (SELECT sum(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev HAVING sum(DISTINCT dev)>2) a;
:PREFIX SELECT c, dev, 'q6_3' FROM (SELECT count(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev) a WHERE dev > 5;
:PREFIX SELECT c, dev, 'q6_4' FROM (SELECT sum(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev) a WHERE c > 2;

--\qecho immutable func in WHERE clause on :TABLE
:PREFIX SELECT count(DISTINCT dev), 'q6_5' FROM :TABLE WHERE dev > int_func_immutable();
--\qecho stable func in WHERE clause on :TABLE
:PREFIX SELECT count(DISTINCT dev), 'q6_6' FROM :TABLE WHERE dev > int_func_stable();
--\qecho volatile func in WHERE clause on :TABLE
:PREFIX SELECT count(DISTINCT dev), 'q6_7' FROM :TABLE WHERE dev > int_func_volatile();
:PREFIX SELECT count(DISTINCT dev), 'q6_8' FROM :TABLE WHERE dev = ANY(inta_func_immutable());
:PREFIX SELECT count(DISTINCT dev), 'q6_9' FROM :TABLE WHERE dev = ANY(inta_func_stable());
:PREFIX SELECT count(DISTINCT dev), 'q6_10' FROM :TABLE WHERE dev = ANY(inta_func_volatile());

-- always false expr similar to our initial skip qual
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev > NULL;
-- no tuples matching
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev > 20;
-- multiple constraints in WHERE clause
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev > 5 AND time = 100;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev > 5 AND time > 200;
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev >= 5 AND dev < 7 AND dev >= 2;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE time > 100 AND time < 200 AND val > 10 AND val < 10000 AND dev > 2 AND dev < 7 GROUP BY dev ORDER BY dev;

:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev IS NULL;

-- CTE
:PREFIX WITH devices AS (
	SELECT count(DISTINCT dev) FROM :TABLE
)
SELECT * FROM devices;

:PREFIX WITH devices AS (
	SELECT dev, count(DISTINCT dev) FROM :TABLE GROUP BY dev
)
SELECT * FROM devices ORDER BY dev;

-- prepared statements
PREPARE prep AS SELECT count(DISTINCT dev) FROM :TABLE;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- ReScan tests
:PREFIX SELECT c, 'q7_1' FROM (SELECT count(DISTINCT dev) c FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT * FROM :TABLE WHERE time != a.v) b) a;

:PREFIX SELECT c, 'q7_2' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT count(DISTINCT dev) c FROM :TABLE WHERE dev != a.v) b) a;

-- RuntimeKeys
:PREFIX SELECT c, 'q8_1' FROM (SELECT * FROM (VALUES (1), (2)) a(v), LATERAL (SELECT count(DISTINCT dev) c FROM :TABLE WHERE dev >= a.v) b) c;

RESET max_parallel_workers_per_gather;
RESET enable_seqscan;
