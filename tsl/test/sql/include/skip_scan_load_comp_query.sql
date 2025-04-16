-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- canary for result diff
SELECT current_setting('timescaledb.enable_compressed_skipscan') AS enable_compressed_skipscan;

-- To avoid ambiguity in test EXPLAIN outputs due to mixing of chunk plans with no SkipScan
SET max_parallel_workers_per_gather = 0;

-- To avoid choosing SeqScan under DecompressChunks before SkipScan can evaluate it,
-- as cost of scanning and sorting a small dataset can be less than scanning an index
-- TODO: address an issue of not preserving IndexPaths under DecompressChunks in "add_path"
SET enable_seqscan = false;

-- Run SkipScan queries on the provided compression layout
-- compressed index on "segmentby='dev'" has default "dev ASC, NULLS LAST" sort order

-- SkipScan used when sort order on distinct column "dev" matches "segmentby" sort order
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev DESC;
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev DESC NULLS FIRST;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE;

-- multicolumn sort with dev as leading column matching (segmentby dev, order by time DESC) compression index
:PREFIX SELECT DISTINCT ON (dev) dev, time FROM :TABLE ORDER BY dev, time DESC;
:PREFIX SELECT DISTINCT ON (dev) dev, time FROM :TABLE ORDER BY dev DESC, time;

\qecho stable expression in targetlist on :TABLE
:PREFIX SELECT DISTINCT dev, 'q1_4', length(md5(now()::text)) FROM :TABLE ORDER BY dev;

-- DISTINCT ON queries
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, 'q2_2' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, 'q2_3', NULL FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, 'q2_4', length(md5(now()::text)) FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, 'q2_5', length(md5(random()::text)) FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) *, 'q2_7' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, time, 'q2_8' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, NULL, 'q2_9' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) time, 'q2_10' FROM :TABLE ORDER by dev, time DESC;
:PREFIX SELECT DISTINCT ON (dev) dev, tableoid::regclass, 'q2_11' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, int_func_immutable(), 'q2_12' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, int_func_stable(), 'q2_13' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, int_func_volatile(), 'q2_14' FROM :TABLE;

\qecho DISTINCT with wholerow var
:PREFIX SELECT DISTINCT ON (dev) :TABLE FROM :TABLE;

\qecho LIMIT queries on :TABLE
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE LIMIT 3;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE ORDER BY dev DESC, time LIMIT 3;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE ORDER BY dev, time DESC LIMIT 3;

\qecho range queries on :TABLE
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE time BETWEEN 100 AND 300;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE time < 200;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE time > 800;

-- Various tests for "segmentby = 'dev'"
---------------------------------------
\qecho SUBSELECTS on :TABLE
:PREFIX SELECT time, dev, val, 'q4_1' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE) a;
:PREFIX SELECT NULL, dev, NULL, 'q4_3' FROM (SELECT DISTINCT ON (dev) dev FROM :TABLE) a;
:PREFIX SELECT time, dev, NULL, 'q4_4' FROM (SELECT DISTINCT ON (dev) dev, time FROM :TABLE) a;

\qecho ORDER BY
:PREFIX SELECT time, dev, val, 'q5_1' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE ORDER BY dev, time DESC) a;
:PREFIX SELECT time, dev, val, 'q5_2' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE ORDER BY dev DESC, time) a;

\qecho WHERE CLAUSES
:PREFIX SELECT time, dev, val, 'q6_1' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev > 5) a;
:PREFIX SELECT time, dev, val, 'q6_2' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE WHERE time > 5) a;
:PREFIX SELECT time, dev, val, 'q6_3' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE) a WHERE dev > 5;
:PREFIX SELECT time, dev, val, 'q6_4' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE) a WHERE time > 5;

--\qecho immutable func in WHERE clause on :TABLE
:PREFIX SELECT DISTINCT ON (dev) *, 'q6_5' FROM :TABLE WHERE dev > int_func_immutable();
--\qecho stable func in WHERE clause on :TABLE
:PREFIX SELECT DISTINCT ON (dev) *, 'q6_6' FROM :TABLE WHERE dev > int_func_stable();
--\qecho volatile func in WHERE clause on :TABLE
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev > int_func_volatile();
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev = ANY(inta_func_immutable());
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev = ANY(inta_func_stable());
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev = ANY(inta_func_volatile());
-- RowCompareExpr
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE (dev, time) > (5,100);

:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev > NULL;
-- no tuples matching
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev > 20;
-- multiple constraints in WHERE clause
:PREFIX SELECT DISTINCT ON (dev) dev,time FROM :TABLE WHERE dev > 5 AND time = 100;
:PREFIX SELECT DISTINCT ON (dev) dev,time FROM :TABLE WHERE dev > 5 AND time > 200;
:PREFIX SELECT DISTINCT ON (dev) dev,time FROM :TABLE WHERE dev >= 5 AND dev < 7 AND dev >= 2;
:PREFIX SELECT DISTINCT ON (dev) dev,time,val FROM :TABLE WHERE time > 100 AND time < 200 AND val > 10 AND val < 10000 AND dev > 2 AND dev < 7 ORDER BY dev,time DESC;

:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE dev IS NULL;

-- test constants in ORDER BY
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev = 1 ORDER BY dev, time DESC;

-- CTE
:PREFIX WITH devices AS (
	SELECT DISTINCT ON (dev) dev FROM :TABLE
)
SELECT * FROM devices;

:PREFIX WITH devices AS (
	SELECT DISTINCT dev FROM :TABLE
)
SELECT * FROM devices ORDER BY dev;

-- prepared statements
PREPARE prep AS SELECT DISTINCT ON (dev) dev FROM :TABLE;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- ReScan tests
-- have SkipScan as Distinct is over index
:PREFIX SELECT time, dev, val, 'q7_2' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev != a.v) b) a;

-- RuntimeKeys
:PREFIX SELECT time, dev, val, 'q8_1' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev >= a.v) b) c;

RESET max_parallel_workers_per_gather;
RESET enable_seqscan;
