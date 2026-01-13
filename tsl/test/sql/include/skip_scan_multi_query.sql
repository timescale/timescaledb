-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- canary for result diff
SELECT current_setting('timescaledb.enable_multikey_skipscan') AS enable_multikey_skipscan;

-- multicolumn index with uniform order
:PREFIX SELECT DISTINCT ON(status, region, dev, dev_name) * FROM :TABLE ORDER BY status, region, dev, dev_name;
:PREFIX SELECT DISTINCT ON(region, dev) * FROM :TABLE WHERE status = 1 ORDER BY region DESC, dev DESC;

-- index with reverse column orders (applied to non-columnstore ony)
:PREFIX SELECT DISTINCT ON (status, region, dev) * FROM :TABLE ORDER BY status, region DESC, dev;
:PREFIX SELECT DISTINCT ON(region, dev) * FROM :TABLE WHERE status = 1 ORDER BY region, dev DESC;

set enable_seqscan=0;
-- (1,N) distinct scenario
:PREFIX SELECT DISTINCT status, dev FROM :TABLE WHERE time < 100 AND region = 'reg_1' ORDER BY 1,2;

-- (N,1) distinct scenario
:PREFIX SELECT DISTINCT dev, dev_name FROM :TABLE WHERE status = 1 AND region = 'reg_1' ORDER BY 1,2;

-- SELECT list order of distinct columns shouldn't matter if OBY matches index order
:PREFIX SELECT DISTINCT dev, status, dev_name FROM :TABLE WHERE region = 'reg_2' ORDER BY status, dev, dev_name;
reset enable_seqscan;

-- Basic queries
:PREFIX SELECT DISTINCT region, dev, 'q1_3', NULL FROM :TABLE WHERE status = 1 ORDER BY region, dev;

-- DISTINCT ON queries
:PREFIX SELECT DISTINCT ON (region, dev, dev_name) region, dev, dev_name, tableoid::regclass, 'q2_11' FROM :TABLE WHERE status = 1;
:PREFIX SELECT DISTINCT ON (region, dev, dev_name) region, dev, dev_name, int_func_immutable(), 'q2_12' FROM :TABLE WHERE status = 1;
:PREFIX SELECT DISTINCT ON (region, dev, dev_name) region, dev, dev_name, int_func_stable(), 'q2_13' FROM :TABLE WHERE status = 1;
:PREFIX SELECT DISTINCT ON (region, dev, dev_name) region, dev, dev_name, int_func_volatile(), 'q2_14' FROM :TABLE WHERE status = 1;

\qecho ordered append on :TABLE
:PREFIX SELECT DISTINCT ON (region, dev) region, dev FROM :TABLE WHERE status = 1 AND time BETWEEN 0 AND 5000;

\qecho SUBSELECTS on :TABLE
:PREFIX SELECT time, region, dev, val, 'q4_1' FROM (SELECT DISTINCT ON (region, dev) * FROM :TABLE WHERE status = 1) a;
:PREFIX SELECT NULL, region, dev, NULL, 'q4_3' FROM (SELECT DISTINCT ON (region, dev) region, dev FROM :TABLE WHERE status = 1) a;

\qecho ORDER BY
:PREFIX SELECT time, dev, val, 'q4_1' FROM (SELECT DISTINCT ON (region, dev) * FROM :TABLE WHERE status = 1 ORDER BY region, dev) a;

-- RowCompareExpr
:PREFIX SELECT DISTINCT ON (status, dev) * FROM :TABLE WHERE region = 'reg_1' and (status, dev) > (1,2);
-- always false expr similar to our initial skip qual
:PREFIX SELECT DISTINCT ON (status, region, dev) * FROM :TABLE WHERE dev > NULL and status > NULL and region > NULL;
-- no tuples matching
:PREFIX SELECT DISTINCT ON (status, region, dev) * FROM :TABLE WHERE dev > 20;

-- test constants in ORDER BY
:PREFIX SELECT DISTINCT ON (status, region, dev) * FROM :TABLE WHERE status = 1 ORDER BY status, region;

-- test distinct PathKey with sortref = 0 in PG15 due to FALSE filter not pushed into relation (should not crash in PG15)
:PREFIX SELECT DISTINCT sq.dev FROM (SELECT status, region, dev FROM :TABLE) sq JOIN :TABLE ref
ON (sq.dev = ref.dev) AND (sq.status = ref.status) AND (sq.region = ref.region) WHERE 1 > 2;

-- CTE
:PREFIX WITH devices AS (
	SELECT DISTINCT ON (region, dev) region, dev FROM :TABLE WHERE status = 1
)
SELECT * FROM devices;

:PREFIX WITH devices AS (
	SELECT DISTINCT ON (region, dev) region, dev FROM :TABLE WHERE status = 1
)
SELECT * FROM devices ORDER BY region, dev;

-- prepared statements
PREPARE prep AS SELECT DISTINCT ON (region, dev, dev_name) region, dev, dev_name FROM :TABLE WHERE status = 1;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- ReScan tests
:PREFIX SELECT time, status, region, dev, val, 'q7_1' FROM (SELECT DISTINCT ON (status, region, dev) * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT * FROM :TABLE WHERE time != a.v) b) a;

set enable_seqscan=0;
:PREFIX SELECT time, status, region, dev, val, 'q7_2' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (status, region, dev) * FROM :TABLE WHERE dev != a.v) b) a;

-- RuntimeKeys
:PREFIX SELECT time, status, region, dev, val, 'q8_1' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (status, region, dev) * FROM :TABLE WHERE dev >= a.v) b) c;
reset enable_seqscan;

-- parallel query
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'on', false);
:PREFIX SELECT DISTINCT status, region, dev FROM :TABLE ORDER BY status, region, dev;
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'off', false);
