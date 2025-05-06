-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- canary for result diff
SELECT current_setting('timescaledb.enable_skipscan') AS enable_skipscan;

-- test different index configurations
-- no index so we cant do SkipScan
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev;

-- NULLS LAST index on dev
CREATE INDEX skip_scan_idx_dev_nulls_last ON :TABLE(dev);
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev DESC;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE;
DROP INDEX skip_scan_idx_dev_nulls_last;

-- NULLS FIRST index on dev
CREATE INDEX skip_scan_idx_dev_nulls_first ON :TABLE(dev NULLS FIRST);
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev NULLS FIRST;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE ORDER BY dev NULLS FIRST;
DROP INDEX skip_scan_idx_dev_nulls_first;

-- multicolumn index with dev as leading column
CREATE INDEX skip_scan_idx_dev_time_idx ON :TABLE(dev, time);
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE ORDER BY dev DESC, time DESC;
DROP INDEX skip_scan_idx_dev_time_idx;

-- multicolumn index with dev as non-leading column
CREATE INDEX skip_scan_idx_time_dev_idx ON :TABLE(time, dev);
:PREFIX SELECT DISTINCT dev FROM :TABLE WHERE time = 100 ORDER BY dev;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE time = 100;
DROP INDEX skip_scan_idx_time_dev_idx;

-- hash index is not ordered so can't use skipscan
CREATE INDEX skip_scan_idx_hash ON :TABLE USING hash(dev_name);
:PREFIX SELECT DISTINCT dev_name FROM :TABLE WHERE dev_name IN ('device_1','device_2') ORDER BY dev_name;
DROP INDEX skip_scan_idx_hash;

-- expression indexes
-- currently not supported by skipscan
CREATE INDEX skip_scan_expr_idx ON :TABLE((dev % 3));
:PREFIX SELECT DISTINCT dev%3 FROM :TABLE ORDER BY dev%3;
:PREFIX SELECT DISTINCT ON (dev%3) dev FROM :TABLE ORDER BY dev%3;
DROP INDEX skip_scan_expr_idx;

CREATE INDEX ON :TABLE(dev_name);
CREATE INDEX ON :TABLE(dev);
CREATE INDEX ON :TABLE(dev, time);
CREATE INDEX ON :TABLE(time,dev);
CREATE INDEX ON :TABLE(time,dev,val);

\qecho basic DISTINCT queries on :TABLE
:PREFIX SELECT DISTINCT dev, 'q1_1' FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT dev_name, 'q1_2' FROM :TABLE ORDER BY dev_name;
:PREFIX SELECT DISTINCT dev, 'q1_3', NULL FROM :TABLE ORDER BY dev;

\qecho stable expression in targetlist on :TABLE
:PREFIX SELECT DISTINCT dev, 'q1_4', length(md5(now()::text)) FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT dev_name, 'q1_5', length(md5(now()::text)) FROM :TABLE ORDER BY dev_name;

-- volatile expression in targetlist
:PREFIX SELECT DISTINCT dev, 'q1_6', length(md5(random()::text)) FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT dev_name, 'q1_7', length(md5(random()::text)) FROM :TABLE ORDER BY dev_name;

-- queries without skipscan because distinct is not limited to specific column
:PREFIX SELECT DISTINCT * FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT *, 'q1_9' FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT dev, time, 'q1_10' FROM :TABLE ORDER BY dev;
:PREFIX SELECT DISTINCT dev, NULL, 'q1_11' FROM :TABLE ORDER BY dev;

-- distinct on expressions not supported
:PREFIX SELECT DISTINCT time_bucket(10,time), 'q1_12' FROM :TABLE;
:PREFIX SELECT DISTINCT length(dev_name), 'q1_13' FROM :TABLE;
:PREFIX SELECT DISTINCT 3*time, 'q1_14' FROM :TABLE;
:PREFIX SELECT DISTINCT 'Device ' || dev_name FROM :TABLE;

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
:PREFIX SELECT DISTINCT ON (dev) time, 'q2_10' FROM :TABLE ORDER by dev, time;
:PREFIX SELECT DISTINCT ON (dev) dev, tableoid::regclass, 'q2_11' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, int_func_immutable(), 'q2_12' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, int_func_stable(), 'q2_13' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev) dev, int_func_volatile(), 'q2_14' FROM :TABLE;

-- DISTINCT ON queries on TEXT column
:PREFIX SELECT DISTINCT ON (dev_name) dev_name FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev_name, 'q3_2' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev_name, 'q3_3', NULL FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev_name, 'q3_4', length(md5(now()::text)) FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev_name, 'q3_5', length(md5(random()::text)) FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) * FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) *, 'q3_7' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev_name, time, 'q3_8' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev_name, NULL, 'q3_9' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) time, 'q3_10' FROM :TABLE ORDER by dev_name, time;
:PREFIX SELECT DISTINCT ON (dev_name) dev_name, tableoid::regclass, 'q3_11' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name::varchar) dev_name::varchar FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev, int_func_immutable(), 'q3_13' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev, int_func_stable(), 'q3_14' FROM :TABLE;
:PREFIX SELECT DISTINCT ON (dev_name) dev, int_func_volatile(), 'q3_15' FROM :TABLE;

\qecho DISTINCT with wholerow var
:PREFIX SELECT DISTINCT ON (dev) :TABLE FROM :TABLE;
-- should not use SkipScan since we only support SkipScan on single-column distinct
:PREFIX SELECT DISTINCT :TABLE FROM :TABLE;

\qecho LIMIT queries on :TABLE
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE LIMIT 3;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE ORDER BY dev DESC, time DESC LIMIT 3;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE ORDER BY dev, time LIMIT 3;

\qecho range queries on :TABLE
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE time BETWEEN 100 AND 300;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE time < 200;
:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE time > 800;

\qecho ordered append on :TABLE
:PREFIX SELECT * FROM :TABLE ORDER BY time;
:PREFIX SELECT DISTINCT ON (time) time FROM :TABLE WHERE time BETWEEN 0 AND 5000;

\qecho SUBSELECTS on :TABLE
:PREFIX SELECT time, dev, val, 'q4_1' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE) a;
:PREFIX SELECT NULL, dev, NULL, 'q4_3' FROM (SELECT DISTINCT ON (dev) dev FROM :TABLE) a;
:PREFIX SELECT time, dev, NULL, 'q4_4' FROM (SELECT DISTINCT ON (dev) dev, time FROM :TABLE) a;

\qecho ORDER BY
:PREFIX SELECT time, dev, val, 'q5_1' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE ORDER BY dev, time) a;
:PREFIX SELECT time, dev, val, 'q5_2' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE ORDER BY dev DESC, time DESC) a;

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
-- always false expr similar to our initial skip qual
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev > NULL;
-- no tuples matching
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev > 20;
-- multiple constraints in WHERE clause
:PREFIX SELECT DISTINCT ON (dev) dev,time FROM :TABLE WHERE dev > 5 AND time = 100;
:PREFIX SELECT DISTINCT ON (dev) dev,time FROM :TABLE WHERE dev > 5 AND time > 200;
:PREFIX SELECT DISTINCT ON (dev) dev,time FROM :TABLE WHERE dev >= 5 AND dev < 7 AND dev >= 2;
:PREFIX SELECT DISTINCT ON (dev) dev,time,val FROM :TABLE WHERE time > 100 AND time < 200 AND val > 10 AND val < 10000 AND dev > 2 AND dev < 7 ORDER BY dev,time;

:PREFIX SELECT DISTINCT ON (dev) dev FROM :TABLE WHERE dev IS NULL;
:PREFIX SELECT DISTINCT ON (dev_name) dev_name FROM :TABLE WHERE dev_name IS NULL;

-- test constants in ORDER BY
:PREFIX SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev = 1 ORDER BY dev, time DESC;
:PREFIX SELECT DISTINCT dev, time FROM :TABLE WHERE dev = 1 and time = 100 ORDER BY dev, time DESC;
:PREFIX SELECT DISTINCT dev_name::varchar FROM :TABLE  WHERE dev_name::varchar = 'device_1' ORDER BY 1;

-- test distinct PathKey with sortref = 0 in PG15 due to FALSE filter not pushed into relation (should not crash in PG15)
:PREFIX SELECT DISTINCT sq.dev FROM (SELECT dev FROM :TABLE) sq JOIN :TABLE ref ON (sq.dev = ref.dev) WHERE 1 > 2;

-- test multiple distincts with all but one pinned: #7998
:PREFIX SELECT DISTINCT dev, dev FROM :TABLE ORDER BY 1;
:PREFIX SELECT DISTINCT dev, time FROM :TABLE WHERE time = 100 ORDER BY 1;
:PREFIX SELECT DISTINCT dev, time, val FROM :TABLE WHERE time = 100 and val = 100 ORDER BY 1;
:PREFIX SELECT DISTINCT ON (dev, time) * FROM :TABLE WHERE time = 100 ORDER BY dev;
:PREFIX SELECT DISTINCT ON (dev, time) dev, time FROM :TABLE WHERE dev = 1 AND time < 20 ORDER BY dev, time;

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
PREPARE prep AS SELECT DISTINCT ON (dev_name) dev_name FROM :TABLE;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- ReScan tests
:PREFIX SELECT time, dev, val, 'q7_1' FROM (SELECT DISTINCT ON (dev) * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT * FROM :TABLE WHERE time != a.v) b) a;

:PREFIX SELECT time, dev, val, 'q7_2' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev != a.v) b) a;

-- RuntimeKeys
:PREFIX SELECT time, dev, val, 'q8_1' FROM (SELECT * FROM (
    VALUES (1), (2)) a(v),
    LATERAL (SELECT DISTINCT ON (dev) * FROM :TABLE WHERE dev >= a.v) b) c;

-- Emulate multi-column DISTINCT using multiple SkipSkans
:PREFIX SELECT time, dev, val, 'q9_1' FROM (SELECT b.* FROM
    (SELECT DISTINCT ON (dev) dev FROM :TABLE) a,
    LATERAL (SELECT DISTINCT ON (time) * FROM :TABLE WHERE dev = a.dev) b) c;

:PREFIX SELECT time, dev, NULL, 'q9_2' FROM (SELECT b.* FROM
    (SELECT DISTINCT ON (dev) dev FROM :TABLE) a,
    LATERAL (SELECT DISTINCT ON (time) dev, time FROM :TABLE WHERE dev = a.dev) b) c;

-- Test that the multi-column DISTINCT emulation is equivalent to a real multi-column DISTINCT
:PREFIX SELECT * FROM
   (SELECT DISTINCT ON (dev) dev FROM :TABLE) a,
   LATERAL (SELECT DISTINCT ON (time) dev, time FROM :TABLE WHERE dev = a.dev) b;

:PREFIX SELECT DISTINCT ON (dev, time) dev, time FROM :TABLE WHERE dev IS NOT NULL;

:PREFIX SELECT DISTINCT ON (dev, time) dev, time FROM :TABLE WHERE dev IS NOT NULL
UNION SELECT b.* FROM
   (SELECT DISTINCT ON (dev) dev FROM :TABLE) a,
   LATERAL (SELECT DISTINCT ON (time) dev, time FROM :TABLE WHERE dev = a.dev) b;

-- SkipScan into INSERT
:PREFIX INSERT INTO skip_scan_insert(time, dev, val, query) SELECT time, dev, val, 'q10_1' FROM (SELECT DISTINCT ON (dev) * FROM :TABLE) a;

-- parallel query
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'on', false);
:PREFIX SELECT DISTINCT dev FROM :TABLE ORDER BY dev;
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'off', false);

TRUNCATE skip_scan_insert;

-- table with only nulls
:PREFIX SELECT DISTINCT ON (time) time FROM skip_scan_nulls;

-- no tuples in resultset
:PREFIX SELECT DISTINCT ON (time) time FROM skip_scan_nulls WHERE time IS NOT NULL;

