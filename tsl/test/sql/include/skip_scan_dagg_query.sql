-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- canary for result diff
SELECT current_setting('timescaledb.enable_skipscan_for_distinct_aggregates') AS enable_dagg_skipscan;

-- test different index configurations
-- no index so we cant do SkipScan
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;

-- NULLS LAST index on dev
CREATE INDEX skip_scan_idx_dev_nulls_last ON :TABLE(dev);
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev ORDER BY dev DESC;

DROP INDEX skip_scan_idx_dev_nulls_last;

-- NULLS FIRST index on dev
CREATE INDEX skip_scan_idx_dev_nulls_first ON :TABLE(dev NULLS FIRST);
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev ORDER BY dev NULLS FIRST;
DROP INDEX skip_scan_idx_dev_nulls_first;

-- multicolumn index with dev as leading column
CREATE INDEX skip_scan_idx_dev_time_idx ON :TABLE(dev, time);
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev ORDER BY dev DESC;
DROP INDEX skip_scan_idx_dev_time_idx;

-- multicolumn index with dev as non-leading column
CREATE INDEX skip_scan_idx_time_dev_idx ON :TABLE(time, dev);
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE time = 100;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE time = 100 GROUP BY dev;
DROP INDEX skip_scan_idx_time_dev_idx;

-- hash index is not ordered so can't use skipscan
CREATE INDEX skip_scan_idx_hash ON :TABLE USING hash(dev_name);
:PREFIX SELECT count(DISTINCT dev_name) FROM :TABLE WHERE dev_name IN ('device_1','device_2');
DROP INDEX skip_scan_idx_hash;

-- expression indexes
-- currently not supported by skipscan
CREATE INDEX skip_scan_expr_idx ON :TABLE((dev % 3));
:PREFIX SELECT count(DISTINCT dev%3) FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev%3), dev%3 FROM :TABLE GROUP BY dev%3 ORDER BY dev%3;
DROP INDEX skip_scan_expr_idx;

CREATE INDEX ON :TABLE(dev_name);
CREATE INDEX ON :TABLE(dev);
CREATE INDEX ON :TABLE(dev, time);
CREATE INDEX ON :TABLE(time,dev);
CREATE INDEX ON :TABLE(time,dev,val);

\qecho basic DISTINCT queries on :TABLE
-- Various distint aggs over same column is OK
:PREFIX SELECT count(DISTINCT dev), sum(DISTINCT dev), 'q1_1' FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev_name), max(DISTINCT dev_name), 'q1_2' FROM :TABLE;
-- Distinct agg over Const is OK
:PREFIX SELECT count(DISTINCT dev), count(DISTINCT 2), 'q1_3', NULL FROM :TABLE;
-- DISTINCT over distinct agg is OK
:PREFIX SELECT DISTINCT count(DISTINCT dev), dev, 'q1_4' FROM :TABLE GROUP BY dev ORDER BY dev;

\qecho stable expression in targetlist on :TABLE
:PREFIX SELECT count(DISTINCT dev), 'q1_5', length(md5(now()::text)) FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev_name), 'q1_6', length(md5(now()::text)) FROM :TABLE;

-- volatile expression in targetlist
:PREFIX SELECT count(DISTINCT dev), 'q1_7', length(md5(random()::text)) FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev_name), 'q1_8', length(md5(random()::text)) FROM :TABLE;

-- Mix of aggregates on different columns and distinct/not distinct
-- currently not supported by skipscan
:PREFIX SELECT count(DISTINCT dev), max(DISTINCT dev_name), 'q1_9' FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev), sum(dev), 'q1_10' FROM :TABLE;
:PREFIX SELECT count(DISTINCT dev), dev_name, 'q1_11' FROM :TABLE GROUP BY dev_name ORDER BY dev_name;

-- distinct on expressions not supported
:PREFIX SELECT count(DISTINCT time_bucket(10,time)), 'q1_12' FROM :TABLE;
:PREFIX SELECT count(DISTINCT length(dev_name)), 'q1_13' FROM :TABLE;
:PREFIX SELECT count(DISTINCT 3*time), 'q1_14' FROM :TABLE;

-- But expressions over distinct aggregates are supported
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

-- Cannot do SkipScan as we group on a column which is not the distinct agg argument
:PREFIX SELECT time, count(DISTINCT dev), 'q2_10' FROM :TABLE GROUP BY time;
-- Cannot do SkipScan if we group on 2+ columns
:PREFIX SELECT count(DISTINCT dev), dev, tableoid::regclass, 'q2_11' FROM :TABLE GROUP BY dev, tableoid ORDER BY dev, tableoid;

-- DISTINCT aggs grouped on their TEXT args
:PREFIX SELECT count(DISTINCT dev_name), dev_name, 'q3_1' FROM :TABLE GROUP BY dev_name;
:PREFIX SELECT count(DISTINCT dev_name), dev_name, 'q3_2', NULL FROM :TABLE GROUP BY dev_name;
:PREFIX SELECT count(DISTINCT dev_name), dev_name, 'q3_3', length(md5(now()::text)) FROM :TABLE GROUP BY dev_name;
:PREFIX SELECT count(DISTINCT dev_name), dev_name, 'q3_4', length(md5(random()::text)) FROM :TABLE GROUP BY dev_name;
:PREFIX SELECT count(DISTINCT dev_name::varchar), dev_name::varchar, 'q3_5' FROM :TABLE GROUP BY dev_name;
:PREFIX SELECT count(DISTINCT dev_name), dev_name, int_func_immutable(), 'q3_6' FROM :TABLE GROUP BY dev_name;
:PREFIX SELECT count(DISTINCT dev_name), dev_name, int_func_stable(), 'q3_7' FROM :TABLE GROUP BY dev_name;
:PREFIX SELECT count(DISTINCT dev_name), dev_name, int_func_volatile(), 'q3_8' FROM :TABLE GROUP BY dev_name;

-- Cannot do SkipScan as we group on a column which is not the distinct agg argument
:PREFIX SELECT count(DISTINCT dev_name), dev_name, tableoid::regclass, 'q3_9' FROM :TABLE GROUP BY dev_name, tableoid ORDER BY dev_name, tableoid;

-- Can do SkipScan if extra group column is eliminated by pinning to a Const
-- and when it changes group by ordering
:PREFIX SELECT count(DISTINCT dev_name), dev, dev_name FROM :TABLE WHERE dev = 1 GROUP BY dev, dev_name ORDER BY dev, dev_name;

\qecho LIMIT queries on :TABLE
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev LIMIT 3;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE GROUP BY dev ORDER BY dev DESC LIMIT 3;

\qecho range queries on :TABLE
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE time BETWEEN 100 AND 300 GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE time < 200 GROUP BY dev;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE time > 800 GROUP BY dev;

\qecho ordered append on :TABLE
:PREFIX SELECT count(DISTINCT time), time FROM :TABLE WHERE time BETWEEN 0 AND 5000  GROUP BY time;

\qecho SUBSELECTS on :TABLE
:PREFIX SELECT c1, c2, 'q4_1' FROM (SELECT count(DISTINCT dev) as c1, sum(DISTINCT dev) as c2 FROM :TABLE) a;
:PREFIX SELECT NULL, dev, NULL, 'q4_2' FROM (SELECT count(DISTINCT dev) as dev FROM :TABLE) a;
:PREFIX SELECT NULL, dev, NULL, c1, 2, c3, 'q4_3' FROM (SELECT count(DISTINCT dev) as c1, dev, 1 as c3 FROM :TABLE GROUP BY dev) a;

\qecho ORDER BY
:PREFIX SELECT c, dev, 'q5_1' FROM (SELECT count(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev ORDER BY dev) a;
:PREFIX SELECT c, dev, 'q5_2' FROM (SELECT count(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev ORDER BY dev DESC) a;

\qecho WHERE CLAUSES
:PREFIX SELECT c, dev, 'q6_1' FROM (SELECT count(DISTINCT dev) as c, dev FROM :TABLE WHERE dev > 5 GROUP BY dev) a;
:PREFIX SELECT c, dev, 'q6_2' FROM (SELECT sum(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev HAVING sum(DISTINCT dev)>2) a;
:PREFIX SELECT c, dev, 'q6_3' FROM (SELECT count(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev) a WHERE dev > 5;
:PREFIX SELECT c, dev, 'q6_4' FROM (SELECT sum(DISTINCT dev) as c, dev FROM :TABLE GROUP BY dev) a WHERE c > 2;

--\qecho immutable func in WHERE clause on :TABLE
:PREFIX SELECT count(DISTINCT dev), 'q6_5' FROM :TABLE WHERE dev > int_func_immutable();
--\qecho stable func in WHERE clause on :TABLE
:PREFIX SELECT count(DISTINCT dev), 'q6_6' FROM :TABLE WHERE dev > int_func_stable();
--\qecho volatile func in WHERE clause on :TABLE:PREFIX SELECT count(DISTINCT dev), 'q6_7' FROM :TABLE WHERE dev > int_func_volatile();
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
:PREFIX SELECT count(DISTINCT dev_name) FROM :TABLE WHERE dev_name IS NULL;

-- Distinct aggregate path with no pathkeys because of Const predicate.
-- PG is smart to add LIMIT 1 to SELECT DISTINCT in this case,
-- but not smart enough to add LIMIT 1 to distinct aggregate input.
-- TODO: create an issue for this task, i.e. add LIMIT 1 to distinct aggregate input when input equals to a Const
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE WHERE dev = 1;
:PREFIX SELECT count(DISTINCT dev), dev FROM :TABLE WHERE dev = 1 GROUP BY dev ORDER BY dev DESC;

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
PREPARE prep AS SELECT count(DISTINCT dev_name) FROM :TABLE;
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

--  DISTINCT aggs on different columns in different subqueries
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE UNION ALL SELECT count(DISTINCT time) FROM :TABLE;

:PREFIX SELECT *, 'q9_2' FROM (SELECT count(DISTINCT dev) cd, dev FROM :TABLE GROUP BY dev) a, LATERAL (SELECT count(DISTINCT time) ct FROM :TABLE WHERE dev = a.dev) b;

-- SkipScan into INSERT
:PREFIX INSERT INTO skip_scan_insert(dev, val, query) SELECT dev, sd, 'q10_1' FROM (SELECT sum(DISTINCT dev) sd, dev FROM :TABLE GROUP BY dev) a;

-- parallel query
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'on', false);
:PREFIX SELECT count(DISTINCT dev) FROM :TABLE;
SELECT set_config(CASE WHEN current_setting('server_version_num')::int < 160000 THEN 'force_parallel_mode' ELSE 'debug_parallel_query' END,'off', false);

TRUNCATE skip_scan_insert;

-- table with only nulls
:PREFIX SELECT count(DISTINCT time) FROM skip_scan_nulls;

-- no tuples in resultset
:PREFIX SELECT count(DISTINCT time) FROM skip_scan_nulls WHERE time IS NOT NULL;

