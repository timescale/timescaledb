-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Each table drops one column before loading data and one after, so the by-name
-- column fetch hits attno gaps in both the catalog and the chunk layout.

-- uncompressed hypertable: 5 daily data chunks (4 rows each: device s, value s*10)
-- plus an empty chunk before and after the data range
CREATE TABLE metrics (time timestamptz NOT NULL, dropme1 int, device int, value float8, dropme2 int)
  WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.chunk_interval = '1 day');
ALTER TABLE metrics DROP COLUMN dropme1;
INSERT INTO metrics (time, device, value)
SELECT '2020-01-01'::timestamptz + d * interval '1 day' + s * interval '1 hour', s, s * 10
FROM generate_series(0, 4) d, generate_series(0, 3) s;
-- bracketing empty chunks: insert boundary rows, then delete them
INSERT INTO metrics (time, device, value) VALUES ('2019-12-30', 0, 0), ('2020-01-10', 0, 0);
DELETE FROM metrics WHERE time < '2020-01-01' OR time >= '2020-01-06';
ALTER TABLE metrics DROP COLUMN dropme2;

-- compressed hypertable, same data
CREATE TABLE metrics_compressed (time timestamptz NOT NULL, dropme1 int, device int, value float8, dropme2 int)
  WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.chunk_interval = '1 day',
        tsdb.segmentby = 'device', tsdb.orderby = 'time');
ALTER TABLE metrics_compressed DROP COLUMN dropme1;
INSERT INTO metrics_compressed (time, device, value) SELECT * FROM metrics;
ALTER TABLE metrics_compressed DROP COLUMN dropme2;
SET client_min_messages TO error; -- silence poor-compression-ratio warnings on tiny chunks
SELECT count(compress_chunk(c)) FROM show_chunks('metrics_compressed') c;
RESET client_min_messages;

-- space-partitioned hypertable
CREATE TABLE metrics_space (time timestamptz NOT NULL, dropme1 int, device int, value float8, dropme2 int)
  WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.chunk_interval = '1 day');
ALTER TABLE metrics_space DROP COLUMN dropme1;
SELECT add_dimension('metrics_space', 'device', number_partitions => 3);
INSERT INTO metrics_space (time, device, value) SELECT * FROM metrics;
ALTER TABLE metrics_space DROP COLUMN dropme2;

-- plain table for the join and not-a-hypertable cases
CREATE TABLE metrics_regular (time timestamptz, dropme1 int, x int, dropme2 int);
ALTER TABLE metrics_regular DROP COLUMN dropme1;
ALTER TABLE metrics_regular DROP COLUMN dropme2;

SET timescaledb.enable_deferredchunkscan TO on;

-- queries expected to use deferredchunkscan
SET timescaledb.debug_require_deferred_chunk_scan TO 'require';
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics ORDER BY time LIMIT 1) x;  -- ordered
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics LIMIT 1) x;  -- unordered
SELECT count(x) >= 0 AS ok FROM (SELECT value FROM metrics ORDER BY time LIMIT 5) x;  -- projection
SELECT count(x) >= 0 AS ok FROM (SELECT metrics FROM metrics LIMIT 1) x;  -- whole-row reference
SELECT count(x) >= 0 AS ok FROM (SELECT 1 FROM metrics LIMIT 1) x;  -- no column reference
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics ORDER BY time OFFSET 2 LIMIT 3) x;  -- offset
SELECT count(x) >= 0 AS ok FROM (SELECT time FROM metrics ORDER BY time DESC LIMIT 3) x;  -- descending
SELECT count(*) = 20 AS ok FROM (SELECT * FROM metrics ORDER BY time LIMIT 2000000000) x;  -- large limit batches, no oversized alloc
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics_compressed ORDER BY time LIMIT 1) x;  -- compressed
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics_space LIMIT 1) x;  -- space-partitioned, unordered
RESET timescaledb.debug_require_deferred_chunk_scan;

SET timescaledb.debug_require_deferred_chunk_scan TO 'forbid';

-- queries not currently supported with deferredchunkscan
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics) x;  -- no LIMIT
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics_regular LIMIT 1) x;  -- not a hypertable
SELECT count(x) >= 0 AS ok FROM (SELECT count(*) FROM metrics) x;  -- aggregate
SELECT count(x) >= 0 AS ok FROM (SELECT min(time) FROM metrics) x;  -- min/max rewrite
SELECT count(x) >= 0 AS ok FROM (SELECT max(time) FROM metrics) x;  -- min/max rewrite
SELECT count(x) >= 0 AS ok FROM (SELECT first(value, time) FROM metrics) x;  -- first/last

-- disqualifying query clause
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics WHERE device = 1 LIMIT 1) x;  -- WHERE qual
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics JOIN metrics_regular USING (time) LIMIT 1) x;  -- join
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics, metrics_regular LIMIT 1) x;  -- multiple from-list entries
SELECT count(x) >= 0 AS ok FROM (SELECT device, count(*) FROM metrics GROUP BY device LIMIT 1) x;  -- GROUP BY
SELECT count(x) >= 0 AS ok FROM (SELECT device, count(*) FROM metrics GROUP BY GROUPING SETS ((device), ()) LIMIT 1) x;  -- grouping sets
SELECT count(x) >= 0 AS ok FROM (SELECT count(*) FROM metrics HAVING count(*) > 0 LIMIT 1) x;  -- HAVING
SELECT count(x) >= 0 AS ok FROM (SELECT row_number() OVER () FROM metrics LIMIT 1) x;  -- window function
SELECT count(x) >= 0 AS ok FROM (SELECT DISTINCT device FROM metrics LIMIT 1) x;  -- DISTINCT
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics UNION ALL SELECT * FROM metrics LIMIT 1) x;  -- set operation

-- table or scan decoration
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM ONLY metrics LIMIT 1) x;  -- ONLY
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics TABLESAMPLE BERNOULLI (50) LIMIT 1) x;  -- TABLESAMPLE
SELECT count(x) >= 0 AS ok FROM (SELECT ctid FROM metrics ORDER BY time LIMIT 1) x;  -- system column (ctid)
SELECT count(x) >= 0 AS ok FROM (SELECT tableoid FROM metrics ORDER BY time LIMIT 1) x;  -- system column (tableoid)

-- an ordering or limit the node can't satisfy
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics ORDER BY time FETCH FIRST 1 ROW WITH TIES) x;  -- WITH TIES
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics ORDER BY time LIMIT 1 FOR UPDATE) x;  -- row mark
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics ORDER BY value LIMIT 1) x;  -- ORDER BY non-dimension
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics ORDER BY time, device LIMIT 1) x;  -- multi-column ORDER BY
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics_space ORDER BY time LIMIT 1) x;  -- ordered, space-partitioned

-- a modifying CTE can't sit in a sub-select, so run it at top level
WITH w AS (INSERT INTO metrics_regular VALUES (now(), 1) RETURNING x)
SELECT time FROM metrics ORDER BY time LIMIT 1;  -- modifying CTE
RESET timescaledb.debug_require_deferred_chunk_scan;

-- RLS attaches to the hypertable, not its chunks, so the node must stay off to
-- avoid bypassing the policy with a direct chunk scan. RLS is not allowed on a
-- columnstore hypertable, so build a plain one.
CREATE TABLE metrics_rls (time timestamptz NOT NULL, device int, value float8)
  WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.chunk_interval = '1 day',
        tsdb.columnstore = false);
INSERT INTO metrics_rls
SELECT '2020-01-01'::timestamptz + d * interval '1 day', s, s * 10
FROM generate_series(0, 4) d, generate_series(0, 3) s;
ALTER TABLE metrics_rls ENABLE ROW LEVEL SECURITY;
ALTER TABLE metrics_rls FORCE ROW LEVEL SECURITY; -- apply the policy to the owner too
CREATE POLICY device_one ON metrics_rls FOR SELECT USING (device = 1);

SET timescaledb.debug_require_deferred_chunk_scan TO 'forbid';
-- the node must stay off; the policy must filter to device = 1 (no leaked rows)
SELECT count(*) AS visible, count(*) FILTER (WHERE device <> 1) AS leaked
FROM (SELECT device FROM metrics_rls LIMIT 100) x;
RESET timescaledb.debug_require_deferred_chunk_scan;

-- Column privileges: the node scans chunks directly, so per-column SELECT grants
-- on the hypertable must still be enforced.
CREATE TABLE metrics_colpriv (time timestamptz NOT NULL, device int, value float8)
  WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.chunk_interval = '1 day',
        tsdb.columnstore = false);
INSERT INTO metrics_colpriv
SELECT '2020-01-01'::timestamptz + d * interval '1 day', s, s * 10
FROM generate_series(0, 4) d, generate_series(0, 3) s;
GRANT SELECT (time, device) ON metrics_colpriv TO :ROLE_DEFAULT_PERM_USER_2;

-- reconnect as the grantee (resets session GUCs, so set them again)
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2
SET timescaledb.enable_deferredchunkscan TO on;
-- granted columns: the node is used and returns rows
SET timescaledb.debug_require_deferred_chunk_scan TO 'require';
SELECT count(x) >= 0 AS ok FROM (SELECT device FROM metrics_colpriv ORDER BY time LIMIT 5) x;
RESET timescaledb.debug_require_deferred_chunk_scan;
-- ungranted column: denied at the hypertable before the node runs
\set ON_ERROR_STOP 0
SELECT value FROM metrics_colpriv ORDER BY time LIMIT 1;
\set ON_ERROR_STOP 1

-- back to the owning user for the remaining queries
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SET timescaledb.enable_deferredchunkscan TO on;

-- Feature off: node is never chosen, even for an accepted shape.
SET timescaledb.enable_deferredchunkscan TO off;
SET timescaledb.debug_require_deferred_chunk_scan TO 'forbid';
SELECT count(x) >= 0 AS ok FROM (SELECT * FROM metrics ORDER BY time LIMIT 1) x;
RESET timescaledb.debug_require_deferred_chunk_scan;
SET timescaledb.enable_deferredchunkscan TO on;

EXPLAIN (COSTS OFF) SELECT time FROM metrics ORDER BY time LIMIT 3;
EXPLAIN (COSTS OFF) SELECT time FROM metrics ORDER BY time DESC LIMIT 3;
EXPLAIN (COSTS OFF) SELECT * FROM metrics LIMIT 1;

-- test correctness by diffing optimized and unoptimized results
\set TEST_BASE_NAME deferredchunkscan
SELECT format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_UNOPTIMIZED" \gset
SELECT format('\! diff -u --label "Unoptimized results" --label "Optimized results" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') AS "DIFF_CMD" \gset

SET timescaledb.enable_deferredchunkscan TO on;
SET timescaledb.debug_require_deferred_chunk_scan TO 'require';
\ir :TEST_QUERY_NAME
RESET timescaledb.debug_require_deferred_chunk_scan;

-- Node output must match the expanded plan: run with the node on then off and diff.
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o
SET timescaledb.enable_deferredchunkscan TO off;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
SET timescaledb.enable_deferredchunkscan TO on;
:DIFF_CMD

