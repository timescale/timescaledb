-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- DISTINCT on a regular PostgreSQL partitioned table (not a TimescaleDB
-- hypertable) must produce a correct plan. The SkipScan upper-paths hook
-- is invoked for every UPPERREL_DISTINCT, so get_distinct_var has to bail
-- out when the input rel is not a hypertable child.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE pg_part (a int, b int) PARTITION BY RANGE (a);
CREATE TABLE pg_part_p1 PARTITION OF pg_part FOR VALUES FROM (0) TO (100);
CREATE TABLE pg_part_p2 PARTITION OF pg_part FOR VALUES FROM (100) TO (200);
INSERT INTO pg_part SELECT i % 200, i FROM generate_series(0, 999) i;
CREATE INDEX ON pg_part_p1 (b);
CREATE INDEX ON pg_part_p2 (b);
ANALYZE pg_part;

SET enable_hashagg = off;
SET enable_seqscan = off;

-- A Var from a non-hypertable parent reaches get_distinct_var through the
-- MergeAppend subpath and is rejected; the resulting plan is plain Unique
-- over MergeAppend over per-partition Index Only Scan, with no SkipScan.
EXPLAIN (costs off) SELECT DISTINCT b FROM pg_part ORDER BY b;
SELECT count(*) FROM (SELECT DISTINCT b FROM pg_part ORDER BY b) s;

DROP TABLE pg_part;
