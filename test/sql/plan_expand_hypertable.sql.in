-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set PREFIX 'EXPLAIN (costs off) '
\ir include/plan_expand_hypertable_load.sql
\ir include/plan_expand_hypertable_query.sql
\ir include/plan_expand_hypertable_chunks_in_query.sql

\set ECHO errors
\set TEST_BASE_NAME plan_expand_hypertable
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff -u  --label "Unoptimized result" --label "Optimized result" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') as "DIFF_CMD"
\gset

-- run queries with optimization on and off and diff results
\set PREFIX ''
\o :TEST_RESULTS_OPTIMIZED
SET timescaledb.enable_optimizations TO true;
\ir :TEST_QUERY_NAME
\o
\o :TEST_RESULTS_UNOPTIMIZED
SET timescaledb.enable_optimizations TO false;
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD
\set ECHO queries

\set PREFIX 'EXPLAIN (costs off)'
RESET timescaledb.enable_optimizations;

-- test enable_qual_propagation GUC
CREATE TABLE t(time timestamptz NOT NULL);
SELECT table_name FROM create_hypertable('t','time');
INSERT INTO t VALUES ('2000-01-01'), ('2010-01-01'), ('2020-01-01');

-- time constraint should be in both scans
:PREFIX SELECT * FROM t t1 INNER JOIN t t2 ON t1.time = t2.time WHERE t1.time < timestamptz '2010-01-01';

SET timescaledb.enable_qual_propagation TO false;
-- time constraint should only be in t1 scan
:PREFIX SELECT * FROM t t1 INNER JOIN t t2 ON t1.time = t2.time WHERE t1.time < timestamptz '2010-01-01';
RESET timescaledb.enable_qual_propagation;

-- test hypertable classification when hypertable is not in cache
-- https://github.com/timescale/timescaledb/issues/1832
CREATE TABLE test (a int, time timestamptz NOT NULL);
SELECT table_name FROM create_hypertable('public.test', 'time');
INSERT INTO test SELECT i, '2020-04-01'::date-10-i from generate_series(1,20) i;

CREATE OR REPLACE FUNCTION test_f(_ts timestamptz)
RETURNS SETOF test LANGUAGE SQL STABLE PARALLEL SAFE
AS $f$
   SELECT DISTINCT ON (a) * FROM test WHERE time >= _ts ORDER BY a, time DESC
$f$;

:PREFIX SELECT * FROM test_f(now());

-- create new session
\c

-- plan output should be identical to previous session
:PREFIX SELECT * FROM test_f(now());


-- test hypertable expansion in nested function calls
CREATE TABLE t1 (a int, b int NOT NULL);
SELECT create_hypertable('t1', 'b', chunk_time_interval=>10);

CREATE TABLE t2 (a int, b int NOT NULL);
SELECT create_hypertable('t2', 'b', chunk_time_interval=>10);

CREATE OR REPLACE FUNCTION f_t1(_a int, _b int)
 RETURNS SETOF t1
 LANGUAGE SQL
 STABLE PARALLEL SAFE
AS $function$
   SELECT DISTINCT ON (a) * FROM t1 WHERE a = _a and b = _b ORDER BY a, b DESC
$function$
;

CREATE OR REPLACE FUNCTION f_t2(_a int, _b int) RETURNS SETOF t2 LANGUAGE sql STABLE PARALLEL SAFE
AS $function$
   SELECT DISTINCT ON (j.a) j.*
   FROM
      f_t1(_a, _b) sc,
      t2 j
   WHERE
      j.b = _b AND
      j.a = _a
   ORDER BY j.a, j.b DESC
$function$
;

CREATE OR REPLACE FUNCTION f_t1_2(_b int) RETURNS SETOF t1 LANGUAGE SQL STABLE PARALLEL SAFE
AS $function$
   SELECT DISTINCT ON (j.a) jt.* FROM t1 j, f_t1(j.a, _b) jt
$function$;

:PREFIX SELECT * FROM f_t1_2(10);
:PREFIX SELECT * FROM f_t1_2(10) sc, f_t2(sc.a, 10);

\qecho '--TEST END--'
