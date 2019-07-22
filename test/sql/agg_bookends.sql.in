-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set TEST_BASE_NAME agg_bookends
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff -u  --label "Unoptimized result" --label "Optimized result" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') as "DIFF_CMD"
\gset

-- look at postgres version to decide whether we run with analyze or without
SELECT
  CASE WHEN current_setting('server_version_num')::int >= 100000
    THEN 'EXPLAIN (analyze, costs off, timing off, summary off)'
    ELSE 'EXPLAIN (costs off)'
  END AS "PREFIX"
\gset

\ir :TEST_LOAD_NAME
\ir :TEST_QUERY_NAME

-- we want test results as part of the output too to make sure we produce correct output
\set PREFIX ''
\ir :TEST_QUERY_NAME

-- diff results with optimizations disabled and enabled
\o :TEST_RESULTS_UNOPTIMIZED
SET timescaledb.disable_optimizations TO true;
\ir :TEST_QUERY_NAME
\o

\o :TEST_RESULTS_OPTIMIZED
SET timescaledb.disable_optimizations TO false;
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD
