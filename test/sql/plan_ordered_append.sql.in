-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- we run these with analyze to confirm that nodes that are not
-- needed to fulfill the limit are not executed
-- unfortunately this doesn't work on PostgreSQL 9.6 which lacks
-- the ability to turn off analyze timing summary so we run
-- them without ANALYZE on PostgreSQL 9.6, but since LATERAL plans
-- are different across versions we need version specific output
-- here anyway.

\set TEST_BASE_NAME plan_ordered_append
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

\set PREFIX_NO_ANALYZE 'EXPLAIN (costs off)'

\ir :TEST_LOAD_NAME
\ir :TEST_QUERY_NAME

--generate the results into two different files
\set ECHO errors
--make output contain query results
\set PREFIX ''
\set PREFIX_NO_ANALYZE ''
\o :TEST_RESULTS_OPTIMIZED
SET timescaledb.ordered_append = 'on';
\ir :TEST_QUERY_NAME
\o
\o :TEST_RESULTS_UNOPTIMIZED
SET timescaledb.ordered_append = 'off';
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD
