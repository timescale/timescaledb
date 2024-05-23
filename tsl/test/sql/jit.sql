-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set TEST_BASE_NAME jit
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('include/%s_cleanup.sql', :'TEST_BASE_NAME') as "TEST_CLEANUP_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff -u --label "Unoptimized results" --label "Optimized results" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') as "DIFF_CMD"
\gset

-- enable all jit optimizations
SET jit=on;
SET jit_above_cost=0;
SET jit_inline_above_cost=0;
SET jit_optimize_above_cost=0;
SET jit_tuple_deforming=on;

\ir :TEST_LOAD_NAME
\set PREFIX 'EXPLAIN (VERBOSE, TIMING OFF, COSTS OFF, SUMMARY OFF)'
\ir :TEST_QUERY_NAME

-- generate the results into two different files
\set ECHO errors
SET client_min_messages TO error;
\set PREFIX ''

-- get query results with jit enabled
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

-- disable jit optimizations and repeat
SET jit=off;

\o /dev/null
\ir :TEST_CLEANUP_NAME
\ir :TEST_LOAD_NAME
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o

-- compare jit vs non-jit output
:DIFF_CMD

\set ECHO queries
\qecho '--TEST END--'
