-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/shared/results/%s_results_unmemoized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNMEMOIZED",
       format('%s/shared/results/%s_results_memoized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_MEMOIZED"
\gset
SELECT format('\! diff -u --label "Unmemoized results" --label "Memoized results" %s %s', :'TEST_RESULTS_UNMEMOIZED', :'TEST_RESULTS_MEMOIZED') as "DIFF_CMD"
\gset

-- get EXPLAIN output for all variations
\set PREFIX 'EXPLAIN (analyze, costs off, timing off, summary off)'

SET work_mem TO '64MB';
SET enable_memoize TO on;

\set TEST_TABLE 'metrics'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_compressed'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space_compressed'
\ir :TEST_QUERY_NAME

-- get results for all the queries
-- run queries with and without memoize
\set PREFIX ''
\set ECHO none
SET client_min_messages TO error;

SET enable_memoize TO on;

\set TEST_TABLE 'metrics'
\o :TEST_RESULTS_MEMOIZED
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_compressed'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space_compressed'
\ir :TEST_QUERY_NAME
\o

SET enable_memoize TO off;

\set TEST_TABLE 'metrics'
\o :TEST_RESULTS_UNMEMOIZED
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_compressed'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space_compressed'
\ir :TEST_QUERY_NAME
\o

-- diff memoized and unmemoized results
:DIFF_CMD
