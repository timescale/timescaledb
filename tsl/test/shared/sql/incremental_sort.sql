-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT
       format('include/%s.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/shared/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED",
       format('%s/shared/results/%s_results_incremental_sort.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_INCREMENTAL_SORT",
       format('%s/shared/results/%s_results_ordered_append.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_ORDERED_APPEND"
\gset
SELECT format('\! diff -u --label "Unoptimized results" --label "Incremental Sort results" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_INCREMENTAL_SORT') as "DIFF_CMD"
\gset

-- get EXPLAIN output for all variations
\set PREFIX 'EXPLAIN (costs off, timing off, summary off)'
\set TEST_TABLE 'conditions'
\set UNOPTIMIZED_OUTPUT ''
\set INCR_SORT_OUTPUT ''
\set ORDERED_OUTPUT ''
\ir :TEST_QUERY_NAME

\set PREFIX ''
\set TEST_TABLE 'conditions'
\set UNOPTIMIZED_OUTPUT :TEST_RESULTS_UNOPTIMIZED
\set INCR_SORT_OUTPUT :TEST_RESULTS_INCREMENTAL_SORT
\set ORDERED_OUTPUT :TEST_RESULTS_ORDERED_APPEND
\ir :TEST_QUERY_NAME

-- diff results
:DIFF_CMD

