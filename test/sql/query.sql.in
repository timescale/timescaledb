-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set TEST_BASE_NAME query
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff -u  --label "Unoptimized result" --label "Optimized result" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') as "DIFF_CMD"
\gset

\set PREFIX 'EXPLAIN (costs OFF)'
\ir :TEST_LOAD_NAME
\ir :TEST_QUERY_NAME

--generate the results into two different files
\set ECHO errors
SET client_min_messages TO error;
--make output contain query results
\set PREFIX ''
\o :TEST_RESULTS_OPTIMIZED
SET timescaledb.disable_optimizations = 'off';
\ir :TEST_QUERY_NAME
\o
\o :TEST_RESULTS_UNOPTIMIZED
SET timescaledb.disable_optimizations = 'on';
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD
SELECT 'Done'
