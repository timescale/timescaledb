-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set ECHO errors
\set TEST_BASE_NAME plan_ordered_append
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff %s %s', :'TEST_RESULTS_OPTIMIZED', :'TEST_RESULTS_UNOPTIMIZED') as "DIFF_CMD"
\gset


\o /dev/null
SET client_min_messages = 'error';
\ir :TEST_LOAD_NAME
RESET client_min_messages;
\o

--generate the results into two different files
SET client_min_messages = 'error';
\set ECHO none
--make output contain query results
\set PREFIX ''
\o :TEST_RESULTS_OPTIMIZED
SET timescaledb.ordered_append = 'on';
\ir :TEST_QUERY_NAME
\o
\o :TEST_RESULTS_UNOPTIMIZED
SET timescaledb.ordered_append = 'off';
\ir :TEST_QUERY_NAME
\o
RESET client_min_messages;

:DIFF_CMD

SELECT 'Done';
