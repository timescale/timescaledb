-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\set TEST_BASE_NAME data_fetcher
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_run.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_cursor.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_CURSOR",
       format('%s/results/%s_results_row_by_row.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_ROW_BY_ROW"
\gset
SELECT format('\! diff %s %s', :'TEST_RESULTS_CURSOR', :'TEST_RESULTS_ROW_BY_ROW') as "DIFF_CMD"
\gset

SET client_min_messages TO warning;
\ir :TEST_LOAD_NAME

\set ECHO errors
SET client_min_messages TO error;

-- run the queries using row by row fetcher
\o :TEST_RESULTS_ROW_BY_ROW
\ir :TEST_QUERY_NAME
\o
-- run queries using cursor fetcher
SET timescaledb.remote_data_fetcher = cursor;
\o :TEST_RESULTS_CURSOR
\ir :TEST_QUERY_NAME
\o
-- compare results
:DIFF_CMD

