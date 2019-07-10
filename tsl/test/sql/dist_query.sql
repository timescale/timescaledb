-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

\set TEST_BASE_NAME dist_query
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_run.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED",
       format('%s/results/%s_results_repartitioned.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_REPARTITIONED"
\gset
SELECT format('\! diff %s %s', :'TEST_RESULTS_OPTIMIZED', :'TEST_RESULTS_UNOPTIMIZED') as "DIFF_CMD_1",
       format('\! diff %s %s', :'TEST_RESULTS_REPARTITIONED', :'TEST_RESULTS_UNOPTIMIZED') as "DIFF_CMD_2"
\gset

\set PREFIX 'EXPLAIN (verbose, costs off)'
\set TABLE_NAME 'hyper_repart'
SET client_min_messages TO warning;
\ir :TEST_LOAD_NAME

-- Run the EXPLAINs
SET enable_partitionwise_aggregate = ON;
\ir :TEST_QUERY_NAME

-- Run the queries for each setting. Each setting's result is
-- generated into its own file
\set ECHO errors
SET client_min_messages TO error;
--make output contain query results
\set PREFIX ''
\set TABLE_NAME 'hyper'
\o :TEST_RESULTS_UNOPTIMIZED
SET enable_partitionwise_aggregate = OFF;
\ir :TEST_QUERY_NAME
\o
\o :TEST_RESULTS_OPTIMIZED
SET enable_partitionwise_aggregate = ON;
\ir :TEST_QUERY_NAME
\o
\set TABLE_NAME 'hyper_repart'
\o :TEST_RESULTS_REPARTITIONED
SET enable_partitionwise_aggregate = ON;
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD_1
:DIFF_CMD_2
