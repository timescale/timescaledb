-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\set TEST_BASE_NAME dist_query
-- Run
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') AS "TEST_LOAD_NAME",
       format('include/%s_run.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
       format('%s/results/%s_results_reference.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_REFERENCE",
       format('%s/results/%s_results_repartitioning_reference.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_REPART_REFERENCE",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_repartitioning_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_REPART_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_UNOPTIMIZED",
       format('%s/results/%s_results_1dim.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_1DIM"
\gset
SELECT format('\! diff %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_REFERENCE') AS "DIFF_CMD_UNOPT",
       format('\! diff %s %s', :'TEST_RESULTS_OPTIMIZED', :'TEST_RESULTS_REFERENCE') AS "DIFF_CMD_OPT",
       format('\! diff %s %s', :'TEST_RESULTS_REPART_OPTIMIZED', :'TEST_RESULTS_REPART_REFERENCE') AS "DIFF_CMD_REPART",
       format('\! diff %s %s', :'TEST_RESULTS_1DIM', :'TEST_RESULTS_REPART_REFERENCE') AS "DIFF_CMD_1DIM"
\gset


-- Use a small fetch size to make sure that result are fetched across
-- multiple fetches.
--ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (ADD fetch_size '500');
SET client_min_messages TO notice;

-- Load the data
\ir :TEST_LOAD_NAME
SELECT hypertable_schema, hypertable_name, num_dimensions, num_chunks
FROM timescaledb_information.hypertables
ORDER BY 1,2;
SELECT count(*) FROM hyper;

SELECT count(*) FROM hyper WHERE :CLEAN_PARTITIONING_TIME_RANGE;

SET enable_partitionwise_aggregate = ON;
\set ECHO errors
\set PREFIX 'EXPLAIN (verbose, costs off)'
\set TABLE_NAME 'hyper'
-- Print queries to stdout and send to server
\set OUTPUT_CMD '\\p \\g'

---------------------------------------------------------------------
-- EXPLAINs without ordering
---------------------------------------------------------------------
\set ORDER_BY_1 ''
\set ORDER_BY_1_2 ''
\set LIMIT ''
\echo

-- Run the EXPLAINs on the cleanly partitioned time range (push-downs
-- safe)
\set WHERE_CLAUSE ':CLEAN_PARTITIONING_TIME_RANGE'
\set OUTPUT_CMD '\\p \\g'
\ir :TEST_QUERY_NAME

---------------------------------------------------------------------
-- EXPLAINs with ordering
---------------------------------------------------------------------
\set ORDER_BY_1 'ORDER BY 1'
\set ORDER_BY_1_2 'ORDER BY 1,2'
\set OUTPUT_CMD '\\p \\g'
\ir :TEST_QUERY_NAME

---------------------------------------------------------------------
-- EXPLAINs with ordering and querying only one node
---------------------------------------------------------------------
\set WHERE_CLAUSE ':CLEAN_PARTITIONING_TIME_RANGE AND device = 1'
\set OUTPUT_CMD '\\p \\g'
\ir :TEST_QUERY_NAME

---------------------------------------------------------------------
-- EXPLAINs with LIMIT
---------------------------------------------------------------------
\set WHERE_CLAUSE ':CLEAN_PARTITIONING_TIME_RANGE'
\set LIMIT 'LIMIT 10'
\set OUTPUT_CMD '\\p \\g'
\ir :TEST_QUERY_NAME

---------------------------------------------------------------------
-- Run the EXPLAINs on the repartitioned time range (push-downs
-- unsafe)
---------------------------------------------------------------------
\set ORDER_BY_1 ''
\set LIMIT
\set WHERE_CLAUSE ':REPARTITIONED_TIME_RANGE'
\set OUTPUT_CMD '\\p \\g'
\ir :TEST_QUERY_NAME

---------------------------------------------------------------------
-- EXPLAINs on a one-dimensional hypertable
---------------------------------------------------------------------
\set ORDER_BY_1 'ORDER BY 1'
\set ORDER_BY_1_2 ''
\set TABLE_NAME 'hyper1d'
\set OUTPUT_CMD '\\p \\g'
\ir :TEST_QUERY_NAME

---------------------------------------------------------------------
---------------------------------------------------------------------
-- QUERY results (diff test).  Note that we need to run the diff tests
-- with ordering or else the output will differ depending on table.
---------------------------------------------------------------------
---------------------------------------------------------------------

-- Run the queries for each setting. Each setting's result is
-- generated into its own file

-- Set extra_float_digits to avoid rounding differences between PG
-- versions
SET extra_float_digits=-2;

\set ECHO errors
-- Only execute queries on server (do not print queries)
\set OUTPUT_CMD '\\g'
SET client_min_messages TO error;

\set PREFIX
\set TABLE_NAME 'reference'
\set ORDER_BY_1 'ORDER BY 1'
\set ORDER_BY_1_2 'ORDER BY 1,2'
\set LIMIT ''
\o :TEST_RESULTS_REPART_REFERENCE


---------------------------------------------------------------------
-- Run queries across time range that involve repartitioning (no push
-- down) on a two-dimensional table
---------------------------------------------------------------------
\set WHERE_CLAUSE ':REPARTITIONED_TIME_RANGE'

\ir :TEST_QUERY_NAME

\set TABLE_NAME 'hyper'
\o :TEST_RESULTS_REPART_OPTIMIZED
SET enable_partitionwise_aggregate = ON;
\ir :TEST_QUERY_NAME

---------------------------------------------------------------------
-- Now run queries across time range that does not involve
-- repartitioning (should use push down)
---------------------------------------------------------------------
\set WHERE_CLAUSE ':CLEAN_PARTITIONING_TIME_RANGE'
\set TABLE_NAME 'reference'
\o :TEST_RESULTS_REFERENCE
\ir :TEST_QUERY_NAME

\set TABLE_NAME 'hyper'
\o :TEST_RESULTS_UNOPTIMIZED
SET enable_partitionwise_aggregate = OFF;
\ir :TEST_QUERY_NAME

\o :TEST_RESULTS_OPTIMIZED
SET enable_partitionwise_aggregate = ON;
\ir :TEST_QUERY_NAME

---------------------------------------------------------------------
-- Run queries across a one-dimensional hypertable
---------------------------------------------------------------------
\set TABLE_NAME 'hyper1d'
\set WHERE_CLAUSE ':REPARTITIONED_TIME_RANGE'
\o :TEST_RESULTS_1DIM
\set ORDER_BY_1 'ORDER BY 1'
\set ORDER_BY_1_2 'ORDER BY 1,2'
\set LIMIT ''
SET enable_partitionwise_aggregate = ON;
CALL distributed_exec($$ SET enable_partitionwise_aggregate = ON $$);
\ir :TEST_QUERY_NAME

-----------------------------------------------------------------------
-- Compute the diff (should be no difference)
\set ECHO all
:DIFF_CMD_UNOPT
:DIFF_CMD_OPT
:DIFF_CMD_REPART
:DIFF_CMD_1DIM

RESET ROLE;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;
