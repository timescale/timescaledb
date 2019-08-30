-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_SUPERUSER;
\ir include/remote_exec.sql
SET ROLE :ROLE_1;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;
SET client_min_messages TO NOTICE;

\set TEST_TABLE 'conditions'
\ir 'include/aggregate_table_create.sql'

-- Add data nodes using the TimescaleDB node management API
SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER');
SELECT * FROM add_data_node('data_node_3', host => 'localhost',
                            database => 'data_node_3',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER');

SELECT * FROM test.remote_exec('{ data_node_1, data_node_2, data_node_3 }',
$$
       CREATE TYPE custom_type AS (high int, low int);
$$);

SELECT table_name FROM create_distributed_hypertable( 'conditions', 'timec', 'location', 3, chunk_time_interval => INTERVAL '1 day');

-- We need a lot of data and a lot of chunks to make the planner push down all of the aggregates
\ir 'include/aggregate_table_populate.sql'

SET enable_partitionwise_aggregate = ON;

-- Run an explain on the aggregate queries to make sure expected aggregates are being pushed down.
-- Grouping by the paritioning column should result in full aggregate pushdown where possible,
-- while using a non-partitioning column should result in a partial pushdown
\set PREFIX 'EXPLAIN (VERBOSE, COSTS OFF)'

\set GROUPING 'location'
\ir 'include/aggregate_queries.sql'

\set GROUPING 'region'
\ir 'include/aggregate_queries.sql'

-- Full aggregate pushdown correctness check, compare location grouped query results with partionwise aggregates on and off
\set GROUPING 'location'
SELECT format('%s/results/dist_agg_loc_results_test.out', :'TEST_OUTPUT_DIR') as "RESULTS_TEST1",
       format('%s/results/dist_agg_loc_results_control.out', :'TEST_OUTPUT_DIR') as "RESULTS_CONTROL1"
\gset
SELECT format('\! diff %s %s', :'RESULTS_CONTROL1', :'RESULTS_TEST1') as "DIFF_CMD1"
\gset

--generate the results into two different files
\set ECHO errors
SET client_min_messages TO error;
--make output contain query results
\set PREFIX ''
\o :RESULTS_CONTROL1
SET enable_partitionwise_aggregate = OFF;
\ir 'include/aggregate_queries.sql'
\o
\o :RESULTS_TEST1
SET enable_partitionwise_aggregate = ON;
\ir 'include/aggregate_queries.sql'
\o
\set ECHO all

:DIFF_CMD1

-- Partial aggregate pushdown correctness check, compare region grouped query results with partionwise aggregates on and off
\set GROUPING 'region'
SELECT format('%s/results/dist_agg_region_results_test.out', :'TEST_OUTPUT_DIR') as "RESULTS_TEST2",
       format('%s/results/dist_agg_region_results_control.out', :'TEST_OUTPUT_DIR') as "RESULTS_CONTROL2"
\gset
SELECT format('\! diff %s %s', :'RESULTS_CONTROL2', :'RESULTS_TEST2') as "DIFF_CMD2"
\gset

--generate the results into two different files
\set ECHO errors
SET client_min_messages TO error;
--make output contain query results
\set PREFIX ''
\o :RESULTS_CONTROL2
SET enable_partitionwise_aggregate = OFF;
\ir 'include/aggregate_queries.sql'
\o
\o :RESULTS_TEST2
SET enable_partitionwise_aggregate = ON;
\ir 'include/aggregate_queries.sql'
\o
\set ECHO all

-- Note that some difference in output is expected here because
-- queries include last(col, time) and first(col, time); there are
-- multiple values for "col" that has the same timestamp, so the
-- output depends on the order of arriving tuples.
:DIFF_CMD2
