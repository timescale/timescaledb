-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;

-- Need explicit password for non-super users to connect
ALTER ROLE :ROLE_DEFAULT_CLUSTER_USER CREATEDB PASSWORD 'pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_CLUSTER_USER;

SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS server_1;
DROP DATABASE IF EXISTS server_2;
DROP DATABASE IF EXISTS server_3;
SET client_min_messages TO NOTICE;

CREATE DATABASE server_1;
CREATE DATABASE server_2;
CREATE DATABASE server_3;

\c server_1
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
CREATE TYPE custom_type AS (high int, low int);
\c server_2
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
CREATE TYPE custom_type AS (high int, low int);
\c server_3
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
CREATE TYPE custom_type AS (high int, low int);
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

\set TEST_TABLE 'conditions'
\ir 'include/aggregate_table_create.sql'

-- Add servers using the TimescaleDB server management API
SELECT * FROM add_server('server_1', database => 'server_1', password => 'pass', if_not_exists => true);
SELECT * FROM add_server('server_2', database => 'server_2', password => 'pass', if_not_exists => true);
SELECT * FROM add_server('server_3', database => 'server_3', password => 'pass', if_not_exists => true);

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

:DIFF_CMD2
