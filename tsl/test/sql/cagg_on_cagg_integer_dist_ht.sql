-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

------------------------------------
-- Set up a distributed environment
------------------------------------
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

\ir include/remote_exec.sql

SELECT (add_data_node (name, host => 'localhost', DATABASE => name)).*
FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v (name);

GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;
-- PG15 requires this explicit GRANT on schema public
GRANT CREATE ON SCHEMA public TO :ROLE_DEFAULT_PERM_USER;

-- Setup test variables
\set IS_DISTRIBUTED TRUE
\set IS_TIME_DIMENSION FALSE
\set TIME_DIMENSION_DATATYPE INTEGER
\set CAGG_NAME_1ST_LEVEL conditions_summary_1_1
\set CAGG_NAME_2TH_LEVEL conditions_summary_2_5
\set CAGG_NAME_3TH_LEVEL conditions_summary_3_10
\set BUCKET_WIDTH_1ST 'INTEGER \'1\''
\set BUCKET_WIDTH_2TH 'INTEGER \'5\''
\set BUCKET_WIDTH_3TH 'INTEGER \'10\''

-- Run tests
\ir include/cagg_on_cagg_common.sql

-- cleanup
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;
