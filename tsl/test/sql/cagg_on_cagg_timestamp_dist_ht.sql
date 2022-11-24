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
\set IS_TIME_DIMENSION TRUE
\set TIME_DIMENSION_DATATYPE TIMESTAMP
\set CAGG_NAME_1ST_LEVEL conditions_summary_1_hourly
\set CAGG_NAME_2TH_LEVEL conditions_summary_2_daily
\set CAGG_NAME_3TH_LEVEL conditions_summary_3_weekly
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 hour\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 day\''
\set BUCKET_WIDTH_3TH 'INTERVAL \'1 week\''

SET timezone TO 'UTC';

--
-- Run common tests
--
\ir include/cagg_on_cagg_common.sql

--
-- Validation test for variable bucket on top of fixed bucket
--
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 month\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'60 days\''
\set WARNING_MESSAGE '-- SHOULD ERROR because is not allowed variable-size bucket on top of fixed-size bucket'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for non-multiple bucket sizes
--
\set BUCKET_WIDTH_1ST 'INTERVAL \'2 hours\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'3 hours\''
\set WARNING_MESSAGE '-- SHOULD ERROR because non-multiple bucket sizes'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for equal bucket sizes
--
\set BUCKET_WIDTH_1ST 'INTERVAL \'1 hour\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 hour\''
\set WARNING_MESSAGE '-- SHOULD ERROR because new bucket should not be equal to previous'
\ir include/cagg_on_cagg_validations.sql

--
-- Validation test for bucket size less than source
--
\set BUCKET_WIDTH_1ST 'INTERVAL \'2 hours\''
\set BUCKET_WIDTH_2TH 'INTERVAL \'1 hour\''
\set WARNING_MESSAGE '-- SHOULD ERROR because new bucket should be greater than previous'
\ir include/cagg_on_cagg_validations.sql

-- Cleanup
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;
