-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Cleanup from other potential tests that created this database
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
SET client_min_messages TO NOTICE;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

SELECT * FROM add_data_node('data_node_1',
                            database => 'data_node_1',
                            password => :'ROLE_DEFAULT_CLUSTER_USER_PASS',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER',
                            bootstrap_password => :'ROLE_CLUSTER_SUPERUSER_PASS');

-- Set FDW and libpq options to make sure they are validated correctly
ALTER SERVER data_node_1 OPTIONS (ADD use_remote_estimate 'true');
ALTER SERVER data_node_1 OPTIONS (ADD fdw_startup_cost '110.0');
ALTER SERVER data_node_1 OPTIONS (ADD connect_timeout '3');

-- Test a bad option
\set ON_ERROR_STOP 0
ALTER SERVER data_node_1 OPTIONS (ADD invalid_option '3');
\set ON_ERROR_STOP 1

\c data_node_1
SET client_min_messages TO ERROR;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
CREATE TABLE test_ft (c0 int, c1 varchar(10));
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

CREATE FOREIGN TABLE test_ft (c0 int, c1 varchar(10)) SERVER data_node_1;

SELECT * FROM test_ft;
EXPLAIN (COSTS FALSE) INSERT INTO test_ft VALUES (1, 'value1'), (2, 'value2'), (2, 'value1') RETURNING c1, c0;
INSERT INTO test_ft VALUES (1, 'value1'), (2, 'value2'), (2, 'value1') RETURNING c1, c0;

-- Show tuples inserted on remote data node
\c data_node_1
SELECT * FROM test_ft;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Query from frontend
EXPLAIN (VERBOSE, COSTS FALSE) SELECT * FROM test_ft;
EXPLAIN (VERBOSE, COSTS FALSE) SELECT avg(c0) FROM test_ft GROUP BY c1;
SELECT * FROM test_ft;
SELECT avg(c0) FROM test_ft GROUP BY c1;

-- Update rows
EXPLAIN (VERBOSE, COSTS FALSE)
UPDATE test_ft SET c1 = 'new_test' WHERE c0 = 2
RETURNING c1, c0;

UPDATE test_ft SET c1 = 'new_test' WHERE c0 = 2
RETURNING c1, c0;

-- Show that the update was applied
SELECT * FROM test_ft;

-- Test deletes
DELETE FROM test_ft WHERE c0 = 1;

-- Show tuples deleted locally and on remote data node
SELECT * FROM test_ft;
\c data_node_1
SELECT * FROM test_ft;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

ANALYZE VERBOSE test_ft;
