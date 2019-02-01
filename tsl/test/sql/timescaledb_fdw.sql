-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
ALTER ROLE :ROLE_DEFAULT_CLUSTER_USER CREATEDB PASSWORD 'pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_CLUSTER_USER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Cleanup from other potential tests that created this database
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS server_1;
SET client_min_messages TO NOTICE;
CREATE DATABASE server_1;
SELECT * FROM add_server('server_1', database => 'server_1', password => 'pass');

-- Set FDW and libpq options to make sure they are validated correctly
ALTER SERVER server_1 OPTIONS (ADD use_remote_estimate 'true');
ALTER SERVER server_1 OPTIONS (ADD fdw_startup_cost '110.0');
ALTER SERVER server_1 OPTIONS (ADD connect_timeout '3');

-- Test a bad option
\set ON_ERROR_STOP 0
ALTER SERVER server_1 OPTIONS (ADD invalid_option '3');
\set ON_ERROR_STOP 1

\c server_1
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
CREATE TABLE test_ft (c0 int, c1 varchar(10));
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

CREATE FOREIGN TABLE test_ft (c0 int, c1 varchar(10)) SERVER server_1;

SELECT * FROM test_ft;
EXPLAIN (COSTS FALSE) INSERT INTO test_ft VALUES (1, 'value1'), (2, 'value2'), (2, 'value1') RETURNING c1, c0;
INSERT INTO test_ft VALUES (1, 'value1'), (2, 'value2'), (2, 'value1') RETURNING c1, c0;

-- Show tuples inserted on remote server
\c server_1
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
DELETE FROM test_ft WHERE c0 = 1;
ANALYZE VERBOSE test_ft;
