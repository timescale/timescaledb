-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test functionality of add_server() bootstrapping.
-- Most of this already done in other tests, so we check some corner cases.
\c :TEST_DBNAME :ROLE_SUPERUSER;
ALTER ROLE :ROLE_DEFAULT_PERM_USER PASSWORD 'perm_user_pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_PERM_USER;

CREATE OR REPLACE FUNCTION show_servers()
RETURNS TABLE(server_name NAME, host TEXT, port INT, dbname NAME)
AS :TSL_MODULE_PATHNAME, 'test_server_show' LANGUAGE C;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS bootstrap_test;
SET client_min_messages TO NOTICE;

SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Super user is required to make remote connection without password
--
-- local_user         = :ROLE_DEFAULT_PERM_USER
-- remote_user        = :ROLE_DEFAULT_PERM_USER
-- bootstrap_user     = :ROLE_DEFAULT_PERM_USER
-- bootstrap_database = 'postgres'
\set ON_ERROR_STOP 0
SELECT * FROM add_server('bootstrap_test', database => 'bootstrap_test', password => 'perm_user_pass');
\set ON_ERROR_STOP 1
SELECT * FROM show_servers();

-- local_user         = :ROLE_SUPERUSER
-- remote_user        = :ROLE_SUPERUSER
-- bootstrap_user     = :ROLE_SUPERUSER
-- bootstrap_database = 'postgres'
RESET ROLE;
SELECT * FROM add_server('bootstrap_test', database => 'bootstrap_test', password => 'perm_user_pass');
SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT * FROM show_servers();

-- Ensure database and extensions are installed
\c bootstrap_test :ROLE_SUPERUSER;
SELECT extname FROM pg_extension;
\c :TEST_DBNAME :ROLE_SUPERUSER;

-- After delete database and extension should still be there
SELECT * FROM delete_server('bootstrap_test', cascade => true);
SELECT * FROM show_servers();
\c bootstrap_test :ROLE_SUPERUSER;
SELECT extname FROM pg_extension;
\c :TEST_DBNAME :ROLE_SUPERUSER;

-- Try to recreate server with the same name, database and extension exists
--
-- local_user         = :ROLE_SUPERUSER
-- remote_user        = :ROLE_SUPERUSER
-- bootstrap_user     = :ROLE_SUPERUSER
-- bootstrap_database = 'postgres'
\set ON_ERROR_STOP 0
SELECT * FROM add_server('bootstrap_test', database => 'bootstrap_test', password => 'perm_user_pass');
\set ON_ERROR_STOP 1
SELECT * FROM show_servers();

-- Test if_not_exists functionality (no local server, but remote database and extension exists)
--
-- local_user         = :ROLE_SUPERUSER
-- remote_user        = :ROLE_SUPERUSER
-- bootstrap_user     = :ROLE_SUPERUSER
-- bootstrap_database = 'postgres'
SELECT * FROM add_server('bootstrap_test', database => 'bootstrap_test', password => 'perm_user_pass', if_not_exists => true);
SELECT * FROM show_servers();

-- Test if_not_exists functionality (has local server, has database database but no extension installed)
--
-- local_user         = :ROLE_SUPERUSER
-- remote_user        = :ROLE_SUPERUSER
-- bootstrap_user     = :ROLE_SUPERUSER
-- bootstrap_database = 'postgres'
\c bootstrap_test :ROLE_SUPERUSER;
SELECT extname FROM pg_extension;
DROP EXTENSION timescaledb CASCADE;
SELECT extname FROM pg_extension;
\c :TEST_DBNAME :ROLE_SUPERUSER;
SELECT * FROM add_server('bootstrap_test', database => 'bootstrap_test', password => 'perm_user_pass', if_not_exists => true);
\c bootstrap_test :ROLE_SUPERUSER;
SELECT extname FROM pg_extension;
\c :TEST_DBNAME :ROLE_SUPERUSER;

SELECT * FROM delete_server('bootstrap_test', cascade => true);
DROP DATABASE bootstrap_test;

-- Test automatic schema creation
CREATE DATABASE bootstrap_schema_test;
\c bootstrap_schema_test :ROLE_SUPERUSER;
CREATE SCHEMA bootstrap_schema;
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb WITH SCHEMA bootstrap_schema;
SET client_min_messages TO NOTICE;
SELECT extname FROM pg_extension;
SELECT * FROM bootstrap_schema.add_server('bootstrap_test', database => 'bootstrap_test', password => 'perm_user_pass');
\c bootstrap_test :ROLE_SUPERUSER;
SELECT extname FROM pg_extension;
\c bootstrap_schema_test :ROLE_SUPERUSER;
SELECT * FROM bootstrap_schema.add_server('bootstrap_test', database => 'bootstrap_test', password => 'perm_user_pass', if_not_exists => true);
\c :TEST_DBNAME :ROLE_SUPERUSER;
DROP DATABASE bootstrap_schema_test;
DROP DATABASE bootstrap_test;

-- Test users setting
--
-- local_user         = :ROLE_DEFAULT_PERM_USER
-- remote_user        = :ROLE_DEFAULT_PERM_USER
-- bootstrap_user     = :ROLE_SUPERUSER
-- bootstrap_database = 'template1'
SELECT * FROM add_server('bootstrap_test', database => 'bootstrap_test', local_user => :'ROLE_DEFAULT_PERM_USER', remote_user => :'ROLE_DEFAULT_PERM_USER', password => 'perm_user_pass', bootstrap_user => :'ROLE_SUPERUSER', bootstrap_database => 'template1');
\c bootstrap_test :ROLE_DEFAULT_PERM_USER;
SELECT extname FROM pg_extension;
\c :TEST_DBNAME :ROLE_SUPERUSER;
SELECT * FROM delete_server('bootstrap_test', cascade => true);

-- Test for ongoing transaction
BEGIN;
\set ON_ERROR_STOP 0
SELECT * FROM add_server('bootstrap_test', database => 'bootstrap_test', password => 'perm_user_pass');
\set ON_ERROR_STOP 1
COMMIT;
SELECT * FROM show_servers();

DROP DATABASE bootstrap_test;

-- Test unusual database names
--
-- local_user         = :ROLE_SUPERUSER
-- remote_user        = :ROLE_SUPERUSER
-- bootstrap_user     = :ROLE_SUPERUSER
-- bootstrap_database = 'postgres'
SELECT true FROM add_server('bootstrap_test1', database => 'Unusual Name', password => 'perm_user_pass');
SELECT true FROM add_server('bootstrap_test1', database => 'Unusual Name', password => 'perm_user_pass', if_not_exists => true);

SELECT true FROM add_server('bootstrap_test2', database => U&'\0441\043B\043E\043D', password => 'perm_user_pass');
SELECT true FROM add_server('bootstrap_test2', database => U&'\0441\043B\043E\043D', password => 'perm_user_pass', if_not_exists => true);

SELECT count(*) FROM show_servers();
SELECT true FROM pg_database WHERE datname = 'Unusual Name';
SELECT true FROM pg_database WHERE datname = U&'\0441\043B\043E\043D';

SELECT true FROM delete_server('bootstrap_test1', cascade => true);
SELECT true FROM delete_server('bootstrap_test2', cascade => true);

DROP DATABASE "Unusual Name";
DROP DATABASE U&"\0441\043B\043E\043D";
