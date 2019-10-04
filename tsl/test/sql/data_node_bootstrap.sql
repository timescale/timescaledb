-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER;
CREATE OR REPLACE FUNCTION show_data_nodes()
RETURNS TABLE(data_node_name NAME, host TEXT, port INT, dbname NAME)
AS :TSL_MODULE_PATHNAME, 'test_data_node_show' LANGUAGE C;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS bootstrap_test;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test');
-- Ensure database and extensions are installed
\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
SELECT extname, nspname
FROM pg_extension e, pg_namespace n
WHERE e.extnamespace = n.oid
AND e.extname = 'timescaledb';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
-- After delete database and extension should still be there
SELECT * FROM delete_data_node('bootstrap_test');
SELECT * FROM show_data_nodes();

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;

SELECT extname, nspname
FROM pg_extension e, pg_namespace n
WHERE e.extnamespace = n.oid
AND e.extname = 'timescaledb';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
DROP DATABASE bootstrap_test;

----------------------------------------------------------------------
-- Bootstrap the database and check that calling it without
-- bootstrapping does not find any problems.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);
SELECT * FROM show_data_nodes();

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM delete_data_node('bootstrap_test');

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
-- This should show dist_uuid row on the deleted node since that is
-- not removed by delete_data_node.
SELECT key FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid';

-- Delete the dist_uuid so that we can try to re-add it without
-- bootstrapping.
DELETE FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost',
                            database => 'bootstrap_test', bootstrap => false);

SELECT * FROM delete_data_node('bootstrap_test');
DROP DATABASE bootstrap_test;

----------------------------------------------------------------------
-- Do a manual bootstrap of the data node and check that it can be
-- added.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
CREATE DATABASE bootstrap_test OWNER :ROLE_CLUSTER_SUPERUSER;

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
SET client_min_messages TO ERROR;
CREATE SCHEMA _timescaledb_catalog AUTHORIZATION :ROLE_CLUSTER_SUPERUSER;
CREATE EXTENSION timescaledb WITH SCHEMA _timescaledb_catalog CASCADE;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost',
                            database => 'bootstrap_test', bootstrap => false);

SELECT * FROM delete_data_node('bootstrap_test');
DROP DATABASE bootstrap_test;

-----------------------------------------------------------------------
-- Bootstrap the database and remove it. Check that the missing
-- database is caught when adding the node and not bootstrapping.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);
SELECT * FROM show_data_nodes();
SELECT * FROM delete_data_node('bootstrap_test');
DROP DATABASE bootstrap_test;

\set ON_ERROR_STOP 0
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);
\set ON_ERROR_STOP 1

-----------------------------------------------------------------------
-- Bootstrap the database and remove the extension.
--
-- Check that adding the data node and not bootstrapping will fail
-- indicating that the extension is missing.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);
SELECT * FROM delete_data_node('bootstrap_test');
\c bootstrap_test :ROLE_SUPERUSER;

SELECT extname FROM pg_extension WHERE extname = 'timescaledb';
DROP EXTENSION timescaledb CASCADE;
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';

\set ON_ERROR_STOP 0
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);
\set ON_ERROR_STOP 1

DROP DATABASE bootstrap_test;

----------------------------------------------------------------------
-- Test for ongoing transaction
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
BEGIN;
\set ON_ERROR_STOP 0
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test');
\set ON_ERROR_STOP 1
COMMIT;
SELECT * FROM show_data_nodes();

---------------------------------------------------------------------
-- Test unusual database names
--
-- bootstrap_database = 'postgres'
CREATE FUNCTION test_database_name(name TEXT) RETURNS void AS $$
BEGIN
  PERFORM add_data_node('_test1', host => 'localhost', database => name);
  PERFORM delete_data_node('_test1');
END;
$$ LANGUAGE plpgsql;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT test_database_name('Unusual Name');
SELECT test_database_name(U&'\0441\043B\043E\043D');

DROP DATABASE "Unusual Name";
DROP DATABASE U&"\0441\043B\043E\043D";
