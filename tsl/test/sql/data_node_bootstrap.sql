-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER;
CREATE OR REPLACE FUNCTION show_data_nodes()
RETURNS TABLE(data_node_name NAME, host TEXT, dbname NAME)
AS :TSL_MODULE_PATHNAME, 'ts_test_data_node_show' LANGUAGE C;

-- Fetch the encoding, collation, and ctype as quoted strings into
-- variables.
SELECT QUOTE_LITERAL(PG_ENCODING_TO_CHAR(encoding)) AS enc
     , QUOTE_LITERAL(datcollate) AS coll
     , QUOTE_LITERAL(datctype) AS ctype
  FROM pg_database
 WHERE datname = current_database()
 \gset

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test');

-- Ensure database and extensions are installed and have the correct
-- encoding, ctype and collation.
\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
SELECT extname, extnamespace::regnamespace FROM pg_extension e WHERE extname = 'timescaledb';
SELECT PG_ENCODING_TO_CHAR(encoding) = :enc
     , datcollate = :coll
     , datctype = :ctype
  FROM pg_database
 WHERE datname = current_database();

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
-- After delete_data_node, the database and extension should still
-- exist on the data node
SELECT * FROM delete_data_node('bootstrap_test');
SELECT * FROM show_data_nodes();

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;

SELECT extname, extnamespace::regnamespace FROM pg_extension e WHERE extname = 'timescaledb';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
-- Trying to add the data node again should fail, with or without
-- bootstrapping.
SELECT add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap=>false);
SELECT add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test');
\set ON_ERROR_STOP 0

DROP DATABASE bootstrap_test;

----------------------------------------------------------------------
-- Bootstrap the database and check that calling it without
-- bootstrapping does not find any problems.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);
SELECT * FROM show_data_nodes();

SELECT * FROM delete_data_node('bootstrap_test');

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
-- This should show dist_uuid row on the deleted node since that is
-- not removed by delete_data_node.
SELECT key FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid';

-- Delete the dist_uuid so that we can try to re-add it without
-- bootstrapping.
DELETE FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);

\set ON_ERROR_STOP 0
-- Dropping the database with delete_data_node should not work in a
-- transaction block since it is non-transactional.
BEGIN;
SELECT * FROM delete_data_node('bootstrap_test', drop_database => true);
ROLLBACK;
\set ON_ERROR_STOP 1

-- Using the drop_database option when there are active connections to
-- the data node should fail. But any connections in the current
-- session should be cleared when dropping the database. To test that
-- the connection is cleared, first create a connection in the
-- connection cache by inserting some data
CREATE TABLE conditions (time timestamptz, device int, temp float);
SELECT create_distributed_hypertable('conditions', 'time', 'device');
INSERT INTO conditions VALUES ('2021-12-01 10:30', 1, 20.3);
DROP TABLE conditions;

-- Now drop the data node and it should clear the connection from the
-- cache first
SELECT * FROM delete_data_node('bootstrap_test', drop_database => true);

\set ON_ERROR_STOP 0
-- Dropping the database now should fail since it no longer exists
DROP DATABASE bootstrap_test;
\set ON_ERROR_STOP 1

-- Adding the data node again should work
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);
-- Now drop the database manually before using the drop_database option
DROP DATABASE bootstrap_test;
\set ON_ERROR_STOP 0
-- Expect an error since the database does not exist.
SELECT * FROM delete_data_node('bootstrap_test', drop_database => true);
\set ON_ERROR_STOP 1

-- Delete it without the drop_database option set since the database
-- was manually deleted.
SELECT * FROM delete_data_node('bootstrap_test');

----------------------------------------------------------------------
-- Do a manual bootstrap of the data node and check that it can be
-- added.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
CREATE DATABASE bootstrap_test OWNER :ROLE_CLUSTER_SUPERUSER;

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);

SELECT * FROM delete_data_node('bootstrap_test');
DROP DATABASE bootstrap_test;

----------------------------------------------------------------------
-- Do a manual bootstrap of the data node and check that it can be
-- added even when bootstrap is true. This is to check that we can
-- bootstrap a database with an extension already installed.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
CREATE DATABASE bootstrap_test OWNER :ROLE_CLUSTER_SUPERUSER;

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);

SELECT * FROM delete_data_node('bootstrap_test');
DROP DATABASE bootstrap_test;

----------------------------------------------------------------------
-- Create a database and check that a mismatching encoding is caught
-- when bootstrapping (since it will skip creating the database).
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

CREATE DATABASE bootstrap_test
   ENCODING SQL_ASCII
 LC_COLLATE 'C'
   LC_CTYPE 'C'
   TEMPLATE template0
      OWNER :ROLE_CLUSTER_SUPERUSER;

\set ON_ERROR_STOP 0
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);
\set ON_ERROR_STOP 1

DROP DATABASE bootstrap_test;

----------------------------------------------------------------------
-- Do a manual bootstrap of the data but check that a mismatching
-- encoding, ctype, or collation will be caught.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- Pick an alternative locale for testing from the list of installed
-- collations. This locale needs to be differnt from the current
-- database's locale. We pick it from the list of collations in a
-- platform agnostic way since, e.g., Linux and Windows have very
-- different locale names.
SELECT QUOTE_LITERAL(c.collctype) AS other_locale
  FROM pg_collation c, pg_database d
  WHERE c.collencoding = d.encoding
  AND d.datctype != c.collctype
  AND d.datname = current_database()
  ORDER BY c.oid DESC
  LIMIT 1
 \gset

CREATE DATABASE bootstrap_test
   ENCODING SQL_ASCII
 LC_COLLATE 'C'
   LC_CTYPE 'C'
   TEMPLATE template0
      OWNER :ROLE_CLUSTER_SUPERUSER;

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);
\set ON_ERROR_STOP 1

DROP DATABASE bootstrap_test;

CREATE DATABASE bootstrap_test
   ENCODING :"enc"
 LC_COLLATE :other_locale
   LC_CTYPE :ctype
   TEMPLATE template0
      OWNER :ROLE_CLUSTER_SUPERUSER;

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);
\set ON_ERROR_STOP 1

DROP DATABASE bootstrap_test;

CREATE DATABASE bootstrap_test
   ENCODING :"enc"
 LC_COLLATE :coll
   LC_CTYPE :other_locale
   TEMPLATE template0
      OWNER :ROLE_CLUSTER_SUPERUSER;

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);
\set ON_ERROR_STOP 1

DROP DATABASE bootstrap_test;

-----------------------------------------------------------------------
-- Bootstrap the database and remove it. Check that the missing
-- database is caught when adding the node and not bootstrapping.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);
SELECT * FROM show_data_nodes();
SELECT * FROM delete_data_node('bootstrap_test');
DROP DATABASE bootstrap_test;

\set ON_ERROR_STOP 0
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);
\set ON_ERROR_STOP 1

-----------------------------------------------------------------------
-- Bootstrap the database and remove the extension.
--
-- Check that adding the data node and not bootstrapping will fail
-- indicating that the extension is missing.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => true);
SELECT * FROM delete_data_node('bootstrap_test');
\c bootstrap_test :ROLE_SUPERUSER;

SELECT extname FROM pg_extension WHERE extname = 'timescaledb';
DROP EXTENSION timescaledb CASCADE;
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';

\set ON_ERROR_STOP 0
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test', bootstrap => false);
\set ON_ERROR_STOP 1

DROP DATABASE bootstrap_test;

-----------------------------------------------------------------------
-- Create a new access node manually so that we can set a specific
-- schema for the access node and then bootstrap a data node partially
-- with a non-public schema so that we can see that an error is
-- generated.
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
CREATE DATABASE access_node OWNER :ROLE_CLUSTER_SUPERUSER;

\c access_node :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
CREATE SCHEMA ts_non_default AUTHORIZATION :ROLE_CLUSTER_SUPERUSER;
CREATE EXTENSION timescaledb WITH SCHEMA ts_non_default CASCADE;
SET client_min_messages TO NOTICE;

-- Show the schema for the extension to verify that it is not public.
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
CREATE DATABASE bootstrap_test OWNER :ROLE_CLUSTER_SUPERUSER;

\c bootstrap_test :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
CREATE SCHEMA ts_non_default AUTHORIZATION :ROLE_CLUSTER_SUPERUSER;
SET client_min_messages TO NOTICE;

\c access_node :ROLE_CLUSTER_SUPERUSER

-- Add data node and delete it under error suppression. We want to
-- avoid later tests to have random failures because the add succeeds.
\set ON_ERROR_STOP 0
SELECT node_name, database, node_created, database_created, extension_created
FROM ts_non_default.add_data_node(
       'bootstrap_test', host => 'localhost',
       database => 'bootstrap_test', bootstrap => true);
SELECT * FROM ts_non_default.delete_data_node('bootstrap_test');
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
DROP DATABASE bootstrap_test;
DROP DATABASE access_node;

----------------------------------------------------------------------
-- Test for ongoing transaction
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
BEGIN;
\set ON_ERROR_STOP 0
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test');
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

-- Test Access Node DATABASE DROP NOTICE message
--

-- Make sure the NOTICE message not shown on a DROP DATABASE error
\set ON_ERROR_STOP 0
DROP DATABASE :TEST_DBNAME;
\set ON_ERROR_STOP 1

CREATE DATABASE drop_db_test;
\c drop_db_test :ROLE_CLUSTER_SUPERUSER;

SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
SET client_min_messages TO NOTICE;

-- No security label exists
SELECT label FROM pg_shseclabel
    WHERE objoid = (SELECT oid from pg_database WHERE datname = 'drop_db_test') AND
          provider = 'timescaledb';

SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('drop_db_test_dn', host => 'localhost', database => 'drop_db_test_dn');

-- Make sure security label is created
SELECT substr(label, 0, 10) || ':uuid'
    FROM pg_shseclabel
    WHERE objoid = (SELECT oid from pg_database WHERE datname = 'drop_db_test') AND
          provider = 'timescaledb';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- Check that timescaledb security label cannot be used directly. To
-- support pg_dump, we do not print an error when a proper label is
-- used, but print an error if something that doesn't look like a
-- distributed uuid is used.
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bc-438f-11ec-8919-23804e22321a';
\set ON_ERROR_STOP 0
\set VERBOSITY default
-- No colon
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'bad_label';
-- Bad tag, but still an UUID
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'uuid:4ab3b1bc-438f-11ec-8919-23804e22321a';
-- Length is not correct
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bcd-438f-11ec-8919-23804e2232';
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bcd-438f-11ec-8919-23804e223215';
-- Length is correct, but it contains something that is not a
-- hexadecimal digit.
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bcd-4x8f-11ec-8919-23804e22321';
-- Total length is correct, but not the right number of hyphens.
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3-1bcd-438f-11ec-8919-23804e22321';
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bcd438f-11ec-8919-23804e223213';
-- Total length is correct, but length of groups is not
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bcd-438f-11ec-8919-23804e22321';
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bc-438f-11ec-891-23804e22321ab';
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bc-438f-11e-8919-23804e22321ab';
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bc-438-11ec-8919-23804e22321ab';
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:4ab3b1bca-438f-11ec-8919-23804e22321';
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-- Check that security label functionality is working
CREATE TABLE seclabel_test(id int);
SECURITY LABEL ON TABLE seclabel_test IS 'label';
DROP TABLE seclabel_test;

-- This will generate NOTICE message
DROP DATABASE drop_db_test;
DROP DATABASE drop_db_test_dn;

-- Ensure label is deleted after the DROP
SELECT label FROM pg_shseclabel
    WHERE objoid = (SELECT oid from pg_database WHERE datname = 'drop_db_test') AND
          provider = 'timescaledb';
