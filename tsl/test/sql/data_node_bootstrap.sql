-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER;
CREATE OR REPLACE FUNCTION show_data_nodes()
RETURNS TABLE(data_node_name NAME, host TEXT, port INT, dbname NAME)
AS :TSL_MODULE_PATHNAME, 'ts_test_data_node_show' LANGUAGE C;

-- Fetch the encoding, collation, and ctype as quoted strings into
-- variables.
SELECT QUOTE_LITERAL(PG_ENCODING_TO_CHAR(encoding)) AS enc
     , QUOTE_LITERAL(datcollate) AS coll
     , QUOTE_LITERAL(datctype) AS ctype
  FROM pg_database
 WHERE datname = current_database()
 \gset

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS bootstrap_test;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

SELECT * FROM add_data_node('bootstrap_test', host => 'localhost', database => 'bootstrap_test');

-- Ensure database and extensions are installed and have the correct
-- encoding, ctype and collation.
\c bootstrap_test :ROLE_CLUSTER_SUPERUSER;
SELECT extname, nspname
FROM pg_extension e, pg_namespace n
WHERE e.extnamespace = n.oid
AND e.extname = 'timescaledb';
SELECT PG_ENCODING_TO_CHAR(encoding) = :enc
     , datcollate = :coll
     , datctype = :ctype
  FROM pg_database
 WHERE datname = current_database();

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
CREATE SCHEMA _timescaledb_catalog AUTHORIZATION :ROLE_CLUSTER_SUPERUSER;
CREATE EXTENSION timescaledb WITH SCHEMA _timescaledb_catalog CASCADE;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost',
                            database => 'bootstrap_test', bootstrap => false);
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
CREATE SCHEMA _timescaledb_catalog AUTHORIZATION :ROLE_CLUSTER_SUPERUSER;
CREATE EXTENSION timescaledb WITH SCHEMA _timescaledb_catalog CASCADE;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost',
                            database => 'bootstrap_test', bootstrap => false);
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
CREATE SCHEMA _timescaledb_catalog AUTHORIZATION :ROLE_CLUSTER_SUPERUSER;
CREATE EXTENSION timescaledb WITH SCHEMA _timescaledb_catalog CASCADE;
SET client_min_messages TO NOTICE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
SELECT * FROM add_data_node('bootstrap_test', host => 'localhost',
                            database => 'bootstrap_test', bootstrap => false);
\set ON_ERROR_STOP 1

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

SELECT * FROM add_data_node('drop_db_test_dn', host => 'localhost', database => 'drop_db_test_dn');

-- Make sure security label is created
SELECT substr(label, 0, 10) || ':uuid'
    FROM pg_shseclabel
    WHERE objoid = (SELECT oid from pg_database WHERE datname = 'drop_db_test') AND
          provider = 'timescaledb';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- Check that timescaledb security label cannot be used directly
\set ON_ERROR_STOP 0
SECURITY LABEL FOR timescaledb
    ON DATABASE drop_db_test
    IS 'dist_uuid:00000000-0000-0000-0000-00000000000';
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
