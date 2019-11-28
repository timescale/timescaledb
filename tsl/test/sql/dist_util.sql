-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Cleanup from other potential tests that created these databases
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS backend_1_1;
DROP DATABASE IF EXISTS backend_x_2;
DROP DATABASE IF EXISTS backend_2_1;
DROP DATABASE IF EXISTS frontend_1;
DROP DATABASE IF EXISTS frontend_2;
SET client_min_messages TO NOTICE;

----------------------------------------------------------------
-- Test version compability function

CREATE OR REPLACE FUNCTION compatible_version(version CSTRING, reference CSTRING)
RETURNS TABLE(is_compatible BOOLEAN, is_old_version BOOLEAN)
AS :TSL_MODULE_PATHNAME, 'ts_test_compatible_version'
LANGUAGE C VOLATILE;

SELECT * FROM compatible_version('2.0.0-beta3.19', reference => '2.0.0-beta3.19');
SELECT * FROM compatible_version('2.0.0', reference => '2.0.0');
SELECT * FROM compatible_version('1.9.9', reference => '2.0.0-beta3.19');
SELECT * FROM compatible_version('1.9.9', reference => '2.0.0');
SELECT * FROM compatible_version('2.0.9', reference => '2.0.0-beta3.19');
SELECT * FROM compatible_version('2.0.9', reference => '2.0.0');
SELECT * FROM compatible_version('2.1.9', reference => '2.0.0-beta3.19');
SELECT * FROM compatible_version('2.1.0', reference => '2.1.19-beta3.19');

-- These should not parse and instead generate an error.
\set ON_ERROR_STOP 0
SELECT * FROM compatible_version('2.1.*', reference => '2.1.19-beta3.19');
SELECT * FROM compatible_version('2.1.0', reference => '2.1.*');
\set ON_ERROR_STOP 1

----------------------------------------------------------------
-- Create two distributed databases

CREATE DATABASE frontend_1;
CREATE DATABASE frontend_2;

\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
UPDATE _timescaledb_catalog.metadata SET value = '87c235e9-d857-4f16-b59f-7fbac9b87664' WHERE key = 'uuid';
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE '%uuid';
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => 'backend_1_1');
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE '%uuid';
SET client_min_messages TO NOTICE;

-- Create a second frontend database and add a backend to it
\c frontend_2 :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
UPDATE _timescaledb_catalog.metadata SET value = '77348176-09da-4a80-bc78-e31bdf5e63ec' WHERE key = 'uuid';
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE '%uuid';
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => 'backend_2_1');
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE '%uuid';
SET client_min_messages TO NOTICE;

\set ON_ERROR_STOP 0

----------------------------------------------------------------
-- Adding frontend as backend to a different frontend should fail
\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'frontend_2', bootstrap => true);
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'frontend_2', bootstrap => false);

----------------------------------------------------------------
-- Adding backend from a different group as a backend should fail
\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'backend_2_1', bootstrap => true);
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'backend_2_1', bootstrap => false);

----------------------------------------------------------------
-- Adding a valid backend target but to an existing backend should fail
\c backend_1_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'backend_2_1', bootstrap => true);
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'backend_2_1', bootstrap => false);

----------------------------------------------------------------
-- Adding a frontend (frontend 1) as a backend to a nondistributed node (TEST_DBNAME) should fail
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'frontend_1', bootstrap => true);
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'frontend_1', bootstrap => false);

\set ON_ERROR_STOP 1

----------------------------------------------------------------
-- Test that a data node can be moved to a different frontend if it is
-- removed first.
\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => 'backend_x_2', bootstrap => true);

-- dist_uuid should be added to the metadata on the data node
\c backend_x_2 :ROLE_CLUSTER_SUPERUSER
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';

-- Now remove a backend from this distributed database to add it to the other cluster
\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM delete_data_node('data_node_2');

-- dist_uuid should not be removed from the metadata on the data node,
-- so we need to delete it manually before adding it to another
-- backend.
\c backend_x_2 :ROLE_CLUSTER_SUPERUSER
SELECT key FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid';
DELETE FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid';

-- Add the data node to the second frontend without bootstrapping
\c frontend_2 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => 'backend_x_2', bootstrap => false);

-- dist_uuid should be added to the metadata on the data node
\c backend_x_2 :ROLE_CLUSTER_SUPERUSER
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';

-- Test space reporting functions for distributed and non-distributed tables
\c frontend_2 :ROLE_CLUSTER_SUPERUSER
CREATE TABLE nondisttable(time timestamptz PRIMARY KEY, device int CHECK (device > 0), temp float);
CREATE TABLE disttable(time timestamptz PRIMARY KEY, device int CHECK (device > 0), temp float);
SELECT * FROM create_hypertable('nondisttable', 'time');
SELECT * FROM create_distributed_hypertable('disttable', 'time');
INSERT INTO nondisttable VALUES
       ('2017-01-01 06:01', 1, 1.1),
       ('2017-01-01 08:01', 1, 1.2),
       ('2018-01-02 08:01', 2, 1.3),
       ('2019-01-01 09:11', 3, 2.1),
       ('2017-01-01 06:05', 1, 1.4);
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.1),
       ('2017-01-01 08:01', 1, 1.2),
       ('2018-01-02 08:01', 2, 1.3),
       ('2019-01-01 09:11', 3, 2.1),
       ('2017-01-01 06:05', 1, 1.4);

SELECT * FROM timescaledb_information.data_node;
SELECT * FROM timescaledb_information.hypertable;
SELECT * FROM hypertable_relation_size('disttable');
SELECT * FROM hypertable_relation_size('nondisttable');
SELECT * FROM hypertable_data_node_relation_size('disttable');
SELECT * FROM hypertable_data_node_relation_size('nondisttable');
