-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER;
-- This test sets explicit UUIDs on the frontends to guarantee stable test output
GRANT ALL ON _timescaledb_catalog.metadata TO :ROLE_1;
SET ROLE :ROLE_1;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS backend_1;
DROP DATABASE IF EXISTS backend_2;
DROP DATABASE IF EXISTS frontend_b;
DROP DATABASE IF EXISTS backend_3;
SET client_min_messages TO NOTICE;

CREATE DATABASE backend_1;
CREATE DATABASE backend_2;
CREATE DATABASE backend_3;
CREATE DATABASE frontend_b;

\c backend_1
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
GRANT ALL ON _timescaledb_catalog.metadata TO :ROLE_1;
SET client_min_messages TO NOTICE;
\c backend_2
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
GRANT ALL ON _timescaledb_catalog.metadata TO :ROLE_1;
SET client_min_messages TO NOTICE;
\c backend_3
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
GRANT ALL ON _timescaledb_catalog.metadata TO :ROLE_1;
SET client_min_messages TO NOTICE;

-- Create a second frontend database and add a backend to it
\c frontend_b
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_1;
GRANT ALL ON _timescaledb_catalog.metadata TO :ROLE_1;
SET ROLE :ROLE_1;
INSERT INTO _timescaledb_catalog.metadata VALUES ('uuid', '87c235e9-d857-4f16-b59f-7fbac9b87664', true) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'uuid' OR key LIKE 'dist_uuid';
SELECT * FROM add_data_node('data_node_1', database => 'backend_3', if_not_exists => true);
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'uuid' OR key LIKE 'dist_uuid';

-- Connect back to our original database and add a backend to it
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;
INSERT INTO _timescaledb_catalog.metadata VALUES ('uuid', '77348176-09da-4a80-bc78-e31bdf5e63ec', true) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'uuid' OR key LIKE 'dist_uuid';
SELECT * FROM add_data_node('data_node_1', database => 'backend_1', if_not_exists => true);
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'uuid' OR key LIKE 'dist_uuid';

-- We now have two frontends with one backend each and one undistributed database
-- Let's try some invalid configurations
\set ON_ERROR_STOP 0
-- Adding frontend as backend to a different frontend
SELECT * FROM add_data_node('frontend_b', database => 'frontend_b', if_not_exists => true);

-- Adding backend from a different group as a backend
SELECT * FROM add_data_node('data_node_b', database => 'backend_3', if_not_exists => true);

-- Adding a valid backend target but to an existing backend
\c backend_1
SET ROLE :ROLE_1;
SELECT * FROM add_data_node('data_node_2', database => 'backend_2', if_not_exists => true);
\c backend_2
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_1;
SET ROLE :ROLE_1;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';

-- Adding a frontend as a backend to a nondistributed node
SELECT * FROM add_data_node('frontend_b', database => 'frontend_b', if_not_exists => true);
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';

-- Mocking a data node validation failure
SET ROLE :ROLE_SUPERUSER;
CREATE OR REPLACE FUNCTION _timescaledb_internal.validate_as_data_node()
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RAISE 'data node validation test failure';
END
$BODY$;

\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;
SELECT * FROM add_data_node('invalid_data_node', database => 'backend_2', if_not_exists => true);

-- Restore original validation function
\c backend_2
CREATE OR REPLACE FUNCTION _timescaledb_internal.validate_as_data_node() RETURNS void
AS :MODULE_PATHNAME, 'ts_dist_validate_as_data_node' LANGUAGE C VOLATILE STRICT;
\set ON_ERROR_STOP 1

-- Add a second backend to TEST_DB
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;
SELECT * FROM add_data_node('data_node_2', database => 'backend_2', if_not_exists => true);

\c backend_2
SET ROLE :ROLE_1;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;

-- Now remove a backend from this distributed database and then add it to the other cluster
\c backend_1
SET ROLE :ROLE_1;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;
SELECT * FROM delete_data_node('data_node_1');
\c backend_1
SET ROLE :ROLE_1;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';
\c frontend_b
SET ROLE :ROLE_1;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'uuid' OR key LIKE 'dist_uuid';
SELECT * FROM add_data_node('data_node_2', database => 'backend_1', if_not_exists => true);
\c backend_1
SET ROLE :ROLE_1;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';

-- Now remove both backends from frontend_b, then verify that they and frontend_b are now valid backends for TEST_DB
\c frontend_b
SET ROLE :ROLE_1;
SELECT * FROM delete_data_node('data_node_1');
SELECT * FROM delete_data_node('data_node_2');
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'uuid' OR key LIKE 'dist_uuid';

\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;
SELECT * FROM add_data_node('data_node_1', database => 'backend_1', if_not_exists => true);
SELECT * FROM add_data_node('data_node_3', database => 'backend_3', if_not_exists => true);
SELECT * FROM add_data_node('data_node_4', database => 'frontend_b', if_not_exists => true);

\c frontend_b
SET ROLE :ROLE_1;
SELECT * FROM _timescaledb_catalog.metadata WHERE key LIKE 'uuid' OR key LIKE 'dist_uuid';
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_1;

-- Test space reporting functions for distributed and non-distributed tables
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


-- Clean up for future tests
DROP TABLE disttable;
SELECT * FROM delete_data_node('data_node_1');
SELECT * FROM delete_data_node('data_node_2');
SELECT * FROM delete_data_node('data_node_3');
SELECT * FROM delete_data_node('data_node_4');
DROP DATABASE frontend_b;
DROP DATABASE backend_1;
DROP DATABASE backend_2;
DROP DATABASE backend_3;
