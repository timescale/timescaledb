-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;
SET client_min_messages TO NOTICE;

-- Add data nodes using TimescaleDB data_node management API. NOTE that the
-- extension won't be created since it is installed in the template1
-- database
SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');

SELECT * FROM add_data_node('data_node_2', 'localhost',
                            database => 'data_node_2');
\set ON_ERROR_STOP 0
-- Add again
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2');
-- No host provided
SELECT * FROM add_data_node('data_node_99');
SELECT * FROM add_data_node(NULL);
-- Add NULL data_node
SELECT * FROM add_data_node(NULL, host => 'localhost');
SELECT * FROM add_data_node(NULL, NULL);
-- Adding a data node via ADD SERVER is blocked
CREATE SERVER data_node_4 FOREIGN DATA WRAPPER timescaledb_fdw
OPTIONS (host 'localhost', port '15432', dbname 'data_node_4');
-- Dropping a data node via DROP SERVER is also blocked
DROP SERVER data_node_1, data_node_2;
\set ON_ERROR_STOP 1

-- Should not generate error with if_not_exists option
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => 'data_node_2',
                            if_not_exists => true);

SELECT * FROM add_data_node('data_node_3', host => 'localhost', database => 'data_node_3');


-- Test altering server command is blocked
\set ON_ERROR_STOP 0
ALTER SERVER data_node_1 OPTIONS (ADD use_remote_estimate 'true');
ALTER SERVER data_node_1 OPTIONS (SET fdw_startup_cost '110.0');
ALTER SERVER data_node_1 OPTIONS (DROP sslmode);
ALTER SERVER data_node_1 RENAME TO data_node_k;
ALTER SERVER data_node_1 OWNER TO CURRENT_USER;
\set ON_ERROR_STOP 1

-- List foreign data nodes
SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

-- Delete a data node
SELECT * FROM delete_data_node('data_node_3');

-- List data nodes
SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

\set ON_ERROR_STOP 0
-- Deleting a non-existing data node generates error
SELECT * FROM delete_data_node('data_node_3');
\set ON_ERROR_STOP 1

-- Deleting non-existing data node with "if_exists" set does not generate error
SELECT * FROM delete_data_node('data_node_3', if_exists => true);

SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

SELECT * FROM delete_data_node('data_node_1');
SELECT * FROM delete_data_node('data_node_2');

-- No data nodes left
SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

-- Cleanup databases
RESET ROLE;
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;
SET client_min_messages TO INFO;

SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost',
                            database => 'data_node_3');

-- We need to allow anybody to create distributed tables on these.
GRANT USAGE
   ON FOREIGN SERVER data_node_1, data_node_2, data_node_3
   TO :ROLE_1;

SELECT node_name, "options"
  FROM timescaledb_information.data_node
ORDER BY node_name;

SELECT object_name, object_type, ARRAY_AGG(privilege_type)
FROM information_schema.role_usage_grants
WHERE object_schema NOT IN ('information_schema','pg_catalog')
  AND object_type LIKE 'FOREIGN%'
GROUP BY object_schema, object_name, object_type
ORDER BY object_name, object_type;

SET ROLE :ROLE_1;

-- Now create a distributed hypertable using the data nodes
CREATE TABLE disttable(time timestamptz, device int, temp float);

-- Test that all data nodes are added to a hypertable and that the
-- slices in the device dimension equals the number of data nodes.
BEGIN;
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device');

SELECT column_name, num_slices
FROM _timescaledb_catalog.dimension
WHERE column_name = 'device';

-- All data nodes should be added.
SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

ROLLBACK;

-- Now specify less slices than there are data nodes to generate a
-- warning
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 2);

-- All data nodes should be added.
SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

-- Ensure that replication factor allows to distinguish data node hypertables from regular hypertables
SELECT replication_factor FROM _timescaledb_catalog.hypertable WHERE table_name = 'disttable';
SELECT * FROM test.remote_exec(NULL, $$ SELECT replication_factor
FROM _timescaledb_catalog.hypertable WHERE table_name = 'disttable'; $$);

-- Create one chunk
INSERT INTO disttable VALUES ('2019-02-02 10:45', 1, 23.4);

-- Chunk mapping created
SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

DROP TABLE disttable;

-- data node mappings should be cleaned up
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;

-- Now create tables as cluster user
CREATE TABLE disttable(time timestamptz, device int, temp float);

\set ON_ERROR_STOP 0
-- Attach data node should fail when called on a non-hypertable
SELECT * FROM attach_data_node('data_node_1', 'disttable');

-- Test some bad create_hypertable() parameter values for distributed hypertables
-- Bad replication factor
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', replication_factor => 0, data_nodes => '{ "data_node_2", "data_node_4" }');
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', replication_factor => 32768);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => -1);
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', replication_factor => -1);

-- Non-existing data node
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', replication_factor => 2, data_nodes => '{ "data_node_4" }');
\set ON_ERROR_STOP 1

-- Use a subset of data nodes and a replication factor of two so that
-- each chunk is associated with more than one data node. Set
-- number_partitions lower than number of servers to raise a warning
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', number_partitions => 1, replication_factor => 2, data_nodes => '{ "data_node_2", "data_node_3" }');

-- Create some chunks
INSERT INTO disttable VALUES
       ('2019-02-02 10:45', 1, 23.4),
       ('2019-05-23 10:45', 4, 14.9),
       ('2019-07-23 10:45', 8, 7.6);

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;

-- Dropping a chunk should also clean up data node mappings
SELECT * FROM drop_chunks(older_than => '2019-05-22 17:18'::timestamptz);

SELECT * FROM test.show_subtables('disttable');
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

SELECT table_name, node_name
FROM _timescaledb_catalog.chunk c,
_timescaledb_catalog.chunk_data_node cdn
WHERE c.id = cdn.chunk_id;

-- Setting the same data node should do nothing and return false
SELECT * FROM _timescaledb_internal.set_chunk_default_data_node('_timescaledb_internal._hyper_4_3_dist_chunk', 'data_node_3');

-- Should update the default data node and return true
SELECT * FROM _timescaledb_internal.set_chunk_default_data_node('_timescaledb_internal._hyper_4_3_dist_chunk', 'data_node_2');

SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

-- Reset the default data node
SELECT * FROM _timescaledb_internal.set_chunk_default_data_node('_timescaledb_internal._hyper_4_3_dist_chunk', 'data_node_3');

\set ON_ERROR_STOP 0
-- Will fail because data_node_2 contains chunks
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM delete_data_node('data_node_2');
-- non-existing chunk
SELECT * FROM _timescaledb_internal.set_chunk_default_data_node('x_chunk', 'data_node_3');
-- non-existing data node
SELECT * FROM _timescaledb_internal.set_chunk_default_data_node('_timescaledb_internal._hyper_4_3_dist_chunk', 'data_node_0000');
-- data node exists but does not store the chunk
SELECT * FROM _timescaledb_internal.set_chunk_default_data_node('_timescaledb_internal._hyper_4_3_dist_chunk', 'data_node_1');
-- NULL try
SELECT * FROM _timescaledb_internal.set_chunk_default_data_node(NULL, 'data_node_3');
\set ON_ERROR_STOP 1

-- Deleting a data node removes the "foreign" chunk table(s) that
-- reference that data node as "primary" and should also remove the
-- hypertable_data_node and chunk_data_node mappings for that data node.  In
-- the future we might want to fallback to a replica data node for those
-- chunks that have multiple data nodes so that the chunk is not removed
-- unnecessarily. We use force => true b/c data_node_2 contains chunks.
SELECT * FROM delete_data_node('data_node_2', force => true);

SELECT * FROM test.show_subtables('disttable');
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;

\set ON_ERROR_STOP 0
-- can't delete b/c it's last data replica
SELECT * FROM delete_data_node('data_node_3', force => true);
\set ON_ERROR_STOP 1

-- Removing all data allows us to delete the data node by force, but
-- with WARNING that new data will be under-replicated
TRUNCATE disttable;
SELECT * FROM delete_data_node('data_node_3', force => true);

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;
SELECT * FROM _timescaledb_catalog.chunk;

-- Attach data node should now succeed
SET client_min_messages TO NOTICE;
SELECT * FROM attach_data_node('data_node_1', 'disttable');

SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;

SELECT * FROM _timescaledb_internal.ping_data_node('data_node_1');

-- Create data node referencing postgres_fdw
RESET ROLE;
CREATE EXTENSION postgres_fdw;
CREATE SERVER pg_server_1 FOREIGN DATA WRAPPER postgres_fdw;
SET ROLE :ROLE_1;

CREATE TABLE standalone(time TIMESTAMPTZ, device INT, value FLOAT);
SELECT * FROM create_hypertable('standalone','time');
\set ON_ERROR_STOP 0
-- Throw ERROR for non-existing data node
SELECT * FROM _timescaledb_internal.ping_data_node('data_node_123456789');
-- ERROR on NULL
SELECT * FROM _timescaledb_internal.ping_data_node(NULL);
-- ERROR when not passing TimescaleDB data node
SELECT * FROM _timescaledb_internal.ping_data_node('pg_data_node_1');
-- ERROR on attaching to non-distributed hypertable
SELECT * FROM attach_data_node('data_node_1', 'standalone');
\set ON_ERROR_STOP 1
DROP TABLE standalone;

-- Some attach data node error cases
\set ON_ERROR_STOP 0
-- Invalid arguments
SELECT * FROM attach_data_node('data_node_1', NULL, true);
SELECT * FROM attach_data_node(NULL, 'disttable', true);

-- Deleted data node
SELECT * FROM attach_data_node('data_node_2', 'disttable');

-- Attaching to an already attached data node without 'if_not_exists'
SELECT * FROM attach_data_node('data_node_1', 'disttable', false);
\set ON_ERROR_STOP 1

-- Attach if not exists
SELECT * FROM attach_data_node('data_node_1', 'disttable', true);

-- Should repartition too. First show existing number of slices in
-- 'device' dimension
SELECT column_name, num_slices
FROM _timescaledb_catalog.dimension
WHERE num_slices IS NOT NULL
AND column_name = 'device';

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('data_node_4', host => 'localhost', database => 'data_node_4',
                            if_not_exists => true);
SELECT * FROM attach_data_node('data_node_4', 'disttable');

-- Show updated number of slices in 'device' dimension.
SELECT column_name, num_slices
FROM _timescaledb_catalog.dimension
WHERE num_slices IS NOT NULL
AND column_name = 'device';

-- Clean up
DROP TABLE disttable;
SELECT * FROM delete_data_node('data_node_4');

SET ROLE :ROLE_1;
-- Creating a distributed hypertable without any servers should fail
CREATE TABLE disttable(time timestamptz, device int, temp float);

\set ON_ERROR_STOP 0
-- Creating a distributed hypertable without any data nodes should fail
SELECT * FROM create_distributed_hypertable('disttable', 'time', data_nodes => '{ }');
\set ON_ERROR_STOP 1

SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM delete_data_node('data_node_1');
SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;
SELECT * FROM _timescaledb_catalog.chunk;

\set ON_ERROR_STOP 0
-- No data nodes remain, so should fail
SELECT * FROM create_distributed_hypertable('disttable', 'time');
\set ON_ERROR_STOP 1

-- These data nodes have been deleted, so safe to remove their databases.
DROP DATABASE data_node_1;
DROP DATABASE data_node_2;
DROP DATABASE data_node_3;
DROP DATABASE data_node_4;

-- there should be no data nodes
SELECT node_name, "options" FROM timescaledb_information.data_node ORDER BY node_name;

-- let's add some
SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost',
                            database => 'data_node_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;

SET ROLE :ROLE_1;
DROP TABLE disttable;

CREATE TABLE disttable(time timestamptz, device int, temp float);

SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 2,
                                            replication_factor => 2,
                                            data_nodes => '{"data_node_1", "data_node_2", "data_node_3"}');

-- Create some chunks on all the data_nodes
INSERT INTO disttable VALUES
       ('2019-02-02 10:45', 1, 23.4),
       ('2019-05-23 10:45', 4, 14.9),
       ('2019-07-23 10:45', 8, 7.6);

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;

-- Add additional hypertable
CREATE TABLE disttable_2(time timestamptz, device int, temp float);

SELECT * FROM create_distributed_hypertable('disttable_2', 'time', 'device', 2, replication_factor => 2, data_nodes => '{"data_node_1", "data_node_2", "data_node_3"}');

CREATE TABLE devices(device int, name text);

SELECT * FROM _timescaledb_catalog.hypertable_data_node;

-- Block one data node for specific hypertable
SELECT * FROM block_new_chunks('data_node_1', 'disttable');

-- Block one data node for all hypertables
SELECT * FROM block_new_chunks('data_node_1');

SELECT * FROM _timescaledb_catalog.hypertable_data_node;

-- insert more data
INSERT INTO disttable VALUES
       ('2019-08-02 10:45', 1, 14.4),
       ('2019-08-15 10:45', 4, 14.9),
       ('2019-08-26 10:45', 8, 17.6);

-- no new chunks on data_node_1
SELECT * FROM _timescaledb_catalog.chunk_data_node;

-- some ERROR cases
\set ON_ERROR_STOP 0
-- Will error due to under-replication
SELECT * FROM block_new_chunks('data_node_2');
-- can't block/allow non-existing data node
SELECT * FROM block_new_chunks('data_node_12345', 'disttable');
SELECT * FROM allow_new_chunks('data_node_12345', 'disttable');
-- NULL data node
SELECT * FROM block_new_chunks(NULL, 'disttable');
SELECT * FROM allow_new_chunks(NULL, 'disttable');
-- can't block/allow on non hypertable
SELECT * FROM block_new_chunks('data_node_1', 'devices');
SELECT * FROM allow_new_chunks('data_node_1', 'devices');
\set ON_ERROR_STOP 1

-- Force block all data nodes
SELECT * FROM block_new_chunks('data_node_2', force => true);
SELECT * FROM block_new_chunks('data_node_1', force => true);
SELECT * FROM block_new_chunks('data_node_3', force => true);

-- All data nodes are blocked
SELECT * FROM _timescaledb_catalog.hypertable_data_node;

\set ON_ERROR_STOP 0
-- insert should fail b/c all data nodes are blocked
INSERT INTO disttable VALUES ('2019-11-02 02:45', 1, 13.3);
\set ON_ERROR_STOP 1

-- unblock data nodes for all hypertables
SELECT * FROM allow_new_chunks('data_node_1');
SELECT * FROM allow_new_chunks('data_node_2');
SELECT * FROM allow_new_chunks('data_node_3');

SELECT table_name, node_name, block_chunks
FROM _timescaledb_catalog.hypertable_data_node dn,
_timescaledb_catalog.hypertable h
WHERE dn.hypertable_id = h.id
ORDER BY table_name;

-- Detach should work b/c disttable_2 has no data and more data nodes
-- than replication factor
SELECT * FROM detach_data_node('data_node_2', 'disttable_2');

\set ON_ERROR_STOP 0
-- can't detach non-existing data node
SELECT * FROM detach_data_node('data_node_12345', 'disttable');
-- NULL data node
SELECT * FROM detach_data_node(NULL, 'disttable');
-- Can't detach data node_1 b/c it contains data for disttable
SELECT * FROM detach_data_node('data_node_1');
-- can't detach already detached data node
SELECT * FROM detach_data_node('data_node_2', 'disttable_2');
-- can't detach b/c of replication factor for disttable_2
SELECT * FROM detach_data_node('data_node_3', 'disttable_2');
-- can't detach non hypertable
SELECT * FROM detach_data_node('data_node_3', 'devices');
\set ON_ERROR_STOP 1

-- force detach data node to become under-replicated for new data
SELECT * FROM detach_data_node('data_node_3', 'disttable_2', force => true);

SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;
-- force detach data node with data
SELECT * FROM detach_data_node('data_node_3', 'disttable', force => true);

-- chunk and hypertable metadata should be deleted as well
SELECT * FROM _timescaledb_catalog.chunk_data_node;
SELECT table_name, node_name, block_chunks
FROM _timescaledb_catalog.hypertable_data_node dn,
_timescaledb_catalog.hypertable h
WHERE dn.hypertable_id = h.id
ORDER BY table_name;

-- detached data_node_3 should not show up any more
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

\set ON_ERROR_STOP 0
-- detaching data node with last data replica should ERROR even when forcing
SELECT * FROM detach_data_node('server_2', 'disttable', force => true);
\set ON_ERROR_STOP 1

-- drop all chunks
SELECT * FROM drop_chunks(table_name => 'disttable', older_than => '2200-01-01 00:00'::timestamptz);
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

SELECT * FROM detach_data_node('data_node_2', 'disttable', true);

-- Let's add more data nodes
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('data_node_4', host => 'localhost', database => 'data_node_4');
SELECT * FROM add_data_node('data_node_5', host => 'localhost', database => 'data_node_5');
GRANT ALL ON FOREIGN SERVER data_node_4, data_node_5 TO PUBLIC;
-- Create table as super user
SET ROLE :ROLE_SUPERUSER;
CREATE TABLE disttable_3(time timestamptz, device int, temp float);
SELECT * FROM create_distributed_hypertable('disttable_3', 'time', replication_factor => 1, data_nodes => '{"data_node_4", "data_node_5"}');

SET ROLE :ROLE_1;
CREATE TABLE disttable_4(time timestamptz, device int, temp float);
SELECT * FROM create_distributed_hypertable('disttable_4', 'time', replication_factor => 1, data_nodes => '{"data_node_4", "data_node_5"}');

\set ON_ERROR_STOP 0
-- error due to missing permissions
SELECT * FROM detach_data_node('data_node_4', 'disttable_3');
SELECT * FROM block_new_chunks('data_node_4', 'disttable_3');
SELECT * FROM allow_new_chunks('data_node_4', 'disttable_3');
\set ON_ERROR_STOP 1

-- detach table(s) where user has permissions, otherwise show NOTICE
SELECT * FROM detach_data_node('data_node_4');

-- Cleanup
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM delete_data_node('data_node_1', force =>true);
SELECT * FROM delete_data_node('data_node_2', force =>true);
SELECT * FROM delete_data_node('data_node_3', force =>true);

SET ROLE :ROLE_1;
\set ON_ERROR_STOP 0
-- Cannot delete a data node which is attached to a table that we don't
-- have owner permissions on
SELECT * FROM delete_data_node('data_node_4', force =>true);
SELECT * FROM delete_data_node('data_node_5', force =>true);
\set ON_ERROR_STOP 1
SET ROLE :ROLE_CLUSTER_SUPERUSER;
DROP TABLE disttable_3;

-- Now we should be able to delete the data nodes
SELECT * FROM delete_data_node('data_node_4', force =>true);
SELECT * FROM delete_data_node('data_node_5', force =>true);

\set ON_ERROR_STOP 0
-- Should fail because host has to be provided.
SELECT * FROM add_data_node('data_node_6');
\set ON_ERROR_STOP 1

--
-- Test timescale extension version check during add_data_node()
-- and create_distributed_hypertable() calls.
--
-- Use mock extension and create basic function wrappers to
-- establish connection to a data node.
--
RESET ROLE;
DROP DATABASE data_node_1;
CREATE DATABASE data_node_1 OWNER :ROLE_1;

\c data_node_1
CREATE SCHEMA _timescaledb_internal;
GRANT ALL ON SCHEMA _timescaledb_internal TO :ROLE_1;

CREATE FUNCTION _timescaledb_internal.set_dist_id(uuid UUID)
	RETURNS BOOL LANGUAGE PLPGSQL AS
$BODY$
BEGIN
	RETURN true;
END
$BODY$;

CREATE FUNCTION _timescaledb_internal.set_peer_dist_id(uuid UUID)
	RETURNS BOOL LANGUAGE PLPGSQL AS
$BODY$
BEGIN
	RETURN true;
END
$BODY$;

CREATE FUNCTION _timescaledb_internal.validate_as_data_node()
	RETURNS BOOL LANGUAGE PLPGSQL AS
$BODY$
BEGIN
	RETURN true;
END
$BODY$;

CREATE EXTENSION timescaledb VERSION '0.0.0';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

SELECT * FROM add_data_node('data_node_1', 'localhost', database => 'data_node_1',
                            bootstrap => false);

GRANT USAGE ON FOREIGN SERVER data_node_1 TO :ROLE_1;

SET ROLE :ROLE_1;
CREATE TABLE test_disttable(time timestamptz);

\set ON_ERROR_STOP 0
SELECT * FROM create_distributed_hypertable('test_disttable', 'time');
\set ON_ERROR_STOP 1

RESET ROLE;
DROP DATABASE data_node_1;
DROP DATABASE data_node_2;
DROP DATABASE data_node_3;
DROP DATABASE data_node_4;
DROP DATABASE data_node_5;
