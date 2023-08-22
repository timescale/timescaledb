-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\set DN_DBNAME_3 :TEST_DBNAME _3
\set DN_DBNAME_4 :TEST_DBNAME _4
\set DN_DBNAME_5 :TEST_DBNAME _5
\set DN_DBNAME_6 :TEST_DBNAME _6

-- View to see dimension partitions. Note RIGHT JOIN to see that
-- dimension partitions are cleaned up (deleted) properly.
CREATE VIEW hypertable_partitions AS
SELECT table_name, dimension_id, range_start, data_nodes
FROM _timescaledb_catalog.hypertable h
INNER JOIN _timescaledb_catalog.dimension d ON (d.hypertable_id = h.id)
RIGHT JOIN _timescaledb_catalog.dimension_partition dp ON (dp.dimension_id = d.id)
ORDER BY dimension_id, range_start;
GRANT SELECT ON hypertable_partitions TO :ROLE_1;

-- Add data nodes using TimescaleDB data_node management API.
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_2', 'localhost', database => :'DN_DBNAME_2');
\set ON_ERROR_STOP 0
-- Add again
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
-- No host provided
SELECT * FROM add_data_node('data_node_99');
SELECT * FROM add_data_node(NULL);
-- Add NULL data_node
SELECT * FROM add_data_node(NULL, host => 'localhost');
SELECT * FROM add_data_node(NULL, NULL);

-- Test invalid port numbers
SELECT * FROM add_data_node('data_node_3', 'localhost',
                            port => 65536,
                            database => :'DN_DBNAME_3');
SELECT * FROM add_data_node('data_node_3', 'localhost',
                            port => 0,
                            database => :'DN_DBNAME_3');
SELECT * FROM add_data_node('data_node_3', 'localhost',
                            port => -1,
                            database => :'DN_DBNAME_3');

SELECT inet_server_port() as PGPORT \gset

-- Adding a data node via ADD SERVER is blocked
CREATE SERVER data_node_4 FOREIGN DATA WRAPPER timescaledb_fdw
OPTIONS (host 'localhost', port ':PGPORT', dbname :'DN_DBNAME_4');
-- Dropping a data node via DROP SERVER is also blocked
DROP SERVER data_node_1, data_node_2;
\set ON_ERROR_STOP 1

-- Should not generate error with if_not_exists option
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2',
                            if_not_exists => true);

SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_3', host => 'localhost', database => :'DN_DBNAME_3');


-- Altering the host, dbname, and port should work via ALTER SERVER
BEGIN;
ALTER SERVER data_node_3 OPTIONS (SET host 'data_node_3', SET dbname 'new_db_name', SET port '9999');
SELECT srvname, srvoptions FROM pg_foreign_server WHERE srvname = 'data_node_3';
-- Altering the name should work
ALTER SERVER data_node_3 RENAME TO data_node_4;
SELECT srvname FROM pg_foreign_server WHERE srvname = 'data_node_4';
-- Revert name and options
ROLLBACK;

\set ON_ERROR_STOP 0
-- Should not be possible to set a version:
ALTER SERVER data_node_3 VERSION '2';
-- Should not be possible to set "available"
ALTER SERVER data_node_3 OPTIONS (SET available 'true');
\set ON_ERROR_STOP 1

-- Make sure changing server owner is allowed
ALTER SERVER data_node_1 OWNER TO CURRENT_USER;

-- List foreign data nodes
SELECT node_name FROM timescaledb_information.data_nodes ORDER BY node_name;

SELECT * FROM delete_data_node('data_node_3');

-- List data nodes
SELECT node_name FROM timescaledb_information.data_nodes ORDER BY node_name;

\set ON_ERROR_STOP 0
-- Deleting a non-existing data node generates error
SELECT * FROM delete_data_node('data_node_3');
\set ON_ERROR_STOP 1

-- Deleting non-existing data node with "if_exists" set does not generate error
SELECT * FROM delete_data_node('data_node_3', if_exists => true);

SELECT node_name FROM timescaledb_information.data_nodes ORDER BY node_name;

SELECT * FROM delete_data_node('data_node_1');
SELECT * FROM delete_data_node('data_node_2');

-- No data nodes left
SELECT node_name FROM timescaledb_information.data_nodes ORDER BY node_name;
SELECT * FROM hypertable_partitions;

-- Cleanup databases
RESET ROLE;
SET client_min_messages TO ERROR;
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
SET client_min_messages TO INFO;

SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_3', host => 'localhost', database => :'DN_DBNAME_3');
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
SET ROLE :ROLE_1;

-- Create a distributed hypertable where no nodes can be selected
-- because there are no data nodes with the right permissions.
CREATE TABLE disttable(time timestamptz, device int, temp float);
\set ON_ERROR_STOP 0
\set VERBOSITY default
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device');
\set VERBOSITY terse
\set ON_ERROR_STOP 1

RESET ROLE;

-- Allow ROLE_1 to create distributed tables on these data nodes.
-- We'll test that data_node_3 is properly filtered when ROLE_1
-- creates a distributed hypertable without explicitly specifying
-- data_node_2.
GRANT USAGE
   ON FOREIGN SERVER data_node_1, data_node_2
   TO :ROLE_1;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;

SELECT node_name
  FROM timescaledb_information.data_nodes
ORDER BY node_name;

SELECT object_name, object_type, ARRAY_AGG(privilege_type)
FROM information_schema.role_usage_grants
WHERE object_schema NOT IN ('information_schema','pg_catalog')
  AND object_type LIKE 'FOREIGN%'
GROUP BY object_schema, object_name, object_type
ORDER BY object_name, object_type;

SET ROLE :ROLE_1;

-- Test that all data nodes are added to a hypertable and that the
-- slices in the device dimension equals the number of data nodes.
BEGIN;
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device');

SELECT column_name, num_slices
FROM _timescaledb_catalog.dimension
WHERE column_name = 'device';

-- Dimension partitions should be created
SELECT * FROM hypertable_partitions;

-- All data nodes with USAGE should be added.
SELECT hdn.node_name
FROM _timescaledb_catalog.hypertable_data_node hdn, _timescaledb_catalog.hypertable h
WHERE h.table_name = 'disttable' AND hdn.hypertable_id = h.id;

ROLLBACK;

-- There should be an ERROR if we explicitly try to use a data node we
-- don't have permission to use.
\set ON_ERROR_STOP 0
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device',
                                            data_nodes => '{ data_node_1, data_node_2, data_node_3 }');
\set ON_ERROR_STOP 1

RESET ROLE;
-- Now let ROLE_1 use data_node_3
GRANT USAGE
   ON FOREIGN SERVER data_node_3
   TO :ROLE_1;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
SET ROLE :ROLE_1;

-- Now specify less slices than there are data nodes to generate a
-- warning
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 2);

-- All data nodes should be added.
SELECT hdn.node_name
FROM _timescaledb_catalog.hypertable_data_node hdn, _timescaledb_catalog.hypertable h
WHERE h.table_name = 'disttable' AND hdn.hypertable_id = h.id;

-- Ensure that replication factor allows to distinguish data node hypertables from regular hypertables
SELECT replication_factor FROM _timescaledb_catalog.hypertable WHERE table_name = 'disttable';
SELECT * FROM test.remote_exec(NULL, $$ SELECT replication_factor
FROM _timescaledb_catalog.hypertable WHERE table_name = 'disttable'; $$);

-- Create one chunk
INSERT INTO disttable VALUES ('2019-02-02 10:45', 1, 23.4);

-- Chunk mapping created
SELECT node_name FROM timescaledb_information.data_nodes ORDER BY node_name;

DROP TABLE disttable;

-- data node mappings should be cleaned up
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;
SELECT * FROM hypertable_partitions;

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
SELECT * FROM drop_chunks('disttable', older_than => '2019-05-22 17:18'::timestamptz);

SELECT * FROM test.show_subtables('disttable');
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

SELECT table_name, node_name
FROM _timescaledb_catalog.chunk c,
_timescaledb_catalog.chunk_data_node cdn
WHERE c.id = cdn.chunk_id;

-- Setting the same data node should do nothing and return false
SELECT * FROM _timescaledb_functions.set_chunk_default_data_node('_timescaledb_internal._dist_hyper_3_3_chunk', 'data_node_3');

-- Should update the default data node and return true
SELECT * FROM _timescaledb_functions.set_chunk_default_data_node('_timescaledb_internal._dist_hyper_3_3_chunk', 'data_node_2');

SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

-- Reset the default data node
SELECT * FROM _timescaledb_functions.set_chunk_default_data_node('_timescaledb_internal._dist_hyper_3_3_chunk', 'data_node_3');

\set ON_ERROR_STOP 0
-- Will fail because data_node_2 contains chunks
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM delete_data_node('data_node_2');
-- non-existing chunk
SELECT * FROM _timescaledb_functions.set_chunk_default_data_node('x_chunk', 'data_node_3');
-- non-existing data node
SELECT * FROM _timescaledb_functions.set_chunk_default_data_node('_timescaledb_internal._dist_hyper_3_3_chunk', 'data_node_0000');
-- data node exists but does not store the chunk
SELECT * FROM _timescaledb_functions.set_chunk_default_data_node('_timescaledb_internal._dist_hyper_3_3_chunk', 'data_node_1');
-- NULL try
SELECT * FROM _timescaledb_functions.set_chunk_default_data_node(NULL, 'data_node_3');
\set ON_ERROR_STOP 1

-- Deleting a data node removes the "foreign" chunk table(s) that
-- reference that data node as "primary" and should also remove the
-- hypertable_data_node and chunk_data_node mappings for that data
-- node.  In the future we might want to fallback to a replica data
-- node for those chunks that have multiple data nodes so that the
-- chunk is not removed unnecessarily. We use force => true b/c
-- data_node_2 contains chunks.  Dimension partitions should also be
-- updated to reflect the loss of the data node.
SELECT * FROM hypertable_partitions;
SELECT * FROM delete_data_node('data_node_2', force => true);
SELECT * FROM hypertable_partitions;

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

SELECT * FROM _timescaledb_functions.ping_data_node('data_node_1');

-- Ensure timeout returned by argument
SELECT * FROM _timescaledb_functions.ping_data_node('data_node_1', interval '0s');
SELECT * FROM _timescaledb_functions.ping_data_node('data_node_1', interval '3s');
SELECT * FROM _timescaledb_functions.ping_data_node('data_node_1', interval '1 day');

-- Create data node referencing postgres_fdw
RESET ROLE;
CREATE EXTENSION postgres_fdw;
CREATE SERVER pg_server_1 FOREIGN DATA WRAPPER postgres_fdw;
SET ROLE :ROLE_1;

CREATE TABLE standalone(time TIMESTAMPTZ, device INT, value FLOAT);
SELECT * FROM create_hypertable('standalone','time');
\set ON_ERROR_STOP 0
-- Throw ERROR for non-existing data node
SELECT * FROM _timescaledb_functions.ping_data_node('data_node_123456789');
-- ERROR on NULL
SELECT * FROM _timescaledb_functions.ping_data_node(NULL);
-- ERROR when not passing TimescaleDB data node
SELECT * FROM _timescaledb_functions.ping_data_node('pg_data_node_1');
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

-- Attaching a data node to another data node
\c :DN_DBNAME_1
SELECT * FROM attach_data_node('data_node_4', 'disttable');
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SET ROLE :ROLE_1;

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
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_4', host => 'localhost', database => :'DN_DBNAME_4',
                            if_not_exists => true);
-- Now let ROLE_1 use data_node_4 since it owns this "disttable"
GRANT USAGE
   ON FOREIGN SERVER data_node_4
   TO :ROLE_1;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
SELECT * FROM attach_data_node('data_node_4', 'disttable');
-- Recheck that ownership on data_node_4 is proper
SELECT * FROM test.remote_exec(NULL, $$ SELECT tablename, tableowner from pg_catalog.pg_tables where tablename = 'disttable'; $$);

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
SELECT node_name FROM timescaledb_information.data_nodes ORDER BY node_name;

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
SELECT * FROM _timescaledb_catalog.chunk_data_node;
SELECT * FROM _timescaledb_catalog.chunk;

\set ON_ERROR_STOP 0
-- No data nodes remain, so should fail
SELECT * FROM create_distributed_hypertable('disttable', 'time');
\set ON_ERROR_STOP 1

-- These data nodes have been deleted, so safe to remove their databases.
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
DROP DATABASE :DN_DBNAME_4;

-- there should be no data nodes
SELECT node_name FROM timescaledb_information.data_nodes ORDER BY node_name;

-- let's add some
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_3', host => 'localhost', database => :'DN_DBNAME_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
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
SELECT * FROM hypertable_partitions;

-- Add additional hypertable
CREATE TABLE disttable_2(time timestamptz, device int, temp float);

SELECT * FROM create_distributed_hypertable('disttable_2', 'time', 'device', 2, replication_factor => 2, data_nodes => '{"data_node_1", "data_node_2", "data_node_3"}');

CREATE TABLE devices(device int, name text);

SELECT * FROM _timescaledb_catalog.hypertable_data_node;

-- Block one data node for specific hypertable
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_1', 'disttable');

-- Block one data node for all hypertables
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_1');

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
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_2');
-- can't block/allow non-existing data node
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_12345', 'disttable');
SELECT * FROM timescaledb_experimental.allow_new_chunks('data_node_12345', 'disttable');
-- NULL data node
SELECT * FROM timescaledb_experimental.block_new_chunks(NULL, 'disttable');
SELECT * FROM timescaledb_experimental.allow_new_chunks(NULL, 'disttable');
-- can't block/allow on non hypertable
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_1', 'devices');
SELECT * FROM timescaledb_experimental.allow_new_chunks('data_node_1', 'devices');
\set ON_ERROR_STOP 1

-- Force block all data nodes. Dimension partition information should
-- be updated to elide blocked data nodes
SELECT * FROM hypertable_partitions;
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_2', force => true);
SELECT * FROM hypertable_partitions;
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_1', force => true);
SELECT * FROM hypertable_partitions;
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_3', force => true);
SELECT * FROM hypertable_partitions;

-- All data nodes are blocked
SELECT * FROM _timescaledb_catalog.hypertable_data_node;

\set ON_ERROR_STOP 0
-- insert should fail b/c all data nodes are blocked
INSERT INTO disttable VALUES ('2019-11-02 02:45', 1, 13.3);
\set ON_ERROR_STOP 1

-- unblock data nodes for all hypertables
SELECT * FROM timescaledb_experimental.allow_new_chunks('data_node_1');
SELECT * FROM hypertable_partitions;
SELECT * FROM timescaledb_experimental.allow_new_chunks('data_node_2');
SELECT * FROM hypertable_partitions;
SELECT * FROM timescaledb_experimental.allow_new_chunks('data_node_3');
SELECT * FROM hypertable_partitions;

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
SELECT * FROM detach_data_node('data_node_2', 'disttable_2', if_attached => false);
-- can't detach b/c of replication factor for disttable_2
SELECT * FROM detach_data_node('data_node_3', 'disttable_2');
-- can't detach non hypertable
SELECT * FROM detach_data_node('data_node_3', 'devices');
\set ON_ERROR_STOP 1

-- do nothing if node is not attached
SELECT * FROM detach_data_node('data_node_2', 'disttable_2', if_attached => true);

-- force detach data node to become under-replicated for new data
SELECT * FROM hypertable_partitions;
SELECT * FROM detach_data_node('data_node_3', 'disttable_2', force => true);
-- Dimension partitions should be updated to no longer list the data
-- node for the hypertable
SELECT * FROM hypertable_partitions;

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
SELECT * FROM drop_chunks('disttable', older_than => '2200-01-01 00:00'::timestamptz);
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

SELECT * FROM detach_data_node('data_node_2', 'disttable', force => true);

-- Let's add more data nodes
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_4', host => 'localhost', database => :'DN_DBNAME_4');
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_5', host => 'localhost', database => :'DN_DBNAME_5');
GRANT ALL ON FOREIGN SERVER data_node_4, data_node_5 TO PUBLIC;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
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
SELECT * FROM timescaledb_experimental.block_new_chunks('data_node_4', 'disttable_3');
SELECT * FROM timescaledb_experimental.allow_new_chunks('data_node_4', 'disttable_3');
\set ON_ERROR_STOP 1

-- detach table(s) where user has permissions, otherwise show
-- NOTICE. Drop hypertables on data nodes in the process. Drop a
-- hypertable on a data node manually to ensure the command can handle
-- non-existing hypertables.
CREATE TABLE disttable_5(time timestamptz, device int, temp float);
SELECT * FROM create_distributed_hypertable('disttable_5', 'time', replication_factor => 1, data_nodes => '{"data_node_4", "data_node_5" }');
SELECT * FROM test.remote_exec('{ data_node_4 }', $$ SELECT hypertable_name, owner FROM timescaledb_information.hypertables; $$);
CALL distributed_exec($$ DROP TABLE disttable_4 $$, '{ data_node_4 }');
RESET ROLE;
CALL distributed_exec(format('ALTER TABLE disttable_5 OWNER TO %s', :'ROLE_CLUSTER_SUPERUSER'), '{ data_node_4 }');
SET ROLE :ROLE_1;
-- test first detach without permission to drop the remote data
\set ON_ERROR_STOP 0
SELECT * FROM detach_data_node('data_node_4', drop_remote_data => true);
\set ON_ERROR_STOP 1

-- detach should work with permissions
RESET ROLE;
CALL distributed_exec(format('ALTER TABLE disttable_5 OWNER TO %s', :'ROLE_1'), '{ data_node_4 }');
SET ROLE :ROLE_1;
SELECT * FROM detach_data_node('data_node_4', drop_remote_data => true);

-- Hypertables user had permissions for should be dropped on data nodes
SELECT * FROM test.remote_exec('{ data_node_4 }', $$ SELECT hypertable_name, owner FROM timescaledb_information.hypertables; $$);

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
DROP DATABASE :DN_DBNAME_1;
CREATE DATABASE :DN_DBNAME_1 OWNER :ROLE_1;

\c :DN_DBNAME_1
CREATE SCHEMA _timescaledb_functions;
GRANT ALL ON SCHEMA _timescaledb_functions TO :ROLE_1;

CREATE FUNCTION _timescaledb_functions.set_dist_id(uuid UUID)
    RETURNS BOOL LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RETURN true;
END
$BODY$;

CREATE FUNCTION _timescaledb_functions.set_peer_dist_id(uuid UUID)
    RETURNS BOOL LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RETURN true;
END
$BODY$;

CREATE FUNCTION _timescaledb_functions.validate_as_data_node()
    RETURNS BOOL LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RETURN true;
END
$BODY$;

CREATE EXTENSION timescaledb VERSION '0.0.0';

\c :TEST_DBNAME :ROLE_SUPERUSER;

\set ON_ERROR_STOP 0
SELECT * FROM add_data_node('data_node_1', 'localhost', database => :'DN_DBNAME_1',
                            bootstrap => false);

-- Testing that it is not possible to use oneself as a data node. This
-- is not allowed since it would create a cycle.
--
-- We need to use the same owner for this connection as the extension
-- owner here to avoid triggering another error.
--
-- We cannot use default verbosity here for debugging since the
-- version number is printed in some of the notices.
SELECT * FROM add_data_node('data_node_99', host => 'localhost');
\set ON_ERROR_STOP 1


-- Test adding bootstrapped data node where extension owner is different from user adding a data node
SET ROLE :ROLE_CLUSTER_SUPERUSER;
CREATE DATABASE :DN_DBNAME_6;
\c :DN_DBNAME_6
SET client_min_messages = ERROR;
-- Creating an extension as superuser
CREATE EXTENSION timescaledb;
RESET client_min_messages;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SET ROLE :ROLE_3;
-- Trying to add data node as non-superuser without GRANT on the
-- foreign data wrapper will fail.
\set ON_ERROR_STOP 0
SELECT * FROM add_data_node('data_node_6', host => 'localhost', database => :'DN_DBNAME_6');
\set ON_ERROR_STOP 1

RESET ROLE;
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_3;
SET ROLE :ROLE_3;

\set ON_ERROR_STOP 0
-- ROLE_3 doesn't have a password in the passfile and has not way to
-- authenticate so adding a data node will still fail.
SELECT * FROM add_data_node('data_node_6', host => 'localhost', database => :'DN_DBNAME_6');
\set ON_ERROR_STOP 1

-- Providing the password on the command line should work
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_6', host => 'localhost', database => :'DN_DBNAME_6', password => :'ROLE_3_PASS');

SELECT * FROM delete_data_node('data_node_6');

RESET ROLE;
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
DROP DATABASE :DN_DBNAME_4;
DROP DATABASE :DN_DBNAME_5;
DROP DATABASE :DN_DBNAME_6;


-----------------------------------------------
-- Test alter_data_node()
-----------------------------------------------
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
SELECT node_name, database, node_created, database_created, extension_created FROM add_data_node('data_node_3', host => 'localhost', database => :'DN_DBNAME_3');

GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO :ROLE_1;
GRANT CREATE ON SCHEMA public TO :ROLE_1;
SET ROLE :ROLE_1;

CREATE TABLE hyper1 (time timestamptz, location int, temp float);
CREATE TABLE hyper2 (LIKE hyper1);
CREATE TABLE hyper3 (LIKE hyper1);
CREATE TABLE hyper_1dim (LIKE hyper1);

SELECT create_distributed_hypertable('hyper1', 'time', 'location', replication_factor=>1);
SELECT create_distributed_hypertable('hyper2', 'time', 'location', replication_factor=>2);
SELECT create_distributed_hypertable('hyper3', 'time', 'location', replication_factor=>3);
SELECT create_distributed_hypertable('hyper_1dim', 'time', chunk_time_interval=>interval '2 days', replication_factor=>3);

SELECT setseed(1);
INSERT INTO hyper1
SELECT t, (abs(timestamp_hash(t::timestamp)) % 3) + 1, random() * 30
FROM generate_series('2022-01-01 00:00:00'::timestamptz, '2022-01-05 00:00:00', '1 h') t;

INSERT INTO hyper2 SELECT * FROM hyper1;
INSERT INTO hyper3 SELECT * FROM hyper1;
INSERT INTO hyper_1dim SELECT * FROM hyper1;

-- create view to see the data nodes and default data node of all
-- chunks
CREATE VIEW chunk_query_data_node AS
SELECT ch.hypertable_name, format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass AS chunk, ch.data_nodes, fs.srvname default_data_node
	   FROM timescaledb_information.chunks ch
	   INNER JOIN pg_foreign_table ft ON (format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass = ft.ftrelid)
	   INNER JOIN pg_foreign_server fs ON (ft.ftserver = fs.oid)
	   ORDER BY 1, 2;

SELECT * FROM chunk_query_data_node;

-- test alter_data_node permissions
\set ON_ERROR_STOP 0
-- must be owner to alter a data node
SELECT * FROM alter_data_node('data_node_1', available=>false);
SELECT * FROM alter_data_node('data_node_1', port=>8989);
\set ON_ERROR_STOP 1

-- query some data from all hypertables to show its working before
-- simulating the node being down
SELECT time, location FROM hyper1 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper2 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper3 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper_1dim ORDER BY time LIMIT 1;

-- simulate a node being down by renaming the database for
-- data_node_1, but for that to work we need to reconnect the backend
-- to clear out the connection cache
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
ALTER DATABASE :DN_DBNAME_1 RENAME TO data_node_1_unavailable;
\set ON_ERROR_STOP 0
SELECT time, location FROM hyper1 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper2 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper3 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper_1dim ORDER BY time LIMIT 1;
\set ON_ERROR_STOP 1

-- alter the node as not available
SELECT * FROM alter_data_node('data_node_1', available=>false);

-- the node that is not available for reads should no longer be
-- query data node for chunks, except for those that have no
-- alternative (i.e., the chunk only has one data node).
SELECT * FROM chunk_query_data_node;

-- queries should work again, except on hyper1 which has no
-- replication
\set ON_ERROR_STOP 0
SELECT time, location FROM hyper1 ORDER BY time LIMIT 1;
\set ON_ERROR_STOP 1
SELECT time, location FROM hyper2 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper3 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper_1dim ORDER BY time LIMIT 1;

-- inserts should continue to work and should go to the "live"
-- datanodes
INSERT INTO hyper3 VALUES ('2022-01-03 00:00:00', 1, 1);
INSERT INTO hyper3 VALUES ('2022-01-03 00:00:05', 1, 1);
INSERT INTO hyper_1dim VALUES ('2022-01-03 00:00:00', 1, 1);
INSERT INTO hyper_1dim VALUES ('2022-01-03 00:00:05', 1, 1);

-- Check that the metadata on the AN removes the association with
-- the "unavailable" DN for existing chunks that are being written into
-- above
SELECT * FROM chunk_query_data_node WHERE hypertable_name IN ('hyper3', 'hyper_1dim');

-- Also, inserts should work if going to a new chunk
INSERT INTO hyper3 VALUES ('2022-01-10 00:00:00', 1, 1);
INSERT INTO hyper3 VALUES ('2022-01-10 00:00:05', 1, 1);
INSERT INTO hyper_1dim VALUES ('2022-01-10 00:00:00', 1, 1);
INSERT INTO hyper_1dim VALUES ('2022-01-10 00:00:05', 1, 1);

-- Also check that new chunks only use the "available" DNs
SELECT * FROM chunk_query_data_node WHERE hypertable_name IN ('hyper3', 'hyper_1dim');

-- Updates/Deletes should also work
UPDATE hyper3 SET temp = 10 WHERE time = '2022-01-03 00:00:00';
SELECT * FROM hyper3 WHERE time = '2022-01-03 00:00:00';
UPDATE hyper3 SET temp = 10 WHERE time = '2022-01-03 00:00:05';
SELECT * FROM hyper3 WHERE time = '2022-01-03 00:00:05';
UPDATE hyper_1dim SET temp = 10 WHERE time = '2022-01-03 00:00:00';
SELECT * FROM hyper_1dim WHERE time = '2022-01-03 00:00:00';
UPDATE hyper_1dim SET temp = 10 WHERE time = '2022-01-03 00:00:05';
SELECT * FROM hyper_1dim WHERE time = '2022-01-03 00:00:05';
DELETE FROM hyper3 WHERE time = '2022-01-03 00:00:00';
DELETE FROM hyper3 WHERE time = '2022-01-03 00:00:05';
SELECT * FROM hyper3 WHERE time = '2022-01-03 00:00:00';
SELECT * FROM hyper3 WHERE time = '2022-01-03 00:00:05';
DELETE FROM hyper_1dim WHERE time = '2022-01-03 00:00:00';
DELETE FROM hyper_1dim WHERE time = '2022-01-03 00:00:05';
SELECT * FROM hyper_1dim WHERE time = '2022-01-03 00:00:00';
SELECT * FROM hyper_1dim WHERE time = '2022-01-03 00:00:05';

-- Inserts directly into chunks using FDW should also work and should go to the
-- available DNs appropriately
INSERT INTO _timescaledb_internal._dist_hyper_12_24_chunk VALUES ('2022-01-11 00:00:00', 1, 1);
INSERT INTO _timescaledb_internal._dist_hyper_13_25_chunk VALUES ('2022-01-11 00:00:00', 1, 1);
SELECT * FROM test.remote_exec(ARRAY['data_node_2', 'data_node_3'], $$ SELECT * FROM _timescaledb_internal._dist_hyper_12_24_chunk WHERE time = '2022-01-11 00:00:00'; $$);
SELECT * FROM test.remote_exec(ARRAY['data_node_2', 'data_node_3'], $$ SELECT * FROM _timescaledb_internal._dist_hyper_13_25_chunk WHERE time = '2022-01-11 00:00:00'; $$);

SELECT * FROM chunk_query_data_node WHERE hypertable_name IN ('hyper3', 'hyper_1dim');
SELECT hypertable_name, chunk_name, data_nodes FROM timescaledb_information.chunks
WHERE hypertable_name IN ('hyper3', 'hyper_1dim')
AND range_start::timestamptz <= '2022-01-10 00:00:00'
AND range_end::timestamptz > '2022-01-10 00:00:00'
ORDER BY 1, 2;

-- DDL should error out even if one DN is unavailable
\set ON_ERROR_STOP 0
ALTER TABLE hyper3 ADD COLUMN temp2 int;
ALTER TABLE hyper_1dim ADD COLUMN temp2 int;
\set ON_ERROR_STOP 1

-- Mark all DNs unavailable. Metadata should still retain last DN but all
-- activity should fail
SELECT * FROM alter_data_node('data_node_2', available=>false);
SELECT * FROM alter_data_node('data_node_3', available=>false);
\set ON_ERROR_STOP 0
INSERT INTO hyper3 VALUES ('2022-01-10 00:00:00', 1, 1);
INSERT INTO hyper_1dim VALUES ('2022-01-10 00:00:00', 1, 1);
UPDATE hyper3 SET temp = 10 WHERE time = '2022-01-03 00:00:00';
UPDATE hyper_1dim SET temp = 10 WHERE time = '2022-01-03 00:00:00';
DELETE FROM hyper3 WHERE time = '2022-01-03 00:00:00';
DELETE FROM hyper_1dim WHERE time = '2022-01-03 00:00:00';
SELECT count(*) FROM hyper3;
SELECT count(*) FROM hyper_1dim;
ALTER TABLE hyper3 ADD COLUMN temp2 int;
ALTER TABLE hyper_1dim ADD COLUMN temp2 int;
\set ON_ERROR_STOP 1

-- re-enable the data node and the chunks should "switch back" to
-- using the data node. However, only the chunks for which the node is
-- "primary" should switch to using the data node for queries
ALTER DATABASE data_node_1_unavailable RENAME TO :DN_DBNAME_1;
SELECT * FROM alter_data_node('data_node_1', available=>true);
SELECT * FROM alter_data_node('data_node_2', available=>true);
SELECT * FROM alter_data_node('data_node_3', available=>true);
SELECT * FROM chunk_query_data_node;

--queries should work again on all tables
SELECT time, location FROM hyper1 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper2 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper3 ORDER BY time LIMIT 1;
SELECT time, location FROM hyper_1dim ORDER BY time LIMIT 1;

-- DDL should also work again
ALTER TABLE hyper3 ADD COLUMN temp2 int;
ALTER TABLE hyper_1dim ADD COLUMN temp2 int;

-- save old port so that we can restore connectivity after we test
-- changing the connection information for the data node
WITH options AS (
	 SELECT unnest(options) opt
	 FROM timescaledb_information.data_nodes
	 WHERE node_name = 'data_node_1'
)
SELECT split_part(opt, '=', 2) AS old_port
FROM options WHERE opt LIKE 'port%' \gset

-- also test altering host, port and database
SELECT node_name, options FROM timescaledb_information.data_nodes order by node_name;
SELECT * FROM alter_data_node('data_node_1', available=>true, host=>'foo.bar', port=>8989, database=>'new_db');

SELECT node_name, options FROM timescaledb_information.data_nodes order by node_name;
-- just show current options:
SELECT * FROM alter_data_node('data_node_1');

DROP TABLE hyper1;
DROP TABLE hyper2;
DROP TABLE hyper3;
DROP TABLE hyper_1dim;

\set ON_ERROR_STOP 0
-- test some error cases
SELECT * FROM alter_data_node(NULL);
SELECT * FROM alter_data_node('does_not_exist');
SELECT * FROM alter_data_node('data_node_1', port=>89000);
-- cannot delete data node with "drop_database" since configuration is wrong
SELECT delete_data_node('data_node_1', drop_database=>true);
\set ON_ERROR_STOP 1

-- restore configuration for data_node_1
SELECT * FROM alter_data_node('data_node_1', host=>'localhost', port=>:old_port, database=>:'DN_DBNAME_1');
SELECT node_name, options FROM timescaledb_information.data_nodes order by node_name;

DROP VIEW chunk_query_data_node;

-- create new session to clear out connection cache
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT delete_data_node('data_node_1', drop_database=>true);
SELECT delete_data_node('data_node_2', drop_database=>true);
SELECT delete_data_node('data_node_3', drop_database=>true);
