-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;
ALTER ROLE :ROLE_DEFAULT_PERM_USER PASSWORD 'perm_user_pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_PERM_USER;

-- Support for remote_exec()
\c :TEST_DBNAME :ROLE_SUPERUSER
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

CREATE OR REPLACE FUNCTION show_servers()
RETURNS TABLE(server_name NAME, host TEXT, port INT, dbname NAME)
AS :TSL_MODULE_PATHNAME, 'test_server_show' LANGUAGE C;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS server_1;
DROP DATABASE IF EXISTS server_2;
DROP DATABASE IF EXISTS server_3;
DROP DATABASE IF EXISTS server_4;
SET client_min_messages TO NOTICE;

SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Create server with DDL statements as reference. NOTE, 'IF NOT
-- EXISTS' on 'CREATE SERVER' and 'CREATE USER MAPPING' is not
-- supported on PG 9.6
CREATE SERVER server_1 FOREIGN DATA WRAPPER timescaledb_fdw
OPTIONS (host 'localhost', port '15432', dbname 'server_1');

-- Create a user mapping for the server
CREATE USER MAPPING FOR :ROLE_SUPERUSER SERVER server_1 OPTIONS (user 'cluster_user_1');

-- Add servers using TimescaleDB server management API
RESET ROLE;
SELECT * FROM add_server('server_2', database => 'server_2', local_user => :'ROLE_DEFAULT_PERM_USER', remote_user => :'ROLE_DEFAULT_PERM_USER', password => 'perm_user_pass', bootstrap_user => :'ROLE_SUPERUSER');
SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
-- Add again
SELECT * FROM add_server('server_2', password => 'perm_user_pass');
-- Add without password
SELECT * FROM add_server('server_3');
-- Add NULL server
SELECT * FROM add_server(NULL);
\set ON_ERROR_STOP 1

RESET ROLE;
-- Should not generate error with if_not_exists option
SELECT * FROM add_server('server_2', database => 'server_2', local_user => :'ROLE_DEFAULT_PERM_USER', remote_user => :'ROLE_DEFAULT_PERM_USER', password => 'perm_user_pass', bootstrap_user => :'ROLE_SUPERUSER', if_not_exists => true);

SELECT * FROM add_server('server_3', database => 'server_3', local_user => :'ROLE_DEFAULT_PERM_USER', remote_user => 'cluster_user_2', bootstrap_user => :'ROLE_SUPERUSER');
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Server exists, but no user mapping
CREATE SERVER server_4 FOREIGN DATA WRAPPER timescaledb_fdw
OPTIONS (host 'localhost', port '15432', dbname 'server_4');

-- User mapping should be added with NOTICE
RESET ROLE;
SELECT * FROM add_server('server_4', database => 'server_4', local_user => :'ROLE_DEFAULT_PERM_USER', remote_user => :'ROLE_DEFAULT_PERM_USER', password => 'perm_user_pass', bootstrap_user => :'ROLE_SUPERUSER', if_not_exists => true);
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT * FROM show_servers();

-- List foreign servers and user mappings
RESET ROLE;
SELECT srvname, srvoptions
FROM pg_foreign_server
ORDER BY srvname;

SELECT rolname, srvname, umoptions
FROM pg_user_mapping um, pg_authid a, pg_foreign_server fs
WHERE a.oid = um.umuser AND fs.oid = um.umserver
ORDER BY srvname;
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Delete a server
\set ON_ERROR_STOP 0
-- Cannot delete if not owner
SELECT * FROM delete_server('server_3');
-- Must use cascade because of user mappings
RESET ROLE;
SELECT * FROM delete_server('server_3');
\set ON_ERROR_STOP 1
-- Should work as superuser with cascade
SELECT * FROM delete_server('server_3', cascade => true);
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT srvname, srvoptions
FROM pg_foreign_server;

RESET ROLE;
SELECT rolname, srvname, umoptions
FROM pg_user_mapping um, pg_authid a, pg_foreign_server fs
WHERE a.oid = um.umuser AND fs.oid = um.umserver
ORDER BY srvname;

\set ON_ERROR_STOP 0
-- Deleting a non-existing server generates error
SELECT * FROM delete_server('server_3');
\set ON_ERROR_STOP 1

-- Deleting non-existing server with "if_exists" set does not generate error
SELECT * FROM delete_server('server_3', if_exists => true);

SELECT * FROM show_servers();

DROP SERVER server_1 CASCADE;
SELECT * FROM delete_server('server_2', cascade => true);
SELECT * FROM delete_server('server_4', cascade => true);

SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS server_1;
DROP DATABASE IF EXISTS server_2;
DROP DATABASE IF EXISTS server_4;
SET client_min_messages TO INFO;

SELECT * FROM add_server('server_1', database => 'server_1', password => 'perm_user_pass', bootstrap_user => :'ROLE_SUPERUSER');
SELECT * FROM add_server('server_2', database => 'server_2', password => 'perm_user_pass', bootstrap_user => :'ROLE_SUPERUSER');
SELECT * FROM add_server('server_4', database => 'server_4', password => 'perm_user_pass', bootstrap_user => :'ROLE_SUPERUSER');
\c :TEST_DBNAME :ROLE_SUPERUSER;

SELECT * FROM show_servers();

-- Test that servers are added to a hypertable
CREATE TABLE disttable(time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1);

-- Ensure that replication factor allows to distinguish data node hypertables from regular hypertables
SELECT replication_factor FROM _timescaledb_catalog.hypertable WHERE table_name = 'disttable';
SELECT * FROM test.remote_exec(NULL, $$ SELECT replication_factor FROM _timescaledb_catalog.hypertable WHERE table_name = 'disttable'; $$);

-- All servers should be added.
SELECT * FROM _timescaledb_catalog.hypertable_server;

-- Create one chunk
INSERT INTO disttable VALUES ('2019-02-02 10:45', 1, 23.4);

-- Chunk mapping created
SELECT * FROM _timescaledb_catalog.chunk_server;

DROP TABLE disttable;

-- server mappings should be cleaned up
SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM _timescaledb_catalog.chunk_server;

CREATE TABLE disttable(time timestamptz, device int, temp float);

\set ON_ERROR_STOP 0
-- Attach server should fail when called on a non-hypertable
SELECT * FROM attach_server('disttable', 'server_1');

-- Test some bad create_hypertable() parameter values for distributed hypertables
-- Bad replication factor
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 0, servers => '{ "server_2", "server_4" }');
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 32768);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => -1);
SELECT * FROM create_distributed_hypertable('disttable', 'time', replication_factor => -1);

-- Non-existing server
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 2, servers => '{ "server_3" }');
\set ON_ERROR_STOP 1

-- Use a subset of servers and a replication factor of two so that
-- each chunk is associated with more than one server
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 2, servers => '{ "server_2", "server_4" }');

-- Create some chunks
INSERT INTO disttable VALUES
       ('2019-02-02 10:45', 1, 23.4),
       ('2019-05-23 10:45', 4, 14.9),
       ('2019-07-23 10:45', 8, 7.6);

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM _timescaledb_catalog.chunk_server;

-- Dropping a chunk should also clean up server mappings
SELECT * FROM drop_chunks(older_than => '2019-05-22 17:18'::timestamptz);

SELECT * FROM test.show_subtables('disttable');
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.chunk_server;

SELECT * FROM _timescaledb_internal.set_chunk_default_server('_timescaledb_internal', '_hyper_3_3_dist_chunk', 'server_2');

SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

SELECT * FROM _timescaledb_internal.set_chunk_default_server('_timescaledb_internal', '_hyper_3_3_dist_chunk', 'server_4');

\set ON_ERROR_STOP 0
-- Will fail because server_2 contains chunks
SELECT * FROM delete_server('server_2', cascade => true);
-- non-existing chunk
SELECT * FROM _timescaledb_internal.set_chunk_default_server('x', 'x_chunk', 'server_4');
-- non-existing server
SELECT * FROM _timescaledb_internal.set_chunk_default_server('_timescaledb_internal', '_hyper_3_3_dist_chunk', 'server_0000');
-- NULL try
SELECT * FROM _timescaledb_internal.set_chunk_default_server(NULL, NULL, 'server_4');
\set ON_ERROR_STOP 1

-- Deleting a server removes the "foreign" chunk table(s) that
-- reference that server as "primary" and should also remove the
-- hypertable_server and chunk_server mappings for that server.  In
-- the future we might want to fallback to a replica server for those
-- chunks that have multiple servers so that the chunk is not removed
-- unnecessarily. We use force => true b/c server_2 contains chunks.
SELECT * FROM delete_server('server_2', cascade => true, force => true);

SELECT * FROM test.show_subtables('disttable');
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM _timescaledb_catalog.chunk_server;

\set ON_ERROR_STOP 0
-- can't delete b/c it's last data replica
SELECT * FROM delete_server('server_4', cascade => true, force => true);
\set ON_ERROR_STOP 1

-- Should also clean up hypertable_server when using standard DDL commands
DROP SERVER server_4 CASCADE;

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM _timescaledb_catalog.chunk_server;
SELECT * FROM _timescaledb_catalog.chunk;

-- Attach server should now succeed
SELECT * FROM attach_server('disttable', 'server_1');

SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM _timescaledb_catalog.chunk_server;

SELECT * FROM _timescaledb_internal.server_ping('server_1');

-- Create server referencing postgres_fdw
CREATE EXTENSION postgres_fdw;
CREATE SERVER pg_server_1 FOREIGN DATA WRAPPER postgres_fdw;

\set ON_ERROR_STOP 0
-- Throw ERROR for non-existing server
SELECT * FROM _timescaledb_internal.server_ping('server_123456789');
-- ERROR on NULL
SELECT * FROM _timescaledb_internal.server_ping(NULL);
-- ERROR when not passing TimescaleDB server
SELECT * FROM _timescaledb_internal.server_ping('pg_server_1');
\set ON_ERROR_STOP 1

-- Some attach server error cases
\set ON_ERROR_STOP 0
-- Invalid arguments
SELECT * FROM attach_server(NULL, 'server_1', true);
SELECT * FROM attach_server('disttable', NULL, true);

-- Deleted server
SELECT * FROM attach_server('disttable', 'server_2');

-- Attchinging to an already attached server without 'if_not_exists'
SELECT * FROM attach_server('disttable', 'server_1', false);
\set ON_ERROR_STOP 1

-- Attach if not exists
SELECT * FROM attach_server('disttable', 'server_1', true);

-- Creating a distributed hypertable without any servers should fail
DROP TABLE disttable;
CREATE TABLE disttable(time timestamptz, device int, temp float);

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1, servers => '{ }');
\set ON_ERROR_STOP 1

SELECT * FROM delete_server('server_1', cascade => true);
SELECT * FROM show_servers();

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM _timescaledb_catalog.chunk_server;
SELECT * FROM _timescaledb_catalog.chunk;

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1);
\set ON_ERROR_STOP 1

DROP DATABASE IF EXISTS server_3;
SELECT * FROM add_server('server_3', database => 'server_3');

-- Bring down the database but UserMapping should still be there
DROP DATABASE IF EXISTS server_3;
-- Return false if server is down
SELECT * FROM _timescaledb_internal.server_ping('server_3');

DROP DATABASE server_1;
DROP DATABASE server_2;
DROP DATABASE server_4;

DROP SERVER server_3 CASCADE;

-- there should be no servers
SELECT * FROM show_servers();

-- let's add some
SELECT * FROM add_server('server_1', database => 'server_1');
SELECT * FROM add_server('server_2', database => 'server_2');
SELECT * FROM add_server('server_3', database => 'server_3');

DROP TABLE disttable;

CREATE TABLE disttable(time timestamptz, device int, temp float);

SELECT * FROM create_distributed_hypertable('disttable', 'time', replication_factor => 2, servers => '{"server_1", "server_2", "server_3"}');

-- Create some chunks on all the servers
INSERT INTO disttable VALUES
       ('2019-02-02 10:45', 1, 23.4),
       ('2019-05-23 10:45', 4, 14.9),
       ('2019-07-23 10:45', 8, 7.6);

SELECT * FROM test.show_subtables('disttable');
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.hypertable_server;
SELECT * FROM _timescaledb_catalog.chunk_server;

-- Add additional hypertable
CREATE TABLE disttable_2(time timestamptz, device int, temp float);

SELECT * FROM create_distributed_hypertable('disttable_2', 'time', replication_factor => 2, servers => '{"server_1", "server_2", "server_3"}');

CREATE TABLE devices(device int, name text);

SELECT * FROM _timescaledb_catalog.hypertable_server;

-- Block one server for specific hypertable
SELECT * FROM block_new_chunks_on_server('server_1', 'disttable');

-- Block one server for all hypertables
SELECT * FROM block_new_chunks_on_server('server_1');

SELECT * FROM _timescaledb_catalog.hypertable_server;

-- insert more data
INSERT INTO disttable VALUES
       ('2019-08-02 10:45', 1, 14.4),
       ('2019-08-15 10:45', 4, 14.9),
       ('2019-08-26 10:45', 8, 17.6);

-- no new chunks on server_1
SELECT * FROM _timescaledb_catalog.chunk_server;

-- some ERROR cases
\set ON_ERROR_STOP 0
-- Will error due to under-replication
SELECT * FROM block_new_chunks_on_server('server_2');
-- can't block/allow non-existing server
SELECT * FROM block_new_chunks_on_server('server_12345', 'disttable');
SELECT * FROM allow_new_chunks_on_server('server_12345', 'disttable');
-- NULL server
SELECT * FROM block_new_chunks_on_server(NULL, 'disttable');
SELECT * FROM allow_new_chunks_on_server(NULL, 'disttable');
-- can't block/allow on non hypertable
SELECT * FROM block_new_chunks_on_server('server_1', 'devices');
SELECT * FROM allow_new_chunks_on_server('server_1', 'devices');
\set ON_ERROR_STOP 1

-- Force block all servers
SELECT * FROM block_new_chunks_on_server('server_2', force => true);
SELECT * FROM block_new_chunks_on_server('server_1', force => true);
SELECT * FROM block_new_chunks_on_server('server_3', force => true);

-- All servers are blocked
SELECT * FROM _timescaledb_catalog.hypertable_server;

\set ON_ERROR_STOP 0
-- insert should fail b/c all servers are blocked
INSERT INTO disttable VALUES ('2019-11-02 02:45', 1, 13.3);
\set ON_ERROR_STOP 1

-- unblock serves for all hypertables
SELECT * FROM allow_new_chunks_on_server('server_1');
SELECT * FROM allow_new_chunks_on_server('server_2');
SELECT * FROM allow_new_chunks_on_server('server_3');

SELECT * FROM _timescaledb_catalog.hypertable_server;

-- Detach should work b/c disttable_2 has no data
SELECT * FROM detach_server('server_2', 'disttable_2');

\set ON_ERROR_STOP 0
-- can't detach non-existing server
SELECT * FROM detach_server('server_12345', 'disttable');
-- NULL server
SELECT * FROM detach_server(NULL, 'disttable');
-- Can't detach server_1 b/c it contains data for disttable
SELECT * FROM detach_server('server_1');
-- can't detach already detached server
SELECT * FROM detach_server('server_2', 'disttable_2');
-- can't detach b/c of replication factor for disttable_2
SELECT * FROM detach_server('server_3', 'disttable_2');
-- can't detach non hypertable
SELECT * FROM detach_server('server_3', 'devices');
\set ON_ERROR_STOP 1

-- force detach server to become under-replicated for new data
SELECT * FROM detach_server('server_3', 'disttable_2', true);

SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;
-- force detach server with data
SELECT * FROM detach_server('server_3', 'disttable', true);

-- chunk and hypertable metadata should be deleted as well
SELECT * FROM _timescaledb_catalog.chunk_server;
SELECT * FROM _timescaledb_catalog.hypertable_server;
-- detached server_3 should not show up any more
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

\set ON_ERROR_STOP 0
-- detaching server with last data replica should ERROR even when forcing
SELECT * FROM detach_server('server_2', 'disttable', true);
\set ON_ERROR_STOP 1

-- drop all chunks
SELECT * FROM drop_chunks(table_name => 'disttable', older_than => '2200-01-01 00:00'::timestamptz);
SELECT foreign_table_name, foreign_server_name
FROM information_schema.foreign_tables
ORDER BY foreign_table_name;

SELECT * FROM detach_server('server_2', 'disttable', true);

-- Need explicit password for non-super users to connect
ALTER ROLE :ROLE_DEFAULT_CLUSTER_USER CREATEDB PASSWORD 'pass';

-- Let's add more servers
SELECT * FROM add_server('server_4', database => 'server_4', local_user => :'ROLE_DEFAULT_CLUSTER_USER', remote_user => :'ROLE_DEFAULT_CLUSTER_USER', password => 'pass', bootstrap_user => :'ROLE_SUPERUSER');
SELECT * FROM add_server('server_5', database => 'server_5', local_user => :'ROLE_DEFAULT_CLUSTER_USER', remote_user => :'ROLE_DEFAULT_CLUSTER_USER', password => 'pass', bootstrap_user => :'ROLE_SUPERUSER');

CREATE USER MAPPING FOR :ROLE_SUPERUSER SERVER server_4 OPTIONS (user :'ROLE_DEFAULT_CLUSTER_USER', password 'pass');
CREATE USER MAPPING FOR :ROLE_SUPERUSER SERVER server_5 OPTIONS (user :'ROLE_DEFAULT_CLUSTER_USER', password 'pass');

CREATE TABLE disttable_3(time timestamptz, device int, temp float);
SELECT * FROM create_distributed_hypertable('disttable_3', 'time', replication_factor => 1, servers => '{"server_4", "server_5"}');

SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
CREATE TABLE disttable_4(time timestamptz, device int, temp float);
SELECT * FROM create_distributed_hypertable('disttable_4', 'time', replication_factor => 1, servers => '{"server_4", "server_5"}');

\set ON_ERROR_STOP 0
-- error due to missing permissions
SELECT * FROM detach_server('server_4', 'disttable_3');
SELECT * FROM block_new_chunks_on_server('server_4', 'disttable_3');
SELECT * FROM allow_new_chunks_on_server('server_4', 'disttable_3');
\set ON_ERROR_STOP 1

-- detach table(s) where user has permissions, otherwise show NOTICE
SELECT * FROM detach_server('server_4');

-- Cleanup
RESET ROLE;
SELECT * FROM delete_server('server_1', cascade => true, force =>true);
SELECT * FROM delete_server('server_2', cascade => true, force =>true);
SELECT * FROM delete_server('server_3', cascade => true, force =>true);
SELECT * FROM delete_server('server_4', cascade => true, force =>true);
SELECT * FROM delete_server('server_5', cascade => true, force =>true);
DROP DATABASE server_1;
DROP DATABASE server_2;
DROP DATABASE server_3;
DROP DATABASE server_4;
DROP DATABASE server_5;
