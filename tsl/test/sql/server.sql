-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Timescale License,
-- see LICENSE-TIMESCALE at the top of the tsl directory.

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_PERM_USER;

CREATE OR REPLACE FUNCTION show_servers()
RETURNS TABLE(server_name NAME, host TEXT, port INT, dbname NAME)
AS :TSL_MODULE_PATHNAME, 'test_server_show' LANGUAGE C;

SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Create server with DDL statements as reference. NOTE, 'IF NOT
-- EXISTS' on 'CREATE SERVER' and 'CREATE USER MAPPING' is not
-- supported on PG 9.6
CREATE SERVER server_1 FOREIGN DATA WRAPPER timescaledb_fdw
OPTIONS (host 'localhost', port '5432', dbname 'server_1');

-- Create a user mapping for the server
CREATE USER MAPPING FOR :ROLE_SUPERUSER SERVER server_1 OPTIONS (user 'cluster_user_1');

-- Add servers using TimescaleDB server management API
SELECT * FROM add_server('server_2');

\set ON_ERROR_STOP 0
-- Add again
SELECT * FROM add_server('server_2');

-- Add NULL server
SELECT * FROM add_server(NULL);
\set ON_ERROR_STOP 1

-- Should not generate error with if_not_exists option
SELECT * FROM add_server('server_2', if_not_exists => true);

SELECT * FROM add_server('server_3', host => '192.168.3.4', database => 'server_2', remote_user => 'cluster_user_2');

-- Server exists, but no user mapping
CREATE SERVER server_4 FOREIGN DATA WRAPPER timescaledb_fdw
OPTIONS (host 'localhost', port '5432', dbname 'server_4');

-- User mapping should be added with NOTICE
SELECT * FROM add_server('server_4', if_not_exists => true);

SELECT * FROM show_servers();

-- Should show up in view
SELECT server_name, options FROM timescaledb_information.server
ORDER BY server_name;

-- List foreign servers and user mappings
SELECT srvname, srvoptions
FROM pg_foreign_server
ORDER BY srvname;

-- Need super user permissions to list user mappings
RESET ROLE;

SELECT rolname, srvname, umoptions
FROM pg_user_mapping um, pg_authid a, pg_foreign_server fs
WHERE a.oid = um.umuser AND fs.oid = um.umserver
ORDER BY srvname;
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Delete a server
\set ON_ERROR_STOP 0
-- Must use cascade because of user mappings
SELECT * FROM delete_server('server_3');
\set ON_ERROR_STOP 1

SELECT * FROM delete_server('server_3', cascade => true);

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

-- Test that servers are added to a hypertable
CREATE TABLE disttable(time timestamptz, device int, temp float);

SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1);

-- All servers should be added. Remote hypertable ID should be NULL since not set yet
SELECT * FROM _timescaledb_catalog.hypertable_server;

DROP TABLE disttable;

CREATE TABLE disttable(time timestamptz, device int, temp float);

-- Test some bad create_hypertable() parameter values for distributed hypertables
\set ON_ERROR_STOP 0
-- Bad replication factor
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 0, servers => '{ "server_2", "server_4" }');
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 32768);
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => -1);

-- Non-existing server
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1, servers => '{ "server_3" }');
\set ON_ERROR_STOP 1

-- Use a subset of servers
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1, servers => '{ "server_2", "server_4" }');

SELECT * FROM _timescaledb_catalog.hypertable_server;

-- Deleting a server should remove the hypertable_server mappings for that server
SELECT * FROM delete_server('server_2', cascade => true);

SELECT * FROM _timescaledb_catalog.hypertable_server;

-- Should also clean up hypertable_server when using standard DDL commands
DROP SERVER server_4 CASCADE;

SELECT * FROM _timescaledb_catalog.hypertable_server;

-- Creating a distributed hypertable without any servers should fail
DROP TABLE disttable;
CREATE TABLE disttable(time timestamptz, device int, temp float);

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1, servers => '{ }');
\set ON_ERROR_STOP 1

SELECT * FROM delete_server('server_1', cascade => true);
SELECT * FROM show_servers();

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('disttable', 'time', replication_factor => 1);
\set ON_ERROR_STOP 1
