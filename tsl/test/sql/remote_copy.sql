-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;
ALTER ROLE :ROLE_DEFAULT_PERM_USER PASSWORD 'perm_user_pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_PERM_USER;

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;
-- Need explicit password for non-super users to connect
ALTER ROLE :ROLE_DEFAULT_CLUSTER_USER CREATEDB PASSWORD 'pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_CLUSTER_USER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS server_1;
DROP DATABASE IF EXISTS server_2;
DROP DATABASE IF EXISTS server_3;
SET client_min_messages TO NOTICE;

CREATE DATABASE server_1;
CREATE DATABASE server_2;
CREATE DATABASE server_3;

\c server_1
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c server_2
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c server_3
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Add servers using the TimescaleDB server management API
SELECT * FROM add_server('server_1', database => 'server_1', password => 'pass', if_not_exists => true);
SELECT * FROM add_server('server_2', database => 'server_2', password => 'pass', if_not_exists => true);
SELECT * FROM add_server('server_3', database => 'server_3', password => 'pass', if_not_exists => true);

-- Use some horrible names to make sure the parsing code works
CREATE TABLE "+ri(k33_')" (       
    "thyme" bigint NOT NULL,
    "))_" double precision NOT NULL,
    "flavor" text DEFAULT 'mint',
    "pH" float DEFAULT 7.0,
    optional text
);

SELECT create_hypertable('"+ri(k33_'')"', 'thyme', partitioning_column=>'pH', number_partitions=>4, chunk_time_interval => 100, replication_factor => 2);

-- Run some successful copies
COPY "+ri(k33_')" FROM STDIN;
1	11	strawberry	2.3	stuff
\.

\copy public   	.		"+ri(k33_')" ("pH", 	"))_"   ,	thyme) fROm stdIN deLIMitER '-';
.01-40-208
10.-37-315
\.

cOpy public."+ri(k33_')" (thYme, "pH", "))_", "flavor") FrOm 
StDiN wiTH dElImITeR ','
;
15,1,403,\N
203,1.0,3.21321,something like lemon
333,1.00,2309424231,  _''garbled*(#\\)@#$*)
\.

COPY "+ri(k33_')" FROM STDIN (FORCE_NULL (flavor, "))_"), QUOTE '`', FREEZE, FORMAT csv, NULL 'empties', FORCE_NOT_NULL ("pH", "thyme"));
120321,4.4324424324254352345345,``,0,empties
4201,3333333333333333333333333333,"",1.0000000000000000000000000000000001,`empties`
342,4324,"empties",4,\N
\.

-- Run some error cases
\set ON_ERROR_STOP 0

-- Bad input
COPY "+ri(k33_')" FROM STDIN WITH DELIMITER ',';
red,white,blue,grey,teal
\.

-- Missing paritioning column
COPY "+ri(k33_')" (thYme, "))_", "flavor") FROM STDIN;
1234,\N,resentment
\.

-- Missing required column, these generate a WARNING with a transaction id in them (too flimsy to output)
SET client_min_messages TO ERROR;
COPY "+ri(k33_')" (thyme, flavor, "pH") FROM STDIN WITH DELIMITER ',';
5,blue,2.3
\.
COPY "+ri(k33_')" FROM STDIN WITH DELIMITER ',';
5,\N,blue,1,blah
\.
SET client_min_messages TO INFO;

-- Invalid data after new chunk creation, data and chunks should be rolled back
COPY "+ri(k33_')" FROM STDIN WITH DELIMITER ',';
700,7,neopolitan,7,seven
800,8,pesto,8,eight
900,9,salami,9,nine
red,white,blue,ten,hi
\.

\set ON_ERROR_STOP 1

SELECT * FROM "+ri(k33_')";
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.chunk_server;
SELECT * FROM _timescaledb_catalog.hypertable_server;
select * from show_chunks('"+ri(k33_'')"');
\c server_1
SELECT * FROM "+ri(k33_')";
select * from show_chunks('"+ri(k33_'')"');
\c server_2
SELECT * FROM "+ri(k33_')";
select * from show_chunks('"+ri(k33_'')"');
\c server_3
SELECT * FROM "+ri(k33_')";
select * from show_chunks('"+ri(k33_'')"');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

DROP TABLE "+ri(k33_')" CASCADE;
SELECT * FROM delete_server('server_1', cascade => true);
SELECT * FROM delete_server('server_2', cascade => true);
SELECT * FROM delete_server('server_3', cascade => true);
DROP DATABASE server_1;
DROP DATABASE server_2;
DROP DATABASE server_3;
