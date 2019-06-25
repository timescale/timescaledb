-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_SUPERUSER;
ALTER ROLE :ROLE_DEFAULT_PERM_USER PASSWORD 'perm_user_pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_PERM_USER;

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_SUPERUSER;
-- Need explicit password for non-super users to connect
ALTER ROLE :ROLE_DEFAULT_CLUSTER_USER CREATEDB PASSWORD 'pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_CLUSTER_USER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;
SET client_min_messages TO NOTICE;

CREATE DATABASE data_node_1;
CREATE DATABASE data_node_2;
CREATE DATABASE data_node_3;

\c data_node_1
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c data_node_2
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c data_node_3
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Add data nodes using the TimescaleDB data node management API
SELECT * FROM add_data_node('data_node_1', database => 'data_node_1', password => 'pass', if_not_exists => true);
SELECT * FROM add_data_node('data_node_2', database => 'data_node_2', password => 'pass', if_not_exists => true);
SELECT * FROM add_data_node('data_node_3', database => 'data_node_3', password => 'pass', if_not_exists => true);

-- Start out testing text copy code
SET timescaledb.enable_connection_binary_data=false;

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

-- Now do some testing of the binary frontend/backend path.
SET timescaledb.enable_connection_binary_data=true;

COPY "+ri(k33_')" FROM STDIN;
10	11	strawberry	12.3	stuff
\.

\copy public   	.		"+ri(k33_')" ("pH", 	"))_"   ,	thyme) fROm stdIN deLIMitER '-';
.001-40-2080
100.-37-3150
\.

cOpy public."+ri(k33_')" (thYme, "pH", "))_", "flavor") FrOm 
StDiN wiTH dElImITeR ','
;
150,10,403,\N
2030,10.0,3.21321,something like lemon
3330,10.00,2309424231,  _''garbled*(#\\)@#$*)
\.

COPY "+ri(k33_')" FROM STDIN (FORCE_NULL (flavor, "))_"), QUOTE '`', FREEZE, FORMAT csv, NULL 'empties', FORCE_NOT_NULL ("pH", "thyme"));
1203210,4.4324424324254352345345,``,0,empties
42010,3333333333333333333333333333,"",1.00000000000000000000000000000000001,`empties`
3420,4324,"empties",40,\N
\.

SELECT * FROM "+ri(k33_')";
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.chunk_data_node;
SELECT * FROM _timescaledb_catalog.hypertable_data_node;
select * from show_chunks('"+ri(k33_'')"');
\c data_node_1
SELECT * FROM "+ri(k33_')";
select * from show_chunks('"+ri(k33_'')"');
\c data_node_2
SELECT * FROM "+ri(k33_')";
select * from show_chunks('"+ri(k33_'')"');
\c data_node_3
SELECT * FROM "+ri(k33_')";
select * from show_chunks('"+ri(k33_'')"');
\c :TEST_DBNAME :ROLE_SUPERUSER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

DROP TABLE "+ri(k33_')" CASCADE;
SELECT * FROM delete_data_node('data_node_1', cascade => true);
SELECT * FROM delete_data_node('data_node_2', cascade => true);
SELECT * FROM delete_data_node('data_node_3', cascade => true);
DROP DATABASE data_node_1;
DROP DATABASE data_node_2;
DROP DATABASE data_node_3;
