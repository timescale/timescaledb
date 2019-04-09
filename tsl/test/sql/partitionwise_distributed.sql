-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Need explicit password for non-super users to connect
ALTER ROLE :ROLE_DEFAULT_CLUSTER_USER CREATEDB PASSWORD 'pass';
GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO :ROLE_DEFAULT_CLUSTER_USER;
GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO :ROLE_DEFAULT_CLUSTER_USER;
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

CREATE TABLE hyper (time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('hyper', 'time', 'device', 2, replication_factor => 1);

-- Create a similar PostgreSQL partitioned table
CREATE SERVER IF NOT EXISTS server_pg1 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'server_1', port inet_server_port());
CREATE SERVER IF NOT EXISTS server_pg2 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'server_2', port '5432');

CREATE TABLE pg2dim (time timestamptz, device int, temp float) PARTITION BY HASH (device);
CREATE TABLE pg2dim_h1 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 0) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h2 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 1) PARTITION BY RANGE(time);
CREATE FOREIGN TABLE pg2dim_h1_t1 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-01-01 00:00') TO ('2018-09-01 00:00') SERVER server_pg1;
CREATE FOREIGN TABLE pg2dim_h1_t2 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-09-01 00:00') TO ('2018-12-01 00:00') SERVER server_pg1;
CREATE FOREIGN TABLE pg2dim_h2_t1 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-01-01 00:00') TO ('2018-09-01 00:00') SERVER server_pg2;
CREATE FOREIGN TABLE pg2dim_h2_t2 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-09-01 00:00') TO ('2018-12-01 00:00') SERVER server_pg2;

-- Create a 1-dimensional partitioned table for comparison
CREATE TABLE pg1dim (time timestamptz, device int, temp float) PARTITION BY HASH (device);
CREATE FOREIGN TABLE pg1dim_h1 PARTITION OF pg1dim FOR VALUES WITH (MODULUS 2, REMAINDER 0) SERVER server_pg1;
CREATE FOREIGN TABLE pg1dim_h2 PARTITION OF pg1dim FOR VALUES WITH (MODULUS 2, REMAINDER 1) SERVER server_pg2;
