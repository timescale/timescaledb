-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add servers
\c :TEST_DBNAME :ROLE_SUPERUSER;
\ir include/remote_exec.sql
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
SET client_min_messages TO NOTICE;

CREATE DATABASE server_1;
CREATE DATABASE server_2;

-- Creating extension is only possible as super-user
\c server_1 :ROLE_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c server_2 :ROLE_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

SELECT inet_server_port() AS "port" \gset

CREATE SERVER IF NOT EXISTS server_pg1 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'server_1', port :'port');
CREATE SERVER IF NOT EXISTS server_pg2 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'server_2', port :'port');
CREATE USER MAPPING IF NOT EXISTS FOR :ROLE_DEFAULT_CLUSTER_USER server server_pg1
OPTIONS (user :'ROLE_DEFAULT_CLUSTER_USER', password 'pass');
CREATE USER MAPPING IF NOT EXISTS FOR :ROLE_DEFAULT_CLUSTER_USER server server_pg2
OPTIONS (user :'ROLE_DEFAULT_CLUSTER_USER', password 'pass');

-- Create a 2-dimensional partitioned table for comparision
CREATE TABLE pg2dim (time timestamptz, device int, temp float) PARTITION BY HASH (device);
CREATE TABLE pg2dim_h1 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 0) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h2 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 1) PARTITION BY RANGE(time);
CREATE FOREIGN TABLE pg2dim_h1_t1 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-01-18 00:00') TO ('2018-04-18 00:00') SERVER server_pg1;
CREATE FOREIGN TABLE pg2dim_h1_t2 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-04-18 00:00') TO ('2018-07-18 00:00') SERVER server_pg1;
CREATE FOREIGN TABLE pg2dim_h2_t1 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-01-18 00:00') TO ('2018-04-18 00:00') SERVER server_pg2;
CREATE FOREIGN TABLE pg2dim_h2_t2 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-04-18 00:00') TO ('2018-07-18 00:00') SERVER server_pg2;

-- Create these partitioned tables on the servers
SELECT * FROM test.remote_exec('{ server_pg1, server_pg2 }', $$
CREATE TABLE pg2dim (time timestamptz, device int, temp float) PARTITION BY HASH (device);
CREATE TABLE pg2dim_h1 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 0) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h2 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 1) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h1_t1 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-01-18 00:00') TO ('2018-04-18 00:00');
CREATE TABLE pg2dim_h1_t2 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-04-18 00:00') TO ('2018-07-18 00:00');
CREATE TABLE pg2dim_h2_t1 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-01-18 00:00') TO ('2018-04-18 00:00');
CREATE TABLE pg2dim_h2_t2 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-04-18 00:00') TO ('2018-07-18 00:00');
$$);

-- Add servers using the TimescaleDB server management AP
SELECT * FROM add_server('server_1', database => 'server_1', password => 'pass', if_not_exists => true);
SELECT * FROM add_server('server_2', database => 'server_2', password => 'pass', if_not_exists => true);


CREATE TABLE hyper (time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('hyper', 'time', 'device', 2, replication_factor => 1, chunk_time_interval => '3 months'::interval);

INSERT INTO hyper VALUES
       ('2018-01-19 13:01', 1, 2.3),
       ('2018-01-20 15:05', 1, 5.3),
       ('2018-02-21 13:01', 3, 1.5),
       ('2018-02-28 15:05', 1, 5.6),
       ('2018-02-19 13:02', 3, 3.1),
       ('2018-02-19 13:02', 3, 6.7),
       ('2018-04-19 13:01', 1, 7.6),
       ('2018-04-20 15:08', 3, 8.4),
       ('2018-05-19 13:01', 1, 5.1),
       ('2018-05-20 15:08', 1, 9.4),
       ('2018-05-30 13:02', 3, 9.0);

INSERT INTO pg2dim VALUES
       ('2018-01-19 13:01', 1, 2.3),
       ('2018-01-20 15:05', 1, 5.3),
       ('2018-02-21 13:01', 3, 1.5),
       ('2018-02-28 15:05', 1, 5.6),
       ('2018-02-19 13:02', 3, 3.1),
       ('2018-02-19 13:02', 3, 6.7),
       ('2018-04-19 13:01', 1, 7.6),
       ('2018-04-20 15:08', 3, 8.4),
       ('2018-05-19 13:01', 1, 5.1),
       ('2018-05-20 15:08', 1, 9.4),
       ('2018-05-30 13:02', 3, 9.0);

SELECT * FROM test.show_subtables('hyper');

SELECT * FROM pg2dim_h1_t1;
SELECT * FROM pg2dim_h1_t2;
SELECT * FROM pg2dim_h2_t1;
SELECT * FROM pg2dim_h2_t2;

SELECT * FROM  _timescaledb_internal._hyper_1_1_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_2_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_3_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_4_dist_chunk;

---------------------------------------------------------------------
-- PARTIAL partitionwise - Not all partition keys are covered by GROUP
-- BY
---------------------------------------------------------------------
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM pg2dim
GROUP BY 1
ORDER BY 1;

SELECT device, avg(temp)
FROM pg2dim
GROUP BY 1
ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM hyper
GROUP BY 1
ORDER BY 1;

-- Show result
SELECT device, avg(temp)
FROM hyper
GROUP BY 1
ORDER BY 1;

SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM pg2dim
GROUP BY 1
ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM hyper
GROUP BY 1
ORDER BY 1;

-- Show result
SELECT device, avg(temp)
FROM hyper
GROUP BY 1
ORDER BY 1;

-- Restriction on "time", but not including in target list
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM pg2dim
WHERE time > '2018-04-19 00:01'
GROUP BY 1
ORDER BY 1;

SELECT device, avg(temp)
FROM pg2dim
WHERE time > '2018-04-19 00:01'
GROUP BY 1
ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM hyper
WHERE time > '2018-04-19 00:01'
GROUP BY 1
ORDER BY 1;

SELECT device, avg(temp)
FROM hyper
WHERE time > '2018-04-19 00:01'
GROUP BY 1
ORDER BY 1;

--------------------------------------------------------------
-- FULL partitionwise - All partition keys covered by GROUP BY
--------------------------------------------------------------
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT time, device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT time, device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

-- On hypertable, first show partitionwise aggs without per-server queries
SET timescaledb.enable_per_server_queries = OFF;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT time, device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Enable per-server queries. Aggregate should be pushed down per
-- server instead of per chunk.
SET timescaledb.enable_per_server_queries = ON;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT time, device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Only one chunk per server, still uses per-server plan.  Not
-- choosing pushed down aggregate plan here, probably due to costing.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
WHERE time > '2018-04-19 00:01'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Test HAVING qual
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp) AS temp
FROM pg2dim
WHERE time > '2018-04-19 00:01'
GROUP BY 1, 2
HAVING avg(temp) > 4
ORDER BY 1, 2;

SELECT time, device, avg(temp) AS temp
FROM pg2dim
WHERE time > '2018-04-19 00:01'
GROUP BY 1, 2
HAVING avg(temp) > 4
ORDER BY 1, 2;

-- Test HAVING qual. Not choosing pushed down aggregate plan here,
-- probably due to costing.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp) AS temp
FROM hyper
WHERE time > '2018-04-19 00:01'
GROUP BY 1, 2
HAVING avg(temp) > 4
ORDER BY 1, 2;

SELECT time, device, avg(temp) AS temp
FROM hyper
WHERE time > '2018-04-19 00:01'
GROUP BY 1, 2
HAVING avg(temp) > 4
ORDER BY 1, 2;

-------------------------------------------------------------------
-- All partition keys not covered by GROUP BY because of date_trunc
-- expression on time (partial partitionwise). This won't be pushed
-- down a.t.m., since no way to send partials
-------------------------------------------------------------------
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result by month
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result by year
SELECT date_trunc('year', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-------------------------------------------------------
-- Test time_bucket (only supports up to days grouping)
-------------------------------------------------------
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time_bucket('1 day', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT time_bucket('1 day', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time_bucket('1 day', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT time_bucket('1 day', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

---------------------------------------------------------------------
-- Test expressions that either aren't pushed down or only pushed down
-- in parts
---------------------------------------------------------------------
-- Create a custom aggregate that does not exist on the data nodes
CREATE AGGREGATE custom_sum(int4) (
    SFUNC = int4_sum,
    STYPE = int8
);
-- sum contains random(), so not pushed down to servers
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), sum(temp * (random() <= 1)::int) as sum
FROM pg2dim
GROUP BY 1, 2
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), sum(temp * (random() <= 1)::int) as sum
FROM hyper
GROUP BY 1, 2
LIMIT 1;

-- Pushed down with non-pushable expression taken out
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), random() * device as rand_dev, custom_sum(device)
FROM pg2dim
GROUP BY 1, 2
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), random() * device as rand_dev, custom_sum(device)
FROM hyper
GROUP BY 1, 2
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), sum(temp) * random() * device as sum_temp
FROM pg2dim
GROUP BY 1, 2
HAVING avg(temp) * custom_sum(device) > 0.8
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), sum(temp) * random() * device as sum_temp
FROM hyper
GROUP BY 1, 2
HAVING avg(temp) * custom_sum(device) > 0.8
LIMIT 1;

-- not pushed down because of non-shippable expression on the
-- underlying rel
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM pg2dim
WHERE (pg2dim.temp * random() <= 20)
GROUP BY 1, 2
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
WHERE (hyper.temp * random() <= 20)
GROUP BY 1, 2
LIMIT 1;
