-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_SUPERUSER;
\ir include/remote_exec.sql
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Need explicit password for non-super users to connect
GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO :ROLE_DEFAULT_CLUSTER_USER;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
SET client_min_messages TO NOTICE;

SELECT inet_server_port() AS "port" \gset

CREATE SERVER IF NOT EXISTS server_pg1 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'data_node_1', port :'port');
CREATE SERVER IF NOT EXISTS server_pg2 FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'data_node_2', port :'port');
CREATE USER MAPPING IF NOT EXISTS FOR :ROLE_DEFAULT_CLUSTER_USER server server_pg1
OPTIONS (user :'ROLE_DEFAULT_CLUSTER_USER', password 'pass');
CREATE USER MAPPING IF NOT EXISTS FOR :ROLE_DEFAULT_CLUSTER_USER server server_pg2
OPTIONS (user :'ROLE_DEFAULT_CLUSTER_USER', password 'pass');

-- Add data nodes using the TimescaleDB node management API
SELECT * FROM add_data_node('data_node_1',
                            database => 'data_node_1',
                            password => :'ROLE_DEFAULT_CLUSTER_USER_PASS',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER',
                            bootstrap_password => :'ROLE_CLUSTER_SUPERUSER_PASS');
SELECT * FROM add_data_node('data_node_2',
                            database => 'data_node_2',
                            password => :'ROLE_DEFAULT_CLUSTER_USER_PASS',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER',
                            bootstrap_password => :'ROLE_CLUSTER_SUPERUSER_PASS');

-- Create a 2-dimensional partitioned table for comparision
CREATE TABLE pg2dim (time timestamptz, device int, location int, temp float) PARTITION BY HASH (device);
CREATE TABLE pg2dim_h1 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 0) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h2 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 1) PARTITION BY RANGE(time);
CREATE FOREIGN TABLE pg2dim_h1_t1 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-01-18 00:00') TO ('2018-04-18 00:00') SERVER server_pg1;
CREATE FOREIGN TABLE pg2dim_h1_t2 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-04-18 00:00') TO ('2018-07-18 00:00') SERVER server_pg1;
CREATE FOREIGN TABLE pg2dim_h1_t3 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-07-18 00:00') TO ('2018-10-18 00:00') SERVER server_pg1;
CREATE FOREIGN TABLE pg2dim_h2_t1 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-01-18 00:00') TO ('2018-04-18 00:00') SERVER server_pg2;
CREATE FOREIGN TABLE pg2dim_h2_t2 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-04-18 00:00') TO ('2018-07-18 00:00') SERVER server_pg2;
CREATE FOREIGN TABLE pg2dim_h2_t3 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-07-18 00:00') TO ('2018-10-18 00:00') SERVER server_pg2;

-- Create these partitioned tables on the servers
SELECT * FROM test.remote_exec('{ data_node_1, data_node_2 }', $$
CREATE TABLE pg2dim (time timestamptz, device int, location int, temp float) PARTITION BY HASH (device);
CREATE TABLE pg2dim_h1 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 0) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h2 PARTITION OF pg2dim FOR VALUES WITH (MODULUS 2, REMAINDER 1) PARTITION BY RANGE(time);
CREATE TABLE pg2dim_h1_t1 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-01-18 00:00') TO ('2018-04-18 00:00');
CREATE TABLE pg2dim_h1_t2 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-04-18 00:00') TO ('2018-07-18 00:00');
CREATE TABLE pg2dim_h2_t1 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-01-18 00:00') TO ('2018-04-18 00:00');
CREATE TABLE pg2dim_h2_t2 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-04-18 00:00') TO ('2018-07-18 00:00');
CREATE TABLE pg2dim_h1_t3 PARTITION OF pg2dim_h1 FOR VALUES FROM ('2018-07-18 00:00') TO ('2018-10-18 00:00');
CREATE TABLE pg2dim_h2_t3 PARTITION OF pg2dim_h2 FOR VALUES FROM ('2018-07-18 00:00') TO ('2018-10-18 00:00')
$$);

CREATE TABLE hyper (time timestamptz, device int, location int, temp float);
SELECT * FROM create_distributed_hypertable('hyper', 'time', 'device', 2, chunk_time_interval => '3 months'::interval);

INSERT INTO pg2dim VALUES
       ('2018-01-19 13:01', 1, 2, 2.3),
       ('2018-01-20 15:05', 1, 3, 5.3),
       ('2018-02-21 13:01', 3, 4, 1.5),
       ('2018-02-28 15:05', 1, 1, 5.6),
       ('2018-02-19 13:02', 3, 5, 3.1),
       ('2018-02-19 13:02', 2, 3, 6.7),
       ('2018-03-08 11:05', 6, 2, 8.1),
       ('2018-03-08 11:05', 7, 4, 4.6),
       ('2018-03-10 17:02', 5, 5, 5.1),
       ('2018-03-10 17:02', 1, 6, 9.1),
       ('2018-03-17 12:02', 2, 2, 6.7),
       ('2018-04-19 13:01', 1, 2, 7.6),
       ('2018-04-20 15:08', 5, 5, 6.4),
       ('2018-05-19 13:01', 4, 4, 5.1),
       ('2018-05-20 15:08', 5, 1, 9.4),
       ('2018-05-30 13:02', 3, 2, 9.0),
       ('2018-09-19 13:01', 1, 3, 6.1),
       ('2018-09-20 15:08', 4, 5, 10.4),
       ('2018-09-30 13:02', 3, 4, 9.9);

INSERT INTO hyper VALUES
       ('2018-01-19 13:01', 1, 2, 2.3),
       ('2018-01-20 15:05', 1, 3, 5.3),
       ('2018-02-21 13:01', 3, 4, 1.5),
       ('2018-02-28 15:05', 1, 1, 5.6),
       ('2018-02-19 13:02', 3, 5, 3.1),
       ('2018-02-19 13:02', 2, 3, 6.7),
       ('2018-03-08 11:05', 6, 2, 8.1),
       ('2018-03-08 11:05', 7, 4, 4.6),
       ('2018-03-10 17:02', 5, 5, 5.1),
       ('2018-03-10 17:02', 1, 6, 9.1),
       ('2018-03-17 12:02', 2, 2, 6.7),
       ('2018-04-19 13:01', 1, 2, 7.6),
       ('2018-04-20 15:08', 5, 5, 6.4),
       ('2018-05-19 13:01', 4, 4, 5.1),
       ('2018-05-20 15:08', 5, 1, 9.4),
       ('2018-05-30 13:02', 3, 2, 9.0);

-- Repartition and insert more data to create chunks with the new
-- partitioning
SELECT set_number_partitions('hyper', 3, 'device');
INSERT INTO hyper VALUES
       ('2018-09-19 13:01', 1, 3, 6.1),
       ('2018-09-20 15:08', 4, 5, 10.4),
       ('2018-09-30 13:02', 3, 4, 9.9);


SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('hyper');

-- Show how slices are assigned to data nodes. In particular, verify that
-- there are overlapping slices in the closed "space" dimension
SELECT * FROM test.remote_exec('{ data_node_1, data_node_2 }', $$
       SELECT * FROM _timescaledb_catalog.dimension_slice WHERE dimension_id = 2;
$$);

SELECT * FROM pg2dim_h1_t1;
SELECT * FROM pg2dim_h1_t2;
SELECT * FROM pg2dim_h1_t3;
SELECT * FROM pg2dim_h2_t1;
SELECT * FROM pg2dim_h2_t2;
SELECT * FROM pg2dim_h2_t3;

SELECT * FROM  _timescaledb_internal._hyper_1_1_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_2_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_3_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_4_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_5_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_6_dist_chunk;
SELECT * FROM  _timescaledb_internal._hyper_1_7_dist_chunk;

------------------------------------------------------------------------
-- PARTIAL partitionwise - open "time" dimension covered by GROUP BY.
-- Note that we don't yet support pushing down partials and PG can't
-- do it on partitioned rels.
-----------------------------------------------------------------------
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

SELECT time, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

-- Show result
SELECT time, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

-- Show result
SELECT time, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

-------------------------------------------------------------------------
-- FULL partitionwise - only closed "space" dimension covered by GROUP
-- BY -- this is always safe to fully push down if chunks do not
-- overlap in the space dimension.
-------------------------------------------------------------------------
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

SELECT device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

SET timescaledb.enable_per_data_node_queries = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

-- Show result
SELECT device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

-- Show result
SELECT device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1
ORDER BY 1;

-- Grouping on something which is not a partitioning dimension should
-- not be pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT location, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1;

-- Expand query across repartition boundary. This makes it unsafe to
-- push down the FULL agg, so should expect a PARTIAL agg on
-- hypertables
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM pg2dim
GROUP BY 1
ORDER BY 1;

-- Show result
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

-- Restriction on "time", but not including in target list. Again,
-- this time interval covers a repartitioning, so hypertables should
-- not push down FULL aggs
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
-- This case is always safe to push down
--------------------------------------------------------------
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT time, device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT time, device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- On hypertable, first show partitionwise aggs without per-data node queries
SET timescaledb.enable_per_data_node_queries = OFF;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT time, device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Enable per-data node queries. Aggregate should be pushed down per
-- data node instead of per chunk.
SET timescaledb.enable_per_data_node_queries = ON;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT time, device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Still FULL partitionwise when crossing partitioning boundary
-- because we now cover all partitioning keys.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
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


-- Only one chunk per data node, still uses per-data node plan.  Not
-- choosing pushed down aggregate plan here, probably due to costing.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
WHERE time BETWEEN '2018-04-19 00:01' AND '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Test HAVING qual
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp) AS temp
FROM pg2dim
WHERE time BETWEEN '2018-04-19 00:01' AND '2018-06-01 00:00'
GROUP BY 1, 2
HAVING avg(temp) > 4
ORDER BY 1, 2;

SELECT time, device, avg(temp) AS temp
FROM pg2dim
WHERE time BETWEEN '2018-04-19 00:01' AND '2018-06-01 00:00'
GROUP BY 1, 2
HAVING avg(temp) > 4
ORDER BY 1, 2;

-- Test HAVING qual. Not choosing pushed down aggregate plan here,
-- probably due to costing.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp) AS temp
FROM hyper
WHERE time BETWEEN '2018-04-19 00:01' AND '2018-06-01 00:00'
GROUP BY 1, 2
HAVING avg(temp) > 4
ORDER BY 1, 2;

SELECT time, device, avg(temp) AS temp
FROM hyper
WHERE time BETWEEN '2018-04-19 00:01' AND '2018-06-01 00:00'
GROUP BY 1, 2
HAVING avg(temp) > 4
ORDER BY 1, 2;

-------------------------------------------------------------------
-- date_trunc is a whitelisted bucketing function, so should be pushed
-- down fully.
-------------------------------------------------------------------

-- First with partitionwise aggs disabled
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show reference result
SELECT date_trunc('month', time), device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Now with partitionwise aggs enabled
SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Should do FULL partitionwise also across repartition boundary since
-- we cover all partitioning keys
EXPLAIN (VERBOSE, COSTS OFF)
SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

SELECT date_trunc('month', time), device, avg(temp)
FROM hyper
GROUP BY 1, 2
ORDER BY 1, 2;

-- Reference result
SELECT date_trunc('month', time), device, avg(temp)
FROM pg2dim
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result by year
SELECT date_trunc('year', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-------------------------------------------------------
-- Test time_bucket (only supports up to days grouping)
-------------------------------------------------------
SET enable_partitionwise_aggregate = OFF;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time_bucket('1 day', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT time_bucket('1 day', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

SET enable_partitionwise_aggregate = ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time_bucket('1 day', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Show result
SELECT time_bucket('1 day', time), device, avg(temp)
FROM hyper
WHERE time < '2018-06-01 00:00'
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
-- sum contains random(), so not pushed down to data nodes
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), sum(temp * (random() <= 1)::int) as sum
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), sum(temp * (random() <= 1)::int) as sum
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
LIMIT 1;

-- Pushed down with non-pushable expression taken out
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), random() * device as rand_dev, custom_sum(device)
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), random() * device as rand_dev, custom_sum(device)
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), sum(temp) * random() * device as sum_temp
FROM pg2dim
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
HAVING avg(temp) * custom_sum(device) > 0.8
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp), sum(temp) * random() * device as sum_temp
FROM hyper
WHERE time < '2018-06-01 00:00'
GROUP BY 1, 2
HAVING avg(temp) * custom_sum(device) > 0.8
LIMIT 1;

-- not pushed down because of non-shippable expression on the
-- underlying rel
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM pg2dim
WHERE (pg2dim.temp * random() <= 20)
AND time < '2018-06-01 00:00'
GROUP BY 1, 2
LIMIT 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, device, avg(temp)
FROM hyper
WHERE (hyper.temp * random() <= 20)
AND time < '2018-06-01 00:00'
GROUP BY 1, 2
LIMIT 1;

-- Test one-dimensional push down
CREATE TABLE hyper1d (time timestamptz, device int, temp float);
SELECT * FROM create_distributed_hypertable('hyper1d', 'time', chunk_time_interval => '3 months'::interval);

INSERT INTO hyper1d VALUES
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
       ('2018-05-30 13:02', 3, 9.0),
       ('2018-09-19 13:01', 1, 6.1),
       ('2018-09-20 15:08', 2, 10.4),
       ('2018-09-30 13:02', 3, 9.9);

SET enable_partitionwise_aggregate = ON;
SET timescaledb.enable_per_data_node_queries = ON;

-- Covering partitioning dimension is always safe to push down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT time, avg(temp)
FROM hyper1d
GROUP BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT time_bucket('1 day', time), avg(temp)
FROM hyper1d
GROUP BY 1;

--- Only one chunk in query => safe to fully push down although not on
--- a partitioning dimension.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM hyper1d
WHERE time < '2018-04-01 00:00'
GROUP BY 1;


-- Two chunks in query => not safe to fully push down when not
-- grouping on partitioning dimension
EXPLAIN (VERBOSE, COSTS OFF)
SELECT device, avg(temp)
FROM hyper1d
WHERE time < '2018-06-01 00:00'
GROUP BY 1;
