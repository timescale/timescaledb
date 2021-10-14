-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir debugsupport.sql

\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\set DN_DBNAME_3 :TEST_DBNAME _3

-- Add data nodes
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost', database => :'DN_DBNAME_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO :ROLE_1;

SET ROLE :ROLE_1;

-- Create a "normal" PG table as reference, one two-dimensional
-- distributed hypertable, and a one-dimensional distributed
-- hypertable
CREATE TABLE reference (time timestamptz NOT NULL, device int, location int, temp float);
CREATE TABLE hyper (LIKE reference);
CREATE TABLE hyper1d (LIKE reference);
SELECT create_distributed_hypertable('hyper', 'time', 'device', 3,
                                     chunk_time_interval => interval '18 hours');

SELECT create_distributed_hypertable('hyper1d', 'time', chunk_time_interval => interval '36 hours');

SELECT setseed(1);
INSERT INTO reference
SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, (random() * 20)::int, random() * 80
FROM generate_series('2019-01-01'::timestamptz, '2019-01-04'::timestamptz, '1 minute') as t;

-- Insert the same data into the hypertable but repartition the data
-- set so that we can test the "safeness" of some push-downs across
-- the repartitioning boundary.

INSERT INTO hyper
SELECT * FROM reference
WHERE time < '2019-01-02 05:10'::timestamptz
ORDER BY time;
SELECT * FROM set_number_partitions('hyper', 2);
INSERT INTO hyper
SELECT * FROM reference
WHERE time >= '2019-01-02 05:10'::timestamptz
AND time < '2019-01-03 01:22'::timestamptz
ORDER BY time;
SELECT * FROM set_number_partitions('hyper', 5);
INSERT INTO hyper
SELECT * FROM reference
WHERE time >= '2019-01-03 01:22'::timestamptz
ORDER BY time;

INSERT INTO hyper1d
SELECT * FROM reference ORDER BY time;

SELECT d.hypertable_id, d.id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.dimension d, _timescaledb_catalog.dimension_slice ds
WHERE num_slices IS NOT NULL
AND d.id = ds.dimension_id
ORDER BY 1, 2, 3, 4;

-- Set the max time we can query without hitting the repartitioned
-- chunks. Note that this is before the given repartitioning time
-- above because chunk boundaries do not align exactly with the given
-- timestamp
\set REPARTITIONED_TIME_RANGE 'time >= ''2019-01-01'''
\set CLEAN_PARTITIONING_TIME_RANGE 'time BETWEEN ''2019-01-01'' AND ''2019-01-01 15:00'''


-- Custom agg func for push down tests
CREATE AGGREGATE custom_sum(int4) (
    SFUNC = int4_sum,
    STYPE = int8
);

-- Set seed on all data nodes for ANALYZE to sample consistently
CALL distributed_exec($$ SELECT setseed(1); $$);
ANALYZE reference;
ANALYZE hyper;
ANALYZE hyper1d;
