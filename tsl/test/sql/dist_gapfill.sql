-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SET ROLE :ROLE_1;

\set TEST_BASE_NAME dist_gapfill
\set TEST_METRICS_NAME gapfill_metrics
\set DATA_NODE_1 :TEST_BASE_NAME _1
\set DATA_NODE_2 :TEST_BASE_NAME _2
\set DATA_NODE_3 :TEST_BASE_NAME _3
SELECT format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_singlenode.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_SINGLENODE",
    format('%s/results/%s_partitionwise_off.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_PARTITIONWISE_OFF",
    format('%s/results/%s_partitionwise_on.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_PARTITIONWISE_ON",
    format('include/%s_query.sql', :'TEST_METRICS_NAME') AS "TEST_METRICS_QUERY_NAME",
    format('%s/results/%s_nohyper.out', :'TEST_OUTPUT_DIR', :'TEST_METRICS_NAME') AS "TEST_METRICS_NOHYPER",
    format('%s/results/%s_partitionwise_off.out', :'TEST_OUTPUT_DIR', :'TEST_METRICS_NAME') AS "TEST_METRICS_PARTITIONWISE_OFF" \gset

SELECT format('\! diff %s %s', :'TEST_SINGLENODE', :'TEST_PARTITIONWISE_OFF') AS "DIFF_CMD_PARTITIONWISE_OFF",
    format('\! diff %s %s', :'TEST_SINGLENODE', :'TEST_PARTITIONWISE_ON') AS "DIFF_CMD_PARTITIONWISE_ON",
    format('\! diff %s %s', :'TEST_METRICS_NOHYPER', :'TEST_METRICS_PARTITIONWISE_OFF') AS "DIFF_CMD_METRICS_PARTITIONWISE_OFF" \gset

SET client_min_messages TO ERROR;
DROP TABLE IF EXISTS metrics_int;
DROP TABLE IF EXISTS conditions;
DROP TABLE IF EXISTS devices;
DROP TABLE IF EXISTS sensors;
SET client_min_messages TO NOTICE;

-- Non-distributed hypertables

-- dist_gapfill_query
CREATE TABLE conditions(
    time timestamptz NOT NULL, 
    device int, 
    value float
);
SELECT * FROM create_hypertable('conditions', 'time');
INSERT INTO conditions VALUES
       ('2017-01-01 06:01', 1, 1.2),
       ('2017-01-01 09:11', 3, 4.3),
       ('2017-01-01 08:01', 1, 7.3),
       ('2017-01-02 08:01', 2, 0.23),
       ('2018-07-02 08:01', 87, 0.0),
       ('2018-07-01 06:01', 13, 3.1),
       ('2018-07-01 09:11', 90, 10303.12),
       ('2018-07-01 08:01', 29, 64);
\set ECHO all
\ir :TEST_QUERY_NAME
\set ECHO errors
\o :TEST_SINGLENODE
\ir :TEST_QUERY_NAME
\o

DROP TABLE conditions CASCADE;

-- Run gapfill on a table as in gapfill.sql, where the result is verified

CREATE TABLE metrics_int(
    time int NOT NULL,
    device_id int,
    sensor_id int,
    value float);

INSERT INTO metrics_int VALUES
(-100,1,1,0.0),
(-100,1,2,-100.0),
(0,1,1,5.0),
(5,1,2,10.0),
(100,1,1,0.0),
(100,1,2,-100.0);

CREATE TABLE devices(device_id INT, name TEXT);
INSERT INTO devices VALUES (1,'Device 1'),(2,'Device 2'),(3,'Device 3');

CREATE TABLE sensors(sensor_id INT, name TEXT);
INSERT INTO sensors VALUES (1,'Sensor 1'),(2,'Sensor 2'),(3,'Sensor 3');

\o :TEST_METRICS_NOHYPER
\ir :TEST_METRICS_QUERY_NAME
\o

DROP TABLE metrics_int CASCADE;

-- Distributed hypertables with three data nodes

-- dist_gapfill_query
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS :DATA_NODE_1;
DROP DATABASE IF EXISTS :DATA_NODE_2;
DROP DATABASE IF EXISTS :DATA_NODE_3;
SELECT * FROM add_data_node(:'DATA_NODE_1', host => 'localhost',
                            database => :'DATA_NODE_1');
SELECT * FROM add_data_node(:'DATA_NODE_2', host => 'localhost',
                            database => :'DATA_NODE_2');
SELECT * FROM add_data_node(:'DATA_NODE_3', host => 'localhost',
                            database => :'DATA_NODE_3');
GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;
SET ROLE :ROLE_1;
SET client_min_messages TO NOTICE;

CREATE TABLE conditions(
    time timestamptz NOT NULL, 
    device int, 
    value float
);
SELECT * FROM create_distributed_hypertable('conditions', 'time', 'device', 3);
INSERT INTO conditions VALUES
       ('2017-01-01 06:01', 1, 1.2),
       ('2017-01-01 09:11', 3, 4.3),
       ('2017-01-01 08:01', 1, 7.3),
       ('2017-01-02 08:01', 2, 0.23),
       ('2018-07-02 08:01', 87, 0.0),
       ('2018-07-01 06:01', 13, 3.1),
       ('2018-07-01 09:11', 90, 10303.12),
       ('2018-07-01 08:01', 29, 64);

SET enable_partitionwise_aggregate = 'off';
\o :TEST_PARTITIONWISE_OFF
\ir :TEST_QUERY_NAME
\o

SET enable_partitionwise_aggregate = 'on';
\o :TEST_PARTITIONWISE_ON
\ir :TEST_QUERY_NAME
\o

SET enable_partitionwise_aggregate = 'off';

-- gapfill_metrics_query

CREATE TABLE metrics_int(
    time int NOT NULL,
    device_id int,
    sensor_id int,
    value float);

SELECT create_distributed_hypertable('metrics_int','time','device_id',chunk_time_interval => 50);

INSERT INTO metrics_int VALUES
(-100,1,1,0.0),
(-100,1,2,-100.0),
(0,1,1,5.0),
(5,1,2,10.0),
(100,1,1,0.0),
(100,1,2,-100.0);

\o :TEST_METRICS_PARTITIONWISE_OFF
\ir :TEST_METRICS_QUERY_NAME
\o

SET client_min_messages TO ERROR;
DROP TABLE conditions CASCADE;
DROP TABLE metrics_int CASCADE;
SET client_min_messages TO NOTICE;

\set ECHO all

:DIFF_CMD_PARTITIONWISE_OFF
:DIFF_CMD_PARTITIONWISE_ON
:DIFF_CMD_METRICS_PARTITIONWISE_OFF

-- Distributed hypertables with one data nodes

\set ECHO errors

-- dist_gapfill_query
CREATE TABLE conditions(
    time timestamptz NOT NULL, 
    device int, 
    value float
);
SELECT * FROM create_distributed_hypertable('conditions', 'time', 'device', 1,
    data_nodes => ARRAY[:'DATA_NODE_1']);
INSERT INTO conditions VALUES
       ('2017-01-01 06:01', 1, 1.2),
       ('2017-01-01 09:11', 3, 4.3),
       ('2017-01-01 08:01', 1, 7.3),
       ('2017-01-02 08:01', 2, 0.23),
       ('2018-07-02 08:01', 87, 0.0),
       ('2018-07-01 06:01', 13, 3.1),
       ('2018-07-01 09:11', 90, 10303.12),
       ('2018-07-01 08:01', 29, 64);

SET enable_partitionwise_aggregate = 'off';
\o :TEST_PARTITIONWISE_OFF
\ir :TEST_QUERY_NAME
\o

SET enable_partitionwise_aggregate = 'on';
\o :TEST_PARTITIONWISE_ON
\ir :TEST_QUERY_NAME
\o

SET enable_partitionwise_aggregate = 'off';

-- gapfill_metrics_query

CREATE TABLE metrics_int(
    time int NOT NULL,
    device_id int,
    sensor_id int,
    value float);

SELECT create_distributed_hypertable('metrics_int', 'time', 'device_id', chunk_time_interval => 50,
    data_nodes => ARRAY[:'DATA_NODE_1']);

INSERT INTO metrics_int VALUES
(-100,1,1,0.0),
(-100,1,2,-100.0),
(0,1,1,5.0),
(5,1,2,10.0),
(100,1,1,0.0),
(100,1,2,-100.0);

\o :TEST_METRICS_PARTITIONWISE_OFF
\ir :TEST_METRICS_QUERY_NAME
\o

SET client_min_messages TO ERROR;
DROP TABLE conditions CASCADE;
DROP TABLE metrics_int CASCADE;
DROP TABLE devices;
DROP TABLE sensors;
SET ROLE :ROLE_CLUSTER_SUPERUSER;
SELECT delete_data_node(:'DATA_NODE_1');
SELECT delete_data_node(:'DATA_NODE_2');
SELECT delete_data_node(:'DATA_NODE_3');
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;
SET ROLE :ROLE_1;
SET client_min_messages TO NOTICE;

\set ECHO all

:DIFF_CMD_PARTITIONWISE_OFF
:DIFF_CMD_PARTITIONWISE_ON
:DIFF_CMD_METRICS_PARTITIONWISE_OFF
