-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

\set TEST_BASE_NAME dist_gapfill
\set DATA_NODE_1 :TEST_BASE_NAME _1
\set DATA_NODE_2 :TEST_BASE_NAME _2
\set DATA_NODE_3 :TEST_BASE_NAME _3
SELECT format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_singlenode.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_SINGLENODE",
    format('%s/results/%s_partitionwise_off.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_PARTITIONWISE_OFF",
    format('%s/results/%s_partitionwise_on.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_PARTITIONWISE_ON" \gset

SELECT format('\! diff %s %s', :'TEST_SINGLENODE', :'TEST_PARTITIONWISE_OFF') AS "DIFF_CMD_PARTITIONWISE_OFF",
    format('\! diff %s %s', :'TEST_SINGLENODE', :'TEST_PARTITIONWISE_ON') AS "DIFF_CMD_PARTITIONWISE_ON" \gset

-- Non-distributed hypertable
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

-- Distributed hypertable
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
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

SET client_min_messages TO ERROR;
DROP TABLE conditions CASCADE;
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT delete_data_node(:'DATA_NODE_1');
SELECT delete_data_node(:'DATA_NODE_2');
SELECT delete_data_node(:'DATA_NODE_3');
DROP DATABASE IF EXISTS :DATA_NODE_1;
DROP DATABASE IF EXISTS :DATA_NODE_2;
DROP DATABASE IF EXISTS :DATA_NODE_3;
SET ROLE :ROLE_1;
SET client_min_messages TO NOTICE;

\set ECHO all

:DIFF_CMD_PARTITIONWISE_OFF
:DIFF_CMD_PARTITIONWISE_ON
