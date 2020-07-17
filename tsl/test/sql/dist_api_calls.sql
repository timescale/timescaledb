-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Basic testing of API functions on distributed hypertable

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- Support for execute_sql_and_filter_server_name_on_error()
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;
SET client_min_messages TO NOTICE;

-- Add data nodes
SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost',
                            database => 'data_node_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;

-- Create a distributed hypertable with data
SET ROLE :ROLE_1;
CREATE TABLE disttable(
    time timestamptz NOT NULL, 
    device int, 
    value float
);
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 3);
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.2),
       ('2017-01-01 09:11', 3, 4.3),
       ('2017-01-01 08:01', 1, 7.3),
       ('2017-01-02 08:01', 2, 0.23),
       ('2018-07-02 08:01', 87, 0.0),
       ('2018-07-01 06:01', 13, 3.1),
       ('2018-07-01 09:11', 90, 10303.12),
       ('2018-07-01 08:01', 29, 64);
SELECT * FROM disttable ORDER BY time;
SELECT * FROM test.remote_exec(NULL, $$ SELECT show_chunks('disttable'); $$);

-- Test APIs on the distributed hypertable

-- Call drop_chunks
SELECT drop_chunks('disttable', timestamptz '2017-03-01 00:00');
SELECT * FROM disttable ORDER BY time;
SELECT * FROM test.remote_exec(NULL, $$ SELECT show_chunks('disttable'); $$);

-- Restore the state
INSERT INTO disttable VALUES
       ('2017-01-01 06:01', 1, 1.2),
       ('2017-01-01 09:11', 3, 4.3),
       ('2017-01-01 08:01', 1, 7.3),
       ('2017-01-02 08:01', 2, 0.23);
SELECT * FROM disttable ORDER BY time;
SELECT * FROM test.remote_exec(NULL, $$ SELECT show_chunks('disttable'); $$);
