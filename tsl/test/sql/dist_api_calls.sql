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

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

-- Add data nodes
SELECT node_name, database, node_created, database_created, extension_created
FROM (
  SELECT (add_data_node(name, host => 'localhost', DATABASE => name)).*
  FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v(name)
) a;
GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
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
SELECT * FROM show_chunks('disttable');
SELECT * FROM test.remote_exec(NULL, $$ SELECT show_chunks('disttable'); $$);

-- Ensure that move_chunk() and reorder_chunk() functions cannot be used
-- with distributed hypertable
SET ROLE TO DEFAULT;
SET client_min_messages TO error;
DROP TABLESPACE IF EXISTS tablespace1;
RESET client_min_messages;
CREATE TABLESPACE tablespace1 OWNER :ROLE_CLUSTER_SUPERUSER LOCATION :TEST_TABLESPACE1_PATH;

\set ON_ERROR_STOP 0
SELECT move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_4_chunk', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1', reorder_index=>'disttable_time_idx', verbose=>TRUE);
SELECT reorder_chunk('_timescaledb_internal._dist_hyper_1_4_chunk', verbose => TRUE);
\set ON_ERROR_STOP 1

DROP TABLESPACE tablespace1;

-- Ensure approximate_row_count() works with distributed hypertable
--
SELECT * FROM disttable ORDER BY time;

ANALYZE disttable;

SELECT count(*) FROM disttable;
SELECT approximate_row_count('disttable');
SELECT * FROM test.remote_exec(NULL, $$ SELECT approximate_row_count('disttable'); $$);

-- Test with native replication
--
CREATE TABLE disttable_repl(
    time timestamptz NOT NULL,
    device int,
    value float
);
SELECT * FROM create_distributed_hypertable('disttable_repl', 'time', 'device', 3, replication_factor => 2);
INSERT INTO disttable_repl VALUES
       ('2017-01-01 06:01', 1, 1.2),
       ('2017-01-01 09:11', 3, 4.3),
       ('2017-01-01 08:01', 1, 7.3),
       ('2017-01-02 08:01', 2, 0.23),
       ('2018-07-02 08:01', 87, 0.0),
       ('2018-07-01 06:01', 13, 3.1),
       ('2018-07-01 09:11', 90, 10303.12),
       ('2018-07-01 08:01', 29, 64);
SELECT * FROM disttable_repl ORDER BY time;

SELECT * FROM show_chunks('disttable_repl');
SELECT * FROM test.remote_exec(NULL, $$ SELECT show_chunks('disttable_repl'); $$);

SELECT count(*) FROM disttable_repl;
SELECT approximate_row_count('disttable_repl');

ANALYZE disttable_repl;
SELECT approximate_row_count('disttable_repl');

DROP TABLE disttable_repl;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;
