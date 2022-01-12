-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This test suite is not intended to check the output of the queries
-- but rather make sure that they are generated when certain flags are
-- enabled or disabled.
--
-- The queries below are triggering a call to `tsl_set_rel_pathlist`
-- and `get_foreign_upper_paths` respectively, but if that changes,
-- they might need to be changed.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\set DN_DBNAME_3 :TEST_DBNAME _3

-- Add data nodes using the TimescaleDB node management API
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost', database => :'DN_DBNAME_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;

CREATE TABLE hyper (time timestamptz, device int, location int, temp float);
SELECT * FROM create_distributed_hypertable('hyper', 'time', 'device');

SET enable_partitionwise_aggregate = ON;

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

-- Update table stats
ANALYZE hyper;

-- Optimizer debug messages shown at debug level 2
SET client_min_messages TO DEBUG2;

-- Turning on show_rel should show a message
-- But disable the code which avoids dist chunk planning
SET timescaledb.debug_allow_datanode_only_path = 'off';
SET timescaledb.debug_optimizer_flags = 'show_rel';
SHOW timescaledb.debug_optimizer_flags;

SELECT time, device, avg(temp) AS temp
FROM hyper
WHERE time BETWEEN '2018-04-19 00:01' AND '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Enable session level datanode only path parameter which doesn't
-- plan distributed chunk scans unnecessarily
SET timescaledb.debug_allow_datanode_only_path = 'on';

-- Turning off the show_rel (and turning on another flag) should not
-- show a notice on the relations, but show the upper paths.
SET timescaledb.debug_optimizer_flags = 'show_upper=*';
SHOW timescaledb.debug_optimizer_flags;

SELECT time, device, avg(temp) AS temp
FROM hyper
WHERE time BETWEEN '2018-04-19 00:01' AND '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Turning off both relations should not show anything.
RESET timescaledb.debug_optimizer_flags;
SHOW timescaledb.debug_optimizer_flags;

SELECT time, device, avg(temp) AS temp
FROM hyper
WHERE time BETWEEN '2018-04-19 00:01' AND '2018-06-01 00:00'
GROUP BY 1, 2
ORDER BY 1, 2;

SET client_min_messages TO ERROR;
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
