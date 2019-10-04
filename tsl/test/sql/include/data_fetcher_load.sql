-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET ROLE :ROLE_CLUSTER_SUPERUSER;
-- cleanup from previous tests
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;

SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost',
                            database => 'data_node_3');

CREATE TABLE disttable(time timestamptz, device int, temp float);
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 3);

SELECT setseed(1);
INSERT INTO disttable
SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, random() * 10
FROM generate_series('2019-01-01'::timestamptz, '2019-01-02'::timestamptz, '1 second') as t;
