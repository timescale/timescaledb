-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
SET client_min_messages TO NOTICE;

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

-- Become an access node
SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

-- See telemetry report from a data node
\c data_node_1 :ROLE_CLUSTER_SUPERUSER;
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('data_node_2', 'localhost',
                            database => 'data_node_2');

-- Add hypertables 
CREATE TABLE test_ht(time timestamptz, device int, PRIMARY KEY (time, device));
SELECT * FROM create_hypertable('test_ht', 'time', 'device', 1);

CREATE TABLE disttable(time timestamptz, device int, PRIMARY KEY (time, device));
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 2);

CREATE TABLE disttable2(time timestamptz, device int, PRIMARY KEY (time, device));
SELECT * FROM create_distributed_hypertable('disttable2', 'time', 'device', 2, replication_factor => 2);

-- See telemetry report update from the data node
\c data_node_1 :ROLE_CLUSTER_SUPERUSER;
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
-- See a number of distributed and distributed and replicated hypertables update
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');
