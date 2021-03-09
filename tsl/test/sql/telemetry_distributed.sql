-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

-- Become an access node
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

-- See telemetry report from a data node
\c :DN_DBNAME_1 :ROLE_CLUSTER_SUPERUSER;
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');

-- Add hypertables 
CREATE TABLE test_ht(time timestamptz, device int, PRIMARY KEY (time, device));
SELECT * FROM create_hypertable('test_ht', 'time', 'device', 1);

CREATE TABLE disttable(time timestamptz, device int, PRIMARY KEY (time, device));
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device', 2);

CREATE TABLE disttable2(time timestamptz, device int, PRIMARY KEY (time, device));
SELECT * FROM create_distributed_hypertable('disttable2', 'time', 'device', 2, replication_factor => 2);

-- See telemetry report update from the data node
\c :DN_DBNAME_1 :ROLE_CLUSTER_SUPERUSER;
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
-- See a number of distributed and distributed and replicated hypertables update
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json, 'distributed_db');

DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
