-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--telemetry tests that require a community license
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

SELECT setseed(1);

-- Create a materialized view from the telemetry report so that we
-- don't regenerate telemetry for every query.  Filter heap_size for
-- materialized views since PG14 reports a different heap size for
-- them compared to earlier PG versions.
CREATE MATERIALIZED VIEW telemetry_report AS
SELECT (r #- '{relations,materialized_views,heap_size}') AS r
FROM get_telemetry_report() r;

CREATE VIEW relations AS
SELECT r -> 'relations' AS rels
FROM telemetry_report;

SELECT rels -> 'continuous_aggregates' -> 'num_relations' AS num_continuous_aggs,
	   rels -> 'hypertables' -> 'num_relations' AS num_hypertables
FROM relations;

-- check telemetry picks up flagged content from metadata
SELECT r -> 'db_metadata' AS db_metadata
FROM telemetry_report;

-- check timescaledb_telemetry.cloud
SELECT r -> 'instance_metadata' AS instance_metadata
FROM telemetry_report r;

CREATE TABLE normal (time timestamptz NOT NULL, device int, temp float);
CREATE TABLE part (time timestamptz NOT NULL, device int, temp float) PARTITION BY RANGE (time);
CREATE TABLE part_t1 PARTITION OF part FOR VALUES FROM ('2018-01-01') TO ('2018-02-01') PARTITION BY HASH (device);
CREATE TABLE part_t2 PARTITION OF part FOR VALUES FROM ('2018-02-01') TO ('2018-03-01') PARTITION BY HASH (device);
CREATE TABLE part_t1_d1 PARTITION OF part_t1 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE part_t1_d2 PARTITION OF part_t1 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
CREATE TABLE part_t2_d1 PARTITION OF part_t2 FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE part_t2_d2 PARTITION OF part_t2 FOR VALUES WITH (MODULUS 2, REMAINDER 1);
CREATE TABLE hyper (LIKE normal);

SELECT table_name FROM create_hypertable('hyper', 'time');

CREATE MATERIALIZED VIEW contagg
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS hour,
  device,
  min(time)
FROM
  hyper
GROUP BY hour, device;

-- Create another view (already have the "relations" view)
CREATE VIEW devices AS
SELECT DISTINCT ON (device) device
FROM hyper;

-- Show relations with no data
REFRESH MATERIALIZED VIEW telemetry_report;
SELECT jsonb_pretty(rels) AS relations FROM relations;

-- Insert data
INSERT INTO normal
SELECT t, ceil(random() * 10)::int, random() * 30
FROM generate_series('2018-01-01'::timestamptz, '2018-02-28', '2h') t;

INSERT INTO hyper
SELECT * FROM normal;
INSERT INTO part
SELECT * FROM normal;

CALL refresh_continuous_aggregate('contagg', NULL, NULL);

-- ANALYZE to get updated reltuples stats
ANALYZE normal, hyper, part;

SELECT count(c) FROM show_chunks('hyper') c;
SELECT count(c) FROM show_chunks('contagg') c;

-- Update and show the telemetry report
REFRESH MATERIALIZED VIEW telemetry_report;
SELECT jsonb_pretty(rels) AS relations FROM relations;

-- Actual row count should be the same as reltuples stats for all tables
SELECT (SELECT count(*) FROM normal) num_inserted_rows,
	   (SELECT rels -> 'tables' -> 'num_reltuples' FROM relations) normal_reltuples,
	   (SELECT rels -> 'hypertables' -> 'num_reltuples' FROM relations) hyper_reltuples,
	   (SELECT rels -> 'partitioned_tables' -> 'num_reltuples' FROM relations) part_reltuples;

-- Add compression
ALTER TABLE hyper SET (timescaledb.compress);
SELECT compress_chunk(c) 
FROM show_chunks('hyper') c ORDER BY c LIMIT 4;

ALTER MATERIALIZED VIEW contagg SET (timescaledb.compress);
SELECT compress_chunk(c) 
FROM show_chunks('contagg') c ORDER BY c LIMIT 1;

-- Turn of real-time aggregation
ALTER MATERIALIZED VIEW contagg SET (timescaledb.materialized_only = true);

ANALYZE normal, hyper, part;

REFRESH MATERIALIZED VIEW telemetry_report;
SELECT jsonb_pretty(rels) AS relations FROM relations;

-- Add distributed hypertables
\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2

-- Not an access node or data node
SELECT r -> 'num_data_nodes' AS num_data_nodes,
	   r -> 'distributed_member' AS distributed_member
FROM telemetry_report;

-- Become an access node by adding a data node
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');

-- Telemetry should show one data node and "acces node" status
REFRESH MATERIALIZED VIEW telemetry_report;
SELECT r -> 'num_data_nodes' AS num_data_nodes,
	   r -> 'distributed_member' AS distributed_member
FROM telemetry_report;

-- See telemetry report from a data node
\ir include/remote_exec.sql
SELECT test.remote_exec(NULL, $$
	   SELECT t -> 'num_data_nodes' AS num_data_nodes,
	   		  t -> 'distributed_member' AS distributed_member
	   FROM get_telemetry_report() t;
$$);

SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
CREATE TABLE disthyper (LIKE normal);
SELECT create_distributed_hypertable('disthyper', 'time', 'device');

-- Show distributed hypertables stats with no data
REFRESH MATERIALIZED VIEW telemetry_report;
SELECT
	jsonb_pretty(rels -> 'distributed_hypertables_access_node') AS distributed_hypertables_an
FROM relations;

-- No datanode-related stats on the access node
SELECT
	jsonb_pretty(rels -> 'distributed_hypertables_data_node') AS distributed_hypertables_dn
FROM relations;

-- Insert data into the distributed hypertable
INSERT INTO disthyper
SELECT * FROM normal;

-- Update telemetry stats and show output on access node and data
-- nodes. Note that the access node doesn't store data so shows
-- zero. It should have stats from ANALYZE, though, like
-- num_reltuples.
ANALYZE disthyper;
REFRESH MATERIALIZED VIEW telemetry_report;
SELECT
	jsonb_pretty(rels -> 'distributed_hypertables_access_node') AS distributed_hypertables_an
FROM relations;

-- Show data node stats
SELECT test.remote_exec(NULL, $$
	   SELECT
			jsonb_pretty(t -> 'relations' -> 'distributed_hypertables_data_node') AS distributed_hypertables_dn
	   FROM get_telemetry_report() t;
$$);

-- Add compression
ALTER TABLE disthyper SET (timescaledb.compress);
SELECT compress_chunk(c) 
FROM show_chunks('disthyper') c ORDER BY c LIMIT 4;

ANALYZE disthyper;
-- Update telemetry stats and show updated compression stats
REFRESH MATERIALIZED VIEW telemetry_report;
SELECT
	jsonb_pretty(rels -> 'distributed_hypertables_access_node') AS distributed_hypertables_an
FROM relations;

-- Show data node stats
SELECT test.remote_exec(NULL, $$
	   SELECT
			jsonb_pretty(t -> 'relations' -> 'distributed_hypertables_data_node') AS distributed_hypertables_dn
	   FROM get_telemetry_report() t;
$$);

-- Create a replicated distributed hypertable and show replication stats
CREATE TABLE disthyper_repl (LIKE normal);
SELECT create_distributed_hypertable('disthyper_repl', 'time', 'device', replication_factor => 2);
INSERT INTO disthyper_repl
SELECT * FROM normal;

REFRESH MATERIALIZED VIEW telemetry_report;
SELECT
	jsonb_pretty(rels -> 'distributed_hypertables_access_node') AS distributed_hypertables_an
FROM relations;

-- Create a continuous aggregate on the distributed hypertable
CREATE MATERIALIZED VIEW distcontagg
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS hour,
  device,
  min(time)
FROM
  disthyper
GROUP BY hour, device;

REFRESH MATERIALIZED VIEW telemetry_report;
SELECT
	jsonb_pretty(rels -> 'continuous_aggregates') AS continuous_aggregates
FROM relations;

DROP VIEW relations;
DROP MATERIALIZED VIEW telemetry_report;
