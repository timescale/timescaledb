-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--telemetry tests that require a community license
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- function call info size is too variable for this test, so disable it
SET timescaledb.telemetry_level='no_functions';

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

CREATE MATERIALIZED VIEW contagg_old
WITH (timescaledb.continuous, timescaledb.finalized=false) AS
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
CALL refresh_continuous_aggregate('contagg_old', NULL, NULL);

-- ANALYZE to get updated reltuples stats
ANALYZE normal, hyper, part;

SELECT count(c) FROM show_chunks('hyper') c;
SELECT count(c) FROM show_chunks('contagg') c;
SELECT count(c) FROM show_chunks('contagg_old') c;

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
SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');

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

SELECT node_name, database, node_created, database_created, extension_created
FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
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

CREATE MATERIALIZED VIEW distcontagg_old
WITH (timescaledb.continuous, timescaledb.finalized=false) AS
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

-- check telemetry for fixed schedule jobs works
create or replace procedure job_test_fixed(jobid int, config jsonb) language plpgsql as $$
begin
raise log 'this is job_test_fixed';
end
$$;

create or replace procedure job_test_drifting(jobid int, config jsonb) language plpgsql as $$
begin
raise log 'this is job_test_drifting';
end
$$;
-- before adding the jobs
select get_telemetry_report()->'num_user_defined_actions_fixed';
select get_telemetry_report()->'num_user_defined_actions';

select add_job('job_test_fixed', '1 week');
select add_job('job_test_drifting', '1 week', fixed_schedule => false);
-- add continuous aggregate refresh policy for contagg
select add_continuous_aggregate_policy('contagg', interval '3 weeks', NULL, interval '3 weeks'); -- drifting
select add_continuous_aggregate_policy('contagg_old', interval '3 weeks', NULL, interval '3 weeks', initial_start => now()); -- fixed
-- add retention policy, fixed
select add_retention_policy('hyper', interval '1 year', initial_start => now());
-- add compression policy
select add_compression_policy('hyper', interval '3 weeks', initial_start => now());
select r->'num_user_defined_actions_fixed' as UDA_fixed, r->'num_user_defined_actions' AS UDA_drifting FROM get_telemetry_report() r;
select r->'num_continuous_aggs_policies_fixed' as contagg_fixed, r->'num_continuous_aggs_policies' as contagg_drifting FROM get_telemetry_report() r;
select r->'num_compression_policies_fixed' as compress_fixed, r->'num_retention_policies_fixed' as retention_fixed FROM get_telemetry_report() r;
DELETE FROM _timescaledb_config.bgw_job WHERE id = 2;
TRUNCATE _timescaledb_internal.job_errors;
-- create some "errors" for testing
INSERT INTO
_timescaledb_config.bgw_job(id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name)
VALUES (2000, 'User-Defined Action [2000]', interval '3 days', interval '1 hour', 5, interval '5 min', 'public', 'custom_action_1'),
(2001, 'User-Defined Action [2001]', interval '3 days', interval '1 hour', 5, interval '5 min', 'public', 'custom_action_2'),
(2002, 'Compression Policy [2002]', interval '3 days', interval '1 hour', 5, interval '5 min', '_timescaledb_internal', 'policy_compression'),
(2003, 'Retention Policy [2003]', interval '3 days', interval '1 hour', 5, interval '5 min', '_timescaledb_internal', 'policy_retention'),
(2004, 'Refresh Continuous Aggregate Policy [2004]', interval '3 days', interval '1 hour', 5, interval '5 min', '_timescaledb_internal', 'policy_refresh_continuous_aggregate'),
-- user decided to define a custom action in the _timescaledb_internal schema, we group it with the User-defined actions
(2005, 'User-Defined Action [2005]', interval '3 days', interval '1 hour', 5, interval '5 min', '_timescaledb_internal', 'policy_refresh_continuous_aggregate');
-- create some errors for them
INSERT INTO
_timescaledb_internal.job_errors(job_id, pid, start_time, finish_time, error_data)
values (2000, 12345, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"sqlerrcode":"P0001", "proc_schema":"public", "proc_name": "custom_action_1"}'),
(2000, 23456, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"sqlerrcode":"ABCDE", "proc_schema": "public", "proc_name": "custom_action_1"}'),
(2001, 54321, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"sqlerrcode":"P0001", "proc_schema":"public", "proc_name": "custom_action_2"}'),
(2002, 23443, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"sqlerrcode":"JF009", "proc_schema":"_timescaledb_internal", "proc_name": "policy_compression"}'),
(2003, 14567, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"sqlerrcode":"P0001", "proc_schema":"_timescaledb_internal", "proc_name": "policy_retention"}'),
(2004, 78907, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"sqlerrcode":"P0001", "proc_schema":"_timescaledb_internal", "proc_name": "policy_refresh_continuous_aggregate"}'),
(2005, 45757, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"sqlerrcode":"P0001", "proc_schema":"_timescaledb_internal", "proc_name": "policy_refresh_continuous_aggregate"}');

-- we have 3 error records for user-defined actions, and three for policies, so we expect 4 types of jobs
SELECT jsonb_pretty(get_telemetry_report() -> 'errors_by_sqlerrcode');
-- for job statistics, insert some records into bgw_job_stats
INSERT INTO _timescaledb_internal.bgw_job_stat
values
(2000, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '-infinity'::timestamptz, '-infinity'::timestamptz,
false, 1, interval '00:00:00', interval '00:00:02', 0, 1, 0, 1, 0),
(2001, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '-infinity'::timestamptz, '-infinity'::timestamptz,
false, 1, interval '00:00:00', interval '00:00:02', 0, 1, 0, 1, 0),
(2002, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '-infinity'::timestamptz, '-infinity'::timestamptz,
false, 1, interval '00:00:00', interval '00:00:02', 0, 1, 0, 1, 0),
(2003, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '-infinity'::timestamptz, '-infinity'::timestamptz,
false, 1, interval '00:00:00', interval '00:00:02', 0, 1, 0, 1, 0),
(2004, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '-infinity'::timestamptz, '-infinity'::timestamptz,
false, 1, interval '00:00:00', interval '00:00:02', 0, 1, 0, 1, 0),
(2005, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '-infinity'::timestamptz, '-infinity'::timestamptz,
false, 1, interval '00:00:00', interval '00:00:02', 0, 1, 0, 1, 0);
SELECT jsonb_pretty(get_telemetry_report() -> 'stats_by_job_type');


-- create nested continuous aggregates - copied from cagg_on_cagg_common
CREATE TABLE conditions (
  time timestamptz NOT NULL,
  temperature int
);

SELECT create_hypertable('conditions', 'time');
CREATE MATERIALIZED VIEW conditions_summary_hourly_1
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
  time_bucket('1 hour', "time") AS bucket,
  SUM(temperature) AS temperature
FROM conditions
GROUP BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW conditions_summary_daily_2
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
  time_bucket('1 day', "bucket") AS bucket,
  SUM(temperature) AS temperature
FROM conditions_summary_hourly_1
GROUP BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW conditions_summary_weekly_3
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
  time_bucket('1 week', "bucket") AS bucket,
  SUM(temperature) AS temperature
FROM conditions_summary_daily_2
GROUP BY 1
WITH NO DATA;

SELECT jsonb_pretty(get_telemetry_report() -> 'relations' -> 'continuous_aggregates' -> 'num_caggs_nested');

DROP VIEW relations;
DROP MATERIALIZED VIEW telemetry_report;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;

