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
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
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
-- add retention policy, fixed
select add_retention_policy('hyper', interval '1 year', initial_start => now());
-- add compression policy
select add_compression_policy('hyper', interval '3 weeks', initial_start => now());
select r->'num_user_defined_actions_fixed' as UDA_fixed, r->'num_user_defined_actions' AS UDA_drifting FROM get_telemetry_report() r;
select r->'num_continuous_aggs_policies_fixed' as contagg_fixed, r->'num_continuous_aggs_policies' as contagg_drifting FROM get_telemetry_report() r;
select r->'num_compression_policies_fixed' as compress_fixed, r->'num_retention_policies_fixed' as retention_fixed FROM get_telemetry_report() r;
DELETE FROM _timescaledb_config.bgw_job WHERE id = 2;
TRUNCATE _timescaledb_internal.bgw_job_stat_history;
-- create some "errors" for testing
INSERT INTO
_timescaledb_config.bgw_job(id, application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name)
VALUES (2000, 'User-Defined Action [2000]', interval '3 days', interval '1 hour', 5, interval '5 min', 'public', 'custom_action_1'),
(2001, 'User-Defined Action [2001]', interval '3 days', interval '1 hour', 5, interval '5 min', 'public', 'custom_action_2'),
(2002, 'Compression Policy [2002]', interval '3 days', interval '1 hour', 5, interval '5 min', '_timescaledb_functions', 'policy_compression'),
(2003, 'Retention Policy [2003]', interval '3 days', interval '1 hour', 5, interval '5 min', '_timescaledb_functions', 'policy_retention'),
(2004, 'Refresh Continuous Aggregate Policy [2004]', interval '3 days', interval '1 hour', 5, interval '5 min', '_timescaledb_functions', 'policy_refresh_continuous_aggregate'),
-- user decided to define a custom action in the _timescaledb_functions schema, we group it with the User-defined actions
(2005, 'User-Defined Action [2005]', interval '3 days', interval '1 hour', 5, interval '5 min', '_timescaledb_functions', 'policy_refresh_continuous_aggregate');
-- create some errors for them
INSERT INTO
_timescaledb_internal.bgw_job_stat_history(job_id, pid, succeeded, execution_start, execution_finish, data)
values (2000, 12345, false, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"error_data": {"sqlerrcode":"P0001"}, "job": {"proc_schema":"public", "proc_name": "custom_action_1"}}'),
(2000, 23456, false, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"error_data": {"sqlerrcode":"ABCDE"}, "job": {"proc_schema": "public", "proc_name": "custom_action_1"}}'),
(2001, 54321, false, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"error_data": {"sqlerrcode":"P0001"}, "job": {"proc_schema": "public", "proc_name": "custom_action_2"}}'),
(2002, 23443, false, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"error_data": {"sqlerrcode":"JF009"}, "job": {"proc_schema": "_timescaledb_functions", "proc_name": "policy_compression"}}'),
(2003, 14567, false, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"error_data": {"sqlerrcode":"P0001"}, "job": {"proc_schema": "_timescaledb_functions", "proc_name": "policy_retention"}}'),
(2004, 78907, false, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"error_data": {"sqlerrcode":"P0001"}, "job": {"proc_schema": "_timescaledb_functions", "proc_name": "policy_refresh_continuous_aggregate"}}'),
(2005, 45757, false, '2040-01-01 00:00:00+00'::timestamptz, '2040-01-01 00:00:01+00'::timestamptz, '{"error_data": {"sqlerrcode":"P0001"}, "job": {"proc_schema": "_timescaledb_functions", "proc_name": "policy_refresh_continuous_aggregate"}}');

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
