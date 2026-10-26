-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timezone TO 'UTC';
SET timescaledb.current_timestamp_mock TO '2025-01-10 12:00:00+00';

-- Test 1: A frequent policy on recent buckets must advance the late-arrival
-- tracking window on every run, even when no tracked writes arrive. A slower
-- policy on older buckets must then materialize every tracked late write.
CREATE TABLE policy_readings(time timestamptz NOT NULL, tenant text, value integer);
SELECT create_hypertable('policy_readings', 'time', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE policy_readings SET (
    timescaledb.cagg_enable_granular_refresh = true,
    timescaledb.cagg_granular_refresh_column = 'tenant',
    timescaledb.cagg_granular_refresh_start_offset = '2 hours',
    timescaledb.cagg_granular_refresh_end_offset = '1 hour'
);
INSERT INTO policy_readings VALUES ('2025-01-10 12:00:00+00', 'seed', 1);
CREATE MATERIALIZED VIEW policy_minutes
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 minute', time) AS bucket, tenant, sum(value) AS total
FROM policy_readings GROUP BY 1, 2 WITH DATA;
ALTER MATERIALIZED VIEW policy_minutes SET (timescaledb.enable_granular_refresh = true);

SELECT add_continuous_aggregate_policy('policy_minutes',
    start_offset => INTERVAL '1 hour', end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute', buckets_per_batch => 1) AS recent_job \gset
SELECT add_continuous_aggregate_policy('policy_minutes',
    start_offset => INTERVAL '2 hours', end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '10 minutes', buckets_per_batch => 1) AS older_job \gset
SELECT scheduled FROM alter_job(:recent_job, scheduled => false);
SELECT scheduled FROM alter_job(:older_job, scheduled => false);

-- insert data, updates current mock time and then run the recent policy i.e. the [1 hour, 1 minute] policy
CREATE PROCEDURE run_recent_policy(job_id integer, first_minute integer, last_minute integer)
LANGUAGE plpgsql AS $$
DECLARE
    minute integer;
BEGIN
    FOR minute IN first_minute..last_minute LOOP
        PERFORM set_config('timescaledb.current_timestamp_mock',
            ('2025-01-10 12:00:00+00'::timestamptz + minute * INTERVAL '1 minute')::text, false);
        INSERT INTO policy_readings VALUES (
            '2025-01-10 11:58:30+00'::timestamptz + minute * INTERVAL '1 minute', 'recent', minute);
        CALL run_job(job_id);
    END LOOP;
END;
$$;

INSERT INTO policy_readings VALUES ('2025-01-10 10:20:30+00', 'older', 2);
-- Expected seqnum: 1, before either policy runs.
SELECT seq_num
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');
CALL run_recent_policy(:recent_job, 1, 1);

INSERT INTO policy_readings VALUES ('2025-01-10 11:00:30+00', 'newly_tracked', 4);
-- Expected seqnum: 2; above write is inside the newly advanced tracking window.
SELECT seqnum
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable
                       WHERE table_name = 'policy_readings')
  AND lowest_modified_value = _timescaledb_functions.to_unix_microseconds('2025-01-10 11:00:30+00'::timestamptz);

-- we also see the INSERT being tracked inside the tracking window
SELECT seq_num, nentries,
       _timescaledb_functions.to_timestamp(late_threshold_start) AT TIME ZONE 'UTC' AS window_start,
       _timescaledb_functions.to_timestamp(late_threshold_end) AT TIME ZONE 'UTC' AS window_end
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');

-- will run job 9 times. each time seqnum and late-arrival window will get updated.
CALL run_recent_policy(:recent_job, 2, 10);
-- Expected seqnum: 11, with a tracking window of [10:10, 11:10) UTC.
SELECT seq_num,
       _timescaledb_functions.to_timestamp(late_threshold_start) AT TIME ZONE 'UTC' AS window_start,
       _timescaledb_functions.to_timestamp(late_threshold_end) AT TIME ZONE 'UTC' AS window_end
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');

--run job to process older time range
SET timescaledb.cagg_refresh_stats_level TO summary;
CALL run_job(:older_job);
RESET timescaledb.cagg_refresh_stats_level;
-- Expected seqnum: 12; the older policy flushes once across its two batches.
SELECT seq_num
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');

-- Expected: older=2, newly_tracked=4, seed=1, and ten recent buckets with totals 1..10.
SELECT bucket AT TIME ZONE 'UTC' AS bucket, tenant, total
FROM policy_minutes ORDER BY bucket, tenant;

DROP PROCEDURE run_recent_policy(integer, integer, integer);
DROP MATERIALIZED VIEW policy_minutes;
DROP TABLE policy_readings;
RESET timescaledb.current_timestamp_mock;
RESET timezone;
