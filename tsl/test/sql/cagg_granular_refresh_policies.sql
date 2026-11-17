-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SET timezone TO 'UTC';
SET timescaledb.current_timestamp_mock TO '2025-01-10 12:00:00+00';

-- Persisted tenant tracking rows of policy_readings.
CREATE VIEW policy_readings_tenant_info_view AS
SELECT tenant_id, seqnum,
       _timescaledb_functions.to_timestamp(min_timestamp) AT TIME ZONE 'UTC' AS first_write,
       _timescaledb_functions.to_timestamp(max_timestamp) AT TIME ZONE 'UTC' AS last_write
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable
                       WHERE table_name = 'policy_readings');

-- Test 1: A frequent policy on recent buckets must not flush the tenant
-- tracker, so the generation and its late-arrival window stay unchanged. A
-- slower policy on older buckets flushes once and must materialize every late
-- write.
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
-- Expected seqnum: NULL; the tracking window did not advance, so the above write is outside it.
SELECT seqnum
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable
                       WHERE table_name = 'policy_readings')
  AND lowest_modified_value = _timescaledb_functions.to_unix_microseconds('2025-01-10 11:00:30+00'::timestamptz);

-- still generation 1 with its original window; only the 'older' write is tracked
SELECT seq_num, nentries,
       _timescaledb_functions.to_timestamp(late_threshold_start) AT TIME ZONE 'UTC' AS window_start,
       _timescaledb_functions.to_timestamp(late_threshold_end) AT TIME ZONE 'UTC' AS window_end
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');

-- will run job 9 times. seqnum and late-arrival window must not change.
CALL run_recent_policy(:recent_job, 2, 10);
-- Expected seqnum: 1, with the original tracking window of [10:00, 11:00) UTC.
SELECT seq_num,
       _timescaledb_functions.to_timestamp(late_threshold_start) AT TIME ZONE 'UTC' AS window_start,
       _timescaledb_functions.to_timestamp(late_threshold_end) AT TIME ZONE 'UTC' AS window_end
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');

--run job to process older time range
SET timescaledb.cagg_refresh_stats_level TO summary;
CALL run_job(:older_job);
RESET timescaledb.cagg_refresh_stats_level;
-- Expected seqnum: 2; the older policy flushes once across its two batches.
SELECT seq_num
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');

-- Expected: older=2, newly_tracked=4, seed=1, and ten recent buckets with totals 1..10.
SELECT bucket AT TIME ZONE 'UTC' AS bucket, tenant, total
FROM policy_minutes ORDER BY bucket, tenant;

-- Test 2: A window entirely older than the fresh late window must still flush.
-- The late write remains pending and its tenant tracking row must be persisted.
INSERT INTO policy_readings VALUES ('2025-01-10 10:30:00+00', 'pending', 8);
SELECT seq_num AS before_seq
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings') \gset
CALL refresh_continuous_aggregate('policy_minutes', '2025-01-10 09:00+00', '2025-01-10 10:00+00',
    options => '{"buckets_per_batch":0}');
SELECT seq_num - :before_seq AS rotations
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');
SELECT tenant_id, seqnum, first_write, last_write FROM policy_readings_tenant_info_view WHERE tenant_id = 'pending';

-- Test 3: A window entirely newer than the late window must not flush. The
-- late write stays in the in-memory generation and no tracking row is persisted.
INSERT INTO policy_readings VALUES ('2025-01-10 10:34:00+00', 'still_pending', 128);
SELECT seq_num AS before_seq
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings') \gset
CALL refresh_continuous_aggregate('policy_minutes', '2025-01-10 11:40+00', '2025-01-10 12:10+00',
    options => '{"buckets_per_batch":0}');
SELECT seq_num - :before_seq AS rotations, nentries, status
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');
SELECT count(*) AS tracking_rows FROM policy_readings_tenant_info_view WHERE tenant_id = 'still_pending';

-- Test 4: Adjacent late invalidations interleaved with recent policy runs must
-- retain one seqnum in the log and merge into a single materialization range
-- when the late window is refreshed.
INSERT INTO policy_readings VALUES ('2025-01-10 10:40:00+00', 'merge_a', 1);
CALL run_job(:recent_job);
INSERT INTO policy_readings VALUES ('2025-01-10 10:41:00+00', 'merge_b', 2);
CALL run_job(:recent_job);
INSERT INTO policy_readings VALUES ('2025-01-10 10:42:00+00', 'merge_a', 4);
CALL run_job(:recent_job);
SELECT seqnum,
       _timescaledb_functions.to_timestamp(lowest_modified_value) AT TIME ZONE 'UTC' AS window_start,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AT TIME ZONE 'UTC' AS window_end
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE materialization_id = (SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
                            WHERE user_view_name = 'policy_minutes')
  AND seqnum IS NOT NULL
ORDER BY lowest_modified_value, seqnum;
SET client_min_messages TO log;
SET timescaledb.cagg_refresh_stats_level TO verbose_log;
CALL refresh_continuous_aggregate('policy_minutes', '2025-01-10 10:40+00', '2025-01-10 10:43+00',
    options => '{"buckets_per_batch":0}');
RESET timescaledb.cagg_refresh_stats_level;
RESET client_min_messages;
SELECT bucket AT TIME ZONE 'UTC' AS bucket, tenant, total FROM policy_minutes
WHERE tenant IN ('merge_a', 'merge_b') ORDER BY bucket, tenant;

-- Test 5: Calendar offsets that cross at the mocked time create an empty late
-- window. Even a recent refresh must rotate the generation in this case.
SET timescaledb.current_timestamp_mock TO '2025-03-15 12:00:00+00';
ALTER TABLE policy_readings SET (
    timescaledb.cagg_granular_refresh_start_offset = '1 month',
    timescaledb.cagg_granular_refresh_end_offset = '29 days'
);
INSERT INTO policy_readings VALUES ('2025-03-15 12:00:00+00', 'march_seed', 1);
SELECT seq_num AS before_seq
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings') \gset
CALL refresh_continuous_aggregate('policy_minutes', '2025-03-15 11:00+00', '2025-03-15 12:00+00',
    options => '{"buckets_per_batch":0}');
SELECT seq_num - :before_seq AS rotations
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('policy_readings');

DROP VIEW policy_readings_tenant_info_view;
DROP PROCEDURE run_recent_policy(integer, integer, integer);
DROP MATERIALIZED VIEW policy_minutes;
DROP TABLE policy_readings;

RESET timescaledb.current_timestamp_mock;
RESET timezone;
