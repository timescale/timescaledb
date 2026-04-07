# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.
#
# Test concurrent CAgg refresh policies are executed
#
setup
{
-- create 3 adjacent policies ---
CREATE TABLE test_3pol_timestamptz (
    time timestamptz NOT NULL,
    a INTEGER,
    b INTEGER
);

SELECT create_hypertable('test_3pol_timestamptz', 'time', chunk_time_interval => '1 day'::interval);

INSERT INTO test_3pol_timestamptz
SELECT t, 1, (random() * 100)::int
FROM
generate_series('2025-05-20T11:05:00+00', '2025-05-27T12:05:00+00', INTERVAL '1 hour') t;

CREATE MATERIALIZED VIEW mat_3pol_m1
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    count(a),
    sum(b)
FROM test_3pol_timestamptz
GROUP BY 1
WITH NO DATA;

/* Three adjacent policies on mat_3pol_m1 */
CREATE TABLE cagg_policy_jobs (job_id integer, job_name text);

INSERT INTO cagg_policy_jobs
SELECT add_continuous_aggregate_policy('mat_3pol_m1', '7 days'::interval, '3 days'::interval, '12 h'::interval, buckets_per_batch => 0),
       'job_7d_3d';

INSERT INTO cagg_policy_jobs
SELECT add_continuous_aggregate_policy('mat_3pol_m1', '3 days'::interval, '1 day'::interval, '12 h'::interval, buckets_per_batch => 0),
       'job_3d_1d';

INSERT INTO cagg_policy_jobs
SELECT add_continuous_aggregate_policy('mat_3pol_m1', '1 day'::interval, '1 hour'::interval, '12 h'::interval, buckets_per_batch => 0),
       'job_1d_1h';
}

teardown {
    DROP TABLE test_3pol_timestamptz CASCADE;
}

session "S1"
setup
{
    SET timescaledb.current_timestamp_mock TO '2025-05-27 12:30:00+00';
    SET client_min_messages TO error;
}
step "s1_select" {
   select min(time) at time zone 'UTC' , max(time) at time zone 'UTC'
   FROM test_3pol_timestamptz; 
}
step "s1_run_pol7d_3d_refresh" {
   DO $$
   DECLARE
     jid integer;
   BEGIN
     SELECT job_id INTO jid FROM cagg_policy_jobs WHERE job_name = 'job_7d_3d';
     CALL run_job(jid);
   END;
   $$;
}

session "S12"
setup
{
    SET timescaledb.current_timestamp_mock TO '2025-05-27 12:30:00+00';
    SET client_min_messages TO error;
}
step "s12_run_pol3d_1d_refresh" {
   DO $$
   DECLARE
     jid integer;
   BEGIN
     SELECT job_id INTO jid FROM cagg_policy_jobs WHERE job_name = 'job_3d_1d';
     CALL run_job(jid);
   END;
   $$;
}

session "S13"
setup
{
    SET timescaledb.current_timestamp_mock TO '2025-05-27 12:30:00+00';
    SET client_min_messages TO error;
}
step "s13_run_pol1d_refresh" {
   DO $$
   DECLARE
     jid integer;
   BEGIN
     SELECT job_id INTO jid FROM cagg_policy_jobs WHERE job_name = 'job_1d_1h';
     CALL run_job(jid);
   END;
   $$;
}

session "S3"
step "s3_lock_before_register" {
    -- lock jobs_refresh_ranges table to serialize registration
    BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges;
}

step "s3_release_after_register" {
   -- release lock on jobs_refresh_ranges table
   ROLLBACK;
}

session "S4"
step "s4_enable_before_process_cagg_invalidations" {
    SELECT debug_waitpoint_enable('before_process_cagg_invalidations_for_refresh_lock');
}

step "s4_release_before_process_cagg_invalidations" {
    SELECT debug_waitpoint_release('before_process_cagg_invalidations_for_refresh_lock');
}

session "S5"
step "s5_show_running_jobs" {
  SELECT ca.user_view_name AS cagg_name, r.start_range, r.end_range,
         to_timestamp(r.start_range / 1000000) AT TIME ZONE 'UTC' AS start_ts_utc,
         to_timestamp(r.end_range / 1000000) AT TIME ZONE 'UTC' AS end_ts_utc
  FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges r
  JOIN _timescaledb_catalog.continuous_agg ca ON r.materialization_id = ca.mat_hypertable_id
  ORDER BY ca.user_view_name, start_range;
}

## TEST: when 3 concurrent refresh policies execute, they serialize on registration, then execute succesfully
## since these are adjacent policies 2 concurrent refresh processes, the extend last bucket behavior will apply
## observe the ranges recorded for each policy run
permutation "s1_select" "s3_lock_before_register" "s1_run_pol7d_3d_refresh" "s12_run_pol3d_1d_refresh"("s1_run_pol7d_3d_refresh") "s13_run_pol1d_refresh"("s12_run_pol3d_1d_refresh") "s4_enable_before_process_cagg_invalidations" "s3_release_after_register" "s5_show_running_jobs" "s4_release_before_process_cagg_invalidations"


