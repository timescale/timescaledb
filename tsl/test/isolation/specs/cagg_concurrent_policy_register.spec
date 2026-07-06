# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
#
# Setup:
#   hypertable (test_3pol_timestamptz)
#     -> L1 CAgg (mat_3pol_m1)   3 adjacent refresh policies
#     -> L2 CAgg (mat_2pol_m2)   2 adjacent refresh policies (hierarchical)
#
setup
{
  -- create 3 adjacent policies ---
  SELECT _timescaledb_functions.stop_background_workers();

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

# Materialize L1 so the hierarchical L2 CAgg has source data
setup
{
  CALL refresh_continuous_aggregate('mat_3pol_m1', NULL, NULL);
}

# Create the hierarchical L2 CAgg with two adjacent refresh policies of its own
setup
{
  CREATE MATERIALIZED VIEW mat_2pol_m2
  WITH (timescaledb.continuous, timescaledb.materialized_only=true)
  AS
  SELECT
      time_bucket('1 day', bucket) AS bucket,
      sum(count) AS cnt,
      sum(sum) AS sumb
  FROM mat_3pol_m1
  GROUP BY 1
  WITH NO DATA;

  INSERT INTO cagg_policy_jobs
  SELECT add_continuous_aggregate_policy('mat_2pol_m2', NULL, '3 days'::interval, '12 h'::interval, buckets_per_batch => 0),
        'l2_job_hist';

  INSERT INTO cagg_policy_jobs
  SELECT add_continuous_aggregate_policy('mat_2pol_m2', '3 days'::interval, '1 hour'::interval, '12 h'::interval, buckets_per_batch => 0),
        'l2_job_recent';
}

teardown {
    DROP MATERIALIZED VIEW IF EXISTS mat_2pol_m2 CASCADE;
    DROP MATERIALIZED VIEW IF EXISTS mat_3pol_m1 CASCADE;
    DROP TABLE IF EXISTS test_3pol_timestamptz CASCADE;
    DROP TABLE IF EXISTS cagg_policy_jobs;
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
step "s1_run_l2_hist" {
   DO $$
   DECLARE
     jid integer;
   BEGIN
     SELECT job_id INTO jid FROM cagg_policy_jobs WHERE job_name = 'l2_job_hist';
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
step "s12_run_l2_recent" {
   DO $$
   DECLARE
     jid integer;
   BEGIN
     SELECT job_id INTO jid FROM cagg_policy_jobs WHERE job_name = 'l2_job_recent';
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
step "s5_l2_consistency" {
  -- L2 must match re-aggregation from L1 for all materialized buckets
  SELECT d.bucket AT TIME ZONE 'UTC' AS bucket,
         (d.cnt = h.cnt) AS cnt_match,
         (d.sumb = h.sumb) AS sumb_match
  FROM mat_2pol_m2 d
  JOIN (
      SELECT time_bucket('1 day', bucket) AS bucket,
             sum(count) AS cnt,
             sum(sum) AS sumb
      FROM mat_3pol_m1 GROUP BY 1
  ) h ON h.bucket = d.bucket
  ORDER BY 1;
}

## TEST: when 3 concurrent refresh policies execute, they serialize on registration, then execute succesfully
## since these are adjacent policies 2 concurrent refresh processes, the extend last bucket behavior will apply
## observe the ranges recorded for each policy run
permutation "s1_select" "s3_lock_before_register" "s1_run_pol7d_3d_refresh" "s12_run_pol3d_1d_refresh"("s1_run_pol7d_3d_refresh") "s13_run_pol1d_refresh"("s12_run_pol3d_1d_refresh") "s4_enable_before_process_cagg_invalidations" "s3_release_after_register" "s5_show_running_jobs" "s4_release_before_process_cagg_invalidations"

## TEST: two concurrent refresh policies on the hierarchical L2 CAgg serialize on registration,
## then both execute succesfully. L2 stays consistent with L1.
permutation "s3_lock_before_register" "s1_run_l2_hist" "s12_run_l2_recent"("s1_run_l2_hist") "s4_enable_before_process_cagg_invalidations" "s3_release_after_register" "s5_show_running_jobs" "s4_release_before_process_cagg_invalidations" "s5_l2_consistency"
