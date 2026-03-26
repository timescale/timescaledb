# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.


#
# Test concurrent CAgg refreshes and invalidation threshold updates. This isolation test
# checks that we don't skip CAgg updates when two sessions are trying to modify the
# invalidation threshold at the same time.
#
setup
{
  CREATE TABLE temperature (
    time timestamptz NOT NULL,
    value float
  );

  SELECT create_hypertable('temperature', 'time');

  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,
                          '2000-01-01 23:59:59+0','1m') time;

  CREATE MATERIALIZED VIEW cagg_1
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('4 hour', time), avg(value)
      FROM temperature
      GROUP BY 1 ORDER BY 1
    WITH NO DATA;

  CREATE MATERIALIZED VIEW cagg_2
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('4 hour', time), avg(value)
      FROM temperature
      GROUP BY 1 ORDER BY 1
    WITH NO DATA;
}

# Refresh CAGGs in separate transactions
setup
{
     CALL refresh_continuous_aggregate('cagg_1', NULL, NULL);
}

setup
{
     CALL refresh_continuous_aggregate('cagg_2', NULL, NULL);
}

# Add new data to hypertable. This time in the year 2020 instead of 2000 as we
# did for the setup of the CAgg.
setup
{
  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2020-01-01 0:00:00+0'::timestamptz,
                          '2020-01-01 23:59:59+0','1m') time;
}

teardown {
    DROP TABLE temperature CASCADE;
}

session "S1"
step "s1_run_cagg1_refresh" {
   CALL refresh_continuous_aggregate('cagg_1', '2020-01-01 00:00:00+00', '2020-01-03 00:00:00+00');
}
step "s1_run_cagg2_overlap_refresh" {
   CALL refresh_continuous_aggregate('cagg_2', '2020-01-01 00:00:00+00', '2020-01-07 00:00:00+00');
}
step "s1_run_cagg2_nonoverlap_refresh" {
   CALL refresh_continuous_aggregate('cagg_2', '2020-01-01 00:00:00+00', '2020-01-02 00:00:00+00');
}

session "S2"

step "s2_run_cagg2_overlap_refresh" {
   CALL refresh_continuous_aggregate('cagg_2', '2020-01-03 00:00:00+00', '2020-01-05 00:00:00+00');
}

step "s2_insert_new_data_2020" {
  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2020-01-01 0:00:00+0'::timestamptz,
                          '2020-01-07 00:59:59+0','1m') time;
}

session "S3"
step "s3_lock_before_register" {
    BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges;
}

step "s3_release_after_register" {
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
  SELECT materialization_id, start_range, end_range FROM _timescaledb_catalog.continuous_aggs_jobs_refresh_ranges
  ORDER BY materialization_id;
}

# TEST: when 2 concurrent refresh processes on cagg1 and cagg2 start, 1 will wait for the other one to finish registration
## block the processes before they register, then block again before they finish cagg invalidation processing
## so that we can see the active ranges before the refresh processes exit.
permutation "s2_insert_new_data_2020" "s3_lock_before_register" "s1_run_cagg1_refresh" "s2_run_cagg2_overlap_refresh" "s4_enable_before_process_cagg_invalidations" "s3_release_after_register" "s5_show_running_jobs" "s4_release_before_process_cagg_invalidations"

# TEST: Check that two overlapping refresh on cagg2 will not run concurrently. overlap refresh job will fail.
## so we will see only 1 running job.
permutation "s2_insert_new_data_2020" "s3_lock_before_register" "s1_run_cagg2_overlap_refresh" "s2_run_cagg2_overlap_refresh" "s4_enable_before_process_cagg_invalidations" "s3_release_after_register" "s5_show_running_jobs" "s4_release_before_process_cagg_invalidations"
