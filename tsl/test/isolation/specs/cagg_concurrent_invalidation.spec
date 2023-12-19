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
step "s1_run_update" {
   CALL refresh_continuous_aggregate('cagg_1', '2020-01-01 00:00:00', '2021-01-01 00:00:00');
}

session "S2"
step "s2_run_update" {
   CALL refresh_continuous_aggregate('cagg_2', '2020-01-01 00:00:00', '2021-01-01 00:00:00');
}

session "S3"
step "s3_lock_invalidation" {
   SELECT debug_waitpoint_enable('invalidation_threshold_scan_update_enter');
}

step "s3_release_invalidation" {
   SELECT debug_waitpoint_release('invalidation_threshold_scan_update_enter');
}

# Check that both CAggs have a watermark in 2020 after the updates are executed.
# mat_hypertable_id is not included in the query to make the test independent of the
# actual hypertable ids.
step "s3_check_watermarks" {
  SELECT _timescaledb_functions.to_timestamp(watermark)
      FROM _timescaledb_catalog.continuous_aggs_watermark
      ORDER BY mat_hypertable_id;
}

permutation "s3_lock_invalidation" "s1_run_update" "s2_run_update" "s3_release_invalidation" "s3_check_watermarks"("s1_run_update", "s2_run_update")
