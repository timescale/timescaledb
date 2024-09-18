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
  SET timezone TO PST8PDT;

  CREATE TABLE temperature (
    time timestamptz NOT NULL,
    value float
  );

  SELECT create_hypertable('temperature', 'time');

  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,
                          '2000-01-01 23:59:59+0','1m') time;

  CREATE MATERIALIZED VIEW cagg
    WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
    SELECT time_bucket('4 hour', time), avg(value)
      FROM temperature
      GROUP BY 1 ORDER BY 1
    WITH NO DATA;
}

# Refresh CAGG in separate transaction
setup
{
     CALL refresh_continuous_aggregate('cagg', NULL, NULL);
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

setup
{
  VACUUM ANALYZE;
}

teardown {
    DROP TABLE temperature CASCADE;
}

session "S1"
step "s1_run_update" {
   CALL refresh_continuous_aggregate('cagg', '2020-01-01 00:00:00', '2021-01-01 00:00:00');
}

step "s1_insert_more_data"
{
  INSERT INTO temperature VALUES('2020-01-02 23:59:59+0', 22);
}

step "s1_prepare" {
   PREPARE pstmt AS SELECT * FROM cagg;
}

step "s1_select" {
   EXPLAIN (COSTS OFF) EXECUTE pstmt;
}

session "S2"
step "s2_prepare" {
   PREPARE pstmt AS SELECT * FROM cagg;
}

step "s2_select" {
   EXPLAIN (COSTS OFF) EXECUTE pstmt;
}

session "S3"
step "s3_lock_invalidation" {
   SELECT debug_waitpoint_enable('cagg_watermark_update_internal_before_refresh');
}

step "s3_release_invalidation" {
   SELECT debug_waitpoint_release('cagg_watermark_update_internal_before_refresh');
}

# Updated watermark (01-01-2020) should be seen by s2_select after s3_release_invalidation completes
permutation "s1_prepare" "s2_prepare" "s3_lock_invalidation" "s2_select" "s1_run_update" "s2_select" "s3_release_invalidation" "s2_select" "s1_select"

# Updated watermark (02-01-2020) should be seen by s2_select after second s3_release_invalidation completes
permutation "s3_lock_invalidation" "s2_select" "s1_run_update" "s2_select" "s3_release_invalidation" "s3_lock_invalidation"("s1_run_update") "s1_insert_more_data" "s1_run_update" "s2_select" "s3_release_invalidation"   "s2_select"
