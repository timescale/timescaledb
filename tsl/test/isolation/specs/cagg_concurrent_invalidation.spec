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
   CALL refresh_continuous_aggregate('cagg_1', '2020-01-01 00:00:00', '2025-01-01 00:00:00');
}

session "S2"
step "s2_run_update" {
   CALL refresh_continuous_aggregate('cagg_2', '2020-01-01 00:00:00', '2025-01-01 00:00:00');
}

step "s2_insert_new_data_2022" {
  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2022-01-01 0:00:00+0'::timestamptz,
                          '2022-01-01 23:59:59+0','1m') time;
}

step "s2_insert_new_data_2023" {
  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2023-01-01 0:00:00+0'::timestamptz,
                          '2023-01-01 23:59:59+0','1m') time;
}

session "S3"
step "s3_lock_invalidation" {
   SELECT debug_waitpoint_enable('invalidation_threshold_scan_update_enter');
}

step "s3_release_invalidation" {
   SELECT debug_waitpoint_release('invalidation_threshold_scan_update_enter');
}

step "s3_lock_invalidation_tuple_found" {
   SELECT debug_waitpoint_enable('invalidation_tuple_found_done');
}

step "s3_release_invalidation_tuple_found" {
   SELECT debug_waitpoint_release('invalidation_tuple_found_done');
}

# Check that both CAggs have a watermark in 2020 after the updates are executed.
# mat_hypertable_id is not included in the query to make the test independent of the
# actual hypertable ids.
step "s3_check_watermarks" {
  SELECT _timescaledb_functions.to_timestamp(watermark)
      FROM _timescaledb_catalog.continuous_aggs_watermark
      ORDER BY mat_hypertable_id;
}

# Check that all watermarks in all sessions are up to date after running an update 
permutation "s3_lock_invalidation" "s1_run_update" "s2_run_update" "s3_release_invalidation" "s3_check_watermarks"("s1_run_update", "s2_run_update")

###
# Check that we don't leak the old AND the new value of the invalidation watermark during an update into another session
###

# (1) Session 2 - Inserts new data of the year 2022 to give the CAgg refresh job some work to do.
# (2) Session 3 - Blocks the invalidation watermark scanner after reading the first watermark for the hypertable.
# (3) Session 2 - Inserts new data of the year 2023, during the commit it reads the current invalidation watermark and blocks due to (2).
# (4) Session 1 - Refreshes a CAgg on this hypertable. During the update the invalidation watermark is deleted and a new one is written.
# (5) Session 3 - Unblocks the scanner of session 2.
# (6) Session 2 - Continues the scan. The new inserted invalidation watermark should not be seen since the transaction was started after s2 has started the scan in (3).
permutation "s2_insert_new_data_2022" "s3_lock_invalidation_tuple_found" "s2_insert_new_data_2023" "s1_run_update" "s3_release_invalidation_tuple_found"
