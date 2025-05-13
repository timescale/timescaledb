# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test concurrent move and refresh to make sure that they do not cause
# issues that can lead to deadlocks.
#
# We check this by blocking the processing of the hypertable
# invalidation log by locking all the rows of the hypertable
# invalidation log. This will allow the functions to take locks on the
# table but block on reading the rows.

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
      FROM generate_series('2020-01-01 0:00:00+0'::timestamptz,
                          '2020-01-01 23:59:59+0','1m') time;

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

# Refresh in separate transactions
setup {CALL refresh_continuous_aggregate('cagg_1', '2020-01-01'::timestamptz, '2025-01-01');}
setup {CALL refresh_continuous_aggregate('cagg_2', '2020-01-01'::timestamptz, '2025-01-01');}

# Create some invalidations
setup
{
  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2023-01-01 0:00:00+0'::timestamptz,
                          '2023-01-01 23:59:59+0','1m') time;
}

teardown {
    DROP TABLE temperature CASCADE;
}

session "S1"
step "s1_refresh" {
   CALL refresh_continuous_aggregate('cagg_1', '2020-01-01 00:00:00', '2025-01-01 00:00:00');
}

session "S2"
step "s2_refresh" {
   CALL refresh_continuous_aggregate('cagg_2', '2020-01-01 00:00:00', '2025-01-01 00:00:00');
}

session "S3"
# This will generate an error since the refreshes above already
# processed the hypertable invalidation log. This can happen also for
# two concurrent refreshes.
step "s3_process" {
   CALL _timescaledb_functions.process_hypertable_invalidations('temperature');
}

session "S4"

# Show thresholds just so that we know that they are correctly set.
step s4_show_thresholds {
    SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
           _timescaledb_functions.to_timestamp(watermark) AS watermark
      FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
      JOIN _timescaledb_catalog.hypertable ht
	ON hypertable_id = ht.id
    ORDER BY 1;
}

step s4_before
{
    START TRANSACTION;
    SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
	   _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest_modified_value,
	   _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest_modified_value
      FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
      JOIN _timescaledb_catalog.hypertable ht
	ON hypertable_id = ht.id
    ORDER BY 1, 2, 3 FOR UPDATE;
}

step s4_release
{
    ROLLBACK;
}

step "s4_after" {
    SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
	   _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest_modified_value,
	   _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest_modified_value
      FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
      JOIN _timescaledb_catalog.hypertable ht
	ON hypertable_id = ht.id
    ORDER BY 1, 2, 3;
}

# Check that processing the hypertable and refreshing the associated
# continuous aggerates do not lead to deadlock.
permutation s4_show_thresholds s4_before s1_refresh s2_refresh s3_process s4_release s4_after
