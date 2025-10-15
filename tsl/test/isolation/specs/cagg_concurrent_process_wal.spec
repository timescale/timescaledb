# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Test concurrent hypertable invalidation processing when using
# WAL-based hypertable invalidations.
#
# If two refresh procedures are executing at the same time, the second
# one should wait for the first one to complete before dealing with
# materializations.

setup
{
  SET timezone TO PST8PDT;
  SET timescaledb.enable_cagg_wal_based_invalidation TO true;

  CREATE TABLE temperature (
    time timestamptz NOT NULL,
    value float
  );

  CREATE VIEW my_locks AS
  SELECT locktype, relation::regclass AS relname,
         classid::regclass AS objtyp,
         objid::regclass, 
         mode, granted, application_name
    FROM pg_locks JOIN pg_stat_activity USING (pid)
   WHERE database = (SELECT oid FROM pg_database WHERE current_database() = datname)
     AND pid != pg_backend_pid()
   ORDER BY locktype, application_name, objtyp, objid;

  SELECT create_hypertable('temperature', 'time');

  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2020-01-01 0:00:00+0'::timestamptz,
                          '2020-01-01 23:59:59+0','1m') time;
}

# All the below need to be in separate transactions.
setup {
  CREATE MATERIALIZED VIEW cagg_1
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('4 hour', time), avg(value)
      FROM temperature
      GROUP BY 1 ORDER BY 1
    WITH NO DATA;
}

setup {
  CREATE MATERIALIZED VIEW cagg_2
    WITH (timescaledb.continuous) AS
    SELECT time_bucket('4 hour', time), avg(value)
      FROM temperature
      GROUP BY 1 ORDER BY 1
    WITH NO DATA;
}

setup {
    CALL refresh_continuous_aggregate('cagg_1', '2020-01-01'::timestamptz, '2025-01-01');
}
setup {
    CALL refresh_continuous_aggregate('cagg_2', '2020-01-01'::timestamptz, '2025-01-01');
}

# Create some invalidations
setup
{
  INSERT INTO temperature
    SELECT time, ceil(random() * 100)::int
      FROM generate_series('2023-01-01'::timestamptz, '2023-01-02', '1m') time;
}

teardown {
    DROP VIEW my_locks;
    DROP MATERIALIZED VIEW cagg_2;
    DROP MATERIALIZED VIEW cagg_1;
    DROP TABLE temperature CASCADE;
}

session "s1"
setup { set application_name = 's1'; }
step s1_refresh {CALL _timescaledb_functions.process_hypertable_invalidations(ARRAY['temperature']);}

session "s2"
setup { set application_name = 's2'; }
step s2_refresh {CALL _timescaledb_functions.process_hypertable_invalidations(ARRAY['temperature']);}

session "locking"
step lock_enable {SELECT debug_waitpoint_enable('multi_invalidation_process_invalidations');}
step lock_show {SELECT locktype, granted, application_name, objtyp, objid FROM my_locks WHERE locktype in ('object', 'advisory');}
step lock_release {SELECT debug_waitpoint_release('multi_invalidation_process_invalidations');}

permutation lock_enable s1_refresh s2_refresh lock_show lock_release

