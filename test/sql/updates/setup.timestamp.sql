-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test a hypertable using timestamps
CREATE TABLE PUBLIC.hyper_timestamp (
  time timestamp NOT NULL,
  device_id TEXT NOT NULL,
  value int NOT NULL
);


DO $$
BEGIN
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'interval_to_usec' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    PERFORM create_hypertable('hyper_timestamp'::regclass, 'time'::name, 'device_id'::name, number_partitions => 2, chunk_time_interval=> _timescaledb_internal.interval_to_usec('1 minute'));
  ELSE
    PERFORM create_hypertable('hyper_timestamp'::regclass, 'time'::name, 'device_id'::name, number_partitions => 2, chunk_time_interval=> _timescaledb_functions.interval_to_usec('1 minute'));
  END IF;
END;
$$;

--some old versions use more slice_ids than newer ones. Make this uniform
CALL _timescaledb_testing.restart_dimension_slice_id();
