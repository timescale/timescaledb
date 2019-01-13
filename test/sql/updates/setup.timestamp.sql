-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test a hypertable using timestamps
CREATE TABLE PUBLIC.hyper_timestamp (
  time timestamp NOT NULL,
  device_id TEXT NOT NULL,
  value int NOT NULL
);

SELECT * FROM create_hypertable('hyper_timestamp'::regclass, 'time'::name, 'device_id'::name, number_partitions => 2,
    chunk_time_interval=> _timescaledb_internal.interval_to_usec('1 minute'));

--some old versions use more slice_ids than newer ones. Make this uniform
ALTER SEQUENCE _timescaledb_catalog.dimension_slice_id_seq RESTART WITH 100;
