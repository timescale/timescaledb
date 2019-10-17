-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--telemetry tests that involve compression (not to be run for PG 9.6)
--create a continuous agg

CREATE TABLE device_readings (
      observation_time  TIMESTAMPTZ       NOT NULL
);
SELECT table_name FROM create_hypertable('device_readings', 'observation_time');

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_hypertables');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_compressed_hypertables');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'compressed_heap_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'compressed_index_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'compressed_toast_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'uncompressed_heap_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'uncompressed_index_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'uncompressed_toast_size');

--test compression
ALTER TABLE device_readings SET(timescaledb.compress);
insert into device_readings select  generate_series('2018-01-01 00:00'::timestamp, '2018-01-02 00:00'::timestamp, '1 hour');
select  count(compress_chunk(ch1.schema_name|| '.' || ch1.table_name))
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'device_readings';

SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_hypertables');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'num_compressed_hypertables');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'compressed_heap_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'compressed_index_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'compressed_toast_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'uncompressed_heap_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'uncompressed_index_size');
SELECT json_object_field(get_telemetry_report(always_display_report := true)::json,'uncompressed_toast_size');
