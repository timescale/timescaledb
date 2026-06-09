-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Redefinition of our external views without dependencies on catalog tables.
--
-- During extension upgrade we replace the views with the ones defined in this file,
-- which have the same columns but no dependencies on the underlying tables.
-- This allows users to keep dependencies on the views and not block extension
-- upgrades.

CREATE OR REPLACE VIEW timescaledb_information.hypertables AS
SELECT
  NULL::name AS hypertable_schema,
  NULL::name AS hypertable_name,
  NULL::name AS owner,
  NULL::smallint AS num_dimensions,
  NULL::bigint AS num_chunks,
  NULL::boolean AS compression_enabled,
  NULL::name[] AS tablespaces,
  NULL::name AS primary_dimension,
  NULL::regtype AS primary_dimension_type;

CREATE OR REPLACE VIEW timescaledb_information.job_stats AS
SELECT
  NULL::name AS hypertable_schema,
  NULL::name AS hypertable_name,
  NULL::integer AS job_id,
  NULL::timestamptz AS last_run_started_at,
  NULL::timestamptz AS last_successful_finish,
  NULL::text AS last_run_status,
  NULL::text AS job_status,
  NULL::interval AS last_run_duration,
  NULL::timestamptz AS next_start,
  NULL::bigint AS total_runs,
  NULL::bigint AS total_successes,
  NULL::bigint AS total_failures;

CREATE OR REPLACE VIEW timescaledb_information.jobs AS
SELECT
  NULL::integer AS job_id,
  NULL::name AS application_name,
  NULL::interval AS schedule_interval,
  NULL::interval AS max_runtime,
  NULL::integer AS max_retries,
  NULL::interval AS retry_period,
  NULL::name AS proc_schema,
  NULL::name AS proc_name,
  NULL::regrole AS owner,
  NULL::boolean AS scheduled,
  NULL::boolean AS fixed_schedule,
  NULL::jsonb AS config,
  NULL::timestamptz AS next_start,
  NULL::timestamptz AS initial_start,
  NULL::name AS hypertable_schema,
  NULL::name AS hypertable_name,
  NULL::name AS check_schema,
  NULL::name AS check_name;

CREATE OR REPLACE VIEW timescaledb_information.continuous_aggregates AS
SELECT
  NULL::name AS hypertable_schema,
  NULL::name AS hypertable_name,
  NULL::name AS view_schema,
  NULL::name AS view_name,
  NULL::name AS view_owner,
  NULL::boolean AS materialized_only,
  NULL::boolean AS compression_enabled,
  NULL::name AS materialization_hypertable_schema,
  NULL::name AS materialization_hypertable_name,
  NULL::text AS view_definition;

CREATE OR REPLACE VIEW timescaledb_information.chunks AS
SELECT
  NULL::name AS hypertable_schema,
  NULL::name AS hypertable_name,
  NULL::name AS chunk_schema,
  NULL::name AS chunk_name,
  NULL::name AS primary_dimension,
  NULL::regtype AS primary_dimension_type,
  NULL::timestamptz AS range_start,
  NULL::timestamptz AS range_end,
  NULL::bigint AS range_start_integer,
  NULL::bigint AS range_end_integer,
  NULL::boolean AS is_compressed,
  NULL::name AS chunk_tablespace,
  NULL::timestamptz AS chunk_creation_time;

CREATE OR REPLACE VIEW timescaledb_information.dimensions AS
SELECT
  NULL::name AS hypertable_schema,
  NULL::name AS hypertable_name,
  NULL::bigint AS dimension_number,
  NULL::name AS column_name,
  NULL::regtype AS column_type,
  NULL::text AS dimension_type,
  NULL::interval AS time_interval,
  NULL::bigint AS integer_interval,
  NULL::name AS integer_now_func,
  NULL::smallint AS num_partitions;

CREATE OR REPLACE VIEW timescaledb_information.compression_settings AS
SELECT
  NULL::name AS hypertable_schema,
  NULL::name AS hypertable_name,
  NULL::name AS attname,
  NULL::smallint AS segmentby_column_index,
  NULL::smallint AS orderby_column_index,
  NULL::boolean AS orderby_asc,
  NULL::boolean AS orderby_nullsfirst;

CREATE OR REPLACE VIEW timescaledb_information.job_errors
WITH (security_barrier = true) AS
SELECT
  NULL::integer AS job_id,
  NULL::text AS proc_schema,
  NULL::text AS proc_name,
  NULL::integer AS pid,
  NULL::timestamptz AS start_time,
  NULL::timestamptz AS finish_time,
  NULL::text AS sqlerrcode,
  NULL::text AS err_message;

CREATE OR REPLACE VIEW timescaledb_information.job_history
WITH (security_barrier = true) AS
SELECT
  NULL::bigint AS id,
  NULL::integer AS job_id,
  NULL::boolean AS succeeded,
  NULL::text COLLATE "C" AS proc_schema,
  NULL::text COLLATE "C" AS proc_name,
  NULL::integer AS pid,
  NULL::timestamptz AS start_time,
  NULL::timestamptz AS finish_time,
  NULL::jsonb AS config,
  NULL::text AS sqlerrcode,
  NULL::text AS err_message;

CREATE OR REPLACE VIEW timescaledb_information.hypertable_compression_settings AS
SELECT
  NULL::regclass AS hypertable,
  NULL::text AS segmentby,
  NULL::text AS orderby,
  NULL::text AS compress_interval_length,
  NULL::jsonb AS index;

CREATE OR REPLACE VIEW timescaledb_information.chunk_compression_settings AS
SELECT
  NULL::regclass AS hypertable,
  NULL::regclass AS chunk,
  NULL::text AS segmentby,
  NULL::text AS orderby,
  NULL::jsonb AS index;

CREATE OR REPLACE VIEW timescaledb_information.hypertable_columnstore_settings AS
SELECT
  NULL::regclass AS hypertable,
  NULL::text AS segmentby,
  NULL::text AS orderby,
  NULL::text AS compress_interval_length,
  NULL::jsonb AS index;

CREATE OR REPLACE VIEW timescaledb_information.chunk_columnstore_settings AS
SELECT
  NULL::regclass AS hypertable,
  NULL::regclass AS chunk,
  NULL::text AS segmentby,
  NULL::text AS orderby,
  NULL::jsonb AS index;

DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_class WHERE relname = 'stat_chunk_activity' AND relkind = 'v') THEN
    CREATE OR REPLACE VIEW timescaledb_information.stat_chunk_activity AS
    SELECT
      NULL::regclass AS chunk,
      NULL::regclass AS compressed_chunk,
      NULL::integer AS chunk_id,
      NULL::integer AS hypertable_id,
      NULL::name AS hypertable,
      NULL::bigint AS compressed_batch_count,
      NULL::bigint AS compressed_block_count,
      NULL::bigint AS compressed_batch_rows_min,
      NULL::bigint AS compressed_batch_rows_max,
      NULL::bigint AS compressed_batch_rows_avg,
      NULL::double precision AS compressed_batch_rows_stddev,
      NULL::bigint AS compressed_batch_bytes_min,
      NULL::bigint AS compressed_batch_bytes_max,
      NULL::bigint AS compressed_batch_bytes_avg,
      NULL::double precision AS compressed_batch_bytes_stddev,
      NULL::bigint AS compressed_block_bytes_min,
      NULL::bigint AS compressed_block_bytes_max,
      NULL::bigint AS compressed_block_bytes_avg,
      NULL::double precision AS compressed_block_bytes_stddev,
      NULL::bigint AS total_batches_deleted,
      NULL::bigint AS total_batches_decompressed,
      NULL::bigint AS total_tuples_decompressed,
      NULL::bigint AS total_batches_scanned,
      NULL::bigint AS total_batches_checked_by_bloom,
      NULL::bigint AS total_batches_pruned_by_bloom,
      NULL::bigint AS total_batches_without_bloom,
      NULL::bigint AS total_batches_bloom_false_positives,
      NULL::bigint AS total_batches_filtered_compressed,
      NULL::bigint AS total_batches_filtered_decompressed,
      NULL::bigint AS last_op_batches_deleted,
      NULL::bigint AS last_op_batches_decompressed,
      NULL::bigint AS last_op_tuples_decompressed,
      NULL::bigint AS last_op_batches_scanned,
      NULL::bigint AS last_op_batches_checked_by_bloom,
      NULL::bigint AS last_op_batches_pruned_by_bloom,
      NULL::bigint AS last_op_batches_without_bloom,
      NULL::bigint AS last_op_batches_bloom_false_positives,
      NULL::bigint AS last_op_batches_filtered_compressed,
      NULL::bigint AS last_op_batches_filtered_decompressed,
      NULL::bigint AS n_selects,
      NULL::bigint AS n_inserts,
      NULL::bigint AS n_updates,
      NULL::bigint AS n_deletes,
      NULL::timestamptz AS first_update,
      NULL::timestamptz AS last_update;
  END IF;
END $$;
