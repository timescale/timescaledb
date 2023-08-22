DROP FUNCTION IF EXISTS @extschema@.alter_job(
    INTEGER,
    INTERVAL,
    INTERVAL,
    INTEGER,
    INTERVAL,
    BOOL,
    JSONB,
    TIMESTAMPTZ,
    BOOL,
    REGPROC
);

CREATE FUNCTION @extschema@.alter_job(
    job_id INTEGER,
    schedule_interval INTERVAL = NULL,
    max_runtime INTERVAL = NULL,
    max_retries INTEGER = NULL,
    retry_period INTERVAL = NULL,
    scheduled BOOL = NULL,
    config JSONB = NULL,
    next_start TIMESTAMPTZ = NULL,
    if_exists BOOL = FALSE,
    check_config REGPROC = NULL,
    fixed_schedule BOOL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT DEFAULT NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB,
next_start TIMESTAMPTZ, check_config TEXT, fixed_schedule BOOL, initial_start TIMESTAMPTZ, timezone TEXT)
AS '@MODULE_PATHNAME@', 'ts_job_alter'
LANGUAGE C VOLATILE;

-- when upgrading from old versions on PG13 this function might not be present
-- since there is no ALTER FUNCTION IF EXISTS we have to work around it with a DO block
DO $$
BEGIN
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'drop_dist_ht_invalidation_trigger' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.drop_dist_ht_invalidation_trigger(integer) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'subtract_integer_from_now' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.subtract_integer_from_now(regclass,bigint) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'get_approx_row_count' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.get_approx_row_count(regclass) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'chunk_status' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.chunk_status(regclass) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'create_chunk' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.create_chunk(regclass, jsonb, name, name, regclass) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'create_chunk_table' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.create_chunk_table(regclass, jsonb, name, name) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'freeze_chunk' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.freeze_chunk(regclass) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'unfreeze_chunk' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.unfreeze_chunk(regclass) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'drop_chunk' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.drop_chunk(regclass) SET SCHEMA _timescaledb_functions;
  END IF;
  IF (EXISTS (SELECT FROM pg_proc WHERE proname = 'attach_osm_table_chunk' AND pronamespace='_timescaledb_internal'::regnamespace))
  THEN
    ALTER FUNCTION _timescaledb_internal.attach_osm_table_chunk(regclass, regclass) SET SCHEMA _timescaledb_functions;
  END IF;
END;
$$;

DROP FUNCTION IF EXISTS _timescaledb_internal.get_time_type(integer);

ALTER FUNCTION _timescaledb_internal.insert_blocker() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_create_command(name) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.to_unix_microseconds(timestamptz) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.to_timestamp(bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.to_timestamp_without_timezone(bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.to_date(bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.to_interval(bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.interval_to_usec(interval) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.time_to_internal(anyelement) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.set_dist_id(uuid) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.set_peer_dist_id(uuid) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.validate_as_data_node() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.show_connection_cache() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.ping_data_node(name, interval) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.remote_txn_heal_data_node(oid) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.relation_size(regclass) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.data_node_hypertable_info(name, name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.data_node_chunk_info(name, name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hypertable_local_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hypertable_remote_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.chunks_local_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.chunks_remote_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.range_value_to_pretty(bigint, regtype) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.data_node_compressed_chunk_stats(name, name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_chunk_local_stats(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_chunk_remote_stats(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.indexes_local_size(name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.data_node_index_size(name, name, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.indexes_remote_size(name, name, name) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.generate_uuid() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_git_commit() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_os_info() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.tsl_loaded() SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.calculate_chunk_interval(int, bigint, bigint) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.chunks_in(record, integer[]) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.chunk_id_from_relid(oid) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.show_chunk(regclass) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.set_chunk_default_data_node(regclass, name) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_chunk_relstats(regclass) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_chunk_colstats(regclass) SET SCHEMA _timescaledb_functions;

UPDATE _timescaledb_catalog.hypertable SET chunk_sizing_func_schema = '_timescaledb_functions' WHERE chunk_sizing_func_schema = '_timescaledb_internal' AND chunk_sizing_func_name = 'calculate_chunk_interval';

