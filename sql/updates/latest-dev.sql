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
DECLARE
  foid regprocedure;
  funcs text[] = '{
    drop_dist_ht_invalidation_trigger,
    subtract_integer_from_now,
    get_approx_row_count,
    chunk_status,
    create_chunk,create_chunk_table,
    freeze_chunk,unfreeze_chunk,drop_chunk,
    attach_osm_table_chunk
  }';
BEGIN
  FOR foid IN
    SELECT oid FROM pg_proc WHERE proname = ANY(funcs) AND pronamespace = '_timescaledb_internal'::regnamespace
  LOOP
    EXECUTE format('ALTER FUNCTION %s SET SCHEMA _timescaledb_functions', foid);
  END LOOP;
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

DO $$
DECLARE
  foid regprocedure;
  kind text;
  funcs text[] = '{
    policy_compression_check,policy_compression_execute,policy_compression,
    policy_job_error_retention_check,policy_job_error_retention,
    policy_recompression,
    policy_refresh_continuous_aggregate_check,policy_refresh_continuous_aggregate,
    policy_reorder_check,policy_reorder,policy_retention_check,policy_retention,

    cagg_watermark, cagg_watermark_materialized,
    cagg_migrate_plan_exists, cagg_migrate_pre_validation, cagg_migrate_create_plan, cagg_migrate_execute_create_new_cagg,
    cagg_migrate_execute_disable_policies, cagg_migrate_execute_enable_policies, cagg_migrate_execute_copy_policies,
    cagg_migrate_execute_refresh_new_cagg, cagg_migrate_execute_copy_data, cagg_migrate_execute_override_cagg,
    cagg_migrate_execute_drop_old_cagg, cagg_migrate_execute_plan,

    finalize_agg,

    hypertable_invalidation_log_delete, invalidation_cagg_log_add_entry, invalidation_hyper_log_add_entry,
    invalidation_process_cagg_log, invalidation_process_hypertable_log, materialization_invalidation_log_delete,

    alter_job_set_hypertable_id,

    create_compressed_chunk, get_compressed_chunk_index_for_recompression, recompress_chunk_segmentwise,
    chunk_drop_replica, chunk_index_clone, chunk_index_replace, create_chunk_replica_table, drop_stale_chunks,
		chunk_constraint_add_table_constraint, hypertable_constraint_add_table_fk_constraint,
    health, wait_subscription_sync
  }';
BEGIN
  FOR foid, kind IN
    SELECT oid,
    CASE
      WHEN prokind = 'f' THEN 'FUNCTION'
      WHEN prokind = 'a' THEN 'AGGREGATE'
      ELSE 'PROCEDURE'
    END
    FROM pg_proc WHERE proname = ANY(funcs) AND pronamespace = '_timescaledb_internal'::regnamespace
  LOOP
    EXECUTE format('ALTER %s %s SET SCHEMA _timescaledb_functions', kind, foid);
  END LOOP;
END;
$$;

UPDATE _timescaledb_config.bgw_job SET proc_schema = '_timescaledb_functions' WHERE proc_schema = '_timescaledb_internal';
UPDATE _timescaledb_config.bgw_job SET check_schema = '_timescaledb_functions' WHERE check_schema = '_timescaledb_internal';

ALTER FUNCTION _timescaledb_internal.start_background_workers() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.stop_background_workers() SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.restart_background_workers() SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.process_ddl_event() SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.get_partition_for_key(val anyelement) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.get_partition_hash(val anyelement) SET SCHEMA _timescaledb_functions;

UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = '_timescaledb_functions' WHERE partitioning_func_schema = '_timescaledb_internal' AND partitioning_func IN ('get_partition_for_key','get_partition_hash');

ALTER FUNCTION _timescaledb_internal.finalize_agg_ffunc(internal,text,name,name,name[],bytea,anyelement) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.finalize_agg_sfunc(internal,text,name,name,name[],bytea,anyelement) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.partialize_agg(anyelement) SET SCHEMA _timescaledb_functions;

-- Fix osm chunk ranges
UPDATE _timescaledb_catalog.dimension_slice ds
  SET range_start = 9223372036854775806
FROM _timescaledb_catalog.chunk_constraint cc
INNER JOIN _timescaledb_catalog.chunk c ON c.id = cc.chunk_id AND c.osm_chunk
WHERE cc.dimension_slice_id = ds.id AND ds.range_start <> 9223372036854775806;

