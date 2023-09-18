
-- remove compatibility wrapper functions
-- this needs to happen before we move the actual functions back into _timescaledb_internal
DROP FUNCTION _timescaledb_internal.alter_job_set_hypertable_id(integer,regclass);
DROP FUNCTION _timescaledb_internal.attach_osm_table_chunk(regclass,regclass);
DROP FUNCTION _timescaledb_internal.cagg_migrate_plan_exists(integer);
DROP FUNCTION _timescaledb_internal.cagg_migrate_pre_validation(text,text,text);
DROP FUNCTION _timescaledb_internal.cagg_watermark(integer);
DROP FUNCTION _timescaledb_internal.cagg_watermark_materialized(integer);
DROP FUNCTION _timescaledb_internal.calculate_chunk_interval(integer,bigint,bigint);
DROP FUNCTION _timescaledb_internal.chunk_constraint_add_table_constraint(_timescaledb_catalog.chunk_constraint);
DROP FUNCTION _timescaledb_internal.chunk_drop_replica(regclass,name);
DROP FUNCTION _timescaledb_internal.chunk_id_from_relid(oid);
DROP FUNCTION _timescaledb_internal.chunk_index_clone(oid);
DROP FUNCTION _timescaledb_internal.chunk_index_replace(oid,oid);
DROP FUNCTION _timescaledb_internal.chunk_status(regclass);
DROP FUNCTION _timescaledb_internal.chunks_in(record,integer[]);
DROP FUNCTION _timescaledb_internal.chunks_local_size(name,name);
DROP FUNCTION _timescaledb_internal.chunks_remote_size(name,name);
DROP FUNCTION _timescaledb_internal.compressed_chunk_local_stats(name,name);
DROP FUNCTION _timescaledb_internal.compressed_chunk_remote_stats(name,name);
DROP FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger();
DROP FUNCTION _timescaledb_internal.create_chunk(regclass,jsonb,name,name,regclass);
DROP FUNCTION _timescaledb_internal.create_chunk_replica_table(regclass,name);
DROP FUNCTION _timescaledb_internal.create_chunk_table(regclass,jsonb,name,name);
DROP FUNCTION _timescaledb_internal.create_compressed_chunk(regclass,regclass,bigint,bigint,bigint,bigint,bigint,bigint,bigint,bigint);
DROP FUNCTION _timescaledb_internal.data_node_chunk_info(name,name,name);
DROP FUNCTION _timescaledb_internal.data_node_compressed_chunk_stats(name,name,name);
DROP FUNCTION _timescaledb_internal.data_node_hypertable_info(name,name,name);
DROP FUNCTION _timescaledb_internal.data_node_index_size(name,name,name);
DROP FUNCTION _timescaledb_internal.drop_chunk(regclass);
DROP FUNCTION _timescaledb_internal.drop_dist_ht_invalidation_trigger(integer);
DROP FUNCTION _timescaledb_internal.drop_stale_chunks(name,integer[]);
DROP AGGREGATE _timescaledb_internal.finalize_agg(agg_name TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val anyelement);
DROP FUNCTION _timescaledb_internal.finalize_agg_ffunc(internal, text, name, name, name[][], bytea, anyelement);
DROP FUNCTION _timescaledb_internal.finalize_agg_sfunc(internal, text, name, name, name[][], bytea, anyelement);
DROP FUNCTION _timescaledb_internal.freeze_chunk(regclass);
DROP FUNCTION _timescaledb_internal.generate_uuid();
DROP FUNCTION _timescaledb_internal.get_approx_row_count(regclass);
DROP FUNCTION _timescaledb_internal.get_chunk_colstats(regclass);
DROP FUNCTION _timescaledb_internal.get_chunk_relstats(regclass);
DROP FUNCTION _timescaledb_internal.get_compressed_chunk_index_for_recompression(regclass);
DROP FUNCTION _timescaledb_internal.get_create_command(name);
DROP FUNCTION _timescaledb_internal.get_git_commit();
DROP FUNCTION _timescaledb_internal.get_os_info();
DROP FUNCTION _timescaledb_internal.get_partition_for_key(anyelement);
DROP FUNCTION _timescaledb_internal.get_partition_hash(anyelement);
DROP FUNCTION _timescaledb_internal.health();
DROP FUNCTION _timescaledb_internal.hypertable_constraint_add_table_fk_constraint(name,name,name,integer);
DROP FUNCTION _timescaledb_internal.hypertable_invalidation_log_delete(integer);
DROP FUNCTION _timescaledb_internal.hypertable_local_size(name,name);
DROP FUNCTION _timescaledb_internal.hypertable_remote_size(name,name);
DROP FUNCTION _timescaledb_internal.indexes_local_size(name,name);
DROP FUNCTION _timescaledb_internal.indexes_remote_size(name,name,name);
DROP FUNCTION _timescaledb_internal.insert_blocker();
DROP FUNCTION _timescaledb_internal.interval_to_usec(interval);
DROP FUNCTION _timescaledb_internal.invalidation_cagg_log_add_entry(integer,bigint,bigint);
DROP FUNCTION _timescaledb_internal.invalidation_hyper_log_add_entry(integer,bigint,bigint);
DROP FUNCTION _timescaledb_internal.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[]);
DROP FUNCTION _timescaledb_internal.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[],text[]);
DROP FUNCTION _timescaledb_internal.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[]);
DROP FUNCTION _timescaledb_internal.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[],text[]);
DROP FUNCTION _timescaledb_internal.materialization_invalidation_log_delete(integer);
DROP FUNCTION _timescaledb_internal.partialize_agg(anyelement);
DROP FUNCTION _timescaledb_internal.ping_data_node(name,interval);
DROP FUNCTION _timescaledb_internal.policy_compression_check(jsonb);
DROP FUNCTION _timescaledb_internal.policy_job_error_retention(integer,jsonb);
DROP FUNCTION _timescaledb_internal.policy_job_error_retention_check(jsonb);
DROP FUNCTION _timescaledb_internal.policy_refresh_continuous_aggregate_check(jsonb);
DROP FUNCTION _timescaledb_internal.policy_reorder_check(jsonb);
DROP FUNCTION _timescaledb_internal.policy_retention_check(jsonb);
DROP FUNCTION _timescaledb_internal.process_ddl_event();
DROP FUNCTION _timescaledb_internal.range_value_to_pretty(bigint,regtype);
DROP FUNCTION _timescaledb_internal.recompress_chunk_segmentwise(regclass,boolean);
DROP FUNCTION _timescaledb_internal.relation_size(regclass);
DROP FUNCTION _timescaledb_internal.remote_txn_heal_data_node(oid);
DROP FUNCTION _timescaledb_internal.set_chunk_default_data_node(regclass,name);
DROP FUNCTION _timescaledb_internal.set_dist_id(uuid);
DROP FUNCTION _timescaledb_internal.set_peer_dist_id(uuid);
DROP FUNCTION _timescaledb_internal.show_chunk(regclass);
DROP FUNCTION _timescaledb_internal.show_connection_cache();
DROP FUNCTION _timescaledb_internal.start_background_workers();
DROP FUNCTION _timescaledb_internal.stop_background_workers();
DROP FUNCTION _timescaledb_internal.subtract_integer_from_now(regclass,bigint);
DROP FUNCTION _timescaledb_internal.time_to_internal(anyelement);
DROP FUNCTION _timescaledb_internal.to_date(bigint);
DROP FUNCTION _timescaledb_internal.to_interval(bigint);
DROP FUNCTION _timescaledb_internal.to_timestamp(bigint);
DROP FUNCTION _timescaledb_internal.to_timestamp_without_timezone(bigint);
DROP FUNCTION _timescaledb_internal.to_unix_microseconds(timestamp with time zone);
DROP FUNCTION _timescaledb_internal.tsl_loaded();
DROP FUNCTION _timescaledb_internal.unfreeze_chunk(regclass);
DROP FUNCTION _timescaledb_internal.validate_as_data_node();
DROP PROCEDURE _timescaledb_internal.cagg_migrate_create_plan(_timescaledb_catalog.continuous_agg,text,boolean,boolean);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_data(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_create_new_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_disable_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_drop_old_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_enable_policies(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_override_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_plan(_timescaledb_catalog.continuous_agg);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg(_timescaledb_catalog.continuous_agg,_timescaledb_catalog.continuous_agg_migrate_plan_step);
DROP PROCEDURE _timescaledb_internal.policy_compression(integer,jsonb);
DROP PROCEDURE _timescaledb_internal.policy_compression_execute(integer,integer,anyelement,integer,boolean,boolean);
DROP PROCEDURE _timescaledb_internal.policy_recompression(integer,jsonb);
DROP PROCEDURE _timescaledb_internal.policy_refresh_continuous_aggregate(integer,jsonb);
DROP PROCEDURE _timescaledb_internal.policy_reorder(integer,jsonb);
DROP PROCEDURE _timescaledb_internal.policy_retention(integer,jsonb);
DROP PROCEDURE _timescaledb_internal.wait_subscription_sync(name,name,integer,numeric);

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
    REGPROC,
    BOOL,
    TIMESTAMPTZ,
    TEXT
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
    check_config REGPROC = NULL
)
RETURNS TABLE (job_id INTEGER, schedule_interval INTERVAL, max_runtime INTERVAL, max_retries INTEGER, retry_period INTERVAL, scheduled BOOL, config JSONB,
next_start TIMESTAMPTZ, check_config TEXT)
AS '@MODULE_PATHNAME@', 'ts_job_alter'
LANGUAGE C VOLATILE;

ALTER FUNCTION _timescaledb_functions.insert_blocker() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.continuous_agg_invalidation_trigger() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.drop_dist_ht_invalidation_trigger(integer) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_create_command(name) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.to_unix_microseconds(timestamptz) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.to_timestamp(bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.to_timestamp_without_timezone(bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.to_date(bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.to_interval(bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.interval_to_usec(interval) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.time_to_internal(anyelement) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.subtract_integer_from_now(regclass, bigint) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.set_dist_id(uuid) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.set_peer_dist_id(uuid) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.validate_as_data_node() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.show_connection_cache() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.ping_data_node(name, interval) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.remote_txn_heal_data_node(oid) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.relation_size(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.data_node_hypertable_info(name, name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.data_node_chunk_info(name, name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.hypertable_local_size(name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.hypertable_remote_size(name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.chunks_local_size(name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.chunks_remote_size(name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.range_value_to_pretty(bigint, regtype) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_approx_row_count(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.data_node_compressed_chunk_stats(name, name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.compressed_chunk_local_stats(name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.compressed_chunk_remote_stats(name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.indexes_local_size(name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.data_node_index_size(name, name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.indexes_remote_size(name, name, name) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.generate_uuid() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_git_commit() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_os_info() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.tsl_loaded() SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.calculate_chunk_interval(int, bigint, bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.chunk_status(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.chunks_in(record, integer[]) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.chunk_id_from_relid(oid) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.show_chunk(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.create_chunk(regclass, jsonb, name, name, regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.set_chunk_default_data_node(regclass, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_chunk_relstats(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_chunk_colstats(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.create_chunk_table(regclass, jsonb, name, name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.freeze_chunk(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.unfreeze_chunk(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.drop_chunk(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.attach_osm_table_chunk(regclass, regclass) SET SCHEMA _timescaledb_internal;

UPDATE _timescaledb_catalog.hypertable SET chunk_sizing_func_schema = '_timescaledb_internal' WHERE chunk_sizing_func_schema = '_timescaledb_functions' AND chunk_sizing_func_name = 'calculate_chunk_interval';

ALTER FUNCTION _timescaledb_functions.policy_compression_check(jsonb) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.policy_compression_execute(integer,integer,anyelement,integer,boolean,boolean) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.policy_compression(integer,jsonb) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.policy_job_error_retention_check(jsonb) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.policy_job_error_retention(integer,jsonb) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.policy_recompression(integer,jsonb) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.policy_refresh_continuous_aggregate_check(jsonb) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.policy_refresh_continuous_aggregate(integer,jsonb) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.policy_reorder_check(jsonb) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.policy_reorder(integer,jsonb) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.policy_retention_check(jsonb) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.policy_retention(integer,jsonb) SET SCHEMA _timescaledb_internal;

UPDATE _timescaledb_config.bgw_job SET proc_schema = '_timescaledb_internal' WHERE proc_schema = '_timescaledb_functions';
UPDATE _timescaledb_config.bgw_job SET check_schema = '_timescaledb_internal' WHERE check_schema = '_timescaledb_functions';

ALTER FUNCTION _timescaledb_functions.cagg_migrate_plan_exists(INTEGER) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.cagg_migrate_pre_validation(TEXT, TEXT, TEXT) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_create_plan(_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_create_new_cagg(_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_disable_policies(_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_enable_policies(_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_copy_policies(_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_refresh_new_cagg(_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_copy_data(_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_override_cagg(_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_drop_old_cagg(_timescaledb_catalog.continuous_agg, _timescaledb_catalog.continuous_agg_migrate_plan_step) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.cagg_migrate_execute_plan(_timescaledb_catalog.continuous_agg) SET SCHEMA _timescaledb_internal;

-- pre-update of previous version will have created an additional copy of restart_background_workers
-- since restart_background_workers was handled differently from other functions in previous versions
DROP FUNCTION _timescaledb_internal.restart_background_workers();
ALTER FUNCTION _timescaledb_functions.start_background_workers() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.stop_background_workers() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.restart_background_workers() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.alter_job_set_hypertable_id(integer,regclass) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.cagg_watermark(integer) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.cagg_watermark_materialized(integer) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.hypertable_invalidation_log_delete(integer) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.invalidation_cagg_log_add_entry(integer,bigint,bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.invalidation_hyper_log_add_entry(integer,bigint,bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[]) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[],text[]) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[]) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[],text[]) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.materialization_invalidation_log_delete(integer) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.chunk_constraint_add_table_constraint(_timescaledb_catalog.chunk_constraint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.chunk_drop_replica(regclass,name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.chunk_index_clone(oid) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.chunk_index_replace(oid,oid) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.create_chunk_replica_table(regclass,name) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.create_compressed_chunk(regclass,regclass,bigint,bigint,bigint,bigint,bigint,bigint,bigint,bigint) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.drop_stale_chunks(name,integer[]) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_compressed_chunk_index_for_recompression(regclass) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.health() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.hypertable_constraint_add_table_fk_constraint(name,name,name,integer) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.process_ddl_event() SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.recompress_chunk_segmentwise(regclass,boolean) SET SCHEMA _timescaledb_internal;
ALTER PROCEDURE _timescaledb_functions.wait_subscription_sync(name,name,integer,numeric) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.get_partition_for_key(val anyelement) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.get_partition_hash(val anyelement) SET SCHEMA _timescaledb_internal;

UPDATE _timescaledb_catalog.dimension SET partitioning_func_schema = '_timescaledb_internal' WHERE partitioning_func_schema = '_timescaledb_functions' AND partitioning_func IN ('get_partition_for_key','get_partition_hash');

ALTER FUNCTION _timescaledb_functions.finalize_agg_ffunc(internal,text,name,name,name[],bytea,anyelement) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.finalize_agg_sfunc(internal,text,name,name,name[],bytea,anyelement) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.partialize_agg(anyelement) SET SCHEMA _timescaledb_internal;
ALTER AGGREGATE _timescaledb_functions.finalize_agg(text,name,name,name[][],bytea,anyelement) SET SCHEMA _timescaledb_internal;

DROP FUNCTION _timescaledb_functions.hypertable_osm_range_update(regclass, anyelement, anyelement, boolean);

-- recreate the _timescaledb_catalog.hypertable table as new field was added
-- 1. drop CONSTRAINTS from other tables referencing the existing one
ALTER TABLE _timescaledb_config.bgw_job
    DROP CONSTRAINT bgw_job_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk
    DROP CONSTRAINT chunk_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.chunk_index
    DROP CONSTRAINT chunk_index_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_agg
    DROP CONSTRAINT continuous_agg_mat_hypertable_id_fkey,
    DROP CONSTRAINT continuous_agg_raw_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_bucket_function
    DROP CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold
    DROP CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.dimension
    DROP CONSTRAINT dimension_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable
    DROP CONSTRAINT hypertable_compressed_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable_compression
    DROP CONSTRAINT hypertable_compression_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.hypertable_data_node
    DROP CONSTRAINT hypertable_data_node_hypertable_id_fkey;
ALTER TABLE _timescaledb_catalog.tablespace
    DROP CONSTRAINT tablespace_hypertable_id_fkey;

-- drop dependent views
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.hypertables;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.job_stats;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.jobs;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.continuous_aggregates;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.chunks;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.dimensions;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.compression_settings;
ALTER EXTENSION timescaledb DROP VIEW  _timescaledb_internal.hypertable_chunk_local_size;
ALTER EXTENSION timescaledb DROP VIEW _timescaledb_internal.compressed_chunk_stats;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_experimental.chunk_replication_status;
ALTER EXTENSION timescaledb DROP VIEW timescaledb_experimental.policies;

DROP VIEW timescaledb_information.hypertables;
DROP VIEW timescaledb_information.job_stats;
DROP VIEW timescaledb_information.jobs;
DROP VIEW timescaledb_information.continuous_aggregates;
DROP VIEW timescaledb_information.chunks;
DROP VIEW timescaledb_information.dimensions;
DROP VIEW timescaledb_information.compression_settings;
DROP VIEW _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW _timescaledb_internal.compressed_chunk_stats;
DROP VIEW timescaledb_experimental.chunk_replication_status;
DROP VIEW timescaledb_experimental.policies;

-- recreate table
CREATE TABLE _timescaledb_catalog.hypertable_tmp AS SELECT * FROM _timescaledb_catalog.hypertable;
CREATE TABLE _timescaledb_catalog.tmp_hypertable_seq_value AS SELECT last_value, is_called FROM _timescaledb_catalog.hypertable_id_seq;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.hypertable_id_seq;

SET timescaledb.restoring = on; -- must disable the hooks otherwise we can't do anything without the table _timescaledb_catalog.hypertable

DROP TABLE _timescaledb_catalog.hypertable;

CREATE SEQUENCE _timescaledb_catalog.hypertable_id_seq MINVALUE 1;
SELECT setval('_timescaledb_catalog.hypertable_id_seq', last_value, is_called) FROM _timescaledb_catalog.tmp_hypertable_seq_value;
DROP TABLE _timescaledb_catalog.tmp_hypertable_seq_value;

CREATE TABLE _timescaledb_catalog.hypertable (
    id INTEGER PRIMARY KEY NOT NULL DEFAULT nextval('_timescaledb_catalog.hypertable_id_seq'),
    schema_name name NOT NULL,
    table_name name NOT NULL,
    associated_schema_name name NOT NULL,
    associated_table_prefix name NOT NULL,
    num_dimensions smallint NOT NULL,
    chunk_sizing_func_schema name NOT NULL,
    chunk_sizing_func_name name NOT NULL,
    chunk_target_size bigint NOT NULL, -- size in bytes
    compression_state smallint NOT NULL DEFAULT 0,
    compressed_hypertable_id integer,
    replication_factor smallint NULL
);

SET timescaledb.restoring = off;

INSERT INTO _timescaledb_catalog.hypertable (
    id,
    schema_name,
    table_name,
    associated_schema_name,
    associated_table_prefix,
    num_dimensions,
    chunk_sizing_func_schema,
    chunk_sizing_func_name,
    chunk_target_size,
    compression_state,
    compressed_hypertable_id,
    replication_factor
)
SELECT
    id,
    schema_name,
    table_name,
    associated_schema_name,
    associated_table_prefix,
    num_dimensions,
    chunk_sizing_func_schema,
    chunk_sizing_func_name,
    chunk_target_size,
    compression_state,
    compressed_hypertable_id,
    replication_factor
FROM
    _timescaledb_catalog.hypertable_tmp
ORDER BY id;

ALTER SEQUENCE _timescaledb_catalog.hypertable_id_seq OWNED BY _timescaledb_catalog.hypertable.id;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', 'WHERE id >= 1');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_id_seq', '');

GRANT SELECT ON _timescaledb_catalog.hypertable TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.hypertable_id_seq TO PUBLIC;

DROP TABLE _timescaledb_catalog.hypertable_tmp;
-- now add any constraints
ALTER TABLE _timescaledb_catalog.hypertable
    -- ADD CONSTRAINT hypertable_pkey PRIMARY KEY (id),
    ADD CONSTRAINT hypertable_associated_schema_name_associated_table_prefix_key UNIQUE (associated_schema_name, associated_table_prefix),
    ADD CONSTRAINT hypertable_table_name_schema_name_key UNIQUE (table_name, schema_name),
    ADD CONSTRAINT hypertable_schema_name_check CHECK (schema_name != '_timescaledb_catalog'),
    -- internal compressed hypertables have compression state = 2
    ADD CONSTRAINT hypertable_dim_compress_check CHECK (num_dimensions > 0 OR compression_state = 2),
    ADD CONSTRAINT hypertable_chunk_target_size_check CHECK (chunk_target_size >= 0),
    ADD CONSTRAINT hypertable_compress_check CHECK ( (compression_state = 0 OR compression_state = 1 )  OR (compression_state = 2 AND compressed_hypertable_id IS NULL)),
    -- replication_factor NULL: regular hypertable
    -- replication_factor > 0: distributed hypertable on access node
    -- replication_factor -1: distributed hypertable on data node, which is part of a larger table
    ADD CONSTRAINT hypertable_replication_factor_check CHECK (replication_factor > 0 OR replication_factor = -1),
    ADD CONSTRAINT hypertable_compressed_hypertable_id_fkey FOREIGN KEY (compressed_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id);

GRANT SELECT ON TABLE _timescaledb_catalog.hypertable TO PUBLIC;

-- 3. reestablish constraints on other tables
ALTER TABLE _timescaledb_config.bgw_job
    ADD CONSTRAINT bgw_job_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.chunk
    ADD CONSTRAINT chunk_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id);
ALTER TABLE _timescaledb_catalog.chunk_index
    ADD CONSTRAINT chunk_index_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_agg
    ADD CONSTRAINT continuous_agg_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    ADD CONSTRAINT continuous_agg_raw_hypertable_id_fkey FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_bucket_function
    ADD CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold
    ADD CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.dimension
    ADD CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.hypertable_compression
    ADD CONSTRAINT hypertable_compression_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
ALTER TABLE _timescaledb_catalog.hypertable_data_node
    ADD CONSTRAINT hypertable_data_node_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id);
ALTER TABLE _timescaledb_catalog.tablespace
    ADD CONSTRAINT tablespace_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;
