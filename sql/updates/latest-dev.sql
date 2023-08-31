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

-- OSM support - table must be rebuilt to ensure consistent attribute numbers
-- we cannot just ALTER TABLE .. ADD COLUMN
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

DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.dimensions;
DROP VIEW IF EXISTS timescaledb_information.compression_settings;
DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;
DROP VIEW IF EXISTS timescaledb_experimental.policies;

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
    replication_factor smallint NULL,
    status integer NOT NULL DEFAULT 0
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

UPDATE _timescaledb_catalog.hypertable h
SET status = 3
WHERE EXISTS (
  SELECT FROM _timescaledb_catalog.chunk c WHERE c.osm_chunk AND c.hypertable_id = h.id
);

ALTER SEQUENCE _timescaledb_catalog.hypertable_id_seq OWNED BY _timescaledb_catalog.hypertable.id;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', 'WHERE id >= 1');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_id_seq', '');

GRANT SELECT ON _timescaledb_catalog.hypertable TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.hypertable_id_seq TO PUBLIC;

DROP TABLE _timescaledb_catalog.hypertable_tmp;
-- now add any constraints
ALTER TABLE _timescaledb_catalog.hypertable
    ADD CONSTRAINT hypertable_associated_schema_name_associated_table_prefix_key UNIQUE (associated_schema_name, associated_table_prefix),
    ADD CONSTRAINT hypertable_table_name_schema_name_key UNIQUE (table_name, schema_name),
    ADD CONSTRAINT hypertable_schema_name_check CHECK (schema_name != '_timescaledb_catalog'),
    ADD CONSTRAINT hypertable_dim_compress_check CHECK (num_dimensions > 0 OR compression_state = 2),
    ADD CONSTRAINT hypertable_chunk_target_size_check CHECK (chunk_target_size >= 0),
    ADD CONSTRAINT hypertable_compress_check CHECK ( (compression_state = 0 OR compression_state = 1 )  OR (compression_state = 2 AND compressed_hypertable_id IS NULL)),
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
