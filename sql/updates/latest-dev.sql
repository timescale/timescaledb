
-- ERROR if trying to update the extension while multinode is present
DO $$
DECLARE
  data_nodes TEXT;
  dist_hypertables TEXT;
BEGIN
  SELECT string_agg(format('%I.%I', schema_name, table_name), ', ')
  INTO dist_hypertables
  FROM _timescaledb_catalog.hypertable
  WHERE replication_factor > 0;

  IF dist_hypertables IS NOT NULL THEN
    RAISE USING
      ERRCODE = 'feature_not_supported',
      MESSAGE = 'cannot upgrade because multi-node has been removed in 2.14.0',
      DETAIL = 'The following distributed hypertables should be migrated to regular: '||dist_hypertables;
  END IF;

  SELECT string_agg(format('%I', srv.srvname), ', ')
  INTO data_nodes
  FROM pg_foreign_server srv
  JOIN pg_foreign_data_wrapper fdw ON srv.srvfdw = fdw.oid AND fdw.fdwname = 'timescaledb_fdw';

  IF data_nodes IS NOT NULL THEN
    RAISE USING
      ERRCODE = 'feature_not_supported',
      MESSAGE = 'cannot upgrade because multi-node has been removed in 2.14.0',
      DETAIL = 'The following data nodes should be removed: '||data_nodes;
  END IF;

  IF EXISTS(SELECT FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid') THEN
    RAISE USING
      ERRCODE = 'feature_not_supported',
      MESSAGE = 'cannot upgrade because multi-node has been removed in 2.14.0',
      DETAIL = 'This node appears to be part of a multi-node installation';
  END IF;
END $$;

DROP FUNCTION IF EXISTS _timescaledb_functions.ping_data_node;
DROP FUNCTION IF EXISTS _timescaledb_internal.ping_data_node;
DROP FUNCTION IF EXISTS _timescaledb_functions.remote_txn_heal_data_node;
DROP FUNCTION IF EXISTS _timescaledb_internal.remote_txn_heal_data_node;

DROP FUNCTION IF EXISTS _timescaledb_functions.set_dist_id;
DROP FUNCTION IF EXISTS _timescaledb_internal.set_dist_id;
DROP FUNCTION IF EXISTS _timescaledb_functions.set_peer_dist_id;
DROP FUNCTION IF EXISTS _timescaledb_internal.set_peer_dist_id;
DROP FUNCTION IF EXISTS _timescaledb_functions.validate_as_data_node;
DROP FUNCTION IF EXISTS _timescaledb_internal.validate_as_data_node;
DROP FUNCTION IF EXISTS _timescaledb_functions.show_connection_cache;
DROP FUNCTION IF EXISTS _timescaledb_internal.show_connection_cache;

DROP FUNCTION IF EXISTS @extschema@.create_hypertable(relation REGCLASS, time_column_name NAME, partitioning_column NAME, number_partitions INTEGER, associated_schema_name NAME, associated_table_prefix NAME, chunk_time_interval ANYELEMENT, create_default_indexes BOOLEAN, if_not_exists BOOLEAN, partitioning_func REGPROC, migrate_data BOOLEAN, chunk_target_size TEXT, chunk_sizing_func REGPROC, time_partitioning_func REGPROC, replication_factor INTEGER, data_nodes NAME[], distributed BOOLEAN);

CREATE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     ANYELEMENT = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    partitioning_func       REGPROC = NULL,
    migrate_data            BOOLEAN = FALSE,
    chunk_target_size       TEXT = NULL,
    chunk_sizing_func       REGPROC = '_timescaledb_functions.calculate_chunk_interval'::regproc,
    time_partitioning_func  REGPROC = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create' LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS @extschema@.create_distributed_hypertable;

DROP FUNCTION IF EXISTS @extschema@.add_data_node;
DROP FUNCTION IF EXISTS @extschema@.delete_data_node;
DROP FUNCTION IF EXISTS @extschema@.attach_data_node;
DROP FUNCTION IF EXISTS @extschema@.detach_data_node;
DROP FUNCTION IF EXISTS @extschema@.alter_data_node;

DROP PROCEDURE IF EXISTS @extschema@.distributed_exec;
DROP FUNCTION IF EXISTS @extschema@.create_distributed_restore_point;
DROP FUNCTION IF EXISTS @extschema@.set_replication_factor;

CREATE TABLE _timescaledb_catalog.compression_settings (
  relid regclass NOT NULL,
  segmentby text[],
  orderby text[],
  orderby_desc bool[],
  orderby_nullsfirst bool[],
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ( (orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL) ),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
);

INSERT INTO _timescaledb_catalog.compression_settings(relid, segmentby, orderby, orderby_desc, orderby_nullsfirst)
  SELECT
    format('%I.%I', ht.schema_name, ht.table_name)::regclass,
    array_agg(attname ORDER BY segmentby_column_index) FILTER(WHERE segmentby_column_index >= 1) AS compress_segmentby,
    array_agg(attname ORDER BY orderby_column_index) FILTER(WHERE orderby_column_index >= 1) AS compress_orderby,
    array_agg(NOT orderby_asc ORDER BY orderby_column_index) FILTER(WHERE orderby_column_index >= 1) AS compress_orderby_desc,
    array_agg(orderby_nullsfirst ORDER BY orderby_column_index) FILTER(WHERE orderby_column_index >= 1) AS compress_orderby_nullsfirst
  FROM _timescaledb_catalog.hypertable_compression hc
    INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id = hc.hypertable_id
  GROUP BY hypertable_id, ht.schema_name, ht.table_name;

GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable_compression;
DROP VIEW IF EXISTS timescaledb_information.compression_settings;
DROP TABLE _timescaledb_catalog.hypertable_compression;

DROP FOREIGN DATA WRAPPER IF EXISTS timescaledb_fdw;
DROP FUNCTION IF EXISTS @extschema@.timescaledb_fdw_handler();
DROP FUNCTION IF EXISTS @extschema@.timescaledb_fdw_validator(text[], oid);


DROP FUNCTION IF EXISTS _timescaledb_functions.create_chunk_replica_table;
DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_drop_replica;
DROP PROCEDURE IF EXISTS _timescaledb_functions.wait_subscription_sync;
DROP FUNCTION IF EXISTS _timescaledb_functions.health;
DROP FUNCTION IF EXISTS _timescaledb_functions.drop_stale_chunks;

DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_replica_table;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_drop_replica;
DROP PROCEDURE IF EXISTS _timescaledb_internal.wait_subscription_sync;
DROP FUNCTION IF EXISTS _timescaledb_internal.health;
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_stale_chunks;

ALTER TABLE _timescaledb_catalog.remote_txn DROP CONSTRAINT remote_txn_remote_transaction_id_check;

DROP TYPE IF EXISTS @extschema@.rxid CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_functions.rxid_in;
DROP FUNCTION IF EXISTS _timescaledb_functions.rxid_out;

DROP FUNCTION IF EXISTS _timescaledb_functions.data_node_hypertable_info;
DROP FUNCTION IF EXISTS _timescaledb_functions.data_node_chunk_info;
DROP FUNCTION IF EXISTS _timescaledb_functions.data_node_compressed_chunk_stats;
DROP FUNCTION IF EXISTS _timescaledb_functions.data_node_index_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.data_node_hypertable_info;
DROP FUNCTION IF EXISTS _timescaledb_internal.data_node_chunk_info;
DROP FUNCTION IF EXISTS _timescaledb_internal.data_node_compressed_chunk_stats;
DROP FUNCTION IF EXISTS _timescaledb_internal.data_node_index_size;

DROP FUNCTION IF EXISTS timescaledb_experimental.block_new_chunks;
DROP FUNCTION IF EXISTS timescaledb_experimental.allow_new_chunks;
DROP FUNCTION IF EXISTS timescaledb_experimental.subscription_exec;
DROP PROCEDURE IF EXISTS timescaledb_experimental.move_chunk;
DROP PROCEDURE IF EXISTS timescaledb_experimental.copy_chunk;
DROP PROCEDURE IF EXISTS timescaledb_experimental.cleanup_copy_chunk_operation;

DROP FUNCTION IF EXISTS _timescaledb_functions.set_chunk_default_data_node;
DROP FUNCTION IF EXISTS _timescaledb_internal.set_chunk_default_data_node;

DROP FUNCTION IF EXISTS _timescaledb_functions.drop_dist_ht_invalidation_trigger;
DROP FUNCTION IF EXISTS _timescaledb_internal.drop_dist_ht_invalidation_trigger;

-- remove multinode catalog tables
DROP VIEW IF EXISTS timescaledb_information.chunks;
DROP VIEW IF EXISTS timescaledb_information.data_nodes;
DROP VIEW IF EXISTS timescaledb_information.hypertables;
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.remote_txn;
DROP TABLE _timescaledb_catalog.remote_txn;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable_data_node;
DROP TABLE _timescaledb_catalog.hypertable_data_node;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_data_node;
DROP TABLE _timescaledb_catalog.chunk_data_node;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_copy_operation;
DROP TABLE _timescaledb_catalog.chunk_copy_operation;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_copy_operation_id_seq;
DROP SEQUENCE _timescaledb_catalog.chunk_copy_operation_id_seq;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension_partition;
DROP TABLE _timescaledb_catalog.dimension_partition;

DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_functions.chunks_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunks_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_functions.indexes_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_internal.indexes_remote_size;
DROP FUNCTION IF EXISTS _timescaledb_functions.compressed_chunk_remote_stats;
DROP FUNCTION IF EXISTS _timescaledb_internal.compressed_chunk_remote_stats;

-- rebuild _timescaledb_catalog.hypertable
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
    compressed_hypertable_id
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
    compressed_hypertable_id
FROM
    _timescaledb_catalog.hypertable_tmp
ORDER BY id;

UPDATE _timescaledb_catalog.hypertable h
SET status = 3
WHERE EXISTS (
  SELECT FROM _timescaledb_catalog.chunk c WHERE c.osm_chunk AND c.hypertable_id = h.id
);

ALTER SEQUENCE _timescaledb_catalog.hypertable_id_seq OWNED BY _timescaledb_catalog.hypertable.id;
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
ALTER TABLE _timescaledb_catalog.tablespace
    ADD CONSTRAINT tablespace_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE;

CREATE SCHEMA _timescaledb_debug;

-- Migrate existing compressed hypertables to new internal format
DO $$
DECLARE
  chunk regclass;
  hypertable regclass;
  ht_id integer;
  index regclass;
  column_name name;
  cmd text;
BEGIN
  SET timescaledb.restoring TO ON;

  -- Detach compressed chunks from their parent hypertables
  FOR chunk, hypertable, ht_id IN
    SELECT
      format('%I.%I',ch.schema_name,ch.table_name)::regclass chunk,
      format('%I.%I',ht.schema_name,ht.table_name)::regclass hypertable,
      ht.id
    FROM _timescaledb_catalog.chunk ch
    INNER JOIN _timescaledb_catalog.hypertable ht_uncomp
      ON ch.hypertable_id = ht_uncomp.compressed_hypertable_id
    INNER JOIN _timescaledb_catalog.hypertable ht
      ON ht.id = ht_uncomp.compressed_hypertable_id
  LOOP

    cmd := format('ALTER TABLE %s NO INHERIT %s', chunk, hypertable);
    EXECUTE cmd;
    -- remove references to indexes from the compressed hypertable
    DELETE FROM _timescaledb_catalog.chunk_index WHERE hypertable_id = ht_id;

  END LOOP;


  FOR hypertable IN
    SELECT
      format('%I.%I',ht.schema_name,ht.table_name)::regclass hypertable
    FROM _timescaledb_catalog.hypertable ht_uncomp
    INNER JOIN _timescaledb_catalog.hypertable ht
      ON ht.id = ht_uncomp.compressed_hypertable_id
  LOOP

    -- remove indexes from the compressed hypertable (but not chunks)
    FOR index IN
      SELECT indexrelid::regclass FROM pg_index WHERE indrelid = hypertable
    LOOP
      cmd := format('DROP INDEX %s', index);
      EXECUTE cmd;
    END LOOP;

    -- remove columns from the compressed hypertable (but not chunks)
    FOR column_name IN
      SELECT attname FROM pg_attribute WHERE attrelid = hypertable AND attnum > 0
    LOOP
      cmd := format('ALTER TABLE %s DROP COLUMN %I', hypertable, column_name);
      EXECUTE cmd;
    END LOOP;

  END LOOP;

  SET timescaledb.restoring TO OFF;
END $$;

DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_constraint_add_table_fk_constraint;
DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_constraint_add_table_fk_constraint;

-- only define stub here, actual code will be filled in at end of update script
CREATE FUNCTION _timescaledb_functions.constraint_clone(constraint_oid OID,target_oid REGCLASS) RETURNS VOID LANGUAGE PLPGSQL AS $$BEGIN END$$ SET search_path TO pg_catalog, pg_temp;

DROP FUNCTION IF EXISTS _timescaledb_functions.chunks_in;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunks_in;

CREATE FUNCTION _timescaledb_functions.metadata_insert_trigger() RETURNS TRIGGER LANGUAGE PLPGSQL
AS $$
BEGIN
  IF EXISTS (SELECT FROM _timescaledb_catalog.metadata WHERE key = NEW.key) THEN
    UPDATE _timescaledb_catalog.metadata SET value = NEW.value WHERE key = NEW.key;
    RETURN NULL;
  END IF;
  RETURN NEW;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE TRIGGER metadata_insert_trigger BEFORE INSERT ON _timescaledb_catalog.metadata FOR EACH ROW EXECUTE PROCEDURE _timescaledb_functions.metadata_insert_trigger();

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.metadata', $$ WHERE key <> 'uuid' $$);

-- Remove unwanted entries from extconfig and extcondition in pg_extension
-- We use ALTER EXTENSION DROP TABLE to remove these entries.
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_cache.cache_inval_hypertable;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_cache.cache_inval_extension;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_cache.cache_inval_bgw_job;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_internal.job_errors;

-- Associate the above tables back to keep the dependencies safe
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_cache.cache_inval_hypertable;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_cache.cache_inval_extension;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_cache.cache_inval_bgw_job;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_internal.job_errors;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.hypertable;
ALTER EXTENSION timescaledb ADD TABLE _timescaledb_catalog.hypertable;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', 'WHERE id >= 1');

CREATE FUNCTION _timescaledb_functions.relation_approximate_size(relation REGCLASS)
RETURNS TABLE (total_size BIGINT, heap_size BIGINT, index_size BIGINT, toast_size BIGINT)
AS '@MODULE_PATHNAME@', 'ts_relation_approximate_size' LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION @extschema@.hypertable_approximate_detailed_size(relation REGCLASS)
RETURNS TABLE (table_bytes BIGINT, index_bytes BIGINT, toast_bytes BIGINT, total_bytes BIGINT)
AS '@MODULE_PATHNAME@', 'ts_hypertable_approximate_size' LANGUAGE C VOLATILE;

--- returns approximate total-bytes for a hypertable (includes table + index)
CREATE FUNCTION @extschema@.hypertable_approximate_size(
    hypertable              REGCLASS)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
   SELECT sum(total_bytes)::bigint
   FROM @extschema@.hypertable_approximate_detailed_size(hypertable);
$BODY$ SET search_path TO pg_catalog, pg_temp;
