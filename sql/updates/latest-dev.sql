
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

