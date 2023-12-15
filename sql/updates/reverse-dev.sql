-- Manually drop the following functions / procedures since 'OR REPLACE' is missing in 2.13.0
DROP PROCEDURE IF EXISTS _timescaledb_functions.repair_relation_acls();
DROP FUNCTION IF EXISTS _timescaledb_functions.makeaclitem(regrole, regrole, text, bool);

CREATE FUNCTION _timescaledb_functions.ping_data_node(node_name NAME, timeout INTERVAL = NULL) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_data_node_ping' LANGUAGE C VOLATILE;

CREATE FUNCTION _timescaledb_functions.remote_txn_heal_data_node(foreign_server_oid oid)
RETURNS INT
AS '@MODULE_PATHNAME@', 'ts_remote_txn_heal_data_node'
LANGUAGE C STRICT;

CREATE FUNCTION _timescaledb_functions.set_dist_id(dist_id UUID) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_dist_set_id' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION _timescaledb_functions.set_peer_dist_id(dist_id UUID) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_dist_set_peer_id' LANGUAGE C VOLATILE STRICT;

-- Function to validate that a node has local settings to function as
-- a data node. Throws error if validation fails.
CREATE FUNCTION _timescaledb_functions.validate_as_data_node() RETURNS void
AS '@MODULE_PATHNAME@', 'ts_dist_validate_as_data_node' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION _timescaledb_functions.show_connection_cache()
RETURNS TABLE (
    node_name           name,
    user_name           name,
    host                text,
    port                int,
    database            name,
    backend_pid         int,
    connection_status   text,
    transaction_status  text,
    transaction_depth   int,
    processing          boolean,
    invalidated         boolean)
AS '@MODULE_PATHNAME@', 'ts_remote_connection_cache_show' LANGUAGE C VOLATILE STRICT;

DROP FUNCTION IF EXISTS @extschema@.create_hypertable(relation REGCLASS, time_column_name NAME, partitioning_column NAME, number_partitions INTEGER, associated_schema_name NAME, associated_table_prefix NAME, chunk_time_interval ANYELEMENT, create_default_indexes BOOLEAN, if_not_exists BOOLEAN, partitioning_func REGPROC, migrate_data BOOLEAN, chunk_target_size TEXT, chunk_sizing_func REGPROC, time_partitioning_func REGPROC);

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
    time_partitioning_func  REGPROC = NULL,
    replication_factor      INTEGER = NULL,
    data_nodes              NAME[] = NULL,
    distributed             BOOLEAN = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.create_distributed_hypertable(
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
    time_partitioning_func  REGPROC = NULL,
    replication_factor      INTEGER = NULL,
    data_nodes              NAME[] = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_distributed_create' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.add_data_node(
    node_name              NAME,
    host                   TEXT,
    database               NAME = NULL,
    port                   INTEGER = NULL,
    if_not_exists          BOOLEAN = FALSE,
    bootstrap              BOOLEAN = TRUE,
    password               TEXT = NULL
) RETURNS TABLE(node_name NAME, host TEXT, port INTEGER, database NAME,
                node_created BOOL, database_created BOOL, extension_created BOOL)
AS '@MODULE_PATHNAME@', 'ts_data_node_add' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.delete_data_node(
    node_name              NAME,
    if_exists              BOOLEAN = FALSE,
    force                  BOOLEAN = FALSE,
    repartition            BOOLEAN = TRUE,
    drop_database          BOOLEAN = FALSE
) RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'ts_data_node_delete' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.attach_data_node(
    node_name              NAME,
    hypertable             REGCLASS,
    if_not_attached        BOOLEAN = FALSE,
    repartition            BOOLEAN = TRUE
) RETURNS TABLE(hypertable_id INTEGER, node_hypertable_id INTEGER, node_name NAME)
AS '@MODULE_PATHNAME@', 'ts_data_node_attach' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.detach_data_node(
    node_name              NAME,
    hypertable             REGCLASS = NULL,
    if_attached            BOOLEAN = FALSE,
    force                  BOOLEAN = FALSE,
    repartition            BOOLEAN = TRUE,
    drop_remote_data       BOOLEAN = FALSE
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_detach' LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.alter_data_node(
    node_name              NAME,
    host                   TEXT = NULL,
    database               NAME = NULL,
    port                   INTEGER = NULL,
    available              BOOLEAN = NULL
) RETURNS TABLE(node_name NAME, host TEXT, port INTEGER, database NAME, available BOOLEAN)

AS '@MODULE_PATHNAME@', 'ts_data_node_alter' LANGUAGE C VOLATILE;
CREATE PROCEDURE @extschema@.distributed_exec(
       query TEXT,
       node_list name[] = NULL,
       transactional BOOLEAN = TRUE)
AS '@MODULE_PATHNAME@', 'ts_distributed_exec' LANGUAGE C;

CREATE FUNCTION @extschema@.create_distributed_restore_point(
    name                   TEXT
) RETURNS TABLE(node_name NAME, node_type TEXT, restore_point pg_lsn)
AS '@MODULE_PATHNAME@', 'ts_create_distributed_restore_point' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION @extschema@.set_replication_factor(
    hypertable              REGCLASS,
    replication_factor      INTEGER
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_hypertable_distributed_set_replication_factor' LANGUAGE C VOLATILE;

CREATE TABLE _timescaledb_catalog.hypertable_compression (
  hypertable_id integer NOT NULL,
  attname name NOT NULL,
  compression_algorithm_id smallint,
  segmentby_column_index smallint,
  orderby_column_index smallint,
  orderby_asc boolean,
  orderby_nullsfirst boolean,
  -- table constraints
  CONSTRAINT hypertable_compression_pkey PRIMARY KEY (hypertable_id, attname),
  CONSTRAINT hypertable_compression_hypertable_id_orderby_column_index_key UNIQUE (hypertable_id, orderby_column_index),
  CONSTRAINT hypertable_compression_hypertable_id_segmentby_column_index_key UNIQUE (hypertable_id, segmentby_column_index),
  CONSTRAINT hypertable_compression_compression_algorithm_id_fkey FOREIGN KEY (compression_algorithm_id) REFERENCES _timescaledb_catalog.compression_algorithm (id),
  CONSTRAINT hypertable_compression_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

INSERT INTO _timescaledb_catalog.hypertable_compression(
	hypertable_id,
	attname,
	compression_algorithm_id,
	segmentby_column_index,
	orderby_column_index,
	orderby_asc,
	orderby_nullsfirst
) SELECT
  ht.id,
  att.attname,
  CASE
    WHEN att.attname = ANY(cs.segmentby) THEN 0
		WHEN att.atttypid IN ('numeric'::regtype) THEN 1
		WHEN att.atttypid IN ('float4'::regtype,'float8'::regtype) THEN 3
		WHEN att.atttypid IN ('int2'::regtype,'int4'::regtype,'int8'::regtype,'date'::regtype,'timestamp'::regtype,'timestamptz'::regtype) THEN 4
		WHEN EXISTS(SELECT FROM pg_operator op WHERE op.oprname = '=' AND op.oprkind = 'b' AND op.oprcanhash = true AND op.oprleft = att.atttypid AND op.oprright = att.atttypid) THEN 2
    ELSE 1
  END AS compression_algorithm_id,
  CASE WHEN att.attname = ANY(cs.segmentby) THEN array_position(cs.segmentby, att.attname::text) ELSE NULL END AS segmentby_column_index,
  CASE WHEN att.attname = ANY(cs.orderby) THEN array_position(cs.orderby, att.attname::text) ELSE NULL END AS orderby_column_index,
  CASE WHEN att.attname = ANY(cs.orderby) THEN NOT cs.orderby_desc[array_position(cs.orderby, att.attname::text)] ELSE false END AS orderby_asc,
  CASE WHEN att.attname = ANY(cs.orderby) THEN cs.orderby_nullsfirst[array_position(cs.orderby, att.attname::text)] ELSE false END AS orderby_nullsfirst
FROM _timescaledb_catalog.hypertable ht
INNER JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = format('%I.%I',ht.schema_name,ht.table_name)::regclass
LEFT JOIN pg_attribute att ON att.attrelid = format('%I.%I',ht.schema_name,ht.table_name)::regclass AND attnum > 0
WHERE compressed_hypertable_id IS NOT NULL;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_compression', '');
GRANT SELECT ON _timescaledb_catalog.hypertable_compression TO PUBLIC;

DROP VIEW timescaledb_information.compression_settings;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_settings;
DROP TABLE _timescaledb_catalog.compression_settings;

CREATE FUNCTION @extschema@.timescaledb_fdw_handler() RETURNS fdw_handler AS '@MODULE_PATHNAME@', 'ts_timescaledb_fdw_handler' LANGUAGE C STRICT;
CREATE FUNCTION @extschema@.timescaledb_fdw_validator(text[], oid) RETURNS void AS '@MODULE_PATHNAME@', 'ts_timescaledb_fdw_validator' LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER timescaledb_fdw HANDLER @extschema@.timescaledb_fdw_handler VALIDATOR @extschema@.timescaledb_fdw_validator;

CREATE FUNCTION _timescaledb_functions.create_chunk_replica_table(
    chunk REGCLASS,
    data_node_name NAME
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_chunk_create_replica_table' LANGUAGE C VOLATILE;

CREATE FUNCTION  _timescaledb_functions.chunk_drop_replica(
    chunk                   REGCLASS,
    node_name               NAME
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunk_drop_replica' LANGUAGE C VOLATILE;

CREATE PROCEDURE _timescaledb_functions.wait_subscription_sync(
    schema_name    NAME,
    table_name     NAME,
    retry_count    INT DEFAULT 18000,
    retry_delay_ms NUMERIC DEFAULT 0.200
)
LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    in_sync BOOLEAN;
BEGIN
    FOR i in 1 .. retry_count
    LOOP
        SELECT pgs.srsubstate = 'r'
        INTO in_sync
        FROM pg_subscription_rel pgs
        JOIN pg_class pgc ON relname = table_name
        JOIN pg_namespace n ON (n.OID = pgc.relnamespace)
        WHERE pgs.srrelid = pgc.oid AND schema_name = n.nspname;

        if (in_sync IS NULL OR NOT in_sync) THEN
          PERFORM pg_sleep(retry_delay_ms);
        ELSE
          RETURN;
        END IF;
    END LOOP;
    RAISE 'subscription sync wait timedout';
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.health() RETURNS
TABLE (node_name NAME, healthy BOOL, in_recovery BOOL, error TEXT)
AS '@MODULE_PATHNAME@', 'ts_health_check' LANGUAGE C VOLATILE;

CREATE FUNCTION _timescaledb_functions.drop_stale_chunks(
    node_name NAME,
    chunks integer[] = NULL
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunks_drop_stale' LANGUAGE C VOLATILE;

CREATE FUNCTION _timescaledb_functions.rxid_in(cstring) RETURNS @extschema@.rxid
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_in' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION _timescaledb_functions.rxid_out(@extschema@.rxid) RETURNS cstring
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_out' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE @extschema@.rxid (
  internallength = 16,
  input = _timescaledb_functions.rxid_in,
  output = _timescaledb_functions.rxid_out
);

CREATE FUNCTION _timescaledb_functions.data_node_hypertable_info(
    node_name              NAME,
    schema_name_in name,
    table_name_in name
)
RETURNS TABLE (
    table_bytes     bigint,
    index_bytes     bigint,
    toast_bytes     bigint,
    total_bytes     bigint)
AS '@MODULE_PATHNAME@', 'ts_dist_remote_hypertable_info' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION _timescaledb_functions.data_node_chunk_info(
    node_name              NAME,
    schema_name_in name,
    table_name_in name
)
RETURNS TABLE (
    chunk_id        integer,
    chunk_schema    name,
    chunk_name      name,
    table_bytes     bigint,
    index_bytes     bigint,
    toast_bytes     bigint,
    total_bytes     bigint)
AS '@MODULE_PATHNAME@', 'ts_dist_remote_chunk_info' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION _timescaledb_functions.data_node_compressed_chunk_stats(node_name name, schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint
    )
AS '@MODULE_PATHNAME@' , 'ts_dist_remote_compressed_chunk_info' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION _timescaledb_functions.data_node_index_size(node_name name, schema_name_in name, index_name_in name)
RETURNS TABLE ( hypertable_id INTEGER, total_bytes BIGINT)
AS '@MODULE_PATHNAME@' , 'ts_dist_remote_hypertable_index_info' LANGUAGE C VOLATILE STRICT;

CREATE FUNCTION timescaledb_experimental.block_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL, force BOOLEAN = FALSE) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_block_new_chunks' LANGUAGE C VOLATILE;

CREATE FUNCTION timescaledb_experimental.allow_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_allow_new_chunks' LANGUAGE C VOLATILE;

CREATE PROCEDURE timescaledb_experimental.move_chunk(
    chunk REGCLASS,
    source_node NAME = NULL,
    destination_node NAME = NULL,
    operation_id NAME = NULL)
AS '@MODULE_PATHNAME@', 'ts_move_chunk_proc' LANGUAGE C;

CREATE PROCEDURE timescaledb_experimental.copy_chunk(
    chunk REGCLASS,
    source_node NAME = NULL,
    destination_node NAME = NULL,
    operation_id NAME = NULL)
AS '@MODULE_PATHNAME@', 'ts_copy_chunk_proc' LANGUAGE C;

CREATE FUNCTION timescaledb_experimental.subscription_exec(
    subscription_command TEXT
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_subscription_exec' LANGUAGE C VOLATILE;

CREATE PROCEDURE timescaledb_experimental.cleanup_copy_chunk_operation(
    operation_id NAME)
AS '@MODULE_PATHNAME@', 'ts_copy_chunk_cleanup_proc' LANGUAGE C;

CREATE FUNCTION _timescaledb_functions.set_chunk_default_data_node(chunk REGCLASS, node_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_chunk_set_default_data_node' LANGUAGE C VOLATILE;

CREATE FUNCTION _timescaledb_functions.drop_dist_ht_invalidation_trigger(
    raw_hypertable_id INTEGER
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_drop_dist_ht_invalidation_trigger' LANGUAGE C STRICT VOLATILE;

-- restore multinode catalog tables
CREATE TABLE _timescaledb_catalog.remote_txn (
  data_node_name name, --this is really only to allow us to cleanup stuff on a per-node basis.
  remote_transaction_id text NOT NULL,
  -- table constraints
  CONSTRAINT remote_txn_pkey PRIMARY KEY (remote_transaction_id)
);

ALTER TABLE _timescaledb_catalog.remote_txn ADD CONSTRAINT remote_txn_remote_transaction_id_check CHECK (remote_transaction_id::@extschema@.rxid IS NOT NULL);

CREATE INDEX remote_txn_data_node_name_idx ON _timescaledb_catalog.remote_txn (data_node_name);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.remote_txn', '');
GRANT SELECT ON TABLE _timescaledb_catalog.remote_txn TO PUBLIC;

CREATE TABLE _timescaledb_catalog.hypertable_data_node (
  hypertable_id integer NOT NULL,
  node_hypertable_id integer NULL,
  node_name name NOT NULL,
  block_chunks boolean NOT NULL,
  -- table constraints
  CONSTRAINT hypertable_data_node_hypertable_id_node_name_key UNIQUE (hypertable_id, node_name),
  CONSTRAINT hypertable_data_node_node_hypertable_id_node_name_key UNIQUE (node_hypertable_id, node_name),
  CONSTRAINT hypertable_data_node_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_data_node', '');
GRANT SELECT ON TABLE _timescaledb_catalog.hypertable_data_node TO PUBLIC;

CREATE TABLE _timescaledb_catalog.chunk_data_node (
  chunk_id integer NOT NULL,
  node_chunk_id integer NOT NULL,
  node_name name NOT NULL,
  -- table constraints
  CONSTRAINT chunk_data_node_chunk_id_node_name_key UNIQUE (chunk_id, node_name),
  CONSTRAINT chunk_data_node_node_chunk_id_node_name_key UNIQUE (node_chunk_id, node_name),
  CONSTRAINT chunk_data_node_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id)
);

CREATE INDEX chunk_data_node_node_name_idx ON _timescaledb_catalog.chunk_data_node (node_name);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_data_node', '');
GRANT SELECT ON TABLE _timescaledb_catalog.chunk_data_node TO PUBLIC;

CREATE SEQUENCE _timescaledb_catalog.chunk_copy_operation_id_seq MINVALUE 1;
GRANT SELECT ON SEQUENCE _timescaledb_catalog.chunk_copy_operation_id_seq TO PUBLIC;

CREATE TABLE _timescaledb_catalog.chunk_copy_operation (
  operation_id name NOT NULL, -- the publisher/subscriber identifier used
  backend_pid integer NOT NULL, -- the pid of the backend running this activity
  completed_stage name NOT NULL, -- the completed stage/step
  time_start timestamptz NOT NULL DEFAULT NOW(), -- start time of the activity
  chunk_id integer NOT NULL,
  compress_chunk_name name NOT NULL,
  source_node_name name NOT NULL,
  dest_node_name name NOT NULL,
  delete_on_source_node bool NOT NULL, -- is a move or copy activity
  -- table constraints
  CONSTRAINT chunk_copy_operation_pkey PRIMARY KEY (operation_id),
  CONSTRAINT chunk_copy_operation_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE
);

GRANT SELECT ON TABLE _timescaledb_catalog.chunk_copy_operation TO PUBLIC;

CREATE TABLE _timescaledb_catalog.dimension_partition (
  dimension_id integer NOT NULL REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE,
  range_start bigint NOT NULL,
  data_nodes name[] NULL,
  UNIQUE (dimension_id, range_start)
);

GRANT SELECT ON TABLE _timescaledb_catalog.dimension_partition TO PUBLIC;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_partition', '');
CREATE FUNCTION _timescaledb_functions.hypertable_remote_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint,
    node_name   NAME)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.chunks_remote_size(
    schema_name_in name,
    table_name_in name)
RETURNS TABLE (
    chunk_id    integer,
    chunk_schema NAME,
    chunk_name  NAME,
    table_bytes bigint,
    index_bytes bigint,
    toast_bytes bigint,
    total_bytes bigint,
    node_name NAME)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.indexes_remote_size(
    schema_name_in             NAME,
    table_name_in              NAME,
    index_name_in              NAME
)
RETURNS BIGINT
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_functions.compressed_chunk_remote_stats(schema_name_in name, table_name_in name)
    RETURNS TABLE (
        chunk_schema name,
        chunk_name name,
        compression_status text,
        before_compression_table_bytes bigint,
        before_compression_index_bytes bigint,
        before_compression_toast_bytes bigint,
        before_compression_total_bytes bigint,
        after_compression_table_bytes bigint,
        after_compression_index_bytes bigint,
        after_compression_toast_bytes bigint,
        after_compression_total_bytes bigint,
        node_name name)
    LANGUAGE SQL
    STABLE STRICT
    AS
$BODY$
$BODY$ SET search_path TO pg_catalog, pg_temp;

DROP VIEW timescaledb_information.chunks;
DROP VIEW timescaledb_information.hypertables;

CREATE VIEW timescaledb_information.hypertables AS
SELECT ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name,
  t.tableowner AS owner,
  ht.num_dimensions,
  (
    SELECT count(1)
    FROM _timescaledb_catalog.chunk ch
    WHERE ch.hypertable_id = ht.id AND ch.dropped IS FALSE AND ch.osm_chunk IS FALSE) AS num_chunks,
  (
    CASE WHEN compression_state = 1 THEN
      TRUE
    ELSE
      FALSE
    END) AS compression_enabled,
  (
    CASE WHEN ht.replication_factor > 0 THEN
      TRUE
    ELSE
      FALSE
    END) AS is_distributed,
  ht.replication_factor,
  dn.node_list AS data_nodes,
  srchtbs.tablespace_list AS tablespaces
FROM _timescaledb_catalog.hypertable ht
  INNER JOIN pg_tables t ON ht.table_name = t.tablename
    AND ht.schema_name = t.schemaname
  LEFT OUTER JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = ht.id
  LEFT OUTER JOIN (
    SELECT hypertable_id,
      array_agg(tablespace_name ORDER BY id) AS tablespace_list
    FROM _timescaledb_catalog.tablespace
    GROUP BY hypertable_id) srchtbs ON ht.id = srchtbs.hypertable_id
  LEFT OUTER JOIN (
  SELECT hypertable_id,
    array_agg(node_name ORDER BY node_name) AS node_list
  FROM _timescaledb_catalog.hypertable_data_node
  GROUP BY hypertable_id) dn ON ht.id = dn.hypertable_id
WHERE ht.compression_state != 2 --> no internal compression tables
  AND ca.mat_hypertable_id IS NULL;

