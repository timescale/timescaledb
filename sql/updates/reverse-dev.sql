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

