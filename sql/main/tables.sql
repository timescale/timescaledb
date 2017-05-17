CREATE SEQUENCE IF NOT EXISTS _timescaledb_catalog.chunk_replica_node_index_name_prefix;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_replica_node_index_name_prefix', '');

/*
  Keeps track of indexes on local chunk_replica_nodes.
*/
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_replica_node_index (
    schema_name       NAME    NOT NULL,
    table_name        NAME    NOT NULL,
    index_name        NAME    NOT NULL, --not regclass since regclass create problems with database backup/restore (indexes created after data load)
    main_schema_name  NAME    NOT NULL,
    main_index_name   NAME    NOT NULL,
    definition        TEXT    NOT NULL,
    PRIMARY KEY (schema_name, table_name, index_name),
    FOREIGN KEY (schema_name, table_name) REFERENCES _timescaledb_catalog.chunk_replica_node (schema_name, table_name) ON DELETE CASCADE,
    FOREIGN KEY (main_schema_name, main_index_name) REFERENCES _timescaledb_catalog.hypertable_index (main_schema_name, main_index_name) ON DELETE CASCADE
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_replica_node_index', '');
