CREATE SEQUENCE IF NOT EXISTS chunk_replica_node_index_name_prefix;

CREATE TABLE IF NOT EXISTS chunk_replica_node_index (
    schema_name   NAME              NOT NULL,
    table_name    NAME              NOT NULL,
    field_name    NAME              NOT NULL,
    index_name    NAME              NOT NULL, --not regclass since regclass create problems with database backup/restore (indexes created after data load)
    index_type    field_index_type  NOT NULL,
    PRIMARY KEY (schema_name, table_name, field_name, index_name),
    FOREIGN KEY (schema_name, table_name) REFERENCES chunk_replica_node(schema_name, table_name)
);


