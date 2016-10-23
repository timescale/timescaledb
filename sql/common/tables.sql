CREATE TABLE IF NOT EXISTS node (
    database_name NAME PRIMARY KEY NOT NULL,
    schema_name   NAME UNIQUE      NOT NULL, --public schema of remote
    server_name   NAME UNIQUE      NOT NULL
);

CREATE TABLE IF NOT EXISTS namespace (
    name                        NAME PRIMARY KEY NOT NULL,
    schema_name                 NAME UNIQUE      NOT NULL,
    cluster_table_name          NAME             NOT NULL,
    cluster_distinct_table_name NAME             NOT NULL
);

CREATE TABLE IF NOT EXISTS namespace_node (
    namespace_name             NAME REFERENCES namespace (name)     NOT NULL,
    database_name              NAME REFERENCES node (database_name) NOT NULL,
    master_table_name          NAME                                 NOT NULL,
    remote_table_name          NAME                                 NOT NULL,
    distinct_local_table_name  NAME                                 NOT NULL,
    distinct_remote_table_name NAME                                 NOT NULL,
    PRIMARY KEY (namespace_name, database_name)
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_remote_table_name_per_namespace
    ON namespace_node (namespace_name, remote_table_name);
CREATE UNIQUE INDEX IF NOT EXISTS unique_distinct_remote_table_name_per_namespace
    ON namespace_node (namespace_name, distinct_remote_table_name);

CREATE TABLE IF NOT EXISTS field (
    namespace_name  NAME    NOT NULL REFERENCES namespace (name),
    name            NAME    NOT NULL,
    data_type       REGTYPE NOT NULL CHECK (data_type IN
                                            ('double precision' :: REGTYPE, 'text' :: REGTYPE, 'boolean' :: REGTYPE, 'bigint' :: REGTYPE)),
    is_partitioning BOOLEAN DEFAULT FALSE,
    is_distinct     BOOLEAN DEFAULT FALSE,
    index_types     field_index_type [],
    PRIMARY KEY (namespace_name, name)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_partition_field
    ON field (namespace_name)
    WHERE is_partitioning;
