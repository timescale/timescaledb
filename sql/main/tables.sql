CREATE TABLE IF NOT EXISTS partition_table (
    namespace_name     NAME REFERENCES namespace (name)               NOT NULL,
    partition_number   SMALLINT                                       NOT NULL CHECK (partition_number >= 0),
    total_partitions   SMALLINT                                       NOT NULL CHECK (total_partitions > 0),
    partitioning_field NAME                                           NOT NULL,
    table_name         NAME                                           NOT NULL,
    PRIMARY KEY (namespace_name, partition_number, total_partitions), --do not allow multiple partitioning fields, for now
    UNIQUE (namespace_name, partition_number, total_partitions, partitioning_field),
    UNIQUE (namespace_name, table_name),
    CHECK (partition_number < total_partitions)
);

CREATE TABLE IF NOT EXISTS data_table (
    table_oid          REGCLASS PRIMARY KEY                           NOT NULL,
    namespace_name     NAME REFERENCES namespace (name)               NOT NULL,
    partition_number   SMALLINT                                       NOT NULL CHECK (partition_number >= 0),
    total_partitions   SMALLINT                                       NOT NULL CHECK (total_partitions > 0),
    partitioning_field NAME                                           NOT NULL,
    start_time         BIGINT,
    end_time           BIGINT,
    FOREIGN KEY (namespace_name, partition_number, total_partitions, partitioning_field)
    REFERENCES partition_table (namespace_name, partition_number, total_partitions, partitioning_field),
    UNIQUE (namespace_name, partition_number, total_partitions, start_time, end_time),
    CHECK (start_time IS NOT NULL OR end_time IS NOT NULL),
    CHECK (partition_number < total_partitions)
);
--TODO: any constrants for when total_partitions change?

CREATE SEQUENCE IF NOT EXISTS data_table_index_name_prefix;

CREATE TABLE IF NOT EXISTS data_table_index (
    table_oid  REGCLASS         NOT NULL REFERENCES data_table (table_oid) ON DELETE CASCADE,
    field_name NAME             NOT NULL,
    index_name NAME             NOT NULL, --not regclass since regclass create problems with database backup/restore (indexes created after data load)
    index_type field_index_type NOT NULL,
    PRIMARY KEY (table_oid, field_name, index_name)
);


