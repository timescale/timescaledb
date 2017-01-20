CREATE TABLE IF NOT EXISTS _iobeamdb_catalog.kafka_offset_cluster (
    topic            TEXT                                 NOT NULL,
    partition_number SMALLINT                             NOT NULL,
    start_offset     INTEGER                              NOT NULL,
    next_offset      INTEGER,
    database_name    NAME REFERENCES _iobeamdb_catalog.node (database_name) NOT NULL,
    PRIMARY KEY (topic, partition_number, start_offset, database_name)
);

CREATE TABLE IF NOT EXISTS _iobeamdb_catalog.kafka_offset_local (
    PRIMARY KEY (topic, partition_number, start_offset, database_name)
)
    INHERITS (_iobeamdb_catalog.kafka_offset_cluster);

CREATE TABLE IF NOT EXISTS _iobeamdb_catalog.kafka_offset_node (
    database_name     NAME REFERENCES _iobeamdb_catalog.node (database_name) NOT NULL,
    local_table_name  NAME,
    remote_table_name NAME UNIQUE,
    PRIMARY KEY (database_name)
);
