-- This file contains table definitions for various abstractions and data
-- structures for representing hypertables and lower level concepts.

-- Data node information for the cluster. database_name is the postgres database
-- located on at hostname. server_name is used to identify the connection.
-- schema_name is the name of the schema used to represent the node on the meta
-- node (it stores remote wrappers to update meta tables on data nodes).
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.node (
    database_name NAME    NOT NULL PRIMARY KEY,
    schema_name   NAME    NOT NULL UNIQUE, --the schema name with remote tables to _timescaledb_catalog schema of the node
    server_name   NAME    NOT NULL UNIQUE,
    hostname      TEXT    NOT NULL,
    port          INT     NOT NULL CONSTRAINT valid_port CHECK (port >= 0 AND port <= 65535),
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    id            SERIAL  NOT NULL UNIQUE -- id for node. used in naming
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.node', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.node','id'), '');

-- Singleton (i.e. should only contain one row) holding info about meta db.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.meta (
    database_name NAME NOT NULL PRIMARY KEY,
    hostname      TEXT NOT NULL,
    port          INT  NOT NULL,
    server_name   NAME NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.meta', '');
CREATE UNIQUE INDEX there_can_be_only_one_meta
    ON _timescaledb_catalog.meta ((1));

-- Users should exist an all nodes+meta in the cluster.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.cluster_user (
    username TEXT NOT NULL PRIMARY KEY,
    password TEXT NULL --not any more of a security hole than usual since stored in  pg_user_mapping anyway
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.cluster_user', '');

-- The hypertable is an abstraction that represents a replicated table that is
-- partitioned on 2 dimensions: time and another (user-)chosen one.
--
-- Each row, representing a hypertable, creates 3 tables:
--    1) main table - an alias to the 0'th replica for now. Represents the
--       hypertable to the user for insertion and modification.
--    2) root table - ancesstor of all the data tables (across replicas).
--       Should not be queryable for data (TODO).
--
-- Additionally, a schema for associated tables (partitioned, replicated data
-- tables) is created.
--
-- The name and type of the time column (used to partition on time) are defined
-- in `time_column_name` and `time_column_type`.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable (
    id                      SERIAL                                  PRIMARY KEY,
    schema_name             NAME                                    NOT NULL CHECK (schema_name != '_timescaledb_catalog'),
    table_name              NAME                                    NOT NULL,
    associated_schema_name  NAME                                    NOT NULL,
    associated_table_prefix NAME                                    NOT NULL,
    root_schema_name        NAME                                    NOT NULL,
    root_table_name         NAME                                    NOT NULL,
    replication_factor      SMALLINT                                NOT NULL CHECK (replication_factor > 0),
    placement               _timescaledb_catalog.chunk_placement_type  NOT NULL,
    time_column_name        NAME                                    NOT NULL,
    time_column_type        REGTYPE                                 NOT NULL,
    chunk_time_interval     BIGINT                                  NOT NULL CHECK (chunk_time_interval > 0),
    UNIQUE (schema_name, table_name),
    UNIQUE (associated_schema_name, associated_table_prefix),
    UNIQUE (root_schema_name, root_table_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.hypertable','id'), '');

-- hypertable_replica contains information on how a hypertable's data replicas
-- are stored. A replica of the data is across all partitions and time.
--
-- Each row identifies a table for each hypertable + replica_id combination:
--   - data replica table (schema_name.table_name) -
--      All the data for a hypertable.
--      Parent: hypertable's `root table`
--      Children: hypertable's `partition_replica` tables
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_replica (
    hypertable_id        INTEGER  NOT NULL  REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    replica_id           SMALLINT NOT NULL  CHECK (replica_id >= 0),
    schema_name          NAME     NOT NULL,
    table_name           NAME     NOT NULL,
    PRIMARY KEY (hypertable_id, replica_id),
    UNIQUE (schema_name, table_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_replica', '');

-- Mapping that shows which replica is pointed to by the main table on
-- each node. The translation from main table to replica should happen
-- in C tranformation right after the parsing step.
-- (Postgres RULES cannot be used, unfortunately)
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.default_replica_node (
    database_name        NAME     NOT NULL  REFERENCES _timescaledb_catalog.node(database_name),
    hypertable_id        INTEGER  NOT NULL  REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    replica_id           SMALLINT NOT NULL  CHECK (replica_id >= 0),
    PRIMARY KEY (database_name, hypertable_id),
    FOREIGN KEY (hypertable_id, replica_id) REFERENCES _timescaledb_catalog.hypertable_replica(hypertable_id, replica_id)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.default_replica_node', '');

-- A partition_epoch represents a different partitioning of the data.
-- It has a start and end time (data time). Data needs to be placed in the correct epoch by time.
-- Partitionings are defined by a function, column, and modulo:
--   1) partitioning_func - Takes the partitioning_column and returns a number
--      which is modulo'd to place the data correctly
--   2) partitioning_mod - Number used in modulo operation
--   3) partitioning_column - column in data to partition by (input to partitioning_func)
--
-- Changing a data's partitioning, and thus creating a new epoch, should be done
-- INFREQUENTLY as it's expensive operation.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.partition_epoch (
    id                          SERIAL   NOT NULL  PRIMARY KEY,
    hypertable_id               INTEGER  NOT NULL  REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    start_time                  BIGINT   NULL      CHECK (start_time >= 0),
    end_time                    BIGINT   NULL      CHECK (end_time >= 0),
    num_partitions              SMALLINT NOT NULL  CHECK (num_partitions >= 0),
    partitioning_func_schema    NAME     NULL,
    partitioning_func           NAME     NULL,  --function name of a function of the form func(data_value, partitioning_mod) -> [0, partitioning_mod)
    partitioning_mod            INT      NOT NULL  CHECK (partitioning_mod < 65536),
    partitioning_column         NAME     NULL,
    UNIQUE (hypertable_id, start_time),
    UNIQUE (hypertable_id, end_time),
    CHECK (start_time <= end_time),
    CHECK (num_partitions <= partitioning_mod),
    CHECK ((partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL))
);
CREATE INDEX ON  _timescaledb_catalog.partition_epoch(hypertable_id, start_time, end_time);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.partition_epoch', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.partition_epoch','id'), '');

-- A partition defines a partition witin a partition_epoch.
-- For any partition the keyspace is defined as [keyspace_start, keyspace_end].
-- For any epoch, there must be a partition that covers every element in the
-- keyspace, i.e. from [0, partition_epoch.partitioning_mod].
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.partition (
    id             SERIAL   NOT NULL PRIMARY KEY,
    epoch_id       INT      NOT NULL REFERENCES _timescaledb_catalog.partition_epoch (id) ON DELETE CASCADE,
    keyspace_start SMALLINT NOT NULL CHECK (keyspace_start >= 0), --start inclusive
    keyspace_end   SMALLINT NOT NULL CHECK (keyspace_end > 0), --end   inclusive; compatible with between operator
    tablespace     NAME     NULL,
    UNIQUE (epoch_id, keyspace_start),
    CHECK (keyspace_end > keyspace_start)
);
CREATE INDEX ON _timescaledb_catalog.partition(epoch_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.partition', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.partition','id'), '');

--Represents a replica for a partition.
--Each row creates a table:
--   Parent: "hypertable_replica.schema_name"."hypertable_replica.table_name"
--   Children: "chunk_replica_node.schema_name"."chunk_replica_node.table_name"
--TODO: trigger to verify partition_epoch hypertable id matches this hypertable_id
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.partition_replica (
    id              SERIAL   NOT NULL PRIMARY KEY,
    partition_id    INTEGER  NOT NULL REFERENCES _timescaledb_catalog.partition(id) ON DELETE CASCADE,
    hypertable_id   INTEGER  NOT NULL,
    replica_id      SMALLINT NOT NULL,
    schema_name     NAME     NOT NULL,
    table_name      NAME     NOT NULL,
    UNIQUE (schema_name, table_name),
    UNIQUE (partition_id, replica_id),
    FOREIGN KEY (hypertable_id, replica_id) REFERENCES _timescaledb_catalog.hypertable_replica(hypertable_id, replica_id) ON DELETE CASCADE
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.partition_replica', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.partition_replica','id'), '');

-- Represent a (replicated) chunk of data, which is data in a hypertable that is
-- both partitioned by both the partition_column and time.
--
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk (
    id           SERIAL NOT NULL    PRIMARY KEY,
    partition_id INT    NOT NULL    REFERENCES _timescaledb_catalog.partition (id) ON DELETE CASCADE,
    start_time   BIGINT NOT NULL    CHECK (start_time >= 0),
    end_time     BIGINT NOT NULL    CHECK (end_time >= 0),
    UNIQUE (partition_id, start_time),
    UNIQUE (partition_id, end_time),
    CHECK (start_time <= end_time)
);
CREATE UNIQUE INDEX ON  _timescaledb_catalog.chunk (partition_id) WHERE start_time IS NULL;
CREATE UNIQUE INDEX ON  _timescaledb_catalog.chunk (partition_id) WHERE end_time IS NULL;
CREATE INDEX ON _timescaledb_catalog.chunk(partition_id, start_time, end_time);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk','id'), '');

-- A mapping between chunks, partition_replica, and nodes representing where
-- actual data is stored. That is, a chunk_replica_node is a particular
-- replication instance of a chunk.
--
-- Each row represents a table:
--   Parent table: "partition_replica.schema_name"."partition_replica.table_name"
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_replica_node (
    chunk_id             INT  NOT NULL  REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    partition_replica_id INT  NOT NULL  REFERENCES _timescaledb_catalog.partition_replica(id) ON DELETE CASCADE,
    database_name        NAME NOT NULL  REFERENCES _timescaledb_catalog.node(database_name),
    schema_name          NAME NOT NULL,
    table_name           NAME NOT NULL,
    PRIMARY KEY (chunk_id, partition_replica_id), --a single chunk, replica tuple
    UNIQUE (chunk_id, database_name), --no two chunk replicas on same node
    UNIQUE (schema_name, table_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_replica_node', '');

-- Represents a hypertable column.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_column (
    hypertable_id   INTEGER             NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    name            NAME                NOT NULL,
    attnum          INT2                NOT NULL, --MUST match pg_attribute.attnum on main table. SHOULD match on root/hierarchy table as well.
    data_type       REGTYPE             NOT NULL,
    default_value   TEXT                NULL,
    not_null        BOOLEAN             NOT NULL,
    PRIMARY KEY (hypertable_id, name),
    UNIQUE(hypertable_id, attnum)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_column', '');

-- TODO(mat) - Description?
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.deleted_hypertable_column (
  LIKE _timescaledb_catalog.hypertable_column,
  deleted_on NAME
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.deleted_hypertable_column', '');

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_index (
    hypertable_id    INTEGER             NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    main_schema_name NAME                NOT NULL, --schema name of main table (needed for a uniqueness constraint)
    main_index_name  NAME                NOT NULL, --index name on main table
    definition       TEXT                NOT NULL, --def with /*INDEX_NAME*/ and /*TABLE_NAME*/ placeholders
    PRIMARY KEY (hypertable_id, main_index_name),
    UNIQUE(main_schema_name, main_index_name) --globally unique since index names globally unique
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_index', '');


