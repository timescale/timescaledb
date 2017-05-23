-- This file contains table definitions for various abstractions and data
-- structures for representing hypertables and lower level concepts.

-- The hypertable is an abstraction that represents a replicated table that is
-- partitioned on 2 dimensions: time and another (user-)chosen one.
--
-- The table representing the hypertable is named by `schema_name`.`table_name`
--
-- The name and type of the time column (used to partition on time) are defined
-- in `time_column_name` and `time_column_type`.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable (
    id                      SERIAL                                  PRIMARY KEY,
    schema_name             NAME                                    NOT NULL CHECK (schema_name != '_timescaledb_catalog'),
    table_name              NAME                                    NOT NULL,
    associated_schema_name  NAME                                    NOT NULL,
    associated_table_prefix NAME                                    NOT NULL,
    time_column_name        NAME                                    NOT NULL,
    time_column_type        REGTYPE                                 NOT NULL,
    chunk_time_interval     BIGINT                                  NOT NULL CHECK (chunk_time_interval > 0),
    UNIQUE (schema_name, table_name),
    UNIQUE (associated_schema_name, associated_table_prefix)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.hypertable','id'), '');

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
    partitioning_func           NAME     NULL,  -- function name of a function of the form func(data_value, partitioning_mod) -> [0, partitioning_mod)
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
--   Parent: "hypertable.schema_name"."hypertable.table_name"
--   Children: "chunk.schema_name"."chunk.table_name"
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.partition (
    id             SERIAL   NOT NULL PRIMARY KEY,
    epoch_id       INT      NOT NULL REFERENCES _timescaledb_catalog.partition_epoch (id) ON DELETE CASCADE,
    keyspace_start SMALLINT NOT NULL CHECK (keyspace_start >= 0), -- start inclusive
    keyspace_end   SMALLINT NOT NULL CHECK (keyspace_end > 0), -- end inclusive; compatible with between operator
    tablespace     NAME     NULL,
    UNIQUE (epoch_id, keyspace_start),
    CHECK (keyspace_end > keyspace_start)
);
CREATE INDEX ON _timescaledb_catalog.partition(epoch_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.partition', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.partition','id'), '');

-- Represent a chunk of data, which is data in a hypertable that is
-- partitioned by both the partition_column and time.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk (
    id           SERIAL NOT NULL    PRIMARY KEY,
    partition_id INT    NOT NULL    REFERENCES _timescaledb_catalog.partition (id) ON DELETE CASCADE,
    start_time   BIGINT NOT NULL    CHECK (start_time >= 0),
    end_time     BIGINT NOT NULL    CHECK (end_time >= 0),
    schema_name  NAME   NOT NULL,
    table_name   NAME   NOT NULL,
    UNIQUE (schema_name, table_name),
    UNIQUE (partition_id, start_time),
    UNIQUE (partition_id, end_time),
    CHECK (start_time <= end_time)
);
CREATE UNIQUE INDEX ON  _timescaledb_catalog.chunk (partition_id) WHERE start_time IS NULL;
CREATE UNIQUE INDEX ON  _timescaledb_catalog.chunk (partition_id) WHERE end_time IS NULL;
CREATE INDEX ON _timescaledb_catalog.chunk(partition_id, start_time, end_time);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk','id'), '');

-- Represents an index on the hypertable
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_index (
    hypertable_id    INTEGER             NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    main_schema_name NAME                NOT NULL, -- schema name of main table (needed for a uniqueness constraint)
    main_index_name  NAME                NOT NULL, -- index name on main table
    definition       TEXT                NOT NULL, -- def with /*INDEX_NAME*/ and /*TABLE_NAME*/ placeholders
    PRIMARY KEY (hypertable_id, main_index_name),
    UNIQUE(main_schema_name, main_index_name) -- globally unique since index names globally unique
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_index', '');

-- Represents an index on a chunk
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_index (
    id                SERIAL  PRIMARY KEY,
    schema_name       NAME    NOT NULL,
    table_name        NAME    NOT NULL,
    index_name        NAME    NOT NULL, -- not regclass since regclass create problems with database backup/restore (indexes created after data load)
    main_schema_name  NAME    NOT NULL,
    main_index_name   NAME    NOT NULL,
    definition        TEXT    NOT NULL,
    UNIQUE (schema_name, table_name, index_name),
    FOREIGN KEY (schema_name, table_name) REFERENCES _timescaledb_catalog.chunk (schema_name, table_name) ON DELETE CASCADE,
    FOREIGN KEY (main_schema_name, main_index_name) REFERENCES _timescaledb_catalog.hypertable_index (main_schema_name, main_index_name) ON DELETE CASCADE
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_index', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk_index','id'), '');
