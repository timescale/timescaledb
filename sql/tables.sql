--NOTICE: UPGRADE-SCRIPT-NEEDED contents in this file are not auto-upgraded.

-- This file contains table definitions for various abstractions and data
-- structures for representing hypertables and lower-level concepts.

-- Hypertable
-- ==========
--
-- The hypertable is an abstraction that represents a table that is
-- partitioned in N dimensions, where each dimension maps to a column
-- in the table. A dimension can either be 'open' or 'closed', which
-- reflects the scheme that divides the dimension's keyspace into
-- "slices".
--
-- Conceptually, a partition -- called a "chunk", is a hypercube in
-- the N-dimensional space. A chunk stores a subset of the
-- hypertable's tuples on disk in its own distinct table. The slices
-- that span the chunk's hypercube each correspond to a constraint on
-- the chunk's table, enabling constraint exclusion during queries on
-- the hypertable's data.
--
--
-- Open dimensions
------------------
-- An open dimension does on-demand slicing, creating a new slice
-- based on a configurable interval whenever a tuple falls outside the
-- existing slices. Open dimensions fit well with columns that are
-- incrementally increasing, such as time-based ones.
--
-- Closed dimensions
--------------------
-- A closed dimension completely divides its keyspace into a
-- configurable number of slices. The number of slices can be
-- reconfigured, but the new partitioning only affects newly created
-- chunks.
--
-- NOTE: Due to current restrictions, a maximum of two dimensions are
-- allowed, typically one open (time) and one closed (space)
-- dimension.
--
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable (
    id                      SERIAL    PRIMARY KEY,
    schema_name             NAME      NOT NULL CHECK (schema_name != '_timescaledb_catalog'),
    table_name              NAME      NOT NULL,
    associated_schema_name  NAME      NOT NULL,
    associated_table_prefix NAME      NOT NULL,
    num_dimensions          SMALLINT  NOT NULL CHECK (num_dimensions > 0),
    UNIQUE (id, schema_name), -- So hypertable_index can use it as foreign key
    UNIQUE (schema_name, table_name),
    UNIQUE (associated_schema_name, associated_table_prefix)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.hypertable','id'), '');

-- The tablespace table maps tablespaces to hypertables.
-- This allows spreading a hypertable's chunks across multiple disks.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.tablespace (
   id                SERIAL PRIMARY KEY,
   hypertable_id     INT  NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
   tablespace_name   NAME NOT NULL,
   UNIQUE (hypertable_id, tablespace_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.tablespace', '');

-- A dimension represents an axis along which data is partitioned.
CREATE TABLE _timescaledb_catalog.dimension (
    id                          SERIAL   NOT NULL PRIMARY KEY,
    hypertable_id               INTEGER  NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    column_name                 NAME     NOT NULL,
    column_type                 REGTYPE  NOT NULL,
    aligned                     BOOLEAN  NOT NULL,
    -- closed dimensions
    num_slices                  SMALLINT NULL,
    partitioning_func_schema    NAME     NULL,
    partitioning_func           NAME     NULL,
    -- open dimensions (e.g., time)
    interval_length             BIGINT   NULL CHECK(interval_length IS NULL OR interval_length > 0),
    CHECK (
        (partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR
        (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)
    ),
    CHECK (
        (num_slices IS NULL AND interval_length IS NOT NULL) OR
        (num_slices IS NOT NULL AND interval_length IS NULL)
    ),
    UNIQUE (hypertable_id, column_name)
);
CREATE INDEX ON  _timescaledb_catalog.dimension(hypertable_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension','id'), '');

-- A dimension slice defines a keyspace range along a dimension axis.
CREATE TABLE _timescaledb_catalog.dimension_slice (
    id            SERIAL   NOT NULL PRIMARY KEY,
    dimension_id  INTEGER  NOT NULL REFERENCES _timescaledb_catalog.dimension(id) ON DELETE CASCADE,
    range_start   BIGINT   NOT NULL CHECK (range_start >= 0),
    range_end     BIGINT   NOT NULL CHECK (range_end >= 0),
    CHECK (range_start <= range_end),
    UNIQUE (dimension_id, range_start, range_end)
);
CREATE INDEX ON  _timescaledb_catalog.dimension_slice(dimension_id, range_start, range_end);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_slice', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension_slice','id'), '');

-- A chunk is a partition (hypercube) in an N-dimensional
-- hyperspace. Each chunk is associated with N constraints that define
-- the chunk's hypercube. Tuples that fall within the chunk's
-- hypercube are stored in the chunk's data table, as given by
-- 'schema_name' and 'table_name'.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk (
    id              SERIAL  NOT NULL    PRIMARY KEY,
    hypertable_id   INT     NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    schema_name     NAME    NOT NULL,
    table_name      NAME    NOT NULL,
    UNIQUE (schema_name, table_name)
);
CREATE INDEX ON _timescaledb_catalog.chunk(hypertable_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk','id'), '');

-- A chunk constraint maps a dimension slice to a chunk. Each
-- constraint associated with a chunk will also be a table constraint
-- on the chunk's data table.
CREATE TABLE _timescaledb_catalog.chunk_constraint (
    chunk_id            INTEGER  NOT NULL REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    dimension_slice_id  INTEGER  NOT NULL REFERENCES _timescaledb_catalog.dimension_slice(id) ON DELETE CASCADE,
    constraint_name     NAME     NOT NULL,
    PRIMARY KEY(chunk_id, dimension_slice_id)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint', '');

-- Represents an index on the hypertable
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_index (
    hypertable_id    INTEGER             NOT NULL,
    main_schema_name NAME                NOT NULL, -- schema name of main table (needed for a uniqueness constraint)
    main_index_name  NAME                NOT NULL, -- index name on main table
    definition       TEXT                NOT NULL, -- def with /*INDEX_NAME*/ and /*TABLE_NAME*/ placeholders
    FOREIGN KEY (hypertable_id, main_schema_name) REFERENCES _timescaledb_catalog.hypertable(id, schema_name) ON DELETE CASCADE ON UPDATE CASCADE,
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
    FOREIGN KEY (main_schema_name, main_index_name) REFERENCES _timescaledb_catalog.hypertable_index (main_schema_name, main_index_name) ON DELETE CASCADE ON UPDATE CASCADE
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_index', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk_index','id'), '');
