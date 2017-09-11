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
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable (
    id                      SERIAL    PRIMARY KEY,
    schema_name             NAME      NOT NULL CHECK (schema_name != '_timescaledb_catalog'),
    table_name              NAME      NOT NULL,
    associated_schema_name  NAME      NOT NULL,
    associated_table_prefix NAME      NOT NULL,
    num_dimensions          SMALLINT  NOT NULL CHECK (num_dimensions > 0),
    UNIQUE (id, schema_name),
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
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.dimension (
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
CREATE INDEX IF NOT EXISTS dimension_hypertable_id_idx
ON _timescaledb_catalog.dimension(hypertable_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension','id'), '');

-- A dimension slice defines a keyspace range along a dimension axis.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.dimension_slice (
    id            SERIAL   NOT NULL PRIMARY KEY,
    dimension_id  INTEGER  NOT NULL REFERENCES _timescaledb_catalog.dimension(id) ON DELETE CASCADE,
    range_start   BIGINT   NOT NULL,
    range_end     BIGINT   NOT NULL,
    CHECK (range_start <= range_end),
    UNIQUE (dimension_id, range_start, range_end)
);
CREATE INDEX IF NOT EXISTS dimension_slice_dimension_id_range_start_range_end_idx
ON _timescaledb_catalog.dimension_slice(dimension_id, range_start, range_end);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_slice', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension_slice','id'), '');

-- A chunk is a partition (hypercube) in an N-dimensional
-- hyperspace. Each chunk is associated with N constraints that define
-- the chunk's hypercube. Tuples that fall within the chunk's
-- hypercube are stored in the chunk's data table, as given by
-- 'schema_name' and 'table_name'.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk (
    id              SERIAL  NOT NULL    PRIMARY KEY,
    hypertable_id   INT     NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id),
    schema_name     NAME    NOT NULL,
    table_name      NAME    NOT NULL,
    UNIQUE (schema_name, table_name)
);
CREATE INDEX IF NOT EXISTS chunk_hypertable_id_idx
ON _timescaledb_catalog.chunk(hypertable_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk','id'), '');

-- A chunk constraint maps a dimension slice to a chunk. Each
-- constraint associated with a chunk will also be a table constraint
-- on the chunk's data table.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_constraint (
    chunk_id            INTEGER  NOT NULL REFERENCES _timescaledb_catalog.chunk(id),
    dimension_slice_id  INTEGER  NULL REFERENCES _timescaledb_catalog.dimension_slice(id),
    constraint_name     NAME NOT NULL,
    hypertable_constraint_name NAME NULL,
    UNIQUE(chunk_id, constraint_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint', '');
CREATE INDEX IF NOT EXISTS chunk_constraint_chunk_id_dimension_slice_id_idx
ON _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id);

CREATE SEQUENCE IF NOT EXISTS _timescaledb_catalog.chunk_constraint_name;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_index (
    chunk_id              INTEGER NOT NULL REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    index_name            NAME NOT NULL,
    hypertable_id         INTEGER NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    hypertable_index_name NAME NOT NULL,
    UNIQUE(chunk_id, index_name)
);
CREATE INDEX IF NOT EXISTS chunk_index_hypertable_id_hypertable_index_name_idx
ON _timescaledb_catalog.chunk_index(hypertable_id, hypertable_index_name);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_index', '');
