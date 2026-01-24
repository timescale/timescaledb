-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file defines DDL functions for adding and manipulating hypertables.

-- Converts a regular postgres table to a hypertable.
--
-- relation - The OID of the table to be converted
-- time_column_name - Name of the column that contains time for a given record
-- partitioning_column - Name of the column to partition data by
-- number_partitions - (Optional) Number of partitions for data
-- associated_schema_name - (Optional) Schema for internal hypertable tables
-- associated_table_prefix - (Optional) Prefix for internal hypertable table names
-- chunk_time_interval - (Optional) Initial time interval for a chunk
-- create_default_indexes - (Optional) Whether or not to create the default indexes
-- if_not_exists - (Optional) Do not fail if table is already a hypertable
-- partitioning_func - (Optional) The partitioning function to use for spatial partitioning
-- migrate_data - (Optional) Set to true to migrate any existing data in the table to chunks
-- chunk_target_size - (Optional) The target size for chunks (e.g., '1000MB', 'estimate', or 'off')
-- chunk_sizing_func - (Optional) A function to calculate the chunk time interval for new chunks
-- time_partitioning_func - (Optional) The partitioning function to use for "time" partitioning
CREATE OR REPLACE FUNCTION @extschema@.create_hypertable(
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
    chunk_time_origin       "any" = NULL::timestamptz
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create' LANGUAGE C VOLATILE;

-- A generalized hypertable creation API that can be used to convert a PostgreSQL table
-- with TIME/SERIAL/BIGSERIAL columns to a hypertable.
--
-- relation - The OID of the table to be converted
-- dimension - The dimension to use for partitioning
-- create_default_indexes (Optional) Whether or not to create the default indexes
-- if_not_exists (Optional) Do not fail if table is already a hypertable
-- migrate_data (Optional) Set to true to migrate any existing data in the table to chunks
CREATE OR REPLACE FUNCTION @extschema@.create_hypertable(
    relation                REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    migrate_data            BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create_general' LANGUAGE C VOLATILE;

-- Set adaptive chunking. To disable, set chunk_target_size => 'off'.
CREATE OR REPLACE FUNCTION @extschema@.set_adaptive_chunking(
    hypertable                     REGCLASS,
    chunk_target_size              TEXT,
    INOUT chunk_sizing_func        REGPROC = '_timescaledb_functions.calculate_chunk_interval'::regproc,
    OUT chunk_target_size          BIGINT
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_chunk_adaptive_set' LANGUAGE C VOLATILE;

-- Update chunk_time_interval for a hypertable [DEPRECATED].
--
-- hypertable - The OID of the table corresponding to a hypertable whose time
--     interval should be updated
-- chunk_time_interval - The new time interval. For hypertables with integral
--     time columns, this must be an integral type. For hypertables with a
--     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
--     microseconds, or an INTERVAL type.
CREATE OR REPLACE FUNCTION @extschema@.set_chunk_time_interval(
    hypertable              REGCLASS,
    chunk_time_interval     ANYELEMENT,
    dimension_name          NAME = NULL,
    chunk_time_origin       "any" = NULL::TIMESTAMPTZ,
    calendar_chunking       BOOL = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

-- Update partition_interval for a hypertable.
--
-- hypertable - The OID of the table corresponding to a hypertable whose
--     partition interval should be updated
-- partition_interval - The new interval. For hypertables with integral/serial/bigserial
--     time columns, this must be an integral type. For hypertables with a
--     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
--     microseconds, or an INTERVAL type.
CREATE OR REPLACE FUNCTION @extschema@.set_partitioning_interval(
    hypertable              REGCLASS,
    partition_interval      ANYELEMENT,
    dimension_name          NAME = NULL,
    partition_origin        "any" = NULL::TIMESTAMPTZ,
    calendar_chunking       BOOL = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.set_number_partitions(
    hypertable              REGCLASS,
    number_partitions       INTEGER,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_num_slices' LANGUAGE C VOLATILE;

-- Drop chunks older than the given timestamp for the specific
-- hypertable or continuous aggregate.
CREATE OR REPLACE FUNCTION @extschema@.drop_chunks(
    relation               REGCLASS,
    older_than             "any" = NULL,
    newer_than             "any" = NULL,
    verbose                BOOLEAN = FALSE,
    created_before         "any" = NULL,
    created_after          "any" = NULL
) RETURNS SETOF TEXT AS '@MODULE_PATHNAME@', 'ts_chunk_drop_chunks'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

-- show chunks older than or newer than a specific time.
-- `relation` must be a valid hypertable or continuous aggregate.
CREATE OR REPLACE FUNCTION @extschema@.show_chunks(
    relation               REGCLASS,
    older_than             "any" = NULL,
    newer_than             "any" = NULL,
    created_before         "any" = NULL,
    created_after          "any" = NULL
) RETURNS SETOF REGCLASS AS '@MODULE_PATHNAME@', 'ts_chunk_show_chunks'
LANGUAGE C STABLE PARALLEL SAFE;

-- Add a dimension (of partitioning) to a hypertable [DEPRECATED]
--
-- hypertable - OID of the table to add a dimension to
-- column_name - NAME of the column to use in partitioning for this dimension
-- number_partitions - Number of partitions, for non-time dimensions
-- chunk_time_interval - Size of intervals for time dimensions (can be integral or INTERVAL)
-- partitioning_func - Function used to partition the column
-- if_not_exists - If set, and the dimension already exists, generate a notice instead of an error
CREATE OR REPLACE FUNCTION @extschema@.add_dimension(
    hypertable              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    chunk_time_interval     ANYELEMENT = NULL::BIGINT,
    partitioning_func       REGPROC = NULL,
    if_not_exists           BOOLEAN = FALSE,
    chunk_time_origin       "any" = NULL::timestamptz
) RETURNS TABLE(dimension_id INT, schema_name NAME, table_name NAME, column_name NAME, created BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_add' LANGUAGE C VOLATILE;

-- Add a dimension (of partitioning) to a hypertable.
--
-- hypertable - OID of the table to add a dimension to
-- dimension - Dimension to add
-- if_not_exists - If set, and the dimension already exists, generate a notice instead of an error
CREATE OR REPLACE FUNCTION @extschema@.add_dimension(
    hypertable              REGCLASS,
    dimension               _timescaledb_internal.dimension_info,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, created BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_add_general' LANGUAGE C VOLATILE;

-- Enable tracking of statistics on a column of a hypertable.
--
-- hypertable - OID of the table to which the column belongs to
-- column_name - The column to track statistics for
-- if_not_exists - If set, and the entry already exists, generate a notice instead of an error
-- Returns the "id" of the entry created. The "enabled" field
-- is set to true if entry is created or exists already.
CREATE OR REPLACE FUNCTION @extschema@.enable_chunk_skipping(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(column_stats_id INT, enabled BOOL)
AS '@MODULE_PATHNAME@', 'ts_chunk_column_stats_enable' LANGUAGE C VOLATILE;

-- Disable tracking of statistics on a column of a hypertable.
--
-- hypertable - OID of the table to remove from
-- column_name - NAME of the column on which the stats are tracked
-- if_not_exists - If set, and the entry does not exist,
-- generate a notice instead of an error. The "disabled" field
-- is set to true if entry is deleted successfully.
CREATE OR REPLACE FUNCTION @extschema@.disable_chunk_skipping(
    hypertable              REGCLASS,
    column_name             NAME,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INT, column_name NAME, disabled BOOL)
AS '@MODULE_PATHNAME@', 'ts_chunk_column_stats_disable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.by_hash(column_name NAME, number_partitions INTEGER,
                                               partition_func regproc = NULL)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '@MODULE_PATHNAME@', 'ts_hash_dimension';

CREATE OR REPLACE FUNCTION @extschema@.by_range(column_name NAME,
                                                partition_interval ANYELEMENT = NULL::bigint,
                                                partition_func regproc = NULL,
                                                partition_origin "any" = NULL::TIMESTAMPTZ)
    RETURNS _timescaledb_internal.dimension_info LANGUAGE C
    AS '@MODULE_PATHNAME@', 'ts_range_dimension';

CREATE OR REPLACE FUNCTION @extschema@.attach_tablespace(
    tablespace NAME,
    hypertable REGCLASS,
    if_not_attached BOOLEAN = false
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_tablespace_attach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.detach_tablespace(
    tablespace NAME,
    hypertable REGCLASS = NULL,
    if_attached BOOLEAN = false
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_tablespace_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.detach_tablespaces(hypertable REGCLASS) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_tablespace_detach_all_from_hypertable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.show_tablespaces(hypertable REGCLASS) RETURNS SETOF NAME
AS '@MODULE_PATHNAME@', 'ts_tablespace_show' LANGUAGE C VOLATILE STRICT;

-- Refresh a continuous aggregate across the given window.
CREATE OR REPLACE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any",
    force                    BOOLEAN = FALSE,
    options                  JSONB = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_continuous_agg_refresh';

