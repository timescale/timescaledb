-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file defines DDL functions for adding and manipulating hypertables.

-- Converts a regular postgres table to a hypertable.
--
-- main_table - The OID of the table to be converted
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
-- replication_factor - (Optional) A value of 1 or greater makes this hypertable distributed
-- data_nodes - (Optional) The specific data nodes to distribute this hypertable across
CREATE OR REPLACE FUNCTION  create_hypertable(
    main_table              REGCLASS,
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
    chunk_sizing_func       REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
    time_partitioning_func  REGPROC = NULL,
    replication_factor      INTEGER = NULL,
    data_nodes              NAME[] = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_create' LANGUAGE C VOLATILE;

-- Same functionality as create_hypertable, only must have a replication factor > 0 (defaults to 1)
CREATE OR REPLACE FUNCTION  create_distributed_hypertable(
    main_table              REGCLASS,
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
    chunk_sizing_func       REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
    time_partitioning_func  REGPROC = NULL,
    replication_factor      INTEGER = 1,
    data_nodes              NAME[] = NULL
) RETURNS TABLE(hypertable_id INT, schema_name NAME, table_name NAME, created BOOL) AS '@MODULE_PATHNAME@', 'ts_hypertable_distributed_create' LANGUAGE C VOLATILE;

-- Set adaptive chunking. To disable, set chunk_target_size => 'off'.
CREATE OR REPLACE FUNCTION  set_adaptive_chunking(
    hypertable                     REGCLASS,
    chunk_target_size              TEXT,
    INOUT chunk_sizing_func        REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
    OUT chunk_target_size          BIGINT
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_chunk_adaptive_set' LANGUAGE C VOLATILE;

-- Update chunk_time_interval for a hypertable.
--
-- main_table - The OID of the table corresponding to a hypertable whose time
--     interval should be updated
-- chunk_time_interval - The new time interval. For hypertables with integral
--     time columns, this must be an integral type. For hypertables with a
--     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
--     microseconds, or an INTERVAL type.
CREATE OR REPLACE FUNCTION  set_chunk_time_interval(
    main_table              REGCLASS,
    chunk_time_interval     ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_interval' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION  set_number_partitions(
    main_table              REGCLASS,
    number_partitions       INTEGER,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_dimension_set_num_slices' LANGUAGE C VOLATILE;

-- Drop chunks older than the given timestamp. If a hypertable name is given,
-- drop only chunks associated with this table. Any of the first three arguments
-- can be NULL meaning "all values".
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than "any" = NULL,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE,
    newer_than "any" = NULL,
    verbose BOOLEAN = FALSE,
    cascade_to_materializations BOOLEAN = NULL
) RETURNS SETOF TEXT AS '@MODULE_PATHNAME@', 'ts_chunk_drop_chunks'
LANGUAGE C VOLATILE PARALLEL UNSAFE;

-- show chunks older than or newer than a specific time.
-- `hypertable` argument can be a valid hypertable or NULL.
-- In the latter case the function will try to list all
-- the chunks from all of the hypertables in the database.
-- older_than or newer_than or both can be NULL.
-- if `hypertable` argument is null but a time constraint is specified
-- through older_than or newer_than, the call will succeed
-- if and only if all the hypertables in the database
-- have the same type as the given time constraint argument
CREATE OR REPLACE FUNCTION show_chunks(
    hypertable  REGCLASS = NULL,
    older_than "any" = NULL,
    newer_than "any" = NULL
) RETURNS SETOF REGCLASS AS '@MODULE_PATHNAME@', 'ts_chunk_show_chunks'
LANGUAGE C STABLE PARALLEL SAFE;

-- Add a dimension (of partitioning) to a hypertable
--
-- main_table - OID of the table to add a dimension to
-- column_name - NAME of the column to use in partitioning for this dimension
-- number_partitions - Number of partitions, for non-time dimensions
-- interval_length - Size of intervals for time dimensions (can be integral or INTERVAL)
-- partitioning_func - Function used to partition the column
-- if_not_exists - If set, and the dimension already exists, generate a notice instead of an error
CREATE OR REPLACE FUNCTION  add_dimension(
    main_table              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    chunk_time_interval     ANYELEMENT = NULL::BIGINT,
    partitioning_func       REGPROC = NULL,
    if_not_exists           BOOLEAN = FALSE
) RETURNS TABLE(dimension_id INT, schema_name NAME, table_name NAME, column_name NAME, created BOOL)
AS '@MODULE_PATHNAME@', 'ts_dimension_add' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION attach_tablespace(
    tablespace NAME,
    hypertable REGCLASS,
    if_not_attached BOOLEAN = false
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_tablespace_attach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION detach_tablespace(
    tablespace NAME,
    hypertable REGCLASS = NULL,
    if_attached BOOLEAN = false
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_tablespace_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION detach_tablespaces(hypertable REGCLASS) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_tablespace_detach_all_from_hypertable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION show_tablespaces(hypertable REGCLASS) RETURNS SETOF NAME
AS '@MODULE_PATHNAME@', 'ts_tablespace_show' LANGUAGE C VOLATILE STRICT;

-- Add a data node to a TimescaleDB distributed database. This also add a
-- corresponding user mapping, if one does not already exist.
CREATE OR REPLACE FUNCTION add_data_node(
    node_name              NAME,
    host                   TEXT = 'localhost',
    database               NAME = current_database(),
    port                   INTEGER = inet_server_port(),
    password               TEXT = NULL,
    if_not_exists          BOOLEAN = FALSE,
    bootstrap_database     NAME = 'postgres',
    bootstrap_user         NAME = NULL,
    bootstrap_password     TEXT = NULL
) RETURNS TABLE(node_name NAME, host TEXT, port INTEGER, database NAME,
                node_created BOOL, database_created BOOL, extension_created BOOL)
AS '@MODULE_PATHNAME@', 'ts_data_node_add' LANGUAGE C VOLATILE;

-- Delete a data node from the distributed database
CREATE OR REPLACE FUNCTION delete_data_node(
    node_name            NAME,
    if_exists              BOOLEAN = FALSE,
    cascade                BOOLEAN = FALSE,
    force                  BOOLEAN = FALSE
) RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'ts_data_node_delete' LANGUAGE C VOLATILE;

-- Attach a data node to a distributed hypertable
CREATE OR REPLACE FUNCTION attach_data_node(
    hypertable             REGCLASS,
    node_name            NAME,
    if_not_attached        BOOLEAN = FALSE
) RETURNS TABLE(hypertable_id INTEGER, node_hypertable_id INTEGER, node_name NAME)
AS '@MODULE_PATHNAME@', 'ts_data_node_attach' LANGUAGE C VOLATILE;

-- Detach a data node from a distributed hypertable. NULL hypertable means it will detach from all distributed hypertables
CREATE OR REPLACE FUNCTION detach_data_node(node_name NAME, hypertable REGCLASS = NULL, force BOOLEAN = FALSE) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_detach' LANGUAGE C VOLATILE;

-- Block new chunk creation on a data node for a distributed hypertable. NULL hypertable means it will block
-- chunks for all distributed hypertables
CREATE OR REPLACE FUNCTION block_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL, force BOOLEAN = FALSE) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_block_new_chunks' LANGUAGE C VOLATILE;

-- Reallow chunk creations on a blocked data node for a distributed hypertable. NULL hypertable means it will
-- allow chunks for all distributed hypertables
CREATE OR REPLACE FUNCTION allow_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_allow_new_chunks' LANGUAGE C VOLATILE;
