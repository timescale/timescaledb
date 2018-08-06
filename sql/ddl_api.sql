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
CREATE OR REPLACE FUNCTION  create_hypertable(
    main_table              REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     anyelement = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE,
    partitioning_func       REGPROC = NULL,
    migrate_data            BOOLEAN = FALSE,
    chunk_target_size       TEXT = NULL,
    chunk_sizing_func       REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc
) RETURNS VOID AS '@MODULE_PATHNAME@', 'hypertable_create' LANGUAGE C VOLATILE;

-- Set adaptive chunking. To disable, set chunk_target_size => 'off'.
CREATE OR REPLACE FUNCTION  set_adaptive_chunking(
    hypertable                     REGCLASS,
    chunk_target_size              TEXT,
    INOUT chunk_sizing_func        REGPROC = '_timescaledb_internal.calculate_chunk_interval'::regproc,
    OUT chunk_target_size          BIGINT
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'chunk_adaptive_set' LANGUAGE C VOLATILE;

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
) RETURNS VOID AS '@MODULE_PATHNAME@', 'dimension_set_interval' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION  set_number_partitions(
    main_table              REGCLASS,
    number_partitions       INTEGER,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'dimension_set_num_slices' LANGUAGE C VOLATILE;

-- Drop chunks that are older than a timestamp.
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than anyelement,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    older_than_internal BIGINT;
BEGIN
    IF older_than IS NULL THEN
        RAISE 'the timestamp provided to drop_chunks cannot be NULL';
    END IF;

    PERFORM  _timescaledb_internal.drop_chunks_type_check(pg_typeof(older_than), table_name, schema_name);
    SELECT _timescaledb_internal.time_to_internal(older_than) INTO older_than_internal;
    PERFORM _timescaledb_internal.drop_chunks_impl(older_than_internal, table_name, schema_name, cascade);
END
$BODY$;

-- Drop chunks older than an interval.
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than  INTERVAL,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade BOOLEAN = false
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    time_type REGTYPE;
BEGIN

    BEGIN
        WITH hypertable_ids AS (
            SELECT id
            FROM _timescaledb_catalog.hypertable h
            WHERE (drop_chunks.schema_name IS NULL OR h.schema_name = drop_chunks.schema_name) AND
            (drop_chunks.table_name IS NULL OR h.table_name = drop_chunks.table_name)
        )
        SELECT DISTINCT time_dim.column_type INTO STRICT time_type
        FROM hypertable_ids INNER JOIN LATERAL _timescaledb_internal.dimension_get_time(hypertable_ids.id) time_dim ON (true);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'no hypertables found';
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'cannot use drop_chunks on multiple tables with different time types';
    END;


    IF time_type = 'TIMESTAMP'::regtype THEN
        PERFORM @extschema@.drop_chunks((now() - older_than)::timestamp, table_name, schema_name, cascade);
    ELSIF time_type = 'DATE'::regtype THEN
        PERFORM @extschema@.drop_chunks((now() - older_than)::date, table_name, schema_name, cascade);
    ELSIF time_type = 'TIMESTAMPTZ'::regtype THEN
        PERFORM @extschema@.drop_chunks(now() - older_than, table_name, schema_name, cascade);
    ELSE
        RAISE 'can only use drop_chunks with an INTERVAL for TIMESTAMP, TIMESTAMPTZ, and DATE types';
    END IF;
END
$BODY$;

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
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'dimension_add' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION attach_tablespace(
    tablespace NAME,
    hypertable REGCLASS,
    if_not_attached BOOLEAN = false
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'tablespace_attach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION detach_tablespace(
    tablespace NAME,
    hypertable REGCLASS = NULL,
    if_attached BOOLEAN = false
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'tablespace_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION detach_tablespaces(hypertable REGCLASS) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'tablespace_detach_all_from_hypertable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION show_tablespaces(hypertable REGCLASS) RETURNS SETOF NAME
AS '@MODULE_PATHNAME@', 'tablespace_show' LANGUAGE C VOLATILE STRICT;
