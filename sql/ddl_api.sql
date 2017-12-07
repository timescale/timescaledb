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
    partitioning_func       REGPROC = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SET search_path = '_timescaledb_catalog'
    AS
$BODY$
<<vars>>
DECLARE
    hypertable_row   _timescaledb_catalog.hypertable;
    table_name                 NAME;
    schema_name                NAME;
    tablespace_oid             OID;
    tablespace_name            NAME;
    main_table_has_items       BOOLEAN;
    is_hypertable              BOOLEAN;
    is_partitioned             BOOLEAN;
    chunk_time_interval_actual BIGINT;
    time_type                  REGTYPE;
    relowner                   OID;
BEGIN
    -- Early abort if lacking permissions
    PERFORM _timescaledb_internal.check_role(main_table);

    SELECT c.relname, n.nspname, c.reltablespace, c.relkind = 'p', c.relowner
    INTO STRICT table_name, schema_name, tablespace_oid, is_partitioned, relowner
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    -- Check that the user has permissions to create chunks in the
    -- associated schema
    PERFORM _timescaledb_internal.check_associated_schema_permissions(associated_schema_name, relowner);

    IF is_partitioned THEN
        RAISE EXCEPTION 'table % is already partitioned', main_table
        USING ERRCODE = 'IO110';
    END IF;

    -- tables that don't have an associated tablespace has reltablespace OID set to 0
    -- in pg_class and there is no matching row in pg_tablespace
    SELECT spcname
    INTO tablespace_name
    FROM pg_tablespace t
    WHERE t.OID = tablespace_oid;

    EXECUTE format('SELECT TRUE FROM _timescaledb_catalog.hypertable WHERE
                    hypertable.schema_name = %L AND
                    hypertable.table_name = %L',
                    schema_name, table_name) INTO is_hypertable;

    IF is_hypertable THEN
       IF if_not_exists THEN
          RAISE NOTICE 'hypertable % already exists, skipping', main_table;
              RETURN;
        ELSE
              RAISE EXCEPTION 'hypertable % already exists', main_table
              USING ERRCODE = 'IO110';
          END IF;
    END IF;

    EXECUTE format('SELECT TRUE FROM %s LIMIT 1', main_table) INTO main_table_has_items;

    PERFORM _timescaledb_internal.set_time_column_constraint(main_table, time_column_name);

    IF main_table_has_items THEN
        RAISE EXCEPTION 'the table being converted to a hypertable must be empty'
        USING ERRCODE = 'IO102';
    END IF;

    -- Validate that the hypertable supports the triggers in the main table
    PERFORM _timescaledb_internal.validate_triggers(main_table);

    time_type := _timescaledb_internal.dimension_type(main_table, time_column_name, true);

    chunk_time_interval_actual := _timescaledb_internal.time_interval_specification_to_internal_with_default_time(
        time_type, chunk_time_interval, 'chunk_time_interval');

    BEGIN
        SELECT *
        INTO hypertable_row
        FROM  _timescaledb_internal.create_hypertable(
            main_table,
            schema_name,
            table_name,
            time_column_name,
            partitioning_column,
            number_partitions,
            associated_schema_name,
            associated_table_prefix,
            chunk_time_interval_actual,
            tablespace_name,
            create_default_indexes,
            partitioning_func
        );
    EXCEPTION
        WHEN unique_violation THEN
            IF if_not_exists THEN
               RAISE NOTICE 'hypertable % already exists, skipping', main_table;
               RETURN;
            ELSE
               RAISE EXCEPTION 'hypertable % already exists', main_table
               USING ERRCODE = 'IO110';
            END IF;
        WHEN foreign_key_violation THEN
            RAISE EXCEPTION 'database not configured for hypertable storage (not setup as a data-node)'
            USING ERRCODE = 'IO101';
    END;
END
$BODY$;

-- Add a dimension (of partitioning) to a hypertable
--
-- main_table - OID of the table to add a dimension to
-- column_name - NAME of the column to use in partitioning for this dimension
-- number_partitions - Number of partitions, for non-time dimensions
-- interval_length - Size of intervals for time dimensions (can be integral or INTERVAL)
-- partitioning_func - Function used to partition the column
CREATE OR REPLACE FUNCTION  add_dimension(
    main_table              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    interval_length         anyelement = NULL::BIGINT,
    partitioning_func       REGPROC = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = '_timescaledb_catalog'
    AS
$BODY$
<<main_block>>
DECLARE
    table_name       NAME;
    schema_name      NAME;
    hypertable_row   _timescaledb_catalog.hypertable;
BEGIN
    PERFORM _timescaledb_internal.check_role(main_table);

    SELECT relname, nspname
    INTO STRICT table_name, schema_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    SELECT *
    INTO STRICT hypertable_row
    FROM _timescaledb_catalog.hypertable h
    WHERE h.schema_name = main_block.schema_name
    AND h.table_name = main_block.table_name
    FOR UPDATE;

    PERFORM _timescaledb_internal.add_dimension(main_table,
                                                hypertable_row,
                                                column_name,
                                                number_partitions,
                                                interval_length,
                                                partitioning_func);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION '"%" is not a hypertable; cannot add dimension', main_block.table_name
        USING ERRCODE = 'IO102';
END
$BODY$;

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
    chunk_time_interval     anyelement
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path='_timescaledb_catalog'
    AS
$BODY$
DECLARE
    main_table_name            NAME;
    main_schema_name           NAME;
    chunk_time_interval_actual BIGINT;
    time_dimension             _timescaledb_catalog.dimension;
    time_type                  REGTYPE;
BEGIN
    PERFORM _timescaledb_internal.check_role(main_table);
    IF chunk_time_interval IS NULL THEN
        RAISE EXCEPTION 'chunk_time_interval cannot be NULL'
        USING ERRCODE = 'IO102';
    END IF;

    SELECT relname, nspname
    INTO STRICT main_table_name, main_schema_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    SELECT *
    INTO STRICT time_dimension
    FROM _timescaledb_internal.dimension_get_time(
        (
            SELECT id
            FROM _timescaledb_catalog.hypertable h
            WHERE h.schema_name = main_schema_name AND
            h.table_name = main_table_name
    ));

    time_type := _timescaledb_internal.dimension_type(main_table, time_dimension.column_name, true);
    chunk_time_interval_actual := _timescaledb_internal.time_interval_specification_to_internal_with_default_time(
        time_type, chunk_time_interval, 'chunk_time_interval');

    UPDATE _timescaledb_catalog.dimension d
    SET interval_length = chunk_time_interval_actual
    WHERE time_dimension.id = d.id;
END
$BODY$;

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
        RAISE 'The timestamp provided to drop_chunks cannot be null';
    END IF;

    PERFORM  _timescaledb_internal.drop_chunks_type_check(pg_typeof(older_than), table_name, schema_name);
    SELECT _timescaledb_internal.time_to_internal(older_than, pg_typeof(older_than)) INTO older_than_internal;
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
            RAISE EXCEPTION 'No hypertables found';
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Cannot use drop_chunks on multiple tables with different time types';
    END;


    IF time_type = 'TIMESTAMP'::regtype THEN
        PERFORM drop_chunks((now() - older_than)::timestamp, table_name, schema_name, cascade);
    ELSIF time_type = 'DATE'::regtype THEN
        PERFORM drop_chunks((now() - older_than)::date, table_name, schema_name, cascade);
    ELSIF time_type = 'TIMESTAMPTZ'::regtype THEN
        PERFORM drop_chunks(now() - older_than, table_name, schema_name, cascade);
    ELSE
        RAISE 'Can only use drop_chunks with an INTERVAL for TIMESTAMP, TIMESTAMPTZ, and DATE types';
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION attach_tablespace(tablespace NAME, hypertable REGCLASS)
       RETURNS VOID LANGUAGE SQL AS
$BODY$
    SELECT * FROM _timescaledb_internal.attach_tablespace(tablespace, hypertable);
$BODY$;

-- Detach the given tablespace from a hypertable
CREATE OR REPLACE FUNCTION detach_tablespace(tablespace NAME, hypertable REGCLASS = NULL)
       RETURNS INTEGER LANGUAGE SQL AS
$BODY$
    SELECT * FROM _timescaledb_internal.detach_tablespace(tablespace, hypertable);
$BODY$;

-- Detach all tablespaces from the a hypertable
CREATE OR REPLACE FUNCTION detach_tablespaces(hypertable REGCLASS)
       RETURNS INTEGER LANGUAGE SQL AS
$BODY$
    SELECT * FROM _timescaledb_internal.detach_tablespaces(hypertable);
$BODY$;

CREATE OR REPLACE FUNCTION show_tablespaces(hypertable REGCLASS)
       RETURNS SETOF NAME LANGUAGE SQL AS
$BODY$
    SELECT * FROM _timescaledb_internal.show_tablespaces(hypertable);
$BODY$;
