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
    SET search_path = ''
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
BEGIN
    -- Early abort if lacking permissions
    PERFORM _timescaledb_internal.check_role(main_table);

    SELECT relname, nspname, reltablespace, relkind = 'p'
    INTO STRICT table_name, schema_name, tablespace_oid, is_partitioned
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

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

    chunk_time_interval_actual := _timescaledb_internal.time_interval_specification_to_internal(
        time_type, chunk_time_interval, INTERVAL '1 month', 'chunk_time_interval');

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

CREATE OR REPLACE FUNCTION  add_dimension(
    main_table              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    interval_length         BIGINT = NULL,
    partitioning_func       REGPROC = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
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
END
$BODY$;

-- Update chunk_time_interval for a hypertable
CREATE OR REPLACE FUNCTION  set_chunk_time_interval(
    main_table              REGCLASS,
    chunk_time_interval     BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path=''
    AS
$BODY$
DECLARE
    main_table_name       NAME;
    main_schema_name      NAME;
BEGIN
    PERFORM _timescaledb_internal.check_role(main_table);

    SELECT relname, nspname
    INTO STRICT main_table_name, main_schema_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    UPDATE _timescaledb_catalog.dimension d
    SET interval_length = set_chunk_time_interval.chunk_time_interval
    FROM _timescaledb_internal.dimension_get_time(
        (
            SELECT id
            FROM _timescaledb_catalog.hypertable h
            WHERE h.schema_name = main_schema_name AND
            h.table_name = main_table_name
    )) time_dim
    WHERE time_dim.id = d.id;
END
$BODY$;

CREATE OR REPLACE FUNCTION drop_chunks(
    older_than  BIGINT,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade    BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    IF older_than IS NULL THEN
        RAISE 'The time provided to drop_chunks cannot be null';
    END IF;
    PERFORM _timescaledb_internal.drop_chunks_impl(older_than, table_name, schema_name, cascade);
END
$BODY$;

-- Drop chunks that are older than a timestamp.
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than TIMESTAMPTZ,
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

    SELECT (EXTRACT(epoch FROM older_than)*1e6)::BIGINT INTO older_than_internal;
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
    older_than_ts TIMESTAMPTZ;
BEGIN
    older_than_ts := now() - older_than;
    PERFORM drop_chunks(older_than_ts, table_name, schema_name, cascade);
END
$BODY$;

CREATE OR REPLACE FUNCTION attach_tablespace(
       hypertable REGCLASS,
       tablespace NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    hypertable_id     INTEGER;
    tablespace_oid    OID;
BEGIN
    PERFORM _timescaledb_internal.check_role(hypertable);

    SELECT id
    FROM _timescaledb_catalog.hypertable h, pg_class c, pg_namespace n
    WHERE h.schema_name = n.nspname
    AND h.table_name = c.relname
    AND c.oid = hypertable
    AND n.oid = c.relnamespace
    INTO hypertable_id;

    IF hypertable_id IS NULL THEN
       RAISE EXCEPTION 'No hypertable "%" exists', main_table_name
       USING ERRCODE = 'IO101';
    END IF;

    PERFORM _timescaledb_internal.attach_tablespace(hypertable_id, tablespace);
END
$BODY$;
