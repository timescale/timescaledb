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
-- create_default_indexes - (Optional) Whether or not to create the default indexes.
-- TODO: order of params doesn't match docs.
CREATE OR REPLACE FUNCTION  create_hypertable(
    main_table              REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     BIGINT =  _timescaledb_internal.interval_to_usec('1 month'),
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    hypertable_row   _timescaledb_catalog.hypertable;
    table_name       NAME;
    schema_name      NAME;
    tablespace_oid   OID;
    tablespace_name  NAME;
    time_column_type REGTYPE;
    partitioning_column_type REGTYPE;
    att_row          pg_attribute;
    main_table_has_items BOOLEAN;
    is_hypertable    BOOLEAN;
BEGIN
    SELECT relname, nspname, reltablespace
    INTO STRICT table_name, schema_name, tablespace_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    -- tables that don't have an associated tablespace has reltablespace OID set to 0
    -- in pg_class and there is no matching row in pg_tablespace
    SELECT spcname
    INTO tablespace_name
    FROM pg_tablespace t
    WHERE t.OID = tablespace_oid;

    BEGIN
        SELECT atttypid
        INTO STRICT time_column_type
        FROM pg_attribute
        WHERE attrelid = main_table AND attname = time_column_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'column "%" does not exist', time_column_name
            USING ERRCODE = 'IO102';
    END;

    IF time_column_type NOT IN ('BIGINT', 'INTEGER', 'SMALLINT', 'TIMESTAMP', 'TIMESTAMPTZ') THEN
        RAISE EXCEPTION 'illegal type for time column "%": %', time_column_name, time_column_type
        USING ERRCODE = 'IO102';
    END IF;

    IF partitioning_column IS NOT NULL THEN
        BEGIN
            SELECT atttypid
            INTO STRICT partitioning_column_type
            FROM pg_attribute
            WHERE attrelid = main_table AND attname = partitioning_column;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE EXCEPTION 'column "%" does not exist', partitioning_column
                USING ERRCODE = 'IO102';
        END;
    END IF;

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

    IF main_table_has_items THEN
        RAISE EXCEPTION 'the table being converted to a hypertable must be empty'
        USING ERRCODE = 'IO102';
    END IF;

    BEGIN
        SELECT *
        INTO hypertable_row
        FROM  _timescaledb_internal.create_hypertable_row(
            schema_name,
            table_name,
            time_column_name,
            time_column_type,
            partitioning_column,
            partitioning_column_type,
            number_partitions,
            associated_schema_name,
            associated_table_prefix,
            chunk_time_interval,
            tablespace_name
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

    PERFORM 1
    FROM pg_index,
    LATERAL _timescaledb_internal.add_index(
        hypertable_row.id,
        hypertable_row.schema_name,
        (SELECT relname FROM pg_class WHERE oid = indexrelid::regclass),
        _timescaledb_internal.get_general_index_definition(indexrelid, indrelid, hypertable_row)
    )
    WHERE indrelid = main_table;

    IF create_default_indexes THEN
        PERFORM _timescaledb_internal.create_default_indexes(hypertable_row, main_table, partitioning_column);
    END IF;
END
$BODY$;

-- Update chunk_time_interval for a hypertable
CREATE OR REPLACE FUNCTION  set_chunk_time_interval(
    main_table              REGCLASS,
    chunk_time_interval     BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    main_table_name       NAME;
    main_schema_name      NAME;
BEGIN
    SELECT relname, nspname
    INTO STRICT main_table_name, main_schema_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    UPDATE _timescaledb_catalog.hypertable h 
    SET chunk_time_interval = set_chunk_time_interval.chunk_time_interval
    WHERE h.schema_name = main_schema_name AND
          h.table_name = main_table_name;
END
$BODY$;

-- Restore the database after a pg_restore.
CREATE OR REPLACE FUNCTION restore_timescaledb()
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    SELECT _timescaledb_internal.setup_main(true);
$BODY$;

-- Drop chunks that are older than a timestamp.
-- TODO how does drop_chunks work with integer time tables?
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than TIMESTAMPTZ,
    table_name  NAME = NULL,
    schema_name NAME = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    older_than_internal BIGINT;
BEGIN
    SELECT (EXTRACT(epoch FROM older_than)*1e6)::BIGINT INTO older_than_internal;
    PERFORM _timescaledb_internal.drop_chunks_older_than(older_than_internal, table_name, schema_name);
END
$BODY$;

-- Drop chunks older than an interval.
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than  INTERVAL,
    table_name  NAME = NULL,
    schema_name NAME = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    older_than_ts TIMESTAMPTZ;
BEGIN
    older_than_ts := now() - older_than;
    PERFORM drop_chunks(older_than_ts, table_name, schema_name);
END
$BODY$;
