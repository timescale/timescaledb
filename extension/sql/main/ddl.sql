-- This file defines DDL functions for adding and manipulating hypertables.

-- Converts a regular postgres table to a hypertable.
--
-- main_table - The OID of the table to be converted
-- time_column_name - Name of the column that contains time for a given record
-- partitioning_column - Name of the column to partition data by
-- replication_factor -- (Optional) Number of replicas for data
-- number_partitions - (Optional) Number of partitions for data
-- associated_schema_name - (Optional) Schema for internal hypertable tables
-- associated_table_prefix - (Optional) Prefix for internal hypertable table names
CREATE OR REPLACE FUNCTION  create_hypertable(
    main_table              REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME,
    replication_factor      SMALLINT = 1,
    number_partitions       SMALLINT = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    placement               chunk_placement_type = 'STICKY',
    chunk_size_bytes        BIGINT = 1073741824 -- 1 GB
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    hypertable_row   _iobeamdb_catalog.hypertable;
    table_name       NAME;
    schema_name      NAME;
    time_column_type REGTYPE;
    att_row          pg_attribute;
    main_table_has_items BOOLEAN;
BEGIN
    SELECT relname, nspname
    INTO STRICT table_name, schema_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

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

    PERFORM atttypid
    FROM pg_attribute
    WHERE attrelid = main_table AND attname = partitioning_column;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'column "%" does not exist', partitioning_column
        USING ERRCODE = 'IO102';
    END IF;

    EXECUTE format('SELECT TRUE FROM %s LIMIT 1', main_table) INTO main_table_has_items;

    IF main_table_has_items THEN
        RAISE EXCEPTION 'the table being converted to a hypertable must be empty'
        USING ERRCODE = 'IO102';
    END IF;

    BEGIN
        SELECT *
        INTO hypertable_row
        FROM  _iobeamdb_meta_api.create_hypertable(
            schema_name,
            table_name,
            time_column_name,
            time_column_type,
            partitioning_column,
            replication_factor,
            number_partitions,
            associated_schema_name,
            associated_table_prefix,
            placement,
            chunk_size_bytes
        );
    EXCEPTION
        WHEN unique_violation THEN
            RAISE EXCEPTION 'hypertable % already exists', main_table
            USING ERRCODE = 'IO110';
    END;

    FOR att_row IN SELECT *
    FROM pg_attribute att
    WHERE attrelid = main_table AND attnum > 0 AND NOT attisdropped
        LOOP
            PERFORM  _iobeamdb_internal.create_column_from_attribute(hypertable_row.id, att_row);
        END LOOP;


    PERFORM 1
    FROM pg_index,
    LATERAL _iobeamdb_meta_api.add_index(
        hypertable_row.id,
        hypertable_row.schema_name,
        (SELECT relname FROM pg_class WHERE oid = indexrelid::regclass),
        _iobeamdb_internal.get_general_index_definition(indexrelid, indrelid)
    )
    WHERE indrelid = main_table;
END
$BODY$;

-- Sets the is_distinct flag for column on a hypertable.
-- The is_distinct flag determines whether the system keep a materialized list
-- of distinct values for the column.
CREATE OR REPLACE FUNCTION set_is_distinct_flag(
    main_table    REGCLASS,
    column_name    NAME,
    is_distinct   BOOLEAN

)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    t_name         NAME;
    s_name         NAME;
    hypertable_row _iobeamdb_catalog.hypertable;
BEGIN
    SELECT relname, nspname
    INTO STRICT t_name, s_name
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    SELECT * INTO hypertable_row
    FROM _iobeamdb_catalog.hypertable h
    WHERE h.schema_name = s_name AND h.table_name = t_name;

    PERFORM atttypid
    FROM pg_attribute
    WHERE attrelid = main_table AND attname = column_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'column "%" does not exist', column_name
        USING ERRCODE = 'IO100';
    END IF;


    PERFORM _iobeamdb_internal.meta_transaction_exec(
        format('SELECT _iobeamdb_meta.alter_column_set_is_distinct(%L, %L, %L, %L)',
            hypertable_row.id,
            column_name,
            is_distinct,
            current_database()
        )
    );
END
$BODY$;
