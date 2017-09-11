-- Creates a hypertable row.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_hypertable_row(
    main_table               REGCLASS,
    schema_name              NAME,
    table_name               NAME,
    time_column_name         NAME,
    partitioning_column      NAME,
    number_partitions        INTEGER,
    associated_schema_name   NAME,
    associated_table_prefix  NAME,
    chunk_time_interval      BIGINT,
    tablespace               NAME
)
    RETURNS _timescaledb_catalog.hypertable LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    id                       INTEGER;
    hypertable_row           _timescaledb_catalog.hypertable;
BEGIN
    id :=  nextval(pg_get_serial_sequence('_timescaledb_catalog.hypertable','id'));

    IF associated_schema_name IS NULL THEN
        associated_schema_name = '_timescaledb_internal';
    END IF;

    IF associated_table_prefix IS NULL THEN
        associated_table_prefix = format('_hyper_%s', id);
    END IF;

    IF partitioning_column IS NULL THEN
        IF number_partitions IS NULL THEN
            number_partitions := 1;
        ELSIF number_partitions <> 1 THEN
            RAISE EXCEPTION 'The number of partitions must be 1 without a partitioning column'
            USING ERRCODE ='IO101';
        END IF;
    ELSIF number_partitions IS NULL THEN
        RAISE EXCEPTION 'The number of partitions must be specified when there is a partitioning column'
        USING ERRCODE ='IO101';
    END IF;

    INSERT INTO _timescaledb_catalog.hypertable (
        id, schema_name, table_name,
        associated_schema_name, associated_table_prefix, num_dimensions)
    VALUES (
        id, schema_name, table_name,
        associated_schema_name, associated_table_prefix, 1
    )
    RETURNING * INTO hypertable_row;

    --add default tablespace, if any
    IF tablespace IS NOT NULL THEN
       PERFORM _timescaledb_internal.attach_tablespace(hypertable_row.id, tablespace);
    END IF;

    --create time dimension
    PERFORM _timescaledb_internal.add_dimension(main_table,
                                                hypertable_row,
                                                time_column_name,
                                                NULL,
                                                chunk_time_interval,
                                                FALSE);

    IF partitioning_column IS NOT NULL THEN
        --create space dimension
        PERFORM _timescaledb_internal.add_dimension(main_table,
                                                    hypertable_row,
                                                    partitioning_column,
                                                    number_partitions,
                                                    NULL);
    END IF;

    PERFORM _timescaledb_internal.verify_hypertable_indexes(main_table);

    RETURN hypertable_row;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_type(
    main_table REGCLASS,
    column_name NAME,
    is_open BOOLEAN
)
 RETURNS REGTYPE LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    column_type              REGTYPE;
BEGIN
    BEGIN
        SELECT atttypid
        INTO STRICT column_type
        FROM pg_attribute
        WHERE attrelid = main_table AND attname = column_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'column "%" does not exist', column_name
            USING ERRCODE = 'IO102';
    END;

    IF is_open THEN
        -- Open dimension
        IF column_type NOT IN ('BIGINT', 'INTEGER', 'SMALLINT', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ') THEN
            RAISE EXCEPTION 'illegal type for column "%": %', column_name, column_type
            USING ERRCODE = 'IO102';
        END IF;
    END IF;
    RETURN column_type;
END
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.add_dimension(
    main_table               REGCLASS,
    hypertable_row           _timescaledb_catalog.hypertable, -- should be locked FOR UPDATE
    column_name              NAME,
    num_slices               INTEGER = NULL,
    interval_length          BIGINT = NULL,
    increment_num_dimensions BOOLEAN = TRUE
)
    RETURNS _timescaledb_catalog.dimension LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    partitioning_func        _timescaledb_catalog.dimension.partitioning_func%TYPE = 'get_partition_for_key';
    partitioning_func_schema _timescaledb_catalog.dimension.partitioning_func_schema%TYPE = '_timescaledb_internal';
    aligned                  BOOL;
    column_type              REGTYPE;
    dimension_row            _timescaledb_catalog.dimension;
    table_has_items          BOOLEAN;
BEGIN
    IF num_slices IS NULL AND interval_length IS NULL THEN
        RAISE EXCEPTION 'The number of slices/partitions or an interval must be specified'
        USING ERRCODE = 'IO101';
    ELSIF num_slices IS NOT NULL AND interval_length IS NOT NULL THEN
        RAISE EXCEPTION 'Cannot specify both interval and number of slices/partitions for a single dimension'
        USING ERRCODE = 'IO101';
    END IF;
    EXECUTE format('SELECT TRUE FROM %s LIMIT 1', main_table) INTO table_has_items;

    IF table_has_items THEN
        RAISE EXCEPTION 'Cannot add new dimension to a non-empty table'
        USING ERRCODE = 'IO102';
    END IF;

    column_type = _timescaledb_internal.dimension_type(main_table, column_name, num_slices IS NULL);

    IF column_type = 'DATE'::regtype AND interval_length IS NOT NULL AND
        (interval_length <= 0 OR interval_length % _timescaledb_internal.interval_to_usec('1 day') != 0)
        THEN
        RAISE EXCEPTION 'The interval for a hypertable with a DATE time column must be at least one day and given in multiples of days'
        USING ERRCODE = 'IO102';
    END IF;

    IF num_slices IS NULL THEN
        partitioning_func := NULL;
        partitioning_func_schema := NULL;
        aligned = TRUE;
    ELSE
        -- Closed dimension
        IF (num_slices < 1 OR num_slices > 32767) THEN
            RAISE EXCEPTION 'Invalid number of partitions'
            USING ERRCODE ='IO101';
        END IF;
        aligned = FALSE;
    END IF;

    BEGIN
        INSERT INTO _timescaledb_catalog.dimension(
            hypertable_id, column_name, column_type, aligned,
            num_slices, partitioning_func_schema, partitioning_func,
            interval_length
        ) VALUES (
            hypertable_row.id, column_name, column_type, aligned,
            num_slices::smallint, partitioning_func_schema, partitioning_func,
            interval_length
        ) RETURNING * INTO dimension_row;
    EXCEPTION
        WHEN unique_violation THEN
            RAISE EXCEPTION 'A dimension on column "%" already exists', column_name
            USING ERRCODE = 'IO101';
    END;

    IF increment_num_dimensions THEN
        UPDATE _timescaledb_catalog.hypertable
        SET num_dimensions = hypertable_row.num_dimensions + 1
        WHERE id = hypertable_row.id;
    END IF;

    RETURN dimension_row;
END
$BODY$;

-- Drops a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_hypertable(
    hypertable_id INT,
    is_cascade BOOLEAN
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM _timescaledb_internal.drop_chunk(c.id, is_cascade)
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.chunk c ON (c.hypertable_id = h.id)
    WHERE h.id = drop_hypertable.hypertable_id;

    DELETE FROM _timescaledb_catalog.hypertable h
    WHERE h.id = drop_hypertable.hypertable_id;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_get_time(
    hypertable_id INT
)
    RETURNS _timescaledb_catalog.dimension LANGUAGE SQL STABLE AS
$BODY$
    SELECT *
    FROM _timescaledb_catalog.dimension d
    WHERE d.hypertable_id = dimension_get_time.hypertable_id AND
          d.interval_length IS NOT NULL
$BODY$;

-- Drop chunks older than the given timestamp. If a hypertable name is given,
-- drop only chunks associated with this table.
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunks_older_than(
    older_than_time  BIGINT,
    table_name  NAME = NULL,
    schema_name NAME = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM _timescaledb_internal.drop_chunk(c.id, false)
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = c.hypertable_id)
    INNER JOIN  _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
    INNER JOIN _timescaledb_catalog.dimension_slice ds
        ON (ds.dimension_id =  time_dimension.id)
    INNER JOIN _timescaledb_catalog.chunk_constraint cc
        ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
    WHERE ds.range_end <= older_than_time
    AND (drop_chunks_older_than.schema_name IS NULL OR h.schema_name = drop_chunks_older_than.schema_name)
    AND (drop_chunks_older_than.table_name IS NULL OR h.table_name = drop_chunks_older_than.table_name);
END
$BODY$;

-- Creates the default indexes on a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_default_indexes(
    hypertable_row _timescaledb_catalog.hypertable,
    main_table REGCLASS,
    partitioning_column NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    index_count INTEGER;
    time_dimension_row _timescaledb_catalog.dimension;
    tablespace_name NAME;
    tablespace_sql  TEXT;
BEGIN
    SELECT * INTO STRICT time_dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE hypertable_id = hypertable_row.id AND partitioning_func IS NULL;

    SELECT t.spcname INTO tablespace_name
    FROM pg_class c, pg_tablespace t
    WHERE c.oid = main_table
    AND t.oid = c.reltablespace;

    IF tablespace_name IS NOT NULL THEN
        tablespace_sql = format('TABLESPACE %s', tablespace_name);
    END IF;

    SELECT count(*) INTO index_count
    FROM pg_index
    WHERE indkey = (
        SELECT attnum::text::int2vector
        FROM pg_attribute WHERE attrelid = main_table AND attname=time_dimension_row.column_name
    ) AND indrelid = main_table;

    IF index_count = 0 THEN
        EXECUTE format($$ CREATE INDEX ON %I.%I(%I DESC) %s $$,
            hypertable_row.schema_name, hypertable_row.table_name, time_dimension_row.column_name, tablespace_sql);
    END IF;

    IF partitioning_column IS NOT NULL THEN
        SELECT count(*) INTO index_count
        FROM pg_index
        WHERE indkey = (
            SELECT array_to_string(ARRAY(
                SELECT attnum::text
                FROM pg_attribute WHERE attrelid = main_table AND attname=partitioning_column
                UNION ALL
                SELECT attnum::text
                FROM pg_attribute WHERE attrelid = main_table AND attname=time_dimension_row.column_name
            ), ' ')::int2vector
        ) AND indrelid = main_table;

        IF index_count = 0 THEN
            EXECUTE format($$ CREATE INDEX ON %I.%I(%I, %I DESC) %s $$,
            hypertable_row.schema_name, hypertable_row.table_name, partitioning_column, time_dimension_row.column_name, tablespace_sql);
        END IF;
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.rename_hypertable(
    old_schema     NAME,
    old_table_name NAME,
    new_schema     NAME,
    new_table_name NAME
)
    RETURNS VOID
    LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    SELECT * INTO STRICT hypertable_row
    FROM _timescaledb_catalog.hypertable
    WHERE schema_name = old_schema AND table_name = old_table_name;

    UPDATE _timescaledb_catalog.hypertable SET
            schema_name = new_schema,
            table_name = new_table_name
            WHERE
            schema_name = old_schema AND
            table_name = old_table_name;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.rename_column(
    hypertable_id INT,
    old_name NAME,
    new_name NAME
)
    RETURNS VOID
    LANGUAGE SQL VOLATILE
    AS
$BODY$
    UPDATE _timescaledb_catalog.dimension d
    SET column_name = new_name
    WHERE d.column_name = old_name AND d.hypertable_id = rename_column.hypertable_id;

    SELECT ''::void; --don't return NULL
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.change_column_type(
    hypertable_id INT,
    column_name NAME,
    new_type REGTYPE
)
    RETURNS VOID
    LANGUAGE PLPGSQL VOLATILE
    AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    dimension_row  _timescaledb_catalog.dimension;
BEGIN
    UPDATE _timescaledb_catalog.dimension d
    SET column_type = new_type
    WHERE d.column_name = change_column_type.column_name AND d.hypertable_id = change_column_type.hypertable_id
    RETURNING * INTO dimension_row;

    IF FOUND THEN
        PERFORM _timescaledb_internal.chunk_constraint_drop_table_constraint(cc)
        FROM _timescaledb_catalog.dimension d
        INNER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = d.id)
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id)
        WHERE d.id = dimension_row.id;

        PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(cc)
        FROM _timescaledb_catalog.dimension d
        INNER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.dimension_id = d.id)
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = ds.id)
        WHERE d.id = dimension_row.id;
    END IF;
END
$BODY$;



CREATE OR REPLACE FUNCTION _timescaledb_internal.truncate_hypertable(
    schema_name     NAME,
    table_name      NAME
)
    RETURNS VOID
    LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN

    SELECT * INTO STRICT hypertable_row
    FROM _timescaledb_catalog.hypertable ht
    WHERE ht.schema_name = truncate_hypertable.schema_name
    AND ht.table_name = truncate_hypertable.table_name;

    PERFORM  _timescaledb_internal.drop_chunk(c.id, FALSE)
    FROM _timescaledb_catalog.chunk c
    WHERE hypertable_id = hypertable_row.id;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.verify_hypertable_indexes(hypertable REGCLASS) RETURNS VOID
AS '$libdir/timescaledb', 'indexing_verify_hypertable_indexes' LANGUAGE C IMMUTABLE STRICT;
