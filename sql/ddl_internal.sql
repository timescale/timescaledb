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

    RETURN hypertable_row;
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
    default_interval         BIGINT = _timescaledb_internal.interval_to_usec('1 month');
    aligned BOOL;
    column_type              REGTYPE;
    dimension_row            _timescaledb_catalog.dimension;
    table_has_items          BOOLEAN;
BEGIN
    IF num_slices IS NULL AND interval_length IS NULL THEN
        RAISE EXCEPTION 'The number of slices/partitions or an interval must be specified'
        USING ERRCODE = 'IO101';
    END IF;

    EXECUTE format('SELECT TRUE FROM %s LIMIT 1', main_table) INTO table_has_items;

    IF table_has_items THEN
        RAISE EXCEPTION 'Cannot add new dimension to a non-empty table'
        USING ERRCODE = 'IO102';
    END IF;

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

    IF num_slices IS NULL THEN
        -- Open dimension
        IF column_type NOT IN ('BIGINT', 'INTEGER', 'SMALLINT', 'TIMESTAMP', 'TIMESTAMPTZ') THEN
            RAISE EXCEPTION 'illegal type for column "%": %', column_name, column_type
            USING ERRCODE = 'IO102';
        END IF;
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

-- Add an index to a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.add_index(
    hypertable_id    INTEGER,
    main_schema_name NAME,
    main_index_name  NAME,
    definition       TEXT
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO _timescaledb_catalog.hypertable_index (hypertable_id, main_schema_name, main_index_name, definition)
VALUES (hypertable_id, main_schema_name, main_index_name, definition);
$BODY$;

-- Drops the index for a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_index(
    main_schema_name NAME,
    main_index_name  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
DELETE FROM _timescaledb_catalog.hypertable_index i
WHERE i.main_index_name = drop_index.main_index_name AND i.main_schema_name = drop_index.main_schema_name;
$BODY$;

-- Drops a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_hypertable(
    schema_name NAME,
    table_name  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    DELETE FROM _timescaledb_catalog.hypertable h
    WHERE h.schema_name = drop_hypertable.schema_name AND
          h.table_name = drop_hypertable.table_name
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
DECLARE
BEGIN
    EXECUTE format(
        $$
        DELETE FROM _timescaledb_catalog.chunk c
        USING _timescaledb_catalog.hypertable h,
        _timescaledb_internal.dimension_get_time(h.id) time_dimension,
        _timescaledb_catalog.dimension_slice ds,
        _timescaledb_catalog.chunk_constraint cc
        WHERE h.id = c.hypertable_id AND ds.dimension_id = time_dimension.id AND cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id
        AND ds.range_end <= %1$L
        AND (%2$L IS NULL OR h.schema_name = %2$L)
        AND (%3$L IS NULL OR h.table_name = %3$L)
        $$, older_than_time, schema_name, table_name
    );
END
$BODY$;

-- Create the "general definition" of an index. The general definition
-- is the corresponding create index command with the placeholders /*TABLE_NAME*/
-- and  /*INDEX_NAME*/
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_general_index_definition(
    index_oid       REGCLASS,
    table_oid       REGCLASS,
    hypertable_row  _timescaledb_catalog.hypertable
)
RETURNS text
LANGUAGE plpgsql VOLATILE AS
$BODY$
DECLARE
    def             TEXT;
    index_name      TEXT;
    c               INTEGER;
    index_row       RECORD;
    missing_column  TEXT;
BEGIN
    -- Get index definition
    def := pg_get_indexdef(index_oid);

    IF def IS NULL THEN
        RAISE EXCEPTION 'Cannot process index with no definition: %', index_oid::TEXT;
    END IF;

    SELECT * INTO STRICT index_row FROM pg_index WHERE indexrelid = index_oid;

    IF index_row.indisunique THEN
        -- unique index must contain time and all partition dimension columns.

        -- get any partitioning columns that are not included in the index.
        SELECT d.column_name INTO missing_column
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = hypertable_row.id AND
              d.column_name NOT IN (
                SELECT attname
                FROM pg_attribute
                WHERE attrelid = table_oid AND
                attnum = ANY(index_row.indkey)
            );

        IF missing_column IS NOT NULL THEN
            RAISE EXCEPTION 'Cannot create a unique index without the column: % (used in partitioning)', missing_column
            USING ERRCODE = 'IO103';
        END IF;
    END IF;


    SELECT count(*) INTO c
    FROM regexp_matches(def, 'ON '||table_oid::TEXT || ' USING', 'g');
    IF c <> 1 THEN
         RAISE EXCEPTION 'Cannot process index with definition(no table name match): %', def
         USING ERRCODE = 'IO103';
    END IF;

    def := replace(def, 'ON '|| table_oid::TEXT || ' USING', 'ON /*TABLE_NAME*/ USING');

    -- Replace index name with /*INDEX_NAME*/
    -- Index name is never schema qualified
    -- Mixed case identifiers are properly handled.
    SELECT format('%I', c.relname) INTO STRICT index_name FROM pg_catalog.pg_class AS c WHERE c.oid = index_oid AND c.relkind = 'i'::CHAR;

    SELECT count(*) INTO c
    FROM regexp_matches(def, 'INDEX '|| index_name || ' ON', 'g');
    IF c <> 1 THEN
         RAISE EXCEPTION 'Cannot process index with definition(no index name match): %', def
         USING ERRCODE = 'IO103';
    END IF;

    def := replace(def, 'INDEX '|| index_name || ' ON',  'INDEX /*INDEX_NAME*/ ON');

    RETURN def;
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
BEGIN
    SELECT * INTO STRICT time_dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE hypertable_id = hypertable_row.id AND partitioning_func IS NULL;

    SELECT count(*) INTO index_count
    FROM pg_index
    WHERE indkey = (
        SELECT attnum::text::int2vector
        FROM pg_attribute WHERE attrelid = main_table AND attname=time_dimension_row.column_name
    ) AND indrelid = main_table;

    IF index_count = 0 THEN
        EXECUTE format($$ CREATE INDEX ON %I.%I(%I DESC) $$,
            hypertable_row.schema_name, hypertable_row.table_name, time_dimension_row.column_name);
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
            EXECUTE format($$ CREATE INDEX ON %I.%I(%I, %I DESC) $$,
            hypertable_row.schema_name, hypertable_row.table_name, partitioning_column, time_dimension_row.column_name);
        END IF;
    END IF;
END
$BODY$;
