CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_open(
        dimension_value   BIGINT,
        interval_length   BIGINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '$libdir/timescaledb', 'dimension_calculate_open_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_closed(
        dimension_value   BIGINT,
        num_slices        SMALLINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '$libdir/timescaledb', 'dimension_calculate_closed_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk(
    chunk_id int,
    is_cascade BOOLEAN,
    drop_table BOOLEAN = TRUE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    cascade_mod TEXT := '';
BEGIN
    -- when deleting the chunk row from the metadata table,
    -- also DROP the actual chunk table that holds data.
    -- Note that the table could already be deleted in case this
    -- is executed as a result of a DROP TABLE on the hypertable
    -- that this chunk belongs to.

    PERFORM _timescaledb_internal.drop_chunk_constraint(cc.chunk_id, cc.constraint_name, false)
    FROM _timescaledb_catalog.chunk_constraint cc
    WHERE cc.chunk_id = drop_chunk.chunk_id;

    DELETE FROM _timescaledb_catalog.chunk WHERE id = chunk_id
    RETURNING * INTO STRICT chunk_row;

    PERFORM 1
    FROM pg_class c
    WHERE relname = quote_ident(chunk_row.table_name) AND relnamespace = quote_ident(chunk_row.schema_name)::regnamespace;

    IF FOUND AND drop_table THEN
        IF is_cascade THEN
            cascade_mod = 'CASCADE';
        END IF;

        EXECUTE format(
                $$
                DROP TABLE %I.%I %s
                $$, chunk_row.schema_name, chunk_row.table_name, cascade_mod
        );
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create(
    chunk_id INTEGER,
    hypertable_id INTEGER,
    schema_name NAME,
    table_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    tablespace_name NAME;
    main_table_oid  OID;
    dimension_slice_ids INT[];
BEGIN
    INSERT INTO _timescaledb_catalog.chunk (id, hypertable_id, schema_name, table_name)
    VALUES (chunk_id, hypertable_id, schema_name, table_name) RETURNING * INTO STRICT chunk_row;

    SELECT array_agg(cc.dimension_slice_id)::int[] INTO STRICT dimension_slice_ids
    FROM _timescaledb_catalog.chunk_constraint cc
    WHERE cc.chunk_id = chunk_create.chunk_id AND cc.dimension_slice_id IS NOT NULL;

    tablespace_name := _timescaledb_internal.select_tablespace(chunk_row.hypertable_id, dimension_slice_ids);

    PERFORM _timescaledb_internal.chunk_create_table(chunk_row.id, tablespace_name);

    --create the dimension-slice-constraints
    PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(cc)
    FROM _timescaledb_catalog.chunk_constraint cc
    WHERE cc.chunk_id = chunk_create.chunk_id AND cc.dimension_slice_id IS NOT NULL;

    PERFORM _timescaledb_internal.create_chunk_index_row(chunk_row.schema_name, chunk_row.table_name,
                            hi.main_schema_name, hi.main_index_name, hi.definition)
    FROM _timescaledb_catalog.hypertable_index hi
    WHERE hi.hypertable_id = chunk_row.hypertable_id
    ORDER BY format('%I.%I',main_schema_name, main_index_name)::regclass;

    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable WHERE id = chunk_row.hypertable_id;
    main_table_oid := format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass;

    --create the hypertable-constraints copy
    PERFORM _timescaledb_internal.create_chunk_constraint(chunk_row.id, oid)
    FROM pg_constraint
    WHERE conrelid = main_table_oid
    AND _timescaledb_internal.need_chunk_constraint(oid);

    PERFORM _timescaledb_internal.create_chunk_trigger(chunk_row.id, tgname,
        _timescaledb_internal.get_general_trigger_definition(oid))
    FROM pg_trigger
    WHERE tgrelid = main_table_oid
    AND _timescaledb_internal.need_chunk_trigger(chunk_row.hypertable_id, oid);

END
$BODY$;
