CREATE OR REPLACE FUNCTION _timescaledb_internal.check_role(rel REGCLASS)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    AS
$BODY$
DECLARE
    username NAME = current_setting('role');
    relowner OID;
BEGIN
    IF username IS NULL OR username = 'none' THEN
       username := session_user;
    END IF;

    SELECT c.relowner
    INTO STRICT relowner
    FROM pg_class c
    WHERE c.oid = rel;

    IF NOT pg_has_role(username, relowner, 'USAGE') THEN
        RAISE 'Permission denied for relation %', rel
        USING ERRCODE = 'insufficient_privilege';
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.check_associated_schema_permissions(schema_name NAME, userid OID)
    RETURNS VOID AS '@MODULE_PATHNAME@', 'hypertable_check_associated_schema_permissions' LANGUAGE C;

-- Creates a hypertable row.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_hypertable(
    main_table               REGCLASS,
    schema_name              NAME,
    table_name               NAME,
    time_column_name         NAME,
    partitioning_column      NAME,
    number_partitions        INTEGER,
    associated_schema_name   NAME,
    associated_table_prefix  NAME,
    chunk_time_interval      ANYELEMENT,
    tablespace               NAME,
    create_default_indexes   BOOLEAN,
    partitioning_func        REGPROC
)
    RETURNS _timescaledb_catalog.hypertable LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER AS
$BODY$
DECLARE
    id                       INTEGER;
    hypertable_row           _timescaledb_catalog.hypertable;
    attr                     pg_attribute;
BEGIN
    PERFORM _timescaledb_internal.check_role(main_table);

    IF associated_schema_name IS NULL THEN
        associated_schema_name = '_timescaledb_internal';
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

    -- Pre-validate dimensions to ensure we do not unnecessarily
    -- increment the hypertable ID
    PERFORM _timescaledb_internal.validate_dimension(main_table,
                                                     time_column_name,
                                                     NULL,
                                                     chunk_time_interval,
                                                     NULL);

    IF partitioning_column IS NOT NULL THEN
        PERFORM _timescaledb_internal.validate_dimension(main_table,
                                                         partitioning_column,
                                                         number_partitions,
                                                         NULL::BIGINT,
                                                         partitioning_func);
    END IF;

    -- Create the schema for the hypertable data if needed
    PERFORM _timescaledb_internal.create_hypertable_schema(associated_schema_name);

    id := nextval(pg_get_serial_sequence('_timescaledb_catalog.hypertable','id'));

    IF associated_table_prefix IS NULL THEN
        associated_table_prefix = format('_hyper_%s', id);
    END IF;

    INSERT INTO _timescaledb_catalog.hypertable (
        id, schema_name, table_name,
        associated_schema_name, associated_table_prefix, num_dimensions)
    VALUES (
        id, schema_name, table_name,
        associated_schema_name, associated_table_prefix, 1
    )
    RETURNING * INTO hypertable_row;

    IF chunk_time_interval IS NULL THEN
        chunk_time_interval = _timescaledb_internal.interval_to_usec('1 month');
    END IF;

    --create time dimension
    PERFORM _timescaledb_internal.add_dimension(main_table,
                                                time_column_name,
                                                NULL,
                                                chunk_time_interval,
                                                NULL);

    IF partitioning_column IS NOT NULL THEN
        --create space dimension
        PERFORM _timescaledb_internal.add_dimension(main_table,
                                                    partitioning_column,
                                                    number_partitions,
                                                    NULL::BIGINT,
                                                    partitioning_func);
    END IF;

    -- Verify indexes
    PERFORM _timescaledb_internal.verify_hypertable_indexes(main_table);

    IF create_default_indexes THEN
        PERFORM _timescaledb_internal.create_default_indexes(hypertable_row, main_table, partitioning_column);
    END IF;

    --add default tablespace, if any
    IF tablespace IS NOT NULL THEN
        PERFORM _timescaledb_internal.attach_tablespace(tablespace, main_table);
    END IF;

    RETURN hypertable_row;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_hypertable_schema(schema_name NAME)
RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    schema_cnt INTEGER;
BEGIN
    SELECT COUNT(*) INTO schema_cnt
    FROM pg_namespace
    WHERE nspname = schema_name;

    IF schema_cnt = 0 THEN
        BEGIN
            EXECUTE format('CREATE SCHEMA %I', schema_name);
        EXCEPTION
            WHEN insufficient_privilege THEN
                SELECT COUNT(*) INTO schema_cnt
                FROM pg_namespace
                WHERE nspname = schema_name;
                IF schema_cnt = 0 THEN
                   RAISE;
                END IF;
        END;
    END IF;
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

-- Drops a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_hypertable(
    hypertable_id INT,
    is_cascade BOOLEAN
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    cascade_mod TEXT := '';
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    IF is_cascade THEN
        cascade_mod = 'CASCADE';
    END IF;
    FOR chunk_row IN SELECT c.*
        FROM _timescaledb_catalog.hypertable h
        INNER JOIN _timescaledb_catalog.chunk c ON (c.hypertable_id = h.id)
        WHERE h.id = drop_hypertable.hypertable_id
        LOOP
            EXECUTE format(
                $$
                DROP TABLE %I.%I %s
                $$, chunk_row.schema_name, chunk_row.table_name, cascade_mod
            );
        END LOOP;

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
-- drop only chunks associated with this table. Any of the first three arguments
-- can be NULL meaning "all values".
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunks_impl(
    older_than_time  BIGINT,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE,
    truncate_before  BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    cascade_mod TEXT = '';
    exist_count INT = 0;
BEGIN
    IF older_than_time IS NULL AND table_name IS NULL AND schema_name IS NULL THEN
        RAISE 'Cannot have all 3 arguments to drop_chunks_older_than be NULL';
    END IF;

    IF cascade THEN
        cascade_mod = 'CASCADE';
    END IF;

    IF table_name IS NOT NULL THEN
        SELECT COUNT(*)
        FROM _timescaledb_catalog.hypertable h
        WHERE (drop_chunks_impl.schema_name IS NULL OR h.schema_name = drop_chunks_impl.schema_name)
        AND drop_chunks_impl.table_name = h.table_name
        INTO STRICT exist_count;

        IF exist_count = 0 THEN
            RAISE 'hypertable % does not exist', drop_chunks_impl.table_name
            USING ERRCODE = 'IO001';
        END IF;
    END IF;

    FOR chunk_row IN SELECT *
        FROM _timescaledb_catalog.chunk c
        INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = c.hypertable_id)
        INNER JOIN _timescaledb_internal.dimension_get_time(h.id) time_dimension ON(true)
        INNER JOIN _timescaledb_catalog.dimension_slice ds
            ON (ds.dimension_id = time_dimension.id)
        INNER JOIN _timescaledb_catalog.chunk_constraint cc
            ON (cc.dimension_slice_id = ds.id AND cc.chunk_id = c.id)
        WHERE (older_than_time IS NULL OR ds.range_end <= older_than_time)
        AND (drop_chunks_impl.schema_name IS NULL OR h.schema_name = drop_chunks_impl.schema_name)
        AND (drop_chunks_impl.table_name IS NULL OR h.table_name = drop_chunks_impl.table_name)
    LOOP
        IF truncate_before THEN
            EXECUTE format(
                $$
                TRUNCATE %I.%I %s
                $$, chunk_row.schema_name, chunk_row.table_name, cascade_mod
            );
        END IF;
        EXECUTE format(
                $$
                DROP TABLE %I.%I %s
                $$, chunk_row.schema_name, chunk_row.table_name, cascade_mod
        );
    END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunks_type_check(
    given_type REGTYPE,
    table_name  NAME,
    schema_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    actual_type regtype;
BEGIN
    BEGIN
        WITH hypertable_ids AS (
            SELECT id
            FROM _timescaledb_catalog.hypertable h
            WHERE (drop_chunks_type_check.schema_name IS NULL OR h.schema_name = drop_chunks_type_check.schema_name) AND
            (drop_chunks_type_check.table_name IS NULL OR h.table_name = drop_chunks_type_check.table_name)
        )
        SELECT DISTINCT time_dim.column_type INTO STRICT actual_type
        FROM hypertable_ids INNER JOIN LATERAL _timescaledb_internal.dimension_get_time(hypertable_ids.id) time_dim ON (true);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'No hypertables found';
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Cannot use drop_chunks on multiple tables with different time types';
    END;

    IF given_type IN ('int'::regtype, 'smallint'::regtype, 'bigint'::regtype ) THEN
        IF actual_type IN ('int'::regtype, 'smallint'::regtype, 'bigint'::regtype ) THEN
            RETURN;
        END IF;
    END IF;
    IF actual_type != given_type THEN
        RAISE EXCEPTION 'Cannot call drop_chunks with a % on hypertables with a time type of: %', given_type, actual_type;
    END IF;
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

CREATE OR REPLACE FUNCTION _timescaledb_internal.truncate_hypertable(
    schema_name     NAME,
    table_name      NAME,
    cascade      BOOLEAN = FALSE
)
    RETURNS VOID
    LANGUAGE PLPGSQL VOLATILE
    SET search_path = '_timescaledb_internal'
    AS
$BODY$
DECLARE
    hypertable_row _timescaledb_catalog.hypertable;
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    --TODO: should this cascade?
    PERFORM  _timescaledb_internal.drop_chunks_impl(NULL, table_name, schema_name, cascade, true);
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.verify_hypertable_indexes(hypertable REGCLASS) RETURNS VOID
AS '@MODULE_PATHNAME@', 'indexing_verify_hypertable_indexes' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_change_owner(main_table OID, new_table_owner NAME)
    RETURNS void LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = '_timescaledb_internal'
    AS
$BODY$
DECLARE
    hypertable_row _timescaledb_catalog.hypertable;
    chunk_row      _timescaledb_catalog.chunk;
BEGIN
    hypertable_row := _timescaledb_internal.hypertable_from_main_table(main_table);
    FOR chunk_row IN
        SELECT *
        FROM _timescaledb_catalog.chunk
        WHERE hypertable_id = hypertable_row.id
        LOOP
            EXECUTE format(
                $$
                ALTER TABLE %1$I.%2$I OWNER TO %3$I
                $$,
                chunk_row.schema_name, chunk_row.table_name,
                new_table_owner
            );
    END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.add_dimension(
    main_table               REGCLASS,
    column_name              NAME,
    num_slices               INTEGER = NULL,
    chunk_time_interval      ANYELEMENT = NULL::BIGINT,
    partitioning_func        REGPROC = NULL,
    if_not_exists            BOOLEAN = FALSE
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'dimension_add' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.validate_dimension(
    main_table               REGCLASS,
    column_name              NAME,
    num_slices               INTEGER,
    interval_length          ANYELEMENT,
    partitioning_func        REGPROC
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'dimension_validate' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.attach_tablespace(tablespace NAME, hypertable REGCLASS) RETURNS VOID
AS '@MODULE_PATHNAME@', 'tablespace_attach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.detach_tablespace(tablespace NAME, hypertable REGCLASS) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'tablespace_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.detach_tablespaces(hypertable REGCLASS) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'tablespace_detach_all_from_hypertable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.show_tablespaces(hypertable REGCLASS) RETURNS SETOF NAME
AS '@MODULE_PATHNAME@', 'tablespace_show' LANGUAGE C VOLATILE STRICT;

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '@MODULE_PATHNAME@', 'chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '@MODULE_PATHNAME@', 'chunk_index_replace' LANGUAGE C VOLATILE STRICT;
