DROP FUNCTION _timescaledb_internal.ddl_is_change_owner(pg_ddl_command);
DROP FUNCTION _timescaledb_internal.ddl_change_owner_to(pg_ddl_command);

DROP FUNCTION _timescaledb_internal.chunk_add_constraints(integer);
DROP FUNCTION _timescaledb_internal.ddl_process_alter_table() CASCADE;

CREATE INDEX ON _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id);

ALTER TABLE _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_pkey,
ADD COLUMN constraint_name NAME;

UPDATE _timescaledb_catalog.chunk_constraint cc
SET constraint_name =
  (SELECT con.conname FROM
   _timescaledb_catalog.chunk c
   INNER JOIN _timescaledb_catalog.dimension_slice ds ON (cc.dimension_slice_id = ds.id)
   INNER JOIN _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
   INNER JOIN pg_constraint con ON (con.contype = 'c' AND con.conrelid = format('%I.%I',c.schema_name, c.table_name)::regclass)
   INNER JOIN pg_attribute att ON (att.attrelid = format('%I.%I',c.schema_name, c.table_name)::regclass AND att.attname = d.column_name)
   WHERE c.id = cc.chunk_id
   AND con.conname = format('constraint_%s', dimension_slice_id)
   AND array_length(con.conkey, 1) = 1 AND con.conkey = ARRAY[att.attnum]
   );

ALTER TABLE _timescaledb_catalog.chunk_constraint 
ALTER COLUMN constraint_name SET NOT NULL,
ALTER COLUMN dimension_slice_id DROP NOT NULL;

ALTER TABLE _timescaledb_catalog.chunk_constraint
ADD COLUMN hypertable_constraint_name NAME NULL,
ADD CONSTRAINT chunk_constraint_chunk_id_constraint_name_key UNIQUE (chunk_id, constraint_name);

CREATE SEQUENCE _timescaledb_catalog.chunk_constraint_name;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

DROP FUNCTION _timescaledb_internal.rename_hypertable(name, name, text, text);

ALTER EXTENSION timescaledb
DROP FUNCTION create_hypertable(REGCLASS, NAME, NAME,INTEGER,NAME,NAME,BIGINT,BOOLEAN, BOOLEAN);
DROP FUNCTION create_hypertable(REGCLASS, NAME, NAME,INTEGER,NAME,NAME,BIGINT,BOOLEAN, BOOLEAN);

DROP FUNCTION IF EXISTS hypertable_relation_size(regclass);
DROP FUNCTION IF EXISTS chunk_relation_size(regclass);
DROP FUNCTION IF EXISTS indexes_relation_size(regclass);
-- Error codes defined in errors.h
-- The functions in this file are just utility commands for throwing common errors.

CREATE OR REPLACE FUNCTION _timescaledb_internal.on_trigger_error(
    tg_op     TEXT,
    tg_schema NAME,
    tg_table  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RAISE EXCEPTION 'Operation % not supported on %.%', tg_op, tg_schema, tg_table
    USING ERRCODE = 'IO101';
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.on_truncate_block()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_open(
        dimension_value   BIGINT,
        interval_length   BIGINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '@MODULE_PATHNAME@', 'dimension_calculate_open_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_closed(
        dimension_value   BIGINT,
        num_slices        SMALLINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '@MODULE_PATHNAME@', 'dimension_calculate_closed_range_default' LANGUAGE C STABLE;

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

END
$BODY$;
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

-- do I need to add a hypertable index to the chunks?;
CREATE OR REPLACE FUNCTION _timescaledb_internal.need_chunk_index(
    hypertable_id INTEGER,
    index_oid OID
)
    RETURNS BOOLEAN LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    associated_constraint BOOLEAN;
BEGIN
    --do not add an index with associated constraints
    SELECT count(*) > 0 INTO STRICT associated_constraint FROM pg_constraint WHERE conindid = index_oid;

    RETURN NOT associated_constraint;
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

--Makes sure the index is valid for a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_internal.check_index(index_oid REGCLASS, hypertable_row  _timescaledb_catalog.hypertable)
RETURNS VOID LANGUAGE plpgsql STABLE AS
$BODY$
DECLARE
    index_row       RECORD;
    missing_column  TEXT;
BEGIN
    SELECT * INTO STRICT index_row FROM pg_index WHERE indexrelid = index_oid;
    IF index_row.indisunique OR index_row.indisexclusion THEN
        -- unique/exclusion index must contain time and all partition dimension columns.

        -- get any partitioning columns that are not included in the index.
        SELECT d.column_name INTO missing_column
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = hypertable_row.id AND
              d.column_name NOT IN (
                SELECT attname
                FROM pg_attribute
                WHERE attrelid = index_row.indrelid AND
                attnum = ANY(index_row.indkey)
            );

        IF missing_column IS NOT NULL THEN
            RAISE EXCEPTION 'Cannot create a unique index without the column: % (used in partitioning)', missing_column
            USING ERRCODE = 'IO103';
        END IF;
    END IF;
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
BEGIN
    -- Get index definition
    def := pg_get_indexdef(index_oid);

    IF def IS NULL THEN
        RAISE EXCEPTION 'Cannot process index with no definition: %', index_oid::TEXT;
    END IF;

    PERFORM _timescaledb_internal.check_index(index_oid, hypertable_row);

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
-- This file contains utilities for time conversion.
CREATE OR REPLACE FUNCTION _timescaledb_internal.to_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'pg_timestamp_to_microseconds' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
	AS '@MODULE_PATHNAME@', 'pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp_pg(postgres_us BIGINT) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'pg_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT;


-- Time can be represented in a hypertable as an int* (bigint/integer/smallint) or as a timestamp type (
-- with or without timezones). In or metatables and other internal systems all time values are stored as bigint.
-- Converting from int* columns to internal representation is a cast to bigint.
-- Converting from timestamps to internal representation is conversion to epoch (in microseconds).

-- Gets the sql code for representing the literal for the given time value (in the internal representation) as the column_type.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_literal_sql(
    time_value      BIGINT,
    column_type     REGTYPE
)
    RETURNS text LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    IF time_value IS NULL THEN
        RETURN format('%L', NULL);
    END IF;
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); -- scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('%2$s %1$L', _timescaledb_internal.to_timestamp(time_value), column_type); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))::date);
    END CASE;
END
$BODY$;

-- Convert a interval to microseconds.
CREATE OR REPLACE FUNCTION _timescaledb_internal.interval_to_usec(
       chunk_interval INTERVAL
)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE AS
$BODY$
    SELECT (int_sec * 1000000)::bigint from extract(epoch from chunk_interval) as int_sec;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.time_interval_specification_to_internal(
    time_type REGTYPE,
    specification anyelement,
    default_value INTERVAL,
    field_name TEXT
)
RETURNS BIGINT LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    IF time_type IN ('TIMESTAMP', 'TIMESTAMPTZ', 'DATE') THEN
        IF specification IS NULL THEN
            RETURN _timescaledb_internal.interval_to_usec(default_value);
        ELSIF pg_typeof(specification) IN ('INT'::regtype, 'SMALLINT'::regtype, 'BIGINT'::regtype) THEN
            IF specification::BIGINT < _timescaledb_internal.interval_to_usec('1 second') THEN
                RAISE WARNING 'You specified a % of less than a second, make sure that this is what you intended', field_name
                USING HINT = 'specification is specified in microseconds';
            END IF;
            RETURN specification::BIGINT;
        ELSIF pg_typeof(specification) = 'INTERVAL'::regtype THEN
            RETURN _timescaledb_internal.interval_to_usec(specification);
        ELSE
            RAISE EXCEPTION '% needs to be an INTERVAL or integer type for TIMESTAMP, TIMESTAMPTZ, or DATE time columns', field_name
            USING ERRCODE = 'IO102';
        END IF;
    ELSIF time_type IN ('SMALLINT', 'INTEGER', 'BIGINT') THEN
        IF specification IS NULL THEN
            RAISE EXCEPTION '% needs to be explicitly set for time columns of type SMALLINT, INTEGER, and BIGINT', field_name
            USING ERRCODE = 'IO102';
        ELSIF pg_typeof(specification) IN ('INT'::regtype, 'SMALLINT'::regtype, 'BIGINT'::regtype) THEN
            --bounds check
            IF time_type = 'INTEGER'::REGTYPE AND specification > 2147483647 THEN
                RAISE EXCEPTION '% is too large for type INTEGER (max: 2147483647)', field_name
                USING ERRCODE = 'IO102';
            ELSIF time_type = 'SMALLINT'::REGTYPE AND specification > 65535 THEN
                RAISE EXCEPTION '% is too large for type SMALLINT (max: 65535)', field_name
                USING ERRCODE = 'IO102';
            END IF;
            RETURN specification::BIGINT;
        ELSE
            RAISE EXCEPTION '% needs to be an integer type for SMALLINT, INTEGER, and BIGINT time columns', field_name
            USING ERRCODE = 'IO102';
        END IF;
    ELSE
        RAISE EXCEPTION 'unknown time column type: %', time_type
        USING ERRCODE = 'IO102';
    END IF;
END
$BODY$;
-- This file contains functions associated with creating new
-- hypertables.

-- Creates a new schema if it does not exist.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_schema(
    schema_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE SCHEMA IF NOT EXISTS %I
        $$, schema_name);
END
$BODY$
SET client_min_messages = WARNING -- suppress NOTICE on IF EXISTS
;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create_table(
    chunk_id INT,
    tablespace_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    tablespace_clause TEXT := '';
    table_owner     NAME;
    tablespace_oid  OID;
BEGIN
    SELECT * INTO STRICT chunk_row
    FROM _timescaledb_catalog.chunk
    WHERE id = chunk_id;

    SELECT * INTO STRICT hypertable_row
    FROM _timescaledb_catalog.hypertable
    WHERE id = chunk_row.hypertable_id;

    SELECT t.oid
    INTO tablespace_oid
    FROM pg_catalog.pg_tablespace t
    WHERE t.spcname = tablespace_name;

    SELECT tableowner
    INTO STRICT table_owner
    FROM pg_catalog.pg_tables
    WHERE schemaname = hypertable_row.schema_name
          AND tablename = hypertable_row.table_name;

    IF tablespace_oid IS NOT NULL THEN
        tablespace_clause := format('TABLESPACE %s', tablespace_name);
    ELSIF tablespace_name IS NOT NULL THEN
        RAISE EXCEPTION 'No tablespace % in database %', tablespace_name, current_database()
        USING ERRCODE = 'IO501';
    END IF;

    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I () INHERITS(%3$I.%4$I) %5$s;
        $$,
        chunk_row.schema_name, chunk_row.table_name,
        hypertable_row.schema_name, hypertable_row.table_name, tablespace_clause
    );

    EXECUTE format(
        $$
            ALTER TABLE %1$I.%2$I OWNER TO %3$I
        $$,
        chunk_row.schema_name, chunk_row.table_name,
        table_owner
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql(
    dimension_slice_id  INTEGER
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    dimension_slice_row _timescaledb_catalog.dimension_slice;
    dimension_row _timescaledb_catalog.dimension;
BEGIN
    SELECT * INTO STRICT dimension_slice_row
    FROM _timescaledb_catalog.dimension_slice
    WHERE id = dimension_slice_id;

    SELECT * INTO STRICT dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE id = dimension_slice_row.dimension_id;

    IF dimension_row.partitioning_func IS NOT NULL THEN
        return format(
            $$
                %1$I.%2$s(%3$I::text) >= %4$L AND %1$I.%2$s(%3$I::text) <  %5$L
            $$,
            dimension_row.partitioning_func_schema, dimension_row.partitioning_func,
            dimension_row.column_name, dimension_slice_row.range_start, dimension_slice_row.range_end);
    ELSE
        --TODO: only works with time for now
        IF _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimension_row.column_type) =
           _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type) THEN
            RAISE 'Time based constraints have the same start and end values for column "%": %',
                    dimension_row.column_name,
                    _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type);
        END IF;
        return format(
            $$
                %1$I >= %2$s AND %1$I < %3$s
            $$,
            dimension_row.column_name,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimension_row.column_type),
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type));
    END IF;
END
$BODY$;

-- Outputs the create_hypertable command to recreate the given hypertable.
--
-- This is currently used internally for our single hypertable backup tool
-- so that it knows how to restore the hypertable without user intervention.
--
-- It only works for hypertables with up to 2 dimensions.
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_create_command(
    table_name NAME
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    h_id             INTEGER;
    schema_name      NAME;
    time_column      NAME;
    time_interval    BIGINT;
    space_column     NAME;
    space_partitions INTEGER;
    dimension_cnt    INTEGER;
    dimension_row    record;
    ret              TEXT;
BEGIN
    SELECT h.id, h.schema_name
    FROM _timescaledb_catalog.hypertable AS h
    WHERE h.table_name = get_create_command.table_name
    INTO h_id, schema_name;

    IF h_id IS NULL THEN
        RAISE EXCEPTION 'hypertable % not found', table_name
        USING ERRCODE = 'IO101';
    END IF;

    SELECT COUNT(*)
    FROM _timescaledb_catalog.dimension d
    WHERE d.hypertable_id = h_id
    INTO STRICT dimension_cnt;

    IF dimension_cnt > 2 THEN
        RAISE EXCEPTION 'get_create_command only supports hypertables with up to 2 dimensions'
        USING ERRCODE = 'IO101';
    END IF;

    FOR dimension_row IN
        SELECT *
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = h_id
        LOOP
        IF dimension_row.interval_length IS NOT NULL THEN
            time_column := dimension_row.column_name;
            time_interval := dimension_row.interval_length;
        ELSIF dimension_row.num_slices IS NOT NULL THEN
            space_column := dimension_row.column_name;
            space_partitions := dimension_row.num_slices;
        END IF;
    END LOOP;

    ret := format($$SELECT create_hypertable('%I.%I', '%I'$$, schema_name, table_name, time_column);
    IF space_column IS NOT NULL THEN
        ret := ret || format($$, '%I', %s$$, space_column, space_partitions);
    END IF;
    ret := ret || format($$, chunk_time_interval => %s, create_default_indexes=>FALSE);$$, time_interval);

    RETURN ret;
END
$BODY$;
-- Creates a constraint on a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_constraint_add_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code    TEXT;
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    constraint_oid OID;
    def TEXT;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id;

    IF chunk_constraint_row.dimension_slice_id IS NOT NULL THEN
        def := format('CHECK (%s)',  _timescaledb_internal.dimension_slice_get_constraint_sql(chunk_constraint_row.dimension_slice_id));
    ELSIF chunk_constraint_row.hypertable_constraint_name IS NOT NULL THEN
        SELECT oid INTO STRICT constraint_oid FROM pg_constraint 
        WHERE conname=chunk_constraint_row.hypertable_constraint_name AND 
              conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid;
        def := pg_get_constraintdef(constraint_oid);
    ELSE
        RAISE 'Unknown constraint type';
    END IF; 

    sql_code := format(
        $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
        chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name, def
    );
    EXECUTE sql_code;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_constraint_drop_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code    TEXT;
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;

    sql_code := format(
        $$ ALTER TABLE %I.%I DROP CONSTRAINT %I $$,
        chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name
    );

    EXECUTE sql_code;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_constraint(
    chunk_id INTEGER,
    constraint_oid OID
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_constraint_row _timescaledb_catalog.chunk_constraint;
    constraint_row pg_constraint;
    constraint_name TEXT;
    hypertable_constraint_name TEXT = NULL;
BEGIN
    SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;
    hypertable_constraint_name := constraint_row.conname;
    constraint_name := format('%s_%s_%s', chunk_id,  nextval('_timescaledb_catalog.chunk_constraint_name'), hypertable_constraint_name); 

    INSERT INTO _timescaledb_catalog.chunk_constraint (chunk_id, constraint_name, dimension_slice_id, hypertable_constraint_name)
    VALUES (chunk_id, constraint_name, NULL, hypertable_constraint_name) RETURNING * INTO STRICT chunk_constraint_row;

    PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(chunk_constraint_row);
END
$BODY$;

-- Drop a constraint on a chunk
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk_constraint(
    chunk_id INTEGER,
    constraint_name NAME,
    alter_table BOOLEAN = true
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    chunk_constraint_row _timescaledb_catalog.chunk_constraint;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_id;

    DELETE FROM _timescaledb_catalog.chunk_constraint cc 
    WHERE  cc.constraint_name = drop_chunk_constraint.constraint_name 
    AND cc.chunk_id = drop_chunk_constraint.chunk_id
    RETURNING * INTO STRICT chunk_constraint_row;

    IF alter_table THEN 
        EXECUTE format(
            $$  ALTER TABLE %I.%I DROP CONSTRAINT %I $$,
                chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name
        );
    END IF;

END
$BODY$;

-- do I need to add a hypertable constraint to the chunks?;
CREATE OR REPLACE FUNCTION _timescaledb_internal.need_chunk_constraint(
    constraint_oid OID
)
    RETURNS BOOLEAN LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    constraint_row record;
BEGIN
    SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;

    IF constraint_row.contype IN ('c') THEN
        -- check and not null constraints handled by regular inheritance (from docs):
        --    All check constraints and not-null constraints on a parent table are automatically inherited by its children,
        --    unless explicitly specified otherwise with NO INHERIT clauses. Other types of constraints
        --    (unique, primary key, and foreign key constraints) are not inherited."

        IF constraint_row.connoinherit THEN
            RAISE 'NO INHERIT option not supported on hypertables: %', constraint_row.conname
            USING ERRCODE = 'IO101';
        END IF;

        RETURN FALSE;
    END IF;
    RETURN TRUE;
END
$BODY$;

-- Creates a constraint on all chunks for a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_internal.add_constraint(
    hypertable_id INTEGER,
    constraint_oid OID
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    constraint_row pg_constraint;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    IF _timescaledb_internal.need_chunk_constraint(constraint_oid) THEN
        SELECT * INTO STRICT constraint_row FROM pg_constraint WHERE OID = constraint_oid;

        --check the validity of an index if a constraint uses an index
        --note: foreign-key constraints are excluded because they point to indexes on the foreign table /not/ the hypertable
        IF constraint_row.conindid <> 0 AND constraint_row.contype != 'f' THEN
            SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable WHERE id = hypertable_id;
            PERFORM _timescaledb_internal.check_index(constraint_row.conindid, hypertable_row);
        END IF;

        PERFORM _timescaledb_internal.create_chunk_constraint(c.id, constraint_oid)
        FROM _timescaledb_catalog.chunk c
        WHERE c.hypertable_id = add_constraint.hypertable_id;
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.add_constraint_by_name(
    hypertable_id INTEGER,
    constraint_name name
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    constraint_oid OID;
BEGIN
    SELECT oid INTO STRICT constraint_oid FROM pg_constraint WHERE conname = constraint_name 
    AND conrelid = _timescaledb_internal.main_table_from_hypertable(hypertable_id);

    PERFORM _timescaledb_internal.add_constraint(hypertable_id, constraint_oid);
END
$BODY$;

-- Drops constraint on all chunks for a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_constraint(
    hypertable_id INTEGER,
    hypertable_constraint_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_internal.drop_chunk_constraint(cc.chunk_id, cc.constraint_name)
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.chunk_id = c.id)
    WHERE c.hypertable_id = drop_constraint.hypertable_id AND cc.hypertable_constraint_name = drop_constraint.hypertable_constraint_name;
END
$BODY$;

-- This file contains triggers that act on the main 'hypertable' table as
-- well as triggers for newly created hypertables.
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_hypertable()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'INSERT' THEN
        DECLARE 
            cnt INTEGER;
        BEGIN
            EXECUTE format(
                $$
                    CREATE SCHEMA IF NOT EXISTS %I
                $$, NEW.associated_schema_name);
        EXCEPTION
            WHEN insufficient_privilege THEN
                SELECT COUNT(*) INTO cnt
                FROM pg_namespace 
                WHERE nspname = NEW.associated_schema_name;
                IF cnt = 0 THEN
                    RAISE;
                END IF;
        END;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
       RETURN NEW;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$
SET client_min_messages = WARNING; -- suppress NOTICE on IF EXISTS schema
-- Creates an index on all chunk for a hypertable.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_index_on_all_chunks(
    hypertable_id    INTEGER,
    main_schema_name NAME,
    main_index_name  NAME,
    definition       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_internal.create_chunk_index_row(c.schema_name, c.table_name, main_schema_name, main_index_name, definition)
    FROM _timescaledb_catalog.chunk c
    WHERE c.hypertable_id = create_index_on_all_chunks.hypertable_id;
END
$BODY$;

-- Drops table on all chunks for a hypertable.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_index_on_all_chunks(
    main_schema_name     NAME,
    main_index_name      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    DELETE FROM _timescaledb_catalog.chunk_index ci
    WHERE ci.main_index_name = drop_index_on_all_chunks.main_index_name AND
    ci.main_schema_name = drop_index_on_all_chunks.main_schema_name
$BODY$;


-- Creates indexes on chunk tables when hypertable_index rows created.
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_hypertable_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
  hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RETURN NEW;
    ELSIF TG_OP = 'INSERT' THEN
        -- create index on all chunks
        PERFORM _timescaledb_internal.create_index_on_all_chunks(NEW.hypertable_id, NEW.main_schema_name, NEW.main_index_name, NEW.definition);

        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        PERFORM _timescaledb_internal.drop_index_on_all_chunks(OLD.main_schema_name, OLD.main_index_name);

        RETURN OLD;
    END IF;
END
$BODY$;


-- our default partitioning function.
-- returns a hash of val modulous the mod_factor
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_for_key(val text) RETURNS int
	AS '@MODULE_PATHNAME@', 'get_partition_for_key' LANGUAGE C IMMUTABLE STRICT;
-- This file contains functions related to getting information about the
-- schema of a hypertable, including columns, their types, etc.


-- Check if a given table OID is a main table (i.e. the table a user
-- targets for SQL operations) for a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.is_main_table(
    table_oid regclass
)
    RETURNS bool LANGUAGE SQL STABLE AS
$BODY$
    SELECT EXISTS(SELECT 1 FROM _timescaledb_catalog.hypertable WHERE table_name = relname AND schema_name = nspname)
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = table_oid;
$BODY$;

-- Check if given table is a hypertable's main table
CREATE OR REPLACE FUNCTION _timescaledb_internal.is_main_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS BOOLEAN LANGUAGE SQL STABLE AS
$BODY$
     SELECT EXISTS(
         SELECT 1 FROM _timescaledb_catalog.hypertable h
         WHERE h.schema_name = is_main_table.schema_name AND 
               h.table_name = is_main_table.table_name
     );
$BODY$;

-- Get a hypertable given its main table OID
CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_from_main_table(
    table_oid regclass
)
    RETURNS _timescaledb_catalog.hypertable LANGUAGE SQL STABLE AS
$BODY$
    SELECT h.*
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    INNER JOIN _timescaledb_catalog.hypertable h ON (h.table_name = c.relname AND h.schema_name = n.nspname)
    WHERE c.OID = table_oid;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.main_table_from_hypertable(
    hypertable_id int
)
    RETURNS regclass LANGUAGE SQL STABLE AS
$BODY$
    SELECT format('%I.%I',h.schema_name, h.table_name)::regclass
    FROM _timescaledb_catalog.hypertable h
    WHERE id = hypertable_id;
$BODY$;


-- Get the name of the time column for a chunk.
--
-- schema_name, table_name - name of the schema and table for the table represented by the crn.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_col_name_for_chunk(
    schema_name NAME,
    table_name  NAME
)
    RETURNS NAME LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_name NAME;
BEGIN
    SELECT h.time_column_name INTO STRICT time_col_name
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.chunk c ON (c.hypertable_id = h.id)
    WHERE c.schema_name = time_col_name_for_chunk.schema_name AND
    c.table_name = time_col_name_for_chunk.table_name;
    RETURN time_col_name;
END
$BODY$;

-- Get the type of the time column for a chunk.
--
-- schema_name, table_name - name of the schema and table for the table represented by the crn.
CREATE OR REPLACE FUNCTION _timescaledb_internal.time_col_type_for_chunk(
    schema_name NAME,
    table_name  NAME
)
    RETURNS REGTYPE LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_type REGTYPE;
BEGIN
    SELECT h.time_column_type INTO STRICT time_col_type
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.chunk c ON (c.hypertable_id = h.id)
    WHERE c.schema_name = time_col_type_for_chunk.schema_name AND
    c.table_name = time_col_type_for_chunk.table_name;
    RETURN time_col_type;
END
$BODY$;
-- Convert a general index definition to a create index sql command for a
-- particular table and index name.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_index_definition_for_table(
    schema_name NAME,
    table_name  NAME,
    index_name NAME,
    general_defintion TEXT
  )
    RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code TEXT;
BEGIN
    sql_code := replace(general_defintion, '/*TABLE_NAME*/', format('%I.%I', schema_name, table_name));
    sql_code = replace(sql_code, '/*INDEX_NAME*/', format('%I', index_name));

    RETURN sql_code;
END
$BODY$;

-- Creates a chunk_index_row.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_index_row(
    schema_name NAME,
    table_name  NAME,
    main_schema_name NAME,
    main_index_name NAME,
    def TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    index_name  NAME;
    id          BIGINT;
    sql_code    TEXT;
BEGIN
    id = nextval(pg_get_serial_sequence('_timescaledb_catalog.chunk_index','id'));
    index_name := format('%s-%s', id, main_index_name);

    sql_code := _timescaledb_internal.get_index_definition_for_table(schema_name, table_name, index_name, def);

    INSERT INTO _timescaledb_catalog.chunk_index (id, schema_name, table_name, index_name, main_schema_name, 
        main_index_name, definition)
    VALUES (id, schema_name, table_name, index_name,main_schema_name, main_index_name, sql_code);
END
$BODY$;
-- Trigger to create/drop indexes on chunk tables when the corresponding
-- chunk_index row is created/deleted.
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_chunk_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'INSERT' THEN
        EXECUTE NEW.definition;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', OLD.schema_name, OLD.index_name);
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
        RETURN NEW;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
--NOTICE: UPGRADE-SCRIPT-NEEDED contents in this file are not auto-upgraded. setup_main will be redefined 
--but not re-run so changes need to be included in upgrade scripts.

CREATE OR REPLACE FUNCTION _timescaledb_internal.setup_main(restore BOOLEAN = FALSE)
    RETURNS void LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    table_name NAME;
BEGIN

    DROP TRIGGER IF EXISTS trigger_main_on_change_chunk_index
    ON _timescaledb_catalog.chunk_index;
    CREATE TRIGGER trigger_main_on_change_chunk_index
    AFTER INSERT OR UPDATE OR DELETE ON _timescaledb_catalog.chunk_index
    FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.on_change_chunk_index();

    -- no DELETE: it would be a no-op
    DROP TRIGGER IF EXISTS trigger_1_main_on_change_hypertable
    ON _timescaledb_catalog.hypertable;
    CREATE TRIGGER trigger_1_main_on_change_hypertable
    AFTER INSERT OR UPDATE OR DELETE ON _timescaledb_catalog.hypertable
    FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.on_change_hypertable();

    -- no DELETE: it would be a no-op
    DROP TRIGGER IF EXISTS trigger_main_on_change_hypertable_index
    ON _timescaledb_catalog.hypertable_index;
    CREATE TRIGGER trigger_main_on_change_hypertable_index
    AFTER INSERT OR UPDATE OR DELETE ON _timescaledb_catalog.hypertable_index
    FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.on_change_hypertable_index();

    -- No support for TRUNCATE currently, so have a trigger to prevent it on
    -- all meta tables.
    FOREACH table_name IN ARRAY ARRAY ['hypertable', 'hypertable_index',
    'dimension', 'dimension_slice', 'chunk_constraint'] :: NAME [] LOOP
        EXECUTE format(
            $$
                DROP TRIGGER IF EXISTS trigger_block_truncate ON _timescaledb_catalog.%1$s;
                CREATE TRIGGER trigger_block_truncate
                BEFORE TRUNCATE ON _timescaledb_catalog.%1$s
                FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_internal.on_truncate_block();
            $$, table_name);
    END LOOP;

    CREATE EVENT TRIGGER ddl_create_index ON ddl_command_end
        WHEN tag IN ('create index')
        EXECUTE PROCEDURE _timescaledb_internal.ddl_process_create_index();

    CREATE EVENT TRIGGER ddl_alter_index ON ddl_command_end
        WHEN tag IN ('alter index')
        EXECUTE PROCEDURE _timescaledb_internal.ddl_process_alter_index();

    CREATE EVENT TRIGGER ddl_drop_index ON sql_drop
        WHEN tag IN ('drop index')
        EXECUTE PROCEDURE _timescaledb_internal.ddl_process_drop_index();

    IF restore THEN
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_create_index;
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_alter_index;
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_drop_index;
    END IF;

END
$BODY$
SET client_min_messages = WARNING -- supress notices for trigger drops
;


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
CREATE OR REPLACE FUNCTION  create_hypertable(
    main_table              REGCLASS,
    time_column_name        NAME,
    partitioning_column     NAME = NULL,
    number_partitions       INTEGER = NULL,
    associated_schema_name  NAME = NULL,
    associated_table_prefix NAME = NULL,
    chunk_time_interval     anyelement = NULL::bigint,
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
<<vars>>
DECLARE
    hypertable_row   _timescaledb_catalog.hypertable;
    table_name                 NAME;
    schema_name                NAME;
    table_owner                NAME;
    tablespace_oid             OID;
    tablespace_name            NAME;
    main_table_has_items       BOOLEAN;
    is_hypertable              BOOLEAN;
    chunk_time_interval_actual BIGINT;
    time_type                  REGTYPE;
BEGIN
    SELECT relname, nspname, reltablespace
    INTO STRICT table_name, schema_name, tablespace_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    SELECT tableowner
    INTO STRICT table_owner
    FROM pg_catalog.pg_tables
    WHERE schemaname = schema_name
          AND tablename = table_name;

    IF table_owner <> session_user THEN
        RAISE 'Must be owner of relation %', table_name
        USING ERRCODE = 'insufficient_privilege';
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

    IF main_table_has_items THEN
        RAISE EXCEPTION 'the table being converted to a hypertable must be empty'
        USING ERRCODE = 'IO102';
    END IF;

    time_type := _timescaledb_internal.dimension_type(main_table, time_column_name, true);

    chunk_time_interval_actual := _timescaledb_internal.time_interval_specification_to_internal(
        time_type, chunk_time_interval, INTERVAL '1 month', 'chunk_time_interval');

    BEGIN
        SELECT *
        INTO hypertable_row
        FROM  _timescaledb_internal.create_hypertable_row(
            main_table,
            schema_name,
            table_name,
            time_column_name,
            partitioning_column,
            number_partitions,
            associated_schema_name,
            associated_table_prefix,
            chunk_time_interval_actual,
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

    PERFORM _timescaledb_internal.add_constraint(hypertable_row.id, oid)
    FROM pg_constraint
    WHERE conrelid = main_table;

    PERFORM 1
    FROM pg_index,
    LATERAL _timescaledb_internal.add_index(
        hypertable_row.id,
        hypertable_row.schema_name,
        (SELECT relname FROM pg_class WHERE oid = indexrelid::regclass),
        _timescaledb_internal.get_general_index_definition(indexrelid, indrelid, hypertable_row)
    )
    WHERE indrelid = main_table AND _timescaledb_internal.need_chunk_index(hypertable_row.id, pg_index.indexrelid)
    ORDER BY pg_index.indexrelid;

    IF create_default_indexes THEN
        PERFORM _timescaledb_internal.create_default_indexes(hypertable_row, main_table, partitioning_column);
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION  add_dimension(
    main_table              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    interval_length         BIGINT = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
<<main_block>>
DECLARE
    table_name       NAME;
    schema_name      NAME;
    hypertable_row   _timescaledb_catalog.hypertable;
BEGIN
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
                                                interval_length);
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

CREATE OR REPLACE FUNCTION attach_tablespace(
       hypertable REGCLASS,
       tablespace NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    main_schema_name  NAME;
    main_table_name   NAME;
    hypertable_id     INTEGER;
    tablespace_oid    OID;
BEGIN
    SELECT nspname, relname
    FROM pg_class c INNER JOIN pg_namespace n
    ON (c.relnamespace = n.oid)
    WHERE c.oid = hypertable
    INTO STRICT main_schema_name, main_table_name;

    SELECT id
    FROM _timescaledb_catalog.hypertable h
    WHERE (h.schema_name = main_schema_name)
    AND (h.table_name = main_table_name)
    INTO hypertable_id;

    IF hypertable_id IS NULL THEN
       RAISE EXCEPTION 'No hypertable "%" exists', main_table_name
       USING ERRCODE = 'IO101';
    END IF;

    PERFORM _timescaledb_internal.attach_tablespace(hypertable_id, tablespace);
END
$BODY$;
/*
  This file creates function to intercept ddl commands and inject
  our own logic to give the illusion that a hypertable is a single table.
  Namely we do 2 things:

  1) When an index on a hypertable is modified, those actions are mirrored
     on chunk tables.
  2) Drop hypertable commands are intercepted to clean up our own metadata tables.

*/

-- Handles ddl create index commands on hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_create_index()
    RETURNS event_trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    info           record;
    table_oid      regclass;
    def            TEXT;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    --NOTE:  pg_event_trigger_ddl_commands prevents this SECURITY DEFINER function from being called outside trigger.
    FOR info IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            -- get table oid
            SELECT indrelid INTO STRICT table_oid
            FROM pg_catalog.pg_index
            WHERE indexrelid = info.objid;

            IF NOT _timescaledb_internal.is_main_table(table_oid) THEN
                RETURN;
            END IF;

            hypertable_row := _timescaledb_internal.hypertable_from_main_table(table_oid);
            def = _timescaledb_internal.get_general_index_definition(info.objid, table_oid, hypertable_row);

            PERFORM _timescaledb_internal.add_index(
                hypertable_row.id,
                hypertable_row.schema_name,
                (SELECT relname FROM pg_class WHERE oid = info.objid::regclass),
                def
            ) WHERE _timescaledb_internal.need_chunk_index(hypertable_row.id, info.objid);
        END LOOP;
END
$BODY$;

-- Handles ddl alter index commands on hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_alter_index()
    RETURNS event_trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    info record;
    table_oid regclass;
BEGIN
    --NOTE:  pg_event_trigger_ddl_commands prevents this SECURITY DEFINER function from being called outside trigger.
    FOR info IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            SELECT indrelid INTO STRICT table_oid
            FROM pg_catalog.pg_index
            WHERE indexrelid = info.objid;

            IF NOT _timescaledb_internal.is_main_table(table_oid) THEN
                RETURN;
            END IF;

            RAISE EXCEPTION 'ALTER INDEX not supported on hypertable %', table_oid
            USING ERRCODE = 'IO101';
        END LOOP;
END
$BODY$;




-- Handles ddl drop index commands on hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_drop_index()
    RETURNS event_trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    info           record;
    table_oid      regclass;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    --NOTE:  pg_event_trigger_ddl_commands prevents this SECURITY DEFINER function from being called outside trigger.
    FOR info IN SELECT * FROM pg_event_trigger_dropped_objects()
        LOOP
            SELECT  format('%I.%I', h.schema_name, h.table_name) INTO table_oid
            FROM _timescaledb_catalog.hypertable h
            INNER JOIN _timescaledb_catalog.hypertable_index i ON (i.hypertable_id = h.id)
            WHERE i.main_schema_name = info.schema_name AND i.main_index_name = info.object_name;

            -- if table_oid is not null, it is a main table
            IF table_oid IS NULL THEN
                RETURN;
            END IF;

            -- TODO: this ignores the concurrently and cascade/restrict modifiers
            PERFORM _timescaledb_internal.drop_index(info.schema_name, info.object_name);
        END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_change_owner(main_table OID, new_table_owner NAME)
    RETURNS void LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = ''
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

-- select_tablespace() is used to assign a tablespace to a chunk.  A
-- tablespace is selected from a set of tablespaces associated with
-- the chunk's hypertable, if any.
CREATE OR REPLACE FUNCTION _timescaledb_internal.select_tablespace(
    hypertable_id INTEGER,
    chunk_dimension_slice_ids      INTEGER[]
)
    RETURNS NAME LANGUAGE PLPGSQL VOLATILE AS
$BODY$
<<main_block>>
DECLARE
    chunk_slice_id INTEGER;
    chunk_slice_index INTEGER;
    dimension_id INTEGER;
    dimension_slice_id INTEGER;
    tablespaces NAME[] = ARRAY(
        SELECT t.tablespace_name FROM _timescaledb_catalog.tablespace t
        WHERE (t.hypertable_id = select_tablespace.hypertable_id)
        ORDER BY id DESC
    );
    all_dimension_slice_ids_for_dimension INT[];
    partitioning_func TEXT;
BEGIN

    IF array_length(tablespaces, 1) = 0 THEN
       RETURN NULL;
    END IF;

    -- Try to pick first closed dimension, otherwise first open.
    -- The partition_func variable will be valid or NULL depending
    -- on the type of dimension found. This can be used to pick
    -- different tablespace assignment strategies depending
    -- on type of dimension.
    SELECT d.id, d.partitioning_func FROM _timescaledb_catalog.dimension d
    WHERE (d.hypertable_id = select_tablespace.hypertable_id)
    ORDER BY partitioning_func NULLS LAST, id DESC
    LIMIT 1
    INTO dimension_id, partitioning_func;

    -- Find all dimension slices for the chosen dimension
    all_dimension_slice_ids_for_dimension := ARRAY(
        SELECT s.id FROM _timescaledb_catalog.dimension_slice s
        WHERE (s.dimension_id = main_block.dimension_id)
        ORDER BY s.id
    );

    -- Find the array index of the chunk's dimension slice
    -- Here chunk_dimension_slice_ids has one slice per dimension of the hypercube
    -- while all_dimension_slice_ids_for_dimension has all the slice for a single dimension.
    SELECT i
    FROM generate_subscripts(all_dimension_slice_ids_for_dimension, 1) AS i
    WHERE all_dimension_slice_ids_for_dimension[i] = ANY(chunk_dimension_slice_ids)
    INTO STRICT chunk_slice_index;

    -- Use the chunk's dimension slice index to pick a tablespace in
    -- the tablespaces array
    RETURN tablespaces[chunk_slice_index % array_length(tablespaces, 1) + 1];
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.select_tablespace(
    hypertable_id INTEGER,
    chunk_id      INTEGER
)
    RETURNS NAME LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_dimension_slice_ids INTEGER[];
BEGIN
    -- Find the chunk's dimension slice ids
    chunk_dimension_slice_ids = ARRAY (
        SELECT s.id FROM _timescaledb_catalog.dimension_slice s
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = s.id)
        INNER JOIN _timescaledb_catalog.chunk c ON (cc.chunk_id = c.id)
        WHERE (c.id = select_tablespace.chunk_id)
    );

    RETURN _timescaledb_internal.select_tablespace(hypertable_id, chunk_dimension_slice_ids);
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.select_tablespace(
    chunk_id INTEGER
)
    RETURNS NAME LANGUAGE SQL AS
$BODY$
    SELECT _timescaledb_internal.select_tablespace(
        (SELECT hypertable_id FROM _timescaledb_catalog.chunk WHERE id = chunk_id),
        chunk_id);
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.attach_tablespace(
       hypertable_id   INTEGER,
       tablespace_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    tablespace_oid OID;
BEGIN
    SELECT oid
    FROM pg_catalog.pg_tablespace
    WHERE spcname = tablespace_name
    INTO tablespace_oid;

    IF tablespace_oid IS NULL THEN
       RAISE EXCEPTION 'No tablespace "%" exists. A tablespace needs to be created before assigning it to a hypertable dimension', tablespace_name
       USING ERRCODE = 'IO101';
    END IF;

    BEGIN
        INSERT INTO _timescaledb_catalog.tablespace (hypertable_id, tablespace_name)
        VALUES (hypertable_id, tablespace_name);
    EXCEPTION
        WHEN unique_violation THEN
            RAISE EXCEPTION 'Tablespace "%" already assigned to hypertable "%"',
            tablespace_name, (SELECT table_name FROM _timescaledb_catalog.hypertable
                              WHERE id = hypertable_id);
    END;
END
$BODY$;
CREATE OR REPLACE FUNCTION _timescaledb_internal.first_sfunc(internal, anyelement, "any")
RETURNS internal
AS '@MODULE_PATHNAME@', 'first_sfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.first_combinefunc(internal, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'first_combinefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any")
RETURNS internal
AS '@MODULE_PATHNAME@', 'last_sfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_combinefunc(internal, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'last_combinefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_finalfunc(internal, anyelement, "any")
RETURNS anyelement
AS '@MODULE_PATHNAME@', 'bookend_finalfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_serializefunc(internal)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'bookend_serializefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_deserializefunc(bytea, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'bookend_deserializefunc'
LANGUAGE C IMMUTABLE;

--This aggregate returns the "first" element of the first argument when ordered by the second argument.
--Ex. first(temp, time) returns the temp value for the row with the lowest time
DROP AGGREGATE IF EXISTS first(anyelement, "any");
CREATE AGGREGATE first(anyelement, "any") (
    SFUNC = _timescaledb_internal.first_sfunc,
    STYPE = internal,
    COMBINEFUNC = _timescaledb_internal.first_combinefunc,
    SERIALFUNC = _timescaledb_internal.bookend_serializefunc,
    DESERIALFUNC = _timescaledb_internal.bookend_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_internal.bookend_finalfunc,
    FINALFUNC_EXTRA
);

--This aggregate returns the "last" element of the first argument when ordered by the second argument.
--Ex. last(temp, time) returns the temp value for the row with highest time
DROP AGGREGATE IF EXISTS last(anyelement, "any");
CREATE AGGREGATE last(anyelement, "any") (
    SFUNC = _timescaledb_internal.last_sfunc,
    STYPE = internal,
    COMBINEFUNC = _timescaledb_internal.last_combinefunc,
    SERIALFUNC = _timescaledb_internal.bookend_serializefunc,
    DESERIALFUNC = _timescaledb_internal.bookend_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_internal.bookend_finalfunc,
    FINALFUNC_EXTRA
);


-- time_bucket returns the left edge of the bucket where ts falls into. 
-- Buckets span an interval of time equal to the bucket_width and are aligned with the epoch.
CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- bucketing of timestamptz happens at UTC time
CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

--bucketing on date should not do any timezone conversion
CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts DATE) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- If an interval is given as the third argument, the bucket alignment is offset by the interval.
CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL)
    RETURNS TIMESTAMP LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT public.time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT public.time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INTERVAL, ts DATE, "offset" INTERVAL)
    RETURNS DATE LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (public.time_bucket(bucket_width, ts-"offset")+"offset")::date;
$BODY$;


CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width BIGINT, ts BIGINT)
    RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INT, ts INT)
    RETURNS INT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width SMALLINT, ts SMALLINT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT)
    RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width INT, ts INT, "offset" INT)
    RETURNS INT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION public.time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_git_commit() RETURNS TEXT
	AS '@MODULE_PATHNAME@', 'get_git_commit' LANGUAGE C IMMUTABLE STRICT;
-- this trigger function causes an invalidation event on the table whose name is
-- passed in as the first element.
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache_trigger()
 RETURNS TRIGGER AS '@MODULE_PATHNAME@', 'invalidate_relcache_trigger' LANGUAGE C;

-- This function is only used for debugging
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache(proxy_oid OID)
 RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'invalidate_relcache' LANGUAGE C;



-- This file contains utility functions to get the relation size
-- of hypertables, chunks, and indexes on hypertables.

-- Get relation size of hypertable
-- like pg_relation_size(hypertable)
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- table_bytes        - Disk space used by main_table (like pg_relation_size(main_table))
-- index_bytes        - Disc space used by indexes
-- toast_bytes        - Disc space of toast tables
-- total_bytes        - Total disk space used by the specified table, including all indexes and TOAST data

CREATE OR REPLACE FUNCTION hypertable_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT
               ) LANGUAGE PLPGSQL STABLE
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$
        SELECT table_bytes,
               index_bytes,
               toast_bytes,
               total_bytes
               FROM (
               SELECT *, total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes FROM (
                      SELECT
                      sum(pg_total_relation_size('"' || c.schema_name || '"."' || c.table_name || '"'))::bigint as total_bytes,
                      sum(pg_indexes_size('"' || c.schema_name || '"."' || c.table_name || '"'))::bigint AS index_bytes,
                      sum(pg_total_relation_size(reltoastrelid))::bigint AS toast_bytes
                      FROM
                      _timescaledb_catalog.hypertable h,
                      _timescaledb_catalog.chunk c,
                      pg_class pgc,
                      pg_namespace pns
                      WHERE h.schema_name = %L
                      AND h.table_name = %L
                      AND c.hypertable_id = h.id
                      AND pgc.relname = h.table_name
                      AND pns.oid = pgc.relnamespace
                      AND pns.nspname = h.schema_name
                      AND relkind = 'r'
                      ) sub1
               ) sub2;
        $$,
        schema_name, table_name);

END;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.range_value_to_pretty(
    time_value      BIGINT,
    column_type     REGTYPE
)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
    IF time_value IS NULL THEN
        RETURN format('%L', NULL);
    END IF;
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); -- scale determined by user.
      WHEN 'TIMESTAMP'::regtype, 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('%1$L', _timescaledb_internal.to_timestamp(time_value)); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))::date);
      ELSE
        RETURN time_value;
    END CASE;
END
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.partitioning_column_to_pretty(
    d   _timescaledb_catalog.dimension
)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
BEGIN
        IF d.partitioning_func IS NULL THEN
           RETURN d.column_name;
        ELSE
           RETURN d.partitioning_func_schema || '.' || d.partitioning_func
                  || '(' || d.column_name || ')';
        END IF;
END
$BODY$;


-- Get relation size of hypertable
-- like pg_relation_size(hypertable)
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- table_size         - Pretty output of table_bytes
-- index_bytes        - Pretty output of index_bytes
-- toast_bytes        - Pretty output of toast_bytes
-- total_size         - Pretty output of total_bytes

CREATE OR REPLACE FUNCTION hypertable_relation_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT) LANGUAGE PLPGSQL STABLE
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        RETURN QUERY
        SELECT pg_size_pretty(table_bytes) as table,
               pg_size_pretty(index_bytes) as index,
               pg_size_pretty(toast_bytes) as toast,
               pg_size_pretty(total_bytes) as total
               FROM hypertable_relation_size(main_table);

END;
$BODY$;


-- Get relation size of the chunks of an hypertable
-- like pg_relation_size
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_id                      - Timescaledb id of a chunk
-- chunk_table                   - Table used for the chunk
-- partitioning_columns          - Partitioning column names
-- partitioning_column_types     - Type of partitioning columns
-- partitioning_hash_functions   - Hash functions of partitioning columns
-- ranges                        - Partition ranges for each dimension of the chunk
-- table_bytes                   - Disk space used by main_table
-- index_bytes                   - Disk space used by indexes
-- toast_bytes                   - Disc space of toast tables
-- total_bytes                   - Disk space used in total

CREATE OR REPLACE FUNCTION chunk_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               partitioning_columns NAME[],
               partitioning_column_types REGTYPE[],
               partitioning_hash_functions TEXT[],
               ranges int8range[],
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT)
               LANGUAGE PLPGSQL STABLE
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$

        SELECT chunk_id,
        chunk_table,
        partitioning_columns,
        partitioning_column_types,
        partitioning_hash_functions,
        ranges,
        table_bytes,
        index_bytes,
        toast_bytes,
        total_bytes
        FROM (
        SELECT *,
              total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
              FROM (
               SELECT c.id as chunk_id,
               '"' || c.schema_name || '"."' || c.table_name || '"' as chunk_table,
               pg_total_relation_size('"' || c.schema_name || '"."' || c.table_name || '"') AS total_bytes,
               pg_indexes_size('"' || c.schema_name || '"."' || c.table_name || '"') AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes,
               array_agg(d.column_name ORDER BY d.interval_length, d.column_name ASC) as partitioning_columns,
               array_agg(d.column_type ORDER BY d.interval_length, d.column_name ASC) as partitioning_column_types,
               array_agg(d.partitioning_func_schema || '.' || d.partitioning_func ORDER BY d.interval_length, d.column_name ASC) as partitioning_hash_functions,
               array_agg(int8range(range_start, range_end) ORDER BY d.interval_length, d.column_name ASC) as ranges
               FROM
               _timescaledb_catalog.hypertable h,
               _timescaledb_catalog.chunk c,
               _timescaledb_catalog.chunk_constraint cc,
               _timescaledb_catalog.dimension d,
               _timescaledb_catalog.dimension_slice ds,
               pg_class pgc,
               pg_namespace pns
               WHERE h.schema_name = %L
                     AND h.table_name = %L
                     AND pgc.relname = h.table_name
                     AND pns.oid = pgc.relnamespace
                     AND pns.nspname = h.schema_name
                     AND relkind = 'r'
                     AND c.hypertable_id = h.id
                     AND c.id = cc.chunk_id
                     AND cc.dimension_slice_id = ds.id
                     AND ds.dimension_id = d.id
               GROUP BY c.id, pgc.reltoastrelid, pgc.oid ORDER BY c.id
               ) sub1
        ) sub2;
        $$,
        schema_name, table_name);

END;
$BODY$;

-- Get relation size of the chunks of an hypertable
-- like pg_relation_size
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_id                      - Timescaledb id of a chunk
-- chunk_table                   - Table used for the chunk
-- partitioning_columns          - Partitioning column names
-- partitioning_column_types     - Type of partitioning columns
-- partitioning_hash_functions   - Hash functions of partitioning columns
-- ranges                        - Partition ranges for each dimension of the chunk
-- table_size                    - Pretty output of table_bytes
-- index_size                    - Pretty output of index_bytes
-- toast_size                    - Pretty output of toast_bytes
-- total_size                    - Pretty output of total_bytes


CREATE OR REPLACE FUNCTION chunk_relation_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               partitioning_columns NAME[],
               partitioning_column_types REGTYPE[],
               partitioning_hash_functions TEXT[],
               ranges TEXT[],
               table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT
               )
               LANGUAGE PLPGSQL STABLE
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$

        SELECT chunk_id,
        chunk_table,
        partitioning_columns,
        partitioning_column_types,
        partitioning_functions,
        ranges,
        pg_size_pretty(table_bytes) AS table,
        pg_size_pretty(index_bytes) AS index,
        pg_size_pretty(toast_bytes) AS toast,
        pg_size_pretty(total_bytes) AS total
        FROM (
        SELECT *,
              total_bytes-index_bytes-COALESCE(toast_bytes,0) AS table_bytes
              FROM (
               SELECT c.id as chunk_id,
               '"' || c.schema_name || '"."' || c.table_name || '"' as chunk_table,
               pg_total_relation_size('"' || c.schema_name || '"."' || c.table_name || '"') AS total_bytes,
               pg_indexes_size('"' || c.schema_name || '"."' || c.table_name || '"') AS index_bytes,
               pg_total_relation_size(reltoastrelid) AS toast_bytes,
               array_agg(d.column_name ORDER BY d.interval_length, d.column_name ASC) as partitioning_columns,
               array_agg(d.column_type ORDER BY d.interval_length, d.column_name ASC) as partitioning_column_types,
               array_agg(d.partitioning_func_schema || '.' || d.partitioning_func ORDER BY d.interval_length, d.column_name ASC) as partitioning_functions,
               array_agg('[' || _timescaledb_internal.range_value_to_pretty(range_start, column_type) ||
                         ',' ||
                         _timescaledb_internal.range_value_to_pretty(range_end, column_type) || ')' ORDER BY d.interval_length, d.column_name ASC) as ranges
               FROM
               _timescaledb_catalog.hypertable h,
               _timescaledb_catalog.chunk c,
               _timescaledb_catalog.chunk_constraint cc,
               _timescaledb_catalog.dimension d,
               _timescaledb_catalog.dimension_slice ds,
               pg_class pgc,
               pg_namespace pns
               WHERE h.schema_name = %L
                     AND h.table_name = %L
                     AND pgc.relname = h.table_name
                     AND pns.oid = pgc.relnamespace
                     AND pns.nspname = h.schema_name
                     AND relkind = 'r'
                     AND c.hypertable_id = h.id
                     AND c.id = cc.chunk_id
                     AND cc.dimension_slice_id = ds.id
                     AND ds.dimension_id = d.id
               GROUP BY c.id, pgc.reltoastrelid, pgc.oid ORDER BY c.id
               ) sub1
        ) sub2;
        $$,
        schema_name, table_name);

END;
$BODY$;


-- Get sizes of indexes on a hypertable
--
-- main_table - hypertable to get index sizes of
--
-- Returns:
-- index_name           - index on hyper table
-- total_bytes          - size of index on disk

CREATE OR REPLACE FUNCTION indexes_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (index_name TEXT,
               total_bytes BIGINT)
               LANGUAGE PLPGSQL STABLE
               AS
$BODY$
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY EXECUTE format(
        $$
        SELECT hi.main_schema_name || '.' || hi.main_index_name,
               sum(pg_relation_size('"' || ci.schema_name || '"."' || ci.index_name || '"'))::bigint
        FROM
        _timescaledb_catalog.hypertable h,
        _timescaledb_catalog.hypertable_index hi,
        _timescaledb_catalog.chunk_index ci
        WHERE h.id = hi.hypertable_id
              AND h.schema_name = %L
              AND h.table_name = %L
              AND ci.main_index_name = hi.main_index_name
              AND ci.main_schema_name = hi.main_schema_name
        GROUP BY hi.main_schema_name || '.' || hi.main_index_name;
        $$,
        schema_name, table_name);

END;
$BODY$;


-- Get sizes of indexes on a hypertable
--
-- main_table - hypertable to get index sizes of
--
-- Returns:
-- index_name           - index on hyper table
-- total_size           - pretty output of total_bytes

CREATE OR REPLACE FUNCTION indexes_relation_size_pretty(
    main_table              REGCLASS
)
RETURNS TABLE (index_name_ TEXT,
               total_size  TEXT) LANGUAGE PLPGSQL STABLE
               AS
$BODY$
BEGIN
        RETURN QUERY
        SELECT index_name,
               pg_size_pretty(total_bytes)
        FROM indexes_relation_size(main_table);
END;
$BODY$;
CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_sfunc (state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTERNAL
AS '@MODULE_PATHNAME@', 'hist_sfunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_combinefunc(state1 INTERNAL, state2 INTERNAL)
RETURNS INTERNAL
AS '@MODULE_PATHNAME@', 'hist_combinefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_serializefunc(INTERNAL)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'hist_serializefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_deserializefunc(bytea, INTERNAL)
RETURNS INTERNAL
AS '@MODULE_PATHNAME@', 'hist_deserializefunc'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTEGER[]
AS '@MODULE_PATHNAME@', 'hist_finalfunc'
LANGUAGE C IMMUTABLE;

-- Tell Postgres how to use the new function
DROP AGGREGATE IF EXISTS histogram (DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, INTEGER);
CREATE AGGREGATE histogram (DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, INTEGER) (
    SFUNC = _timescaledb_internal.hist_sfunc,
    STYPE = INTERNAL,
    COMBINEFUNC = _timescaledb_internal.hist_combinefunc,
    SERIALFUNC = _timescaledb_internal.hist_serializefunc,
    DESERIALFUNC = _timescaledb_internal.hist_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = _timescaledb_internal.hist_finalfunc,
    FINALFUNC_EXTRA
);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_create(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_get(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_create_after_lock(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.dimension_calculate_default_range_closed(BIGINT, SMALLINT, BIGINT, OUT BIGINT, OUT BIGINT);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.dimension_calculate_default_range(INTEGER, BIGINT, OUT BIGINT, OUT BIGINT);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_calculate_new_ranges(INTEGER, BIGINT, INTEGER[], BIGINT[], BOOLEAN, OUT BIGINT, OUT BIGINT);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_id_get_by_dimensions(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_get_dimensions_constraint_sql(INTEGER[], BIGINT[]);
ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.chunk_get_dimension_constraint_sql(INTEGER, BIGINT);

DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_create_after_lock(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_calculate_default_range_closed(BIGINT, SMALLINT, BIGINT, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.dimension_calculate_default_range(INTEGER, BIGINT, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_calculate_new_ranges(INTEGER, BIGINT, INTEGER[], BIGINT[], BOOLEAN, OUT BIGINT, OUT BIGINT) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_id_get_by_dimensions(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get_dimensions_constraint_sql(INTEGER[], BIGINT[]) CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_get_dimension_constraint_sql(INTEGER, BIGINT) CASCADE;

SELECT _timescaledb_internal.add_constraint(h.id, c.oid)
FROM _timescaledb_catalog.hypertable h
INNER JOIN pg_constraint c ON (c.conrelid = format('%I.%I', h.schema_name, h.table_name)::regclass);

DELETE FROM _timescaledb_catalog.hypertable_index hi
WHERE EXISTS (
 SELECT 1 FROM pg_constraint WHERE conindid = format('%I.%I', hi.main_schema_name, hi.main_index_name)::regclass
);

ALTER TABLE _timescaledb_catalog.chunk
DROP CONSTRAINT chunk_hypertable_id_fkey,
ADD CONSTRAINT chunk_hypertable_id_fkey
  FOREIGN KEY (hypertable_id) 
  REFERENCES _timescaledb_catalog.hypertable(id);

ALTER TABLE _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_chunk_id_fkey,
ADD CONSTRAINT chunk_constraint_chunk_id_fkey
  FOREIGN KEY (chunk_id) 
  REFERENCES _timescaledb_catalog.chunk(id);

ALTER TABLE _timescaledb_catalog.chunk_constraint
DROP CONSTRAINT chunk_constraint_dimension_slice_id_fkey,
ADD CONSTRAINT chunk_constraint_dimension_slice_id_fkey
  FOREIGN KEY (dimension_slice_id) 
  REFERENCES _timescaledb_catalog.dimension_slice(id);


DROP EVENT TRIGGER ddl_check_drop_command;

DROP TRIGGER trigger_main_on_change_chunk ON _timescaledb_catalog.chunk;

DROP FUNCTION _timescaledb_internal.chunk_create_table(int);
DROP FUNCTION _timescaledb_internal.ddl_process_drop_table();
DROP FUNCTION _timescaledb_internal.on_change_chunk();
DROP FUNCTION _timescaledb_internal.drop_hypertable(name, name);

DROP EVENT TRIGGER ddl_create_trigger;
DROP EVENT TRIGGER ddl_drop_trigger;

DROP FUNCTION _timescaledb_internal.add_trigger(int, oid);
DROP FUNCTION _timescaledb_internal.create_chunk_trigger(int, name, text);
DROP FUNCTION _timescaledb_internal.create_trigger_on_all_chunks(int, name, text);
DROP FUNCTION _timescaledb_internal.ddl_process_create_trigger();
DROP FUNCTION _timescaledb_internal.ddl_process_drop_trigger();
DROP FUNCTION _timescaledb_internal.drop_chunk_trigger(int, name);
DROP FUNCTION _timescaledb_internal.drop_trigger_on_all_chunks(INTEGER, NAME);
DROP FUNCTION _timescaledb_internal.get_general_trigger_definition(regclass);
DROP FUNCTION _timescaledb_internal.get_trigger_definition_for_table(INTEGER, text);
DROP FUNCTION _timescaledb_internal.need_chunk_trigger(int, oid);
