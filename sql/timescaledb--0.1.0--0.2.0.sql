ALTER TABLE _timescaledb_catalog.hypertable ADD UNIQUE (id, schema_name);

ALTER TABLE _timescaledb_catalog.hypertable_index
DROP CONSTRAINT hypertable_index_hypertable_id_fkey,
ADD CONSTRAINT hypertable_index_hypertable_id_fkey
FOREIGN KEY (hypertable_id, main_schema_name)
REFERENCES _timescaledb_catalog.hypertable(id, schema_name)
ON UPDATE CASCADE
ON DELETE CASCADE;

ALTER TABLE _timescaledb_catalog.chunk_index
DROP CONSTRAINT chunk_index_main_schema_name_fkey,
ADD CONSTRAINT chunk_index_main_schema_name_fkey
FOREIGN KEY (main_schema_name, main_index_name) 
REFERENCES _timescaledb_catalog.hypertable_index(main_schema_name, main_index_name) 
ON UPDATE CASCADE 
ON DELETE CASCADE;
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

-- Add a trigger to a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.need_chunk_trigger(
    hypertable_id INTEGER,
    trigger_oid OID
)
    RETURNS BOOLEAN LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    trigger_row record;
BEGIN
    SELECT * INTO STRICT trigger_row FROM pg_trigger WHERE OID = trigger_oid;

    IF trigger_row.tgname = ANY(_timescaledb_internal.timescale_trigger_names()) THEN
        RETURN FALSE;
    END IF;

    IF (trigger_row.tgtype & (1 << 2) != 0) THEN
        --is INSERT trigger
        IF (trigger_row.tgtype & (1 << 3) != 0) OR  (trigger_row.tgtype & (1 << 4) != 0) THEN
            RAISE 'Combining INSERT triggers with UPDATE or DELETE triggers is not supported.'
            USING HINT = 'Please define separate triggers for each operation';
        END IF;
        IF (trigger_row.tgtype & ((1 << 1) | (1 << 6)) = 0) THEN
            RAISE 'AFTER trigger on INSERT is not supported: %.', trigger_row.tgname;
        END IF;
    ELSE
        IF (trigger_row.tgtype & (1 << 0) != 0) THEN
            RETURN TRUE;
        END IF;
    END IF;
    RETURN FALSE;
END
$BODY$;

-- Add a trigger to a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.add_trigger(
    hypertable_id INTEGER,
    trigger_oid OID
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    trigger_row record;
BEGIN
    IF _timescaledb_internal.need_chunk_trigger(hypertable_id, trigger_oid) THEN
        SELECT * INTO STRICT trigger_row FROM pg_trigger WHERE OID = trigger_oid;
        PERFORM _timescaledb_internal.create_trigger_on_all_chunks(hypertable_id, trigger_row.tgname, 
                _timescaledb_internal.get_general_trigger_definition(trigger_oid));
    END IF;
END
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



-- Create the "general definition" of a trigger. The general definition
-- is the corresponding create trigger command with the placeholders /*TABLE_NAME*/
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_general_trigger_definition(
    trigger_oid       REGCLASS
)
RETURNS text
LANGUAGE plpgsql VOLATILE AS
$BODY$
DECLARE
    def             TEXT;
    c               INTEGER;
    trigger_row     RECORD;
BEGIN
    -- Get trigger definition
    def := pg_get_triggerdef(trigger_oid);

    IF def IS NULL THEN
        RAISE EXCEPTION 'Cannot process trigger with no definition: %', trigger_oid::TEXT;
    END IF;

    SELECT * INTO STRICT trigger_row FROM pg_trigger WHERE oid = trigger_oid;

    SELECT count(*) INTO c
    FROM regexp_matches(def, 'ON '|| trigger_row.tgrelid::regclass::TEXT, 'g');
    IF c <> 1 THEN
         RAISE EXCEPTION 'Cannot process trigger with definition(no table name match: %): %', trigger_row.tgrelid::regclass::TEXT, def
         USING ERRCODE = 'IO103';
    END IF;

    def := replace(def,  'ON '|| trigger_row.tgrelid::regclass::TEXT, 'ON /*TABLE_NAME*/');
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
    new_schema     TEXT,
    new_table_name TEXT
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

CREATE OR REPLACE FUNCTION _timescaledb_internal.timescale_trigger_names()
    RETURNS text[] LANGUAGE SQL IMMUTABLE AS
$BODY$
   SELECT array['_timescaledb_main_insert_trigger', '_timescaledb_main_after_insert_trigger'];
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
    chunk_id INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    tablespace_clause TEXT := '';
    tablespace_name NAME;
    table_owner     NAME;
    tablespace_oid  OID;
BEGIN
    SELECT * INTO STRICT chunk_row
    FROM _timescaledb_catalog.chunk
    WHERE id = chunk_id;

    SELECT * INTO STRICT hypertable_row
    FROM _timescaledb_catalog.hypertable
    WHERE id = chunk_row.hypertable_id;

    tablespace_name := _timescaledb_internal.select_tablespace(chunk_row.hypertable_id,
                                                               chunk_row.id);
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

    PERFORM _timescaledb_internal.chunk_add_constraints(chunk_id);
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

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_add_constraints(
    chunk_id  INTEGER
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    constraint_row record;
BEGIN
    FOR constraint_row IN
        SELECT c.schema_name, c.table_name, ds.id as dimension_slice_id
        FROM _timescaledb_catalog.chunk c
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.chunk_id = c.id)
        INNER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
        WHERE c.id = chunk_add_constraints.chunk_id
        LOOP
        EXECUTE format(
            $$
                ALTER TABLE %1$I.%2$I
                ADD CONSTRAINT constraint_%3$s CHECK(%4$s)
            $$,
            constraint_row.schema_name, constraint_row.table_name,
            constraint_row.dimension_slice_id,
            _timescaledb_internal.dimension_slice_get_constraint_sql(constraint_row.dimension_slice_id)
        );
    END LOOP;
END
$BODY$;
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get_dimension_constraint_sql(
    dimension_id        INTEGER,
    dimension_value     BIGINT
)
 RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format($$
    SELECT cc.chunk_id
    FROM _timescaledb_catalog.dimension_slice ds
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id)
    WHERE ds.dimension_id = %1$L and ds.range_start <= %2$L and ds.range_end > %2$L
    $$,
    dimension_id, dimension_value);
$BODY$;

-- get a chunk if it exists
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get_dimensions_constraint_sql(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$

SELECT string_agg(_timescaledb_internal.chunk_get_dimension_constraint_sql(dimension_id,
                                                                dimension_value),
                                                            ' INTERSECT ')
FROM (SELECT unnest(dimension_ids) AS dimension_id,
             unnest(dimension_values) AS dimension_value
     ) AS sub;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_id_get_by_dimensions(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS SETOF INTEGER LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    IF array_length(dimension_ids, 1) > 0 THEN
        RETURN QUERY EXECUTE _timescaledb_internal.chunk_get_dimensions_constraint_sql(dimension_ids,
        dimension_values);
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    SELECT *
    INTO chunk_row
    FROM _timescaledb_catalog.chunk
    WHERE
    id = (SELECT _timescaledb_internal.chunk_id_get_by_dimensions(dimension_ids,
                                                          dimension_values));
    RETURN chunk_row;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
END
$BODY$;

--todo: unit test
CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range(
        dimension_id      INTEGER,
        dimension_value   BIGINT,
    OUT range_start  BIGINT,
    OUT range_end    BIGINT)
LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    dimension_row _timescaledb_catalog.dimension;
    inter BIGINT;
BEGIN
    SELECT *
    FROM _timescaledb_catalog.dimension
    INTO STRICT dimension_row
    WHERE id = dimension_id;

    IF dimension_row.interval_length IS NOT NULL THEN
        range_start := (dimension_value / dimension_row.interval_length) * dimension_row.interval_length;
        range_end := range_start + dimension_row.interval_length;
    ELSE
       inter := (2147483647 / dimension_row.num_slices);
       IF dimension_value >= inter * (dimension_row.num_slices - 1) THEN
          --put overflow from integer-division errors in last range
          range_start = inter * (dimension_row.num_slices - 1);
          range_end = 2147483647;
       ELSE
          range_start = (dimension_value / inter) * inter;
          range_end := range_start + inter;
       END IF;
    END IF;
END
$BODY$;

-- calculate the range for a free dimension.
-- assumes one other fixed dimension.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_calculate_new_ranges(
        free_dimension_id      INTEGER,
        free_dimension_value   BIGINT,
        fixed_dimension_ids    INTEGER[],
        fixed_dimension_values BIGINT[],
        align                  BOOLEAN,
    OUT new_range_start        BIGINT,
    OUT new_range_end          BIGINT
)
LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    overlap_value BIGINT;
    alignment_found BOOLEAN := FALSE;
BEGIN
    new_range_start := NULL;
    new_range_end := NULL;

    IF align THEN
        --if i am aligning then fix see if other chunks have values that fit me in the free dimension
        SELECT free_slice.range_start, free_slice.range_end
        INTO new_range_start, new_range_end
        FROM _timescaledb_catalog.chunk c
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.chunk_id = c.id)
        INNER JOIN _timescaledb_catalog.dimension_slice free_slice ON (free_slice.id = cc.dimension_slice_id AND free_slice.dimension_id = free_dimension_id)
        WHERE
           free_slice.range_end > free_dimension_value and free_slice.range_start <= free_dimension_value
        LIMIT 1;

        SELECT new_range_start IS NOT NULL INTO alignment_found;
    END IF;

    IF NOT alignment_found THEN
        --either not aligned or did not find an alignment
        SELECT *
        INTO new_range_start, new_range_end
        FROM _timescaledb_internal.dimension_calculate_default_range(free_dimension_id, free_dimension_value);
    END IF;

    -- Check whether the new chunk interval overlaps with existing chunks.
    -- new_range_start overlaps
    SELECT free_slice.range_end
    INTO overlap_value
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.chunk_id = c.id)
    INNER JOIN _timescaledb_catalog.dimension_slice free_slice ON (free_slice.id = cc.dimension_slice_id AND free_slice.dimension_id = free_dimension_id)
    WHERE
    c.id = (
               SELECT _timescaledb_internal.chunk_id_get_by_dimensions(free_dimension_id || fixed_dimension_ids,
                                                         new_range_start || fixed_dimension_values)
           )
    ORDER BY free_slice.range_end DESC
    LIMIT 1;

    IF FOUND THEN
        -- There is a chunk that overlaps with new_range_start, cut
        -- new_range_start to begin where that chunk ends
        IF alignment_found THEN
            RAISE EXCEPTION 'Should never happen: needed to cut an aligned dimension.
            Free_dimension %. Existing(end): %, New(start):%',
            free_dimension_id, overlap_value, new_range_start
            USING ERRCODE = 'IO501';
        END IF;
        new_range_start := overlap_value;
    END IF;

    --check for new_range_end overlap
    SELECT free_slice.range_start
    INTO overlap_value
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.chunk_constraint cc
    ON (cc.chunk_id = c.id)
    INNER JOIN _timescaledb_catalog.dimension_slice free_slice
    ON (free_slice.id = cc.dimension_slice_id AND free_slice.dimension_id = free_dimension_id)
    WHERE
    c.id = (
        SELECT _timescaledb_internal.chunk_id_get_by_dimensions(free_dimension_id || fixed_dimension_ids,
                                                         new_range_end - 1 || fixed_dimension_values)
    )
    ORDER BY free_slice.range_start ASC
    LIMIT 1;

    IF FOUND THEN
        -- there is at least one table that starts inside, cut the end to match
        IF alignment_found THEN
            RAISE EXCEPTION 'Should never happen: needed to cut an aligned dimension.
            Free_dimension %. Existing(start): %, New(end):%',
            free_dimension_id, overlap_value, new_range_end
            USING ERRCODE = 'IO501';
        END IF;
        new_range_end := overlap_value;
    END IF;
END
$BODY$;

-- creates the row in the chunk table. Prerequisite: appropriate lock.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create_after_lock(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    dimension_row _timescaledb_catalog.dimension;
    hypertable_id INTEGER;
    free_index INTEGER;
    fixed_dimension_ids INTEGER[];
    fixed_values BIGINT[];
    free_range_start BIGINT;
    free_range_end BIGINT;
    slice_ids INTEGER[];
    slice_id INTEGER;
BEGIN
    SELECT d.hypertable_id
    INTO STRICT hypertable_id
    FROM _timescaledb_catalog.dimension d
    WHERE d.id = dimension_ids[1];

    slice_ids = NULL;
    FOR free_index IN 1 .. array_upper(dimension_ids, 1)
    LOOP
        --keep one dimension free and the rest fixed
        fixed_dimension_ids = dimension_ids[:free_index-1]
                              || dimension_ids[free_index+1:];
        fixed_values = dimension_values[:free_index-1]
                              || dimension_values[free_index+1:];

        SELECT *
        INTO STRICT dimension_row
        FROM _timescaledb_catalog.dimension
        WHERE id = dimension_ids[free_index];

        SELECT *
        INTO free_range_start, free_range_end
        FROM _timescaledb_internal.chunk_calculate_new_ranges(
            dimension_ids[free_index], dimension_values[free_index],
            fixed_dimension_ids, fixed_values, dimension_row.aligned);

        --do not use RETURNING here (ON CONFLICT DO NOTHING)
        INSERT INTO  _timescaledb_catalog.dimension_slice
        (dimension_id, range_start, range_end)
        VALUES(dimension_ids[free_index], free_range_start, free_range_end)
        ON CONFLICT DO NOTHING;

        SELECT id INTO STRICT slice_id
        FROM _timescaledb_catalog.dimension_slice ds
        WHERE ds.dimension_id = dimension_ids[free_index] AND
        ds.range_start = free_range_start AND ds.range_end = free_range_end;

        slice_ids = slice_ids || slice_id;
    END LOOP;

    WITH chunk AS (
        INSERT INTO _timescaledb_catalog.chunk (id, hypertable_id, schema_name, table_name)
        SELECT seq_id, h.id, h.associated_schema_name,
           format('%s_%s_chunk', h.associated_table_prefix, seq_id)
        FROM
        nextval(pg_get_serial_sequence('_timescaledb_catalog.chunk','id')) seq_id,
        _timescaledb_catalog.hypertable h
        WHERE h.id = hypertable_id
        RETURNING *
    )
    INSERT INTO _timescaledb_catalog.chunk_constraint (dimension_slice_id, chunk_id)
    SELECT slice_id_to_insert, chunk.id FROM chunk, unnest(slice_ids) AS slice_id_to_insert;
END
$BODY$;

-- Creates and returns a new chunk, taking a lock on the chunk table.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    chunk_row     _timescaledb_catalog.chunk;
BEGIN
    LOCK TABLE _timescaledb_catalog.chunk IN EXCLUSIVE MODE;

    -- recheck:
    chunk_row := _timescaledb_internal.chunk_get(dimension_ids, dimension_values);

    IF chunk_row IS NULL THEN
        PERFORM _timescaledb_internal.chunk_create_after_lock(dimension_ids, dimension_values);
        chunk_row := _timescaledb_internal.chunk_get(dimension_ids, dimension_values);
    END IF;

    IF chunk_row IS NULL THEN -- recheck
        RAISE EXCEPTION 'Should never happen: chunk not found after creation'
        USING ERRCODE = 'IO501';
    END IF;

    RETURN chunk_row;
END
$BODY$;

-- Trigger for when chunk rows are changed.
-- On Insert: create chunk table, add indexes, add triggers.
-- On Delete: drop table
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    kind                  pg_class.relkind%type;
    hypertable_row        _timescaledb_catalog.hypertable;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM _timescaledb_internal.chunk_create_table(NEW.id);

        PERFORM _timescaledb_internal.create_chunk_index_row(NEW.schema_name, NEW.table_name,
                                hi.main_schema_name, hi.main_index_name, hi.definition)
        FROM _timescaledb_catalog.hypertable_index hi
        WHERE hi.hypertable_id = NEW.hypertable_id;

        SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable WHERE id = NEW.hypertable_id;

        PERFORM _timescaledb_internal.create_chunk_trigger(NEW.id, tgname,
            _timescaledb_internal.get_general_trigger_definition(oid))
        FROM pg_trigger
        WHERE tgrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass
        AND _timescaledb_internal.need_chunk_trigger(NEW.hypertable_id, oid);

        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        -- when deleting the chunk row from the metadata table,
        -- also DROP the actual chunk table that holds data.
        -- Note that the table could already be deleted in case this
        -- trigger fires as a result of a DROP TABLE on the hypertable
        -- that this chunk belongs to.

        EXECUTE format(
                $$
                SELECT c.relkind FROM pg_class c WHERE relname = '%I' AND relnamespace = '%I'::regnamespace
                $$, OLD.table_name, OLD.schema_name
        ) INTO kind;

        IF kind IS NULL THEN
            RETURN OLD;
        END IF;

        EXECUTE format(
            $$
            DROP TABLE %I.%I
            $$, OLD.schema_name, OLD.table_name
        );
        RETURN OLD;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
-- Convert a general trigger definition to a create trigger sql command for a
-- particular table and trigger name.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_trigger_definition_for_table(
    chunk_id INTEGER,
    general_definition TEXT
  )
    RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
    sql_code TEXT;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk WHERE id = chunk_id;
    sql_code := replace(general_definition, '/*TABLE_NAME*/', format('%I.%I', chunk_row.schema_name, chunk_row.table_name));
    RETURN sql_code;
END
$BODY$;

-- Creates a chunk_trigger_row.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_trigger(
    chunk_id INTEGER,
    trigger_name NAME,
    def TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code    TEXT;
BEGIN
    sql_code := _timescaledb_internal.get_trigger_definition_for_table(chunk_id, def);
    EXECUTE sql_code;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk_trigger(
    chunk_id INTEGER,
    trigger_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk WHERE id = chunk_id;
    EXECUTE format($$ DROP TRIGGER IF EXISTS %I ON %I.%I $$, trigger_name, chunk_row.schema_name, chunk_row.table_name);
END
$BODY$
SET client_min_messages = WARNING;

-- Creates a trigger on all chunk for a hypertable.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_trigger_on_all_chunks(
    hypertable_id INTEGER,
    trigger_name     NAME,
    definition       TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_internal.create_chunk_trigger(c.id, trigger_name, definition)
    FROM _timescaledb_catalog.chunk c
    WHERE c.hypertable_id = create_trigger_on_all_chunks.hypertable_id;
END
$BODY$;

-- Drops trigger on all chunks for a hypertable.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_trigger_on_all_chunks(
    hypertable_id INTEGER,
    trigger_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM _timescaledb_internal.drop_chunk_trigger(c.id, trigger_name)
    FROM _timescaledb_catalog.chunk c
    WHERE c.hypertable_id = drop_trigger_on_all_chunks.hypertable_id;
END
$BODY$;



-- This file contains triggers that act on the main 'hypertable' table as
-- well as triggers for newly created hypertables.

-- These trigger functions intercept regular inserts and implement our smart insert fastpath
CREATE OR REPLACE FUNCTION  _timescaledb_internal.main_table_insert_trigger() RETURNS TRIGGER
	AS '@MODULE_PATHNAME@', 'insert_main_table_trigger' LANGUAGE C;

CREATE OR REPLACE FUNCTION  _timescaledb_internal.main_table_after_insert_trigger() RETURNS TRIGGER
        AS '@MODULE_PATHNAME@', 'insert_main_table_trigger_after' LANGUAGE C;

-- Adds the above triggers to the main table when a hypertable is created.
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

        EXECUTE format(
            $$
                CREATE TRIGGER _timescaledb_main_insert_trigger BEFORE INSERT ON %I.%I
                FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.main_table_insert_trigger();
            $$, NEW.schema_name, NEW.table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER _timescaledb_main_after_insert_trigger AFTER INSERT ON %I.%I
                FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_internal.main_table_after_insert_trigger();
            $$, NEW.schema_name, NEW.table_name);
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

    DROP TRIGGER IF EXISTS trigger_main_on_change_chunk
    ON _timescaledb_catalog.chunk;
    CREATE TRIGGER trigger_main_on_change_chunk
    AFTER UPDATE OR DELETE OR INSERT ON _timescaledb_catalog.chunk
    FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.on_change_chunk();

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

    CREATE EVENT TRIGGER ddl_create_trigger ON ddl_command_end
       WHEN tag IN ('create trigger')
       EXECUTE PROCEDURE _timescaledb_internal.ddl_process_create_trigger();

    CREATE EVENT TRIGGER ddl_drop_trigger
       ON sql_drop
       EXECUTE PROCEDURE _timescaledb_internal.ddl_process_drop_trigger();

    CREATE EVENT TRIGGER ddl_alter_table ON ddl_command_end
       WHEN tag IN ('alter table')
       EXECUTE PROCEDURE _timescaledb_internal.ddl_process_alter_table();

    CREATE EVENT TRIGGER ddl_check_drop_command
       ON sql_drop
       EXECUTE PROCEDURE _timescaledb_internal.ddl_process_drop_table();

    IF restore THEN
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_create_index;
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_alter_index;
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_drop_index;
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_create_trigger;
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_drop_trigger;
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_alter_table;
        ALTER EXTENSION timescaledb ADD EVENT TRIGGER ddl_check_drop_command;
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
    chunk_time_interval     BIGINT =  _timescaledb_internal.interval_to_usec('1 month'),
    create_default_indexes  BOOLEAN = TRUE,
    if_not_exists           BOOLEAN = FALSE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    hypertable_row   _timescaledb_catalog.hypertable;
    table_name       NAME;
    schema_name      NAME;
    table_owner      NAME;
    tablespace_oid   OID;
    tablespace_name  NAME;
    main_table_has_items BOOLEAN;
    is_hypertable    BOOLEAN;
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

    PERFORM 1
    FROM pg_trigger,
    LATERAL _timescaledb_internal.add_trigger(
        hypertable_row.id,
        oid
    )
    WHERE tgrelid = main_table
    AND _timescaledb_internal.need_chunk_trigger(hypertable_row.id, oid);

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

CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_is_change_owner(pg_ddl_command)
   RETURNS bool IMMUTABLE STRICT
   AS '@MODULE_PATHNAME@' LANGUAGE C;

CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_change_owner_to(pg_ddl_command)
   RETURNS name IMMUTABLE STRICT
   AS '@MODULE_PATHNAME@' LANGUAGE C;

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
            );
        END LOOP;
END
$BODY$;

-- Handles ddl create trigger commands on hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_create_trigger()
    RETURNS event_trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    info            record;
    table_oid       regclass;
    index_oid       OID;
    trigger_name    TEXT;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    --NOTE:  pg_event_trigger_ddl_commands prevents this SECURITY DEFINER function from being called outside trigger.
    FOR info IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            SELECT OID, tgrelid, tgname INTO STRICT index_oid, table_oid, trigger_name
            FROM pg_catalog.pg_trigger
            WHERE oid = info.objid;

            IF _timescaledb_internal.is_main_table(table_oid) 
                AND trigger_name <> ALL(_timescaledb_internal.timescale_trigger_names())
            THEN
                hypertable_row := _timescaledb_internal.hypertable_from_main_table(table_oid);
                PERFORM _timescaledb_internal.add_trigger(hypertable_row.id, index_oid);
            END IF;
        END LOOP;
END
$BODY$;

-- Handles ddl drop index commands on hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_drop_trigger()
    RETURNS event_trigger LANGUAGE plpgsql
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    info           record;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    --NOTE:  pg_event_trigger_ddl_commands prevents this SECURITY DEFINER function from being called outside trigger.
    FOR info IN SELECT * FROM pg_event_trigger_dropped_objects()
        LOOP
            IF info.classid = 'pg_trigger'::regclass THEN
                SELECT * INTO hypertable_row
                FROM _timescaledb_catalog.hypertable
                WHERE schema_name = info.address_names[1] AND table_name = info.address_names[2];

                IF hypertable_row IS NOT NULL THEN
                    PERFORM _timescaledb_internal.drop_trigger_on_all_chunks(hypertable_row.id, info.address_names[3]);
                END IF;
            END IF;
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

-- Handles drop table command
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_drop_table()
        RETURNS event_trigger LANGUAGE plpgsql AS $BODY$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF tg_tag = 'DROP TABLE' AND  _timescaledb_internal.is_main_table(obj.schema_name, obj.object_name) THEN
            PERFORM _timescaledb_internal.drop_hypertable(obj.schema_name, obj.object_name);
        END IF;
    END LOOP;
END
$BODY$;

 CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_alter_table()
    RETURNS event_trigger LANGUAGE plpgsql 
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    info           record;
    new_table_owner           TEXT;
    chunk_row      _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    --NOTE:  pg_event_trigger_ddl_commands prevents this SECURITY DEFINER function from being called outside trigger.
    FOR info IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF NOT _timescaledb_internal.is_main_table(info.objid) THEN
            RETURN;
        END IF;

        IF _timescaledb_internal.ddl_is_change_owner(info.command) THEN
            --if change owner then change owners on all chunks
            new_table_owner := _timescaledb_internal.ddl_change_owner_to(info.command);
            hypertable_row := _timescaledb_internal.hypertable_from_main_table(info.objid);

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
        END IF;
    END LOOP;
END
$BODY$;
-- select_tablespace() is used to assign a tablespace to a chunk.  A
-- tablespace is selected from a set of tablespaces associated with
-- the chunk's hypertable, if any.
CREATE OR REPLACE FUNCTION _timescaledb_internal.select_tablespace(
    hypertable_id INTEGER,
    chunk_id      INTEGER
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
    dimension_slices INT[];
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
    dimension_slices := ARRAY(
        SELECT s.id FROM _timescaledb_catalog.dimension_slice s
        WHERE (s.dimension_id = main_block.dimension_id)
    );

    -- Find the chunk's dimension slice for the chosen dimension
    SELECT s.id FROM _timescaledb_catalog.dimension_slice s
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.dimension_slice_id = s.id)
    INNER JOIN _timescaledb_catalog.chunk c ON (cc.chunk_id = c.id)
    WHERE (s.dimension_id = main_block.dimension_id)
    AND (c.id = select_tablespace.chunk_id)
    INTO STRICT chunk_slice_id;

    -- Find the array index of the chunk's dimension slice
    SELECT i
    FROM generate_subscripts(dimension_slices, 1) AS i
    WHERE dimension_slices[i] = chunk_slice_id
    INTO STRICT chunk_slice_index;

    -- Use the chunk's dimension slice index to pick a tablespace in
    -- the tablespaces array
    RETURN tablespaces[chunk_slice_index % array_length(tablespaces, 1) + 1];
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
-- table_size         - Pretty output of table_bytes
-- index_bytes        - Pretty output of index_bytes
-- toast_bytes        - Pretty output of toast_bytes
-- total_size         - Pretty output of total_bytes

CREATE OR REPLACE FUNCTION hypertable_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT) LANGUAGE PLPGSQL VOLATILE
               SECURITY DEFINER SET search_path = ''
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
               total_bytes,
               pg_size_pretty(table_bytes) as table,
               pg_size_pretty(index_bytes) as index,
               pg_size_pretty(toast_bytes) as toast,
               pg_size_pretty(total_bytes) as total
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

-- Get relation size of the chunks of an hypertable
-- like pg_relation_size
-- (https://www.postgresql.org/docs/9.6/static/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE)
--
-- main_table - hypertable to get size of
--
-- Returns:
-- chunk_id          - timescaledb id of a chunk
-- chunk_table       - table used for the chunk
-- table_bytes       - Disk space used by main_table
-- index_bytes       - Disk space used by indexes
-- toast_bytes       - Disc space of toast tables
-- total_bytes       - Disk space used in total
-- table_size        - Pretty output of table_bytes
-- index_size        - Pretty output of index_bytes
-- toast_size        - Pretty output of toast_bytes
-- total_size        - Pretty output of total_bytes

CREATE OR REPLACE FUNCTION chunk_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (chunk_id INT,
               chunk_table TEXT,
               dimensions NAME[],
               ranges int8range[],
               table_bytes BIGINT,
               index_bytes BIGINT,
               toast_bytes BIGINT,
               total_bytes BIGINT,
               table_size  TEXT,
               index_size  TEXT,
               toast_size  TEXT,
               total_size  TEXT)
               LANGUAGE PLPGSQL VOLATILE
               SECURITY DEFINER SET search_path = ''
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
        dimensions,
        ranges,
        table_bytes,
        index_bytes,
        toast_bytes,
        total_bytes,
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
               array_agg(d.column_name) as dimensions,
               array_agg(int8range(range_start, range_end)) as ranges
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
-- total_size           - pretty output of total_bytes

CREATE OR REPLACE FUNCTION indexes_relation_size(
    main_table              REGCLASS
)
RETURNS TABLE (index_name TEXT,
               total_bytes BIGINT,
               total_size  TEXT) LANGUAGE PLPGSQL VOLATILE
               SECURITY DEFINER SET search_path = ''
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
               sum(pg_relation_size('"' || ci.schema_name || '"."' || ci.index_name || '"'))::bigint,
               pg_size_pretty(sum(pg_relation_size('"' || ci.schema_name || '"."' || ci.index_name || '"')))
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
CREATE EVENT TRIGGER ddl_drop_trigger
ON sql_drop
EXECUTE PROCEDURE _timescaledb_internal.ddl_process_drop_trigger();
