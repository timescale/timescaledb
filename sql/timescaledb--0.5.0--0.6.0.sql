DROP FUNCTION public.time_bucket(INTERVAL, TIMESTAMP);
DROP FUNCTION public.time_bucket(INTERVAL, TIMESTAMPTZ);
DROP FUNCTION public.time_bucket(INTERVAL, DATE);
DROP FUNCTION public.time_bucket(INTERVAL, TIMESTAMP, INTERVAL);
DROP FUNCTION public.time_bucket(INTERVAL, TIMESTAMPTZ, INTERVAL);
DROP FUNCTION public.time_bucket(INTERVAL, DATE, INTERVAL);
DROP FUNCTION public.time_bucket(BIGINT, BIGINT);
DROP FUNCTION public.time_bucket(INT, INT);
DROP FUNCTION public.time_bucket(SMALLINT, SMALLINT);
DROP FUNCTION public.time_bucket(BIGINT, BIGINT, BIGINT);
DROP FUNCTION public.time_bucket(INT, INT, INT);
DROP FUNCTION public.time_bucket(SMALLINT, SMALLINT, SMALLINT);

-- Indexing updates
DROP EVENT TRIGGER ddl_create_index;
DROP EVENT TRIGGER ddl_alter_index;
DROP EVENT TRIGGER ddl_drop_index;
DROP TRIGGER trigger_main_on_change_chunk_index ON _timescaledb_catalog.chunk_index;
DROP TRIGGER trigger_main_on_change_hypertable_index ON _timescaledb_catalog.hypertable_index;

DROP FUNCTION _timescaledb_internal.get_index_definition_for_table(NAME, NAME, NAME, TEXT);
DROP FUNCTION _timescaledb_internal.create_chunk_index_row(NAME, NAME, NAME, NAME, TEXT);
DROP FUNCTION _timescaledb_internal.on_change_chunk_index();
DROP FUNCTION _timescaledb_internal.add_index(INTEGER, NAME, NAME, TEXT);
DROP FUNCTION _timescaledb_internal.drop_index(NAME, NAME);
DROP FUNCTION _timescaledb_internal.get_general_index_definition(REGCLASS, REGCLASS, _timescaledb_catalog.hypertable);
DROP FUNCTION _timescaledb_internal.ddl_process_create_index();
DROP FUNCTION _timescaledb_internal.ddl_process_alter_index();
DROP FUNCTION _timescaledb_internal.ddl_process_drop_index();
DROP FUNCTION _timescaledb_internal.create_index_on_all_chunks(INTEGER, NAME, NAME, TEXT);
DROP FUNCTION _timescaledb_internal.drop_index_on_all_chunks(NAME, NAME);
DROP FUNCTION _timescaledb_internal.on_change_hypertable_index();
DROP FUNCTION _timescaledb_internal.need_chunk_index(INTEGER, OID);
DROP FUNCTION _timescaledb_internal.check_index(REGCLASS, _timescaledb_catalog.hypertable);

DROP FUNCTION indexes_relation_size_pretty(REGCLASS);

ALTER TABLE _timescaledb_catalog.chunk_index RENAME TO chunk_index_old;
ALTER SEQUENCE _timescaledb_catalog.chunk_index_id_seq RENAME TO chunk_index_old_id_seq;

-- Remove metadata table triggers
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.hypertable;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.dimension;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.dimension_slice;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.chunk_constraint;
DROP TRIGGER trigger_block_truncate ON _timescaledb_catalog.hypertable_index;
DROP TRIGGER trigger_1_main_on_change_hypertable ON _timescaledb_catalog.hypertable;
DROP FUNCTION _timescaledb_internal.on_truncate_block();
DROP FUNCTION _timescaledb_internal.on_trigger_error(TEXT, NAME, NAME);
DROP FUNCTION _timescaledb_internal.on_change_hypertable();
DROP FUNCTION _timescaledb_internal.setup_main(BOOLEAN);
DROP FUNCTION restore_timescaledb();

DROP FUNCTION _timescaledb_internal.drop_chunk(INTEGER, BOOLEAN, BOOLEAN);
DROP FUNCTION drop_chunks(TIMESTAMPTZ, NAME, NAME);
DROP FUNCTION drop_chunks(INTERVAL, NAME, NAME);
DROP FUNCTION _timescaledb_internal.drop_chunks_older_than(BIGINT, NAME, NAME);
DROP FUNCTION _timescaledb_internal.truncate_hypertable(NAME, NAME);
CREATE SCHEMA IF NOT EXISTS _timescaledb_catalog;
CREATE SCHEMA IF NOT EXISTS _timescaledb_internal;
CREATE SCHEMA IF NOT EXISTS _timescaledb_cache;
--NOTICE: UPGRADE-SCRIPT-NEEDED contents in this file are not auto-upgraded.

-- This file contains table definitions for various abstractions and data
-- structures for representing hypertables and lower-level concepts.

-- Hypertable
-- ==========
--
-- The hypertable is an abstraction that represents a table that is
-- partitioned in N dimensions, where each dimension maps to a column
-- in the table. A dimension can either be 'open' or 'closed', which
-- reflects the scheme that divides the dimension's keyspace into
-- "slices".
--
-- Conceptually, a partition -- called a "chunk", is a hypercube in
-- the N-dimensional space. A chunk stores a subset of the
-- hypertable's tuples on disk in its own distinct table. The slices
-- that span the chunk's hypercube each correspond to a constraint on
-- the chunk's table, enabling constraint exclusion during queries on
-- the hypertable's data.
--
--
-- Open dimensions
------------------
-- An open dimension does on-demand slicing, creating a new slice
-- based on a configurable interval whenever a tuple falls outside the
-- existing slices. Open dimensions fit well with columns that are
-- incrementally increasing, such as time-based ones.
--
-- Closed dimensions
--------------------
-- A closed dimension completely divides its keyspace into a
-- configurable number of slices. The number of slices can be
-- reconfigured, but the new partitioning only affects newly created
-- chunks.
--
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable (
    id                      SERIAL    PRIMARY KEY,
    schema_name             NAME      NOT NULL CHECK (schema_name != '_timescaledb_catalog'),
    table_name              NAME      NOT NULL,
    associated_schema_name  NAME      NOT NULL,
    associated_table_prefix NAME      NOT NULL,
    num_dimensions          SMALLINT  NOT NULL CHECK (num_dimensions > 0),
    UNIQUE (id, schema_name),
    UNIQUE (schema_name, table_name),
    UNIQUE (associated_schema_name, associated_table_prefix)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.hypertable','id'), '');

-- The tablespace table maps tablespaces to hypertables.
-- This allows spreading a hypertable's chunks across multiple disks.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.tablespace (
   id                SERIAL PRIMARY KEY,
   hypertable_id     INT  NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
   tablespace_name   NAME NOT NULL,
   UNIQUE (hypertable_id, tablespace_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.tablespace', '');

-- A dimension represents an axis along which data is partitioned.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.dimension (
    id                          SERIAL   NOT NULL PRIMARY KEY,
    hypertable_id               INTEGER  NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    column_name                 NAME     NOT NULL,
    column_type                 REGTYPE  NOT NULL,
    aligned                     BOOLEAN  NOT NULL,
    -- closed dimensions
    num_slices                  SMALLINT NULL,
    partitioning_func_schema    NAME     NULL,
    partitioning_func           NAME     NULL,
    -- open dimensions (e.g., time)
    interval_length             BIGINT   NULL CHECK(interval_length IS NULL OR interval_length > 0),
    CHECK (
        (partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR
        (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)
    ),
    CHECK (
        (num_slices IS NULL AND interval_length IS NOT NULL) OR
        (num_slices IS NOT NULL AND interval_length IS NULL)
    ),
    UNIQUE (hypertable_id, column_name)
);
CREATE INDEX IF NOT EXISTS dimension_hypertable_id_idx
ON _timescaledb_catalog.dimension(hypertable_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension','id'), '');

-- A dimension slice defines a keyspace range along a dimension axis.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.dimension_slice (
    id            SERIAL   NOT NULL PRIMARY KEY,
    dimension_id  INTEGER  NOT NULL REFERENCES _timescaledb_catalog.dimension(id) ON DELETE CASCADE,
    range_start   BIGINT   NOT NULL,
    range_end     BIGINT   NOT NULL,
    CHECK (range_start <= range_end),
    UNIQUE (dimension_id, range_start, range_end)
);
CREATE INDEX IF NOT EXISTS dimension_slice_dimension_id_range_start_range_end_idx
ON _timescaledb_catalog.dimension_slice(dimension_id, range_start, range_end);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_slice', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension_slice','id'), '');

-- A chunk is a partition (hypercube) in an N-dimensional
-- hyperspace. Each chunk is associated with N constraints that define
-- the chunk's hypercube. Tuples that fall within the chunk's
-- hypercube are stored in the chunk's data table, as given by
-- 'schema_name' and 'table_name'.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk (
    id              SERIAL  NOT NULL    PRIMARY KEY,
    hypertable_id   INT     NOT NULL    REFERENCES _timescaledb_catalog.hypertable(id),
    schema_name     NAME    NOT NULL,
    table_name      NAME    NOT NULL,
    UNIQUE (schema_name, table_name)
);
CREATE INDEX IF NOT EXISTS chunk_hypertable_id_idx
ON _timescaledb_catalog.chunk(hypertable_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk','id'), '');

-- A chunk constraint maps a dimension slice to a chunk. Each
-- constraint associated with a chunk will also be a table constraint
-- on the chunk's data table.
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_constraint (
    chunk_id            INTEGER  NOT NULL REFERENCES _timescaledb_catalog.chunk(id),
    dimension_slice_id  INTEGER  NULL REFERENCES _timescaledb_catalog.dimension_slice(id),
    constraint_name     NAME NOT NULL,
    hypertable_constraint_name NAME NULL,
    UNIQUE(chunk_id, constraint_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint', '');
CREATE INDEX IF NOT EXISTS chunk_constraint_chunk_id_dimension_slice_id_idx
ON _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id);

CREATE SEQUENCE IF NOT EXISTS _timescaledb_catalog.chunk_constraint_name;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_index (
    chunk_id              INTEGER NOT NULL REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    index_name            NAME NOT NULL,
    hypertable_id         INTEGER NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    hypertable_index_name NAME NOT NULL,
    UNIQUE(chunk_id, index_name)
);
CREATE INDEX IF NOT EXISTS chunk_index_hypertable_id_hypertable_index_name_idx
ON _timescaledb_catalog.chunk_index(hypertable_id, hypertable_index_name);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_index', '');
--NOTICE: UPGRADE-SCRIPT-NEEDED contents in this file are not auto-upgraded.
-- This sets up the permissions for entities created by this extension.

-- schema permisions
GRANT USAGE ON SCHEMA _timescaledb_catalog, _timescaledb_internal TO PUBLIC;

-- needed for working with hypertables
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO PUBLIC;




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

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk_metadata(
    chunk_id int
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    -- when deleting the chunk row from the metadata table,
    -- also DROP the actual chunk table that holds data.
    -- Note that the table could already be deleted in case this
    -- is executed as a result of a DROP TABLE on the hypertable
    -- that this chunk belongs to.

    PERFORM _timescaledb_internal.drop_chunk_constraint(cc.chunk_id, cc.constraint_name, false)
    FROM _timescaledb_catalog.chunk_constraint cc
    WHERE cc.chunk_id = drop_chunk_metadata.chunk_id;

    DELETE FROM _timescaledb_catalog.chunk WHERE id = chunk_id
    RETURNING * INTO STRICT chunk_row;

    PERFORM 1
    FROM pg_class c
    WHERE relname = quote_ident(chunk_row.table_name) AND relnamespace = quote_ident(chunk_row.schema_name)::regnamespace;
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

    -- Create the schema for the hypertable data if needed
    PERFORM _timescaledb_internal.create_hypertable_schema(associated_schema_name);

    id :=  nextval(pg_get_serial_sequence('_timescaledb_catalog.hypertable','id'));

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

    -- Verify indexes
    PERFORM _timescaledb_internal.verify_hypertable_indexes(main_table);

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
BEGIN
    IF older_than_time IS NULL AND table_name IS NULL AND schema_name IS NULL THEN
        RAISE 'Cannot have all 3 arguments to drop_chunks_older_than be NULL';
    END IF;

    IF cascade THEN
        cascade_mod = 'CASCADE';
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
    table_name      NAME,
    cascade      BOOLEAN = FALSE
)
    RETURNS VOID
    LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
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
AS '@MODULE_PATHNAME@', 'indexing_verify_hypertable_indexes' LANGUAGE C IMMUTABLE STRICT;

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
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
<<main_block>>
DECLARE
    table_name       NAME;
    schema_name      NAME;
    owner_oid        OID;
    hypertable_row   _timescaledb_catalog.hypertable;
BEGIN
    SELECT relname, nspname, relowner
    INTO STRICT table_name, schema_name, owner_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    IF NOT pg_has_role(session_user, owner_oid, 'USAGE') THEN
        raise 'permission denied for hypertable "%"', main_table;
    END IF;

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
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path=''
    AS
$BODY$
DECLARE
    main_table_name       NAME;
    main_schema_name      NAME;
    owner_oid             OID;
BEGIN
    SELECT relname, nspname, relowner
    INTO STRICT main_table_name, main_schema_name, owner_oid
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = main_table;

    IF NOT pg_has_role(session_user, owner_oid, 'USAGE') THEN
        raise 'permission denied for hypertable "%"', main_table;
    END IF;

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
    older_than INTEGER,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE
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
    main_schema_name  NAME;
    main_table_name   NAME;
    owner_oid         OID;
    hypertable_id     INTEGER;
    tablespace_oid    OID;
BEGIN
    SELECT nspname, relname, relowner
    FROM pg_class c INNER JOIN pg_namespace n
    ON (c.relnamespace = n.oid)
    WHERE c.oid = hypertable
    INTO STRICT main_schema_name, main_table_name, owner_oid;

    IF NOT pg_has_role(session_user, owner_oid, 'USAGE') THEN
        raise 'permission denied for hypertable "%"', hypertable;
    END IF;

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
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_command_end() RETURNS event_trigger
AS '@MODULE_PATHNAME@', 'timescaledb_ddl_command_end' LANGUAGE C IMMUTABLE STRICT;

CREATE EVENT TRIGGER timescaledb_ddl_command_end ON ddl_command_end
EXECUTE PROCEDURE _timescaledb_internal.ddl_command_end();
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
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
	AS '@MODULE_PATHNAME@', 'timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- bucketing of timestamptz happens at UTC time
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
	AS '@MODULE_PATHNAME@', 'timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

--bucketing on date should not do any timezone conversion
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE) RETURNS DATE
	AS '@MODULE_PATHNAME@', 'date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- If an interval is given as the third argument, the bucket alignment is offset by the interval.
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL)
    RETURNS TIMESTAMP LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE, "offset" INTERVAL)
    RETURNS DATE LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (time_bucket(bucket_width, ts-"offset")+"offset")::date;
$BODY$;


CREATE OR REPLACE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT)
    RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INT, ts INT)
    RETURNS INT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT)
    RETURNS SMALLINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (ts / bucket_width)*bucket_width;
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT)
    RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INT, ts INT, "offset" INT)
    RETURNS INT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (((ts-"offset") / bucket_width)*bucket_width)+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT)
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
<<main>>
DECLARE
        table_name       NAME;
        schema_name      NAME;
BEGIN
        SELECT relname, nspname
        INTO STRICT table_name, schema_name
        FROM pg_class c
        INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
        WHERE c.OID = main_table;

        RETURN QUERY
        SELECT h.schema_name || '.' || ci.hypertable_index_name,
               sum(pg_relation_size(c.oid))::bigint
        FROM
        pg_class c,
        pg_namespace n,
        _timescaledb_catalog.hypertable h,
        _timescaledb_catalog.chunk ch,
        _timescaledb_catalog.chunk_index ci
        WHERE ch.schema_name = n.nspname
            AND c.relnamespace = n.oid
            AND c.relname = ci.index_name
            AND ch.id = ci.chunk_id
            AND h.id = ci.hypertable_id
            AND h.schema_name = main.schema_name
            AND h.table_name = main.table_name
        GROUP BY h.schema_name, ci.hypertable_index_name;
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
RETURNS TABLE (index_name TEXT,
               total_size TEXT) LANGUAGE PLPGSQL STABLE
               AS
$BODY$
BEGIN
        RETURN QUERY
        SELECT s.index_name,
               pg_size_pretty(s.total_bytes)
        FROM indexes_relation_size(main_table) s;
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
-- This file contains infrastructure for cache invalidation of TimescaleDB
-- metadata caches kept in C. Please look at cache_invalidate.c for a
-- description of how this works.
CREATE TABLE IF NOT EXISTS  _timescaledb_cache.cache_inval_hypertable();

-- This is pretty subtle. We create this dummy cache_inval_extension table
-- solely for the purpose of getting a relcache invalidation event when it is
-- deleted on DROP extension. It has no related triggers. When the table is
-- invalidated, all backends will be notified and will know that they must
-- invalidate all cached information, including catalog table and index OIDs,
-- etc.
CREATE TABLE IF NOT EXISTS  _timescaledb_cache.cache_inval_extension();

-- not actually strictly needed but good for sanity as all tables should be dumped.
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_hypertable', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_cache.cache_inval_extension', '');

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.hypertable;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.hypertable
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.chunk;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.chunk
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.chunk_constraint;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.chunk_constraint
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.dimension_slice;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.dimension_slice
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.dimension;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.dimension
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger('cache_inval_hypertable');
-- Indexing updates

-- Convert old chunk_index table data to new format
INSERT INTO _timescaledb_catalog.chunk_index
SELECT ch.id, ci.index_name, h.id, ci.main_index_name
FROM _timescaledb_catalog.chunk_index_old ci,
     _timescaledb_catalog.hypertable h,
     _timescaledb_catalog.chunk ch,
     pg_index i,
     pg_class c
WHERE ci.schema_name = ch.schema_name
AND   ci.table_name = ch.table_name
AND   i.indexrelid = format('%I.%I', ci.main_schema_name, ci.main_index_name)::REGCLASS
AND   i.indrelid = c.oid
AND   ci.main_schema_name = h.schema_name
AND   c.relname = h.table_name;

ALTER EXTENSION timescaledb
DROP TABLE _timescaledb_catalog.chunk_index_old;
ALTER EXTENSION timescaledb
DROP TABLE _timescaledb_catalog.hypertable_index;
ALTER EXTENSION timescaledb
DROP SEQUENCE _timescaledb_catalog.chunk_index_old_id_seq;

DROP TABLE _timescaledb_catalog.chunk_index_old;
DROP TABLE _timescaledb_catalog.hypertable_index;
-- No need to drop _timescaledb_catalog.chunk_index_old_id_seq,
-- removed with table.
