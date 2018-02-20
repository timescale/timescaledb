--need to keep this legacy functions around for 0.9 since it's still used in 0.8, delete after 0.9
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_command_end() RETURNS event_trigger
AS '@MODULE_PATHNAME@', 'timescaledb_ddl_command_end' LANGUAGE C;
-- Make sure any trigger functions or event trigger functions defined here.
-- This file is called first in any upgrade or install script to make sure 
-- these functions match the new .so before they are called.
-- All functions here should be disabled -- in c -- during upgrades.

-- This function is called for any ddl event.
CREATE OR REPLACE FUNCTION _timescaledb_internal.process_ddl_event() RETURNS event_trigger
AS '@MODULE_PATHNAME@', 'timescaledb_process_ddl_event' LANGUAGE C;

-- this trigger function causes an invalidation event on the table whose name is
-- passed in as the first element.
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache_trigger()
RETURNS TRIGGER AS '@MODULE_PATHNAME@', 'invalidate_relcache_trigger' LANGUAGE C STRICT;


-- Tablespace changes
DROP FUNCTION _timescaledb_internal.select_tablespace(integer, integer[]);
DROP FUNCTION _timescaledb_internal.select_tablespace(integer, integer);
DROP FUNCTION _timescaledb_internal.select_tablespace(integer);

-- Chunk functions
DROP FUNCTION _timescaledb_internal.chunk_create(integer, integer, name, name);
DROP FUNCTION _timescaledb_internal.drop_chunk_metadata(int);

-- Chunk constraint functions
DROP FUNCTION _timescaledb_internal.create_chunk_constraint(integer, oid);
DROP FUNCTION _timescaledb_internal.drop_constraint(integer, name);
DROP FUNCTION _timescaledb_internal.drop_chunk_constraint(integer, name, boolean);
DROP FUNCTION _timescaledb_internal.chunk_constraint_drop_table_constraint(_timescaledb_catalog.chunk_constraint);

-- Dimension and time functions
DROP FUNCTION _timescaledb_internal.change_column_type(int, name, regtype);
DROP FUNCTION _timescaledb_internal.rename_column(int, name, name);
DROP FUNCTION _timescaledb_internal.set_time_column_constraint(regclass, name);
DROP FUNCTION _timescaledb_internal.add_dimension(regclass, _timescaledb_catalog.hypertable, name, integer, anyelement, regproc, boolean, boolean);
DROP FUNCTION add_dimension(regclass, name, integer, anyelement, regproc);
DROP FUNCTION _timescaledb_internal.time_interval_specification_to_internal(regtype, anyelement, interval, text, boolean);
DROP FUNCTION _timescaledb_internal.time_interval_specification_to_internal_with_default_time(regtype, anyelement, text, boolean);
DROP FUNCTION _timescaledb_internal.create_hypertable(regclass, name, name, name, name, integer, name, name, bigint, name, boolean, regproc);
DROP FUNCTION create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc);
DROP FUNCTION set_chunk_time_interval(regclass, anyelement);

-- Hypertable and related functions
DROP FUNCTION _timescaledb_internal.set_time_columns_not_null();
DROP FUNCTION _timescaledb_internal.create_schema(name);
DROP FUNCTION _timescaledb_internal.check_role(regclass);
DROP FUNCTION _timescaledb_internal.attach_tablespace(name,regclass);
DROP FUNCTION _timescaledb_internal.create_default_indexes(_timescaledb_catalog.hypertable,regclass,name);
DROP FUNCTION _timescaledb_internal.create_hypertable_schema(name);
DROP FUNCTION _timescaledb_internal.detach_tablespace(name,regclass);
DROP FUNCTION _timescaledb_internal.detach_tablespaces(regclass);
DROP FUNCTION _timescaledb_internal.dimension_type(regclass,name,boolean);
DROP FUNCTION _timescaledb_internal.show_tablespaces(regclass);
DROP FUNCTION _timescaledb_internal.verify_hypertable_indexes(regclass);
DROP FUNCTION _timescaledb_internal.validate_triggers(regclass);
DROP FUNCTION _timescaledb_internal.chunk_create_table(int, name);
DROP FUNCTION _timescaledb_internal.ddl_change_owner(oid, name);
DROP FUNCTION _timescaledb_internal.truncate_hypertable(name,name,boolean);
DROP FUNCTION attach_tablespace(name,regclass);
DROP FUNCTION detach_tablespace(name,regclass);

-- Remove redundant index
DROP INDEX _timescaledb_catalog.dimension_slice_dimension_id_range_start_range_end_idx;

DROP FUNCTION _timescaledb_internal.drop_hypertable(int,boolean);

-- Delete orphaned slices
DELETE FROM _timescaledb_catalog.dimension_slice WHERE id IN
(SELECT dimension_id FROM _timescaledb_catalog.chunk_constraint cc
 FULL OUTER JOIN _timescaledb_catalog.dimension_slice ds
 ON (ds.id = cc.dimension_slice_id)
 WHERE dimension_slice_id IS NULL);
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

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '@MODULE_PATHNAME@', 'chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '@MODULE_PATHNAME@', 'chunk_index_replace' LANGUAGE C VOLATILE STRICT;
-- This file contains utilities for time conversion.
CREATE OR REPLACE FUNCTION _timescaledb_internal.to_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'pg_timestamp_to_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp_pg(postgres_us BIGINT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'pg_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;



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
      WHEN 'TIMESTAMP'::regtype THEN
        --the time_value for timestamps w/o tz does not depend on local timezones. So perform at UTC.
        RETURN format('TIMESTAMP %1$L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))); -- microseconds
      WHEN 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('TIMESTAMPTZ %1$L', _timescaledb_internal.to_timestamp(time_value)); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))::date);
    END CASE;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.interval_to_usec(
       chunk_interval INTERVAL
)
RETURNS BIGINT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT (int_sec * 1000000)::bigint from extract(epoch from chunk_interval) as int_sec;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.time_to_internal(time_element anyelement, time_type REGTYPE) RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'time_to_internal' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
-- This file contains functions associated with creating new
-- hypertables.

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_is_finite(
    val      BIGINT
)
    RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    --end values of bigint reserved for infinite
    SELECT val > (-9223372036854775808)::bigint AND val < 9223372036854775807::bigint
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql(
    dimension_slice_id  INTEGER
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    dimension_slice_row _timescaledb_catalog.dimension_slice;
    dimension_row _timescaledb_catalog.dimension;
    parts TEXT[];
BEGIN
    SELECT * INTO STRICT dimension_slice_row
    FROM _timescaledb_catalog.dimension_slice
    WHERE id = dimension_slice_id;

    SELECT * INTO STRICT dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE id = dimension_slice_row.dimension_id;

    IF dimension_row.partitioning_func IS NOT NULL THEN

        IF  _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(
            $$
                %1$I.%2$I(%3$I) >= %4$L
            $$,
            dimension_row.partitioning_func_schema,
            dimension_row.partitioning_func,
            dimension_row.column_name,
            dimension_slice_row.range_start);
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(
            $$
                %1$I.%2$I(%3$I) < %4$L
            $$,
            dimension_row.partitioning_func_schema,
            dimension_row.partitioning_func,
            dimension_row.column_name,
            dimension_slice_row.range_end);
        END IF;

        IF array_length(parts, 1) = 0 THEN
            RETURN NULL;
        END IF;
        return array_to_string(parts, 'AND');
    ELSE
        --TODO: only works with time for now
        IF _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimension_row.column_type) =
           _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type) THEN
            RAISE 'Time based constraints have the same start and end values for column "%": %',
                    dimension_row.column_name,
                    _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type);
        END IF;

        parts = ARRAY[]::text[];

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(
            $$
                 %1$I >= %2$s
            $$,
            dimension_row.column_name,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimension_row.column_type));
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(
            $$
                 %1$I < %2$s
            $$,
            dimension_row.column_name,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type));
        END IF;

        return array_to_string(parts, 'AND');
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
    chunk_row _timescaledb_catalog.chunk;
    hypertable_row _timescaledb_catalog.hypertable;
    constraint_oid OID;
    check_sql TEXT;
    def TEXT;
BEGIN
    SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
    SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h WHERE h.id = chunk_row.hypertable_id;

    IF chunk_constraint_row.dimension_slice_id IS NOT NULL THEN
        check_sql = _timescaledb_internal.dimension_slice_get_constraint_sql(chunk_constraint_row.dimension_slice_id);
        IF check_sql IS NOT NULL THEN
            def := format('CHECK (%s)',  check_sql);
        ELSE
            def := NULL;
        END IF;
    ELSIF chunk_constraint_row.hypertable_constraint_name IS NOT NULL THEN
        SELECT oid INTO STRICT constraint_oid FROM pg_constraint
        WHERE conname=chunk_constraint_row.hypertable_constraint_name AND
              conrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass::oid;
        def := pg_get_constraintdef(constraint_oid);
    ELSE
        RAISE 'Unknown constraint type';
    END IF;

    IF def IS NOT NULL THEN
        EXECUTE format(
            $$ ALTER TABLE %I.%I ADD CONSTRAINT %I %s $$,
            chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name, def
        );
    END IF;
END
$BODY$;
-- Deprecated partition hash function
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_for_key(val anyelement)
    RETURNS int
    AS '@MODULE_PATHNAME@', 'get_partition_for_key' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_hash(val anyelement)
    RETURNS int
    AS '@MODULE_PATHNAME@', 'get_partition_hash' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

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
-- create_default_indexes - (Optional) Whether or not to create the default indexes
-- if_not_exists - (Optional) Do not fail if table is already a hypertable
-- partitioning_func - (Optional) The partitioning function to use for spatial partitioning
-- migrate_data - (Optional) Set to true to migrate any existing data in the table to chunks
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
    partitioning_func       REGPROC = NULL,
    migrate_data            BOOLEAN = FALSE
) RETURNS VOID AS '@MODULE_PATHNAME@', 'hypertable_create' LANGUAGE C VOLATILE;

-- Update chunk_time_interval for a hypertable.
--
-- main_table - The OID of the table corresponding to a hypertable whose time
--     interval should be updated
-- chunk_time_interval - The new time interval. For hypertables with integral
--     time columns, this must be an integral type. For hypertables with a
--     TIMESTAMP/TIMESTAMPTZ/DATE type, it can be integral which is treated as
--     microseconds, or an INTERVAL type.
CREATE OR REPLACE FUNCTION  set_chunk_time_interval(
    main_table              REGCLASS,
    chunk_time_interval     ANYELEMENT,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'dimension_set_interval' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION  set_number_partitions(
    main_table              REGCLASS,
    number_partitions       INTEGER,
    dimension_name          NAME = NULL
) RETURNS VOID AS '@MODULE_PATHNAME@', 'dimension_set_num_slices' LANGUAGE C VOLATILE;

-- Drop chunks that are older than a timestamp.
CREATE OR REPLACE FUNCTION drop_chunks(
    older_than anyelement,
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

    PERFORM  _timescaledb_internal.drop_chunks_type_check(pg_typeof(older_than), table_name, schema_name);
    SELECT _timescaledb_internal.time_to_internal(older_than, pg_typeof(older_than)) INTO older_than_internal;
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
    time_type REGTYPE;
BEGIN

    BEGIN
        WITH hypertable_ids AS (
            SELECT id
            FROM _timescaledb_catalog.hypertable h
            WHERE (drop_chunks.schema_name IS NULL OR h.schema_name = drop_chunks.schema_name) AND
            (drop_chunks.table_name IS NULL OR h.table_name = drop_chunks.table_name)
        )
        SELECT DISTINCT time_dim.column_type INTO STRICT time_type
        FROM hypertable_ids INNER JOIN LATERAL _timescaledb_internal.dimension_get_time(hypertable_ids.id) time_dim ON (true);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE EXCEPTION 'No hypertables found';
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Cannot use drop_chunks on multiple tables with different time types';
    END;


    IF time_type = 'TIMESTAMP'::regtype THEN
        PERFORM drop_chunks((now() - older_than)::timestamp, table_name, schema_name, cascade);
    ELSIF time_type = 'DATE'::regtype THEN
        PERFORM drop_chunks((now() - older_than)::date, table_name, schema_name, cascade);
    ELSIF time_type = 'TIMESTAMPTZ'::regtype THEN
        PERFORM drop_chunks(now() - older_than, table_name, schema_name, cascade);
    ELSE
        RAISE 'Can only use drop_chunks with an INTERVAL for TIMESTAMP, TIMESTAMPTZ, and DATE types';
    END IF;
END
$BODY$;

-- Add a dimension (of partitioning) to a hypertable
--
-- main_table - OID of the table to add a dimension to
-- column_name - NAME of the column to use in partitioning for this dimension
-- number_partitions - Number of partitions, for non-time dimensions
-- interval_length - Size of intervals for time dimensions (can be integral or INTERVAL)
-- partitioning_func - Function used to partition the column
-- if_not_exists - If set, and the dimension already exists, generate a notice instead of an error
CREATE OR REPLACE FUNCTION  add_dimension(
    main_table              REGCLASS,
    column_name             NAME,
    number_partitions       INTEGER = NULL,
    chunk_time_interval     ANYELEMENT = NULL::BIGINT,
    partitioning_func       REGPROC = NULL,
    if_not_exists           BOOLEAN = FALSE
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'dimension_add' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION attach_tablespace(
    tablespace NAME,
    hypertable REGCLASS,
    if_not_attached BOOLEAN = false
) RETURNS VOID
AS '@MODULE_PATHNAME@', 'tablespace_attach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION detach_tablespace(
    tablespace NAME,
    hypertable REGCLASS = NULL,
    if_attached BOOLEAN = false
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'tablespace_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION detach_tablespaces(hypertable REGCLASS) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'tablespace_detach_all_from_hypertable' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION show_tablespaces(hypertable REGCLASS) RETURNS SETOF NAME
AS '@MODULE_PATHNAME@', 'tablespace_show' LANGUAGE C VOLATILE STRICT;
DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_command_end;
--EVENT TRIGGER MUST exclude the ALTER EXTENSION tag.
CREATE EVENT TRIGGER timescaledb_ddl_command_end ON ddl_command_end
WHEN TAG IN ('ALTER TABLE','CREATE TRIGGER','CREATE TABLE','CREATE INDEX','ALTER INDEX')
EXECUTE PROCEDURE _timescaledb_internal.process_ddl_event();

DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_sql_drop;
CREATE EVENT TRIGGER timescaledb_ddl_sql_drop ON sql_drop
EXECUTE PROCEDURE _timescaledb_internal.process_ddl_event();
CREATE OR REPLACE FUNCTION _timescaledb_internal.first_sfunc(internal, anyelement, "any")
RETURNS internal
AS '@MODULE_PATHNAME@', 'first_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.first_combinefunc(internal, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'first_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any")
RETURNS internal
AS '@MODULE_PATHNAME@', 'last_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.last_combinefunc(internal, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'last_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_finalfunc(internal, anyelement, "any")
RETURNS anyelement
AS '@MODULE_PATHNAME@', 'bookend_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_serializefunc(internal)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'bookend_serializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.bookend_deserializefunc(bytea, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'bookend_deserializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

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
    AS '@MODULE_PATHNAME@', 'get_git_commit' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
-- This function is only used for debugging
CREATE OR REPLACE FUNCTION _timescaledb_cache.invalidate_relcache(catalog_table REGCLASS)
RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'invalidate_relcache' LANGUAGE C STRICT;

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
    IF NOT _timescaledb_internal.dimension_is_finite(time_value) THEN
        RETURN '';
    END IF;
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
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_combinefunc(state1 INTERNAL, state2 INTERNAL)
RETURNS INTERNAL
AS '@MODULE_PATHNAME@', 'hist_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_serializefunc(INTERNAL)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'hist_serializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_deserializefunc(bytea, INTERNAL)
RETURNS INTERNAL
AS '@MODULE_PATHNAME@', 'hist_deserializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
RETURNS INTEGER[]
AS '@MODULE_PATHNAME@', 'hist_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

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
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.chunk;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.chunk
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.chunk_constraint;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.chunk_constraint
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.dimension_slice;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.dimension_slice
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();

DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.dimension;
CREATE TRIGGER "0_cache_inval" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON _timescaledb_catalog.dimension
FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_cache.invalidate_relcache_trigger();
DROP FUNCTION _timescaledb_internal.ddl_command_end();
