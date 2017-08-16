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


