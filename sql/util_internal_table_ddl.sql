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

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_table(
    schema_name        NAME,
    table_name         NAME,
    parent_schema_name NAME,
    parent_table_name  NAME,
    tablespace_name    NAME,
    keyspace_start     SMALLINT,
    keyspace_end       SMALLINT,
    epoch_id           INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    tablespace_oid  pg_catalog.pg_tablespace.oid%type;
    tablespace_clause TEXT = '';
BEGIN
    SELECT t.oid
    INTO tablespace_oid
    FROM pg_catalog.pg_tablespace t
    WHERE t.spcname = tablespace_name;

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
        schema_name, table_name, parent_schema_name, parent_table_name, tablespace_clause);

    PERFORM _timescaledb_internal.add_partition_constraint(schema_name, table_name, keyspace_start, keyspace_end, epoch_id);
END
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.add_partition_constraint(
    schema_name    NAME,
    table_name     NAME,
    keyspace_start SMALLINT,
    keyspace_end   SMALLINT,
    epoch_id       INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    epoch_row _timescaledb_catalog.partition_epoch;
BEGIN
    SELECT *
    INTO STRICT epoch_row
    FROM _timescaledb_catalog.partition_epoch pe
    WHERE pe.id = epoch_id;

    IF epoch_row.partitioning_column IS NOT NULL THEN
        EXECUTE format(
            $$
                ALTER TABLE %1$I.%2$I
                ADD CONSTRAINT partition CHECK(%3$I.%4$s(%5$I::text, %6$L) BETWEEN %7$L AND %8$L)
            $$,
            schema_name, table_name,
            epoch_row.partitioning_func_schema, epoch_row.partitioning_func, epoch_row.partitioning_column,
            epoch_row.partitioning_mod, keyspace_start, keyspace_end);
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.set_time_constraint(
    schema_name NAME,
    table_name  NAME,
    start_time  BIGINT,
    end_time    BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    time_col_type regtype;
BEGIN
    time_col_type := _timescaledb_internal.time_col_type_for_chunk(schema_name, table_name);

    EXECUTE format(
        $$
            ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS time_range
        $$,
            schema_name, table_name);

    IF start_time IS NOT NULL AND end_time IS NOT NULL THEN
        EXECUTE format(
            $$
            ALTER TABLE %2$I.%3$I ADD CONSTRAINT time_range CHECK(%1$I >= %4$s AND %1$I <= %5$s)
        $$,
            _timescaledb_internal.time_col_name_for_chunk(schema_name, table_name),
            schema_name, table_name,
            _timescaledb_internal.time_literal_sql(start_time, time_col_type),
            _timescaledb_internal.time_literal_sql(end_time, time_col_type));
    ELSIF start_time IS NOT NULL THEN
        EXECUTE format(
            $$
            ALTER TABLE %I.%I ADD CONSTRAINT time_range CHECK(%I >= %s)
        $$,
            schema_name, table_name,
            _timescaledb_internal.time_col_name_for_chunk(schema_name, table_name),
            _timescaledb_internal.time_literal_sql(start_time, time_col_type));
    ELSIF end_time IS NOT NULL THEN
        EXECUTE format(
            $$
            ALTER TABLE %I.%I ADD CONSTRAINT time_range CHECK(%I <= %s)
            $$,
            schema_name, table_name,
            _timescaledb_internal.time_col_name_for_chunk(schema_name, table_name),
            _timescaledb_internal.time_literal_sql(end_time, time_col_type));
    END IF;
END
$BODY$
SET client_min_messages = WARNING -- supress notice by drop constraint if exists.
;
