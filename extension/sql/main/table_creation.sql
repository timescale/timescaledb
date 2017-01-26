-- This file contains functions and triggers associated with creating new
-- hypertables.

-- Creates a new schema if it does not exist.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_schema(
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
SET client_min_messages = WARNING --suppress NOTICE on IF EXISTS
;

-- Creates a table for a hypertable (e.g. main table or root table)
CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %I.%I (
            )
        $$, schema_name, table_name);
END
$BODY$;

-- Drop main table if it exists.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.drop_main_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            DROP TABLE IF EXISTS %I.%I;
        $$, schema_name, table_name);
END
$BODY$;

-- Drops root table
CREATE OR REPLACE FUNCTION _iobeamdb_internal.drop_root_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            DROP TABLE %I.%I CASCADE;
        $$, schema_name, table_name);
END
$BODY$;

-- Creates a root distinct table for a hypertable.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_root_distinct_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I (
                column_name TEXT,
                value TEXT,
                PRIMARY KEY(column_name, value)
            );
        $$, schema_name, table_name);
END
$BODY$;

-- Drops root distinct table
CREATE OR REPLACE FUNCTION _iobeamdb_internal.drop_root_distinct_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            DROP TABLE %1$I.%2$I CASCADE;
        $$, schema_name, table_name);
END
$BODY$;


CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_local_distinct_table(
    schema_name         NAME,
    table_name          NAME,
    replica_schema_name NAME,
    replica_table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I (PRIMARY KEY(column_name, value))
            INHERITS(%3$I.%4$I)
        $$, schema_name, table_name, replica_schema_name, replica_table_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_remote_table(
    schema_name        NAME,
    table_name         NAME,
    parent_schema_name NAME,
    parent_table_name  NAME,
    database_name      NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    node_row _iobeamdb_catalog.node;
BEGIN
    SELECT *
    INTO STRICT node_row
    FROM _iobeamdb_catalog.node n
    WHERE n.database_name = create_remote_table.database_name;

    EXECUTE format(
        $$
            CREATE FOREIGN TABLE IF NOT EXISTS %1$I.%2$I () INHERITS(%3$I.%4$I) SERVER %5$I
            OPTIONS (schema_name %1$L, table_name %2$L)
        $$,
        schema_name, table_name, parent_schema_name, parent_table_name, node_row.server_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_local_data_table(
    schema_name        NAME,
    table_name         NAME,
    parent_schema_name NAME,
    parent_table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I () INHERITS(%3$I.%4$I);
        $$,
        schema_name, table_name, parent_schema_name, parent_table_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_replica_table(
    schema_name        NAME,
    table_name         NAME,
    parent_schema_name NAME,
    parent_table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I () INHERITS(%3$I.%4$I)
        $$,
        schema_name, table_name, parent_schema_name, parent_table_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_data_partition_table(
    schema_name        NAME,
    table_name         NAME,
    parent_schema_name NAME,
    parent_table_name  NAME,
    keyspace_start     SMALLINT,
    keyspace_end       SMALLINT,
    epoch_id           INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    epoch_row     _iobeamdb_catalog.partition_epoch;
    column_exists BOOLEAN;
BEGIN
    SELECT *
    INTO STRICT epoch_row
    FROM _iobeamdb_catalog.partition_epoch pe
    WHERE pe.id = epoch_id;

    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I (
            ) INHERITS(%3$I.%4$I)
        $$,
        schema_name, table_name, parent_schema_name, parent_table_name);

    SELECT COUNT(*) > 0
    INTO column_exists
    FROM _iobeamdb_catalog.hypertable_column c
    WHERE c.hypertable_id = epoch_row.hypertable_id
          AND c.name = epoch_row.partitioning_column;

    IF column_exists THEN
        PERFORM _iobeamdb_internal.add_partition_constraint(schema_name, table_name, keyspace_start, keyspace_end, epoch_id);
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.add_partition_constraint(
    schema_name    NAME,
    table_name     NAME,
    keyspace_start SMALLINT,
    keyspace_end   SMALLINT,
    epoch_id       INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    epoch_row _iobeamdb_catalog.partition_epoch;
BEGIN
    SELECT *
    INTO STRICT epoch_row
    FROM _iobeamdb_catalog.partition_epoch pe
    WHERE pe.id = epoch_id;

    IF epoch_row.partitioning_column IS NOT NULL THEN
        EXECUTE format(
            $$
                ALTER TABLE %1$I.%2$I
                ADD CONSTRAINT partition CHECK(%3$s(%4$I::text, %5$L) BETWEEN %6$L AND %7$L)
            $$,
            schema_name, table_name,
            epoch_row.partitioning_func, epoch_row.partitioning_column,
            epoch_row.partitioning_mod, keyspace_start, keyspace_end);
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.set_time_constraint(
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
    time_col_type := _iobeamdb_internal.time_col_type_for_crn(schema_name, table_name);

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
            _iobeamdb_internal.time_col_name_for_crn(schema_name, table_name),
            schema_name, table_name,
            _iobeamdb_internal.time_literal_sql(start_time, time_col_type),
            _iobeamdb_internal.time_literal_sql(end_time, time_col_type));
    ELSIF start_time IS NOT NULL THEN
        EXECUTE format(
            $$
            ALTER TABLE %I.%I ADD CONSTRAINT time_range CHECK(%I >= %s)
        $$,
            schema_name, table_name,
            _iobeamdb_internal.time_col_name_for_crn(schema_name, table_name),
            _iobeamdb_internal.time_literal_sql(start_time, time_col_type));
        ELSIF end_time IS NOT NULL THEN
        EXECUTE format(
            $$
            ALTER TABLE %I.%I ADD CONSTRAINT time_range CHECK(%I <= %s)
        $$,
            schema_name, table_name,
            _iobeamdb_internal.time_col_name_for_crn(schema_name, table_name),
            _iobeamdb_internal.time_literal_sql(end_time, time_col_type));
    END IF;
END
$BODY$
SET client_min_messages = WARNING --supress notice by drop constraint if exists.
;
