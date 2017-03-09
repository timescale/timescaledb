-- This file contains triggers that act on the main 'hypertable' table as
-- well as triggers for newly created hypertables.

-- Trigger to prevent unsupported operations on the main table.
-- Our C code should rewrite a DELETE or UPDATE to apply to the replica and not main table.
-- The only exception is if we use ONLY, which should be an error.
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_unsupported_main_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'UPDATE ONLY not supported on hypertables'
        USING ERRCODE = 'IO101';
    ELSIF TG_OP = 'DELETE' AND current_setting('io.ignore_delete_in_trigger', true) IS DISTINCT FROM 'true' THEN
        RAISE EXCEPTION 'DELETE ONLY not currently supported on hypertables'
        USING ERRCODE = 'IO101';
    END IF;
    RETURN NEW;
END
$BODY$;

--Trigger for when stuff is inserted into main table (error because everything should go through a temp table)
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_modify_main_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    RAISE EXCEPTION 'Should never be inserting data directly into main table'
    USING ERRCODE = 'IO501';
END
$BODY$;

CREATE OR REPLACE FUNCTION  _timescaledb_internal.root_table_insert_trigger() RETURNS TRIGGER
	AS '$libdir/timescaledb', 'insert_root_table_trigger' LANGUAGE C;

CREATE OR REPLACE FUNCTION  _timescaledb_internal.root_table_after_insert_trigger() RETURNS TRIGGER
	AS '$libdir/timescaledb', 'insert_root_table_trigger_after' LANGUAGE C;

-- Trigger for handling the addition of new hypertables.
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_hypertable()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    remote_node _timescaledb_catalog.node;
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

        PERFORM _timescaledb_internal.create_table(NEW.root_schema_name, NEW.root_table_name);

        IF NEW.created_on <> current_database() THEN
           PERFORM _timescaledb_internal.create_schema(NEW.schema_name);
           PERFORM _timescaledb_internal.create_table(NEW.schema_name, NEW.table_name);
        END IF;

        -- UPDATE not supported, so do them before action
        EXECUTE format(
            $$
                CREATE TRIGGER modify_trigger BEFORE UPDATE OR DELETE ON %I.%I
                FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_internal.on_unsupported_main_table();
            $$, NEW.schema_name, NEW.table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER insert_trigger AFTER INSERT ON %I.%I
                FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_internal.on_modify_main_table();
            $$, NEW.schema_name, NEW.table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER insert_trigger BEFORE INSERT ON %I.%I
                FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.root_table_insert_trigger(%L, %L);
            $$, NEW.root_schema_name, NEW.root_table_name, NEW.schema_name, NEW.table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER after_insert_trigger AFTER INSERT ON %I.%I
                FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_internal.root_table_after_insert_trigger();
            $$, NEW.root_schema_name, NEW.root_table_name);
        RETURN NEW;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$
SET client_min_messages = WARNING; --suppress NOTICE on IF EXISTS schema


/*
    Drops public hypertable when hypertable rows deleted (row created in deleted_hypertable table).
*/
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_deleted_hypertable()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
    END IF;
    --drop index on all chunks

    PERFORM _timescaledb_internal.drop_root_table(NEW.root_schema_name, NEW.root_table_name);

    IF new.deleted_on <> current_database() THEN
      PERFORM set_config('io.ignore_ddl_in_trigger', 'true', true);
      EXECUTE format('DROP TABLE %I.%I', NEW.schema_name, NEW.table_name);
    END IF;

    RETURN NEW;
END
$BODY$
;
