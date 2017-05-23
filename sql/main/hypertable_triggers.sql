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

CREATE OR REPLACE FUNCTION  _timescaledb_internal.main_table_insert_trigger() RETURNS TRIGGER
	AS '$libdir/timescaledb', 'insert_main_table_trigger' LANGUAGE C;

CREATE OR REPLACE FUNCTION  _timescaledb_internal.main_table_after_insert_trigger() RETURNS TRIGGER
        AS '$libdir/timescaledb', 'insert_main_table_trigger_after' LANGUAGE C;

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

        IF current_setting('timescaledb_internal.originating_node') <> 'on' THEN
           PERFORM _timescaledb_internal.create_schema(NEW.schema_name);
           PERFORM _timescaledb_internal.create_table(NEW.schema_name, NEW.table_name);
        END IF;

        -- UPDATE not supported, so do them before action
        EXECUTE format(
            $$
                CREATE TRIGGER _timescaledb_modify_trigger BEFORE UPDATE OR DELETE ON %I.%I
                FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_internal.on_unsupported_main_table();
            $$, NEW.schema_name, NEW.table_name);
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
        PERFORM _timescaledb_internal.drop_root_table(OLD.root_schema_name, OLD.root_table_name);
        IF current_setting('timescaledb_internal.originating_node') <> 'on' THEN
              PERFORM set_config('timescaledb_internal.ignore_ddl', 'true', true);
              EXECUTE format('DROP TABLE %I.%I', OLD.schema_name, OLD.table_name);
        END IF;
        RETURN OLD;
    ELSIF TG_OP = 'UPDATE' THEN
       RETURN NEW;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$
SET client_min_messages = WARNING; --suppress NOTICE on IF EXISTS schema

