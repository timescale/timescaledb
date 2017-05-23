-- This file contains triggers that act on the main 'hypertable' table as
-- well as triggers for newly created hypertables.

-- These trigger functions intercept regular inserts and implement our smart insert fastpath
CREATE OR REPLACE FUNCTION  _timescaledb_internal.main_table_insert_trigger() RETURNS TRIGGER
	AS '$libdir/timescaledb', 'insert_main_table_trigger' LANGUAGE C;

CREATE OR REPLACE FUNCTION  _timescaledb_internal.main_table_after_insert_trigger() RETURNS TRIGGER
        AS '$libdir/timescaledb', 'insert_main_table_trigger_after' LANGUAGE C;

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

