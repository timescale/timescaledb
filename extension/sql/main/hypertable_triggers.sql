-- This file contains triggers that act on the main 'hypertable' table as
-- well as triggers for newly created hypertables.

-- Trigger to prevent unsupported operations on the main table.
CREATE OR REPLACE FUNCTION _sysinternal.on_unsupported_main_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'UPDATEs not supported on hypertables'
        USING ERRCODE = 'IO101';
    ELSIF TG_OP = 'DELETE' AND current_setting('io.ignore_delete_in_trigger', true) <> 'true' THEN
        RAISE EXCEPTION 'DELETEs not currently supported on hypertables'
        USING ERRCODE = 'IO101';
    END IF;
    RETURN NEW;
END
$BODY$;

-- Trigger function to move INSERT'd data on the main table to child tables.
-- After data is inserted on the main table, it is placed in the correct
-- partition tables based on its partition key and time.
CREATE OR REPLACE FUNCTION _sysinternal.on_modify_main_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            SELECT insert_data(
                (SELECT name FROM hypertable
                WHERE main_schema_name = %1$L AND main_table_name = %2$L)
                , %3$L)
        $$, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_RELID);
    RETURN NEW;
END
$BODY$;

-- Trigger for handling the addition of new hypertables.
CREATE OR REPLACE FUNCTION _sysinternal.on_create_hypertable()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    remote_node node;
BEGIN

    IF TG_OP = 'INSERT' THEN

        EXECUTE format(
            $$
                CREATE SCHEMA IF NOT EXISTS %I
            $$, NEW.associated_schema_name);

        PERFORM _sysinternal.create_table(NEW.root_schema_name, NEW.root_table_name);
        PERFORM _sysinternal.create_root_distinct_table(NEW.distinct_schema_name, NEW.distinct_table_name);

        IF NEW.created_on <> current_database() THEN
           PERFORM _sysinternal.create_table(NEW.main_schema_name, NEW.main_table_name);
        END IF;

        -- UPDATE not supported, so do them before action
        EXECUTE format(
            $$
                CREATE TRIGGER modify_trigger BEFORE UPDATE OR DELETE ON %I.%I
                FOR EACH STATEMENT EXECUTE PROCEDURE _sysinternal.on_unsupported_main_table();
            $$, NEW.main_schema_name, NEW.main_table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER insert_trigger AFTER INSERT ON %I.%I
                FOR EACH STATEMENT EXECUTE PROCEDURE _sysinternal.on_modify_main_table();
            $$, NEW.main_schema_name, NEW.main_table_name);

        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN

        PERFORM _sysinternal.drop_root_table(OLD.root_schema_name, OLD.root_table_name);
        PERFORM _sysinternal.drop_root_distinct_table(OLD.distinct_schema_name, OLD.distinct_table_name);

        if current_setting('io.iobeam_drop', true) <> 'true' THEN
            PERFORM set_config('io.iobeam_drop', 'true', true);
            PERFORM _sysinternal.drop_table(OLD.main_schema_name, OLD.main_table_name);
        END IF;

        RETURN OLD;
    END IF;

    RAISE EXCEPTION 'Only inserts and delete supported on hypertable metadata table'
        USING ERRCODE = 'IO101';

END
$BODY$
SET SEARCH_PATH = 'public';
