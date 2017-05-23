/*
  This file creates function to intercept ddl commands and inject
  our own logic to give the illusion that a hypertable is a single table.
  Namely we do 2 things:

  1) When an index on a hypertable is modified, those actions are mirrored
     on chunk tables.
  2) Drop hypertable commands are intercepted to clean up our own metadata tables.

*/

-- Handles ddl create index commands on hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_create_index()
    RETURNS event_trigger LANGUAGE plpgsql AS
$BODY$
DECLARE
    info           record;
    table_oid      regclass;
    def            TEXT;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
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
    RETURNS event_trigger LANGUAGE plpgsql AS
$BODY$
DECLARE
    info           record;
    table_oid      regclass;
    trigger_name            TEXT;
BEGIN
    FOR info IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            SELECT tgrelid, tgname INTO STRICT table_oid, trigger_name
            FROM pg_catalog.pg_trigger
            WHERE oid = info.objid;

            IF _timescaledb_internal.is_main_table(table_oid) 
                AND trigger_name != '_timescaledb_main_insert_trigger'
                AND trigger_name != '_timescaledb_main_after_insert_trigger'
                AND trigger_name != '_timescaledb_modify_trigger'
            THEN
                RAISE EXCEPTION 'CREATE TRIGGER not supported on hypertable %', table_oid
                USING ERRCODE = 'IO101';
            END IF;
        END LOOP;
END
$BODY$;

-- Handles ddl alter index commands on hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_alter_index()
    RETURNS event_trigger LANGUAGE plpgsql AS
$BODY$
DECLARE
    info record;
    table_oid regclass;
BEGIN
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
    RETURNS event_trigger LANGUAGE plpgsql AS
$BODY$
DECLARE
    info           record;
    table_oid      regclass;
    hypertable_row _timescaledb_catalog.hypertable;
BEGIN
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
