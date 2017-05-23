/*
  This file creates function to intercept ddl commands executed on the
  main tables of hypertables and convert them to commands that alter
  the coresponding schema structure of hypertables.

  The basic idea is that the main table is a regular sql tables that represents
  the hypertable. DDL changes to that table have corresponding changes on the hypertable.

  The intercepted schema changes work as follows:
    1) a ddl command is intercepted and it is determined whether it's on a hypertable, if not exit.
    2) it is determined whether this is a user or trigger initate command. If it is trigger initiated, exit.
    3) send the corresponding hypertable ddl command to the meta node.

  Because ddl commands cannot be "canceled" without an error, all comands are allowed to succeed. This is
  why the command sent to the meta node always contains a parameter for the node the ddl command was executed on
  (so that the meta node can know that the main table on that node was already modified and does not needed to be
    modified again).

*/

--Handles ddl create index commands on hypertables
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
            --get table oid
            SELECT indrelid INTO STRICT table_oid
            FROM pg_catalog.pg_index
            WHERE indexrelid = info.objid;

            IF NOT _timescaledb_internal.is_main_table(table_oid) OR current_setting('timescaledb_internal.ignore_ddl') = 'on' THEN
                RETURN;
            END IF;

            hypertable_row := _timescaledb_internal.hypertable_from_main_table(table_oid);
            def = _timescaledb_internal.get_general_index_definition(info.objid, table_oid, hypertable_row);

            PERFORM _timescaledb_catalog.add_index(
                hypertable_row.id,
                hypertable_row.schema_name,
                (SELECT relname FROM pg_class WHERE oid = info.objid::regclass),
                def
            );
        END LOOP;
END
$BODY$;

--Handles ddl create trigger commands on hypertables
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

--Handles ddl alter index commands on hypertables
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

--Handles ddl drop index commands on hypertables
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

            --if table_oid is not null, it is a main table
            IF table_oid IS NULL OR current_setting('timescaledb_internal.ignore_ddl') = 'on' THEN
                RETURN;
            END IF;

            --TODO: this ignores the concurrently and cascade/restrict modifiers
            PERFORM _timescaledb_catalog.drop_index(info.schema_name, info.object_name);
        END LOOP;
END
$BODY$;

--Handles the following ddl alter table commands on hypertables:
-- ADD COLUMN, DROP COLUMN, ALTER COLUMN SET DEFAULT, ALTER COLUMN DROP DEFAULT, ALTER COLUMN SET/DROP NOT NULL
-- RENAME COLUMN
-- not supported (explicit):
-- ALTER COLUMN SET DATA TYPE
-- Other alter commands also not supported
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_alter_table()
    RETURNS event_trigger LANGUAGE plpgsql AS
$BODY$
DECLARE
    info           record;
    hypertable_row _timescaledb_catalog.hypertable;
    found_action   BOOLEAN;
    att_row        pg_attribute;
    rec            record;
BEGIN
    FOR info IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            --exit if not hypertable or inside trigger
            IF NOT _timescaledb_internal.is_main_table(info.objid) OR current_setting('timescaledb_internal.ignore_ddl') = 'on' THEN
                RETURN;
            END IF;

            hypertable_row := _timescaledb_internal.hypertable_from_main_table(info.objid);
            --Try to find what was done. If you can't find it error out.
            found_action = FALSE;

            --was a column added?

            IF ddl_is_add_column(info.command) THEN
               FOR att_row IN
               SELECT *
               FROM pg_attribute att
               WHERE attrelid = info.objid AND
                  attnum > 0 AND
                  NOT attisdropped AND
                  att.attnum NOT IN (SELECT c.attnum FROM _timescaledb_catalog.hypertable_column c WHERE hypertable_id = hypertable_row.id)
                LOOP
                    PERFORM  _timescaledb_internal.create_column_from_attribute(hypertable_row.id, att_row);
                END LOOP;

            found_action = TRUE;
            END IF;

            --was a column deleted
            IF ddl_is_drop_column(info.command) THEN
               FOR rec IN
               SELECT name
               FROM _timescaledb_catalog.hypertable_column c
               INNER JOIN pg_attribute att ON (attrelid = info.objid AND att.attnum = c.attnum AND attisdropped) --do not match on att.attname here. it gets mangled
               WHERE hypertable_id = hypertable_row.id
                     LOOP
                         PERFORM _timescaledb_catalog.drop_column(hypertable_row.id, rec.name);
                     END LOOP;
               found_action = TRUE;
            END IF;

            --was a column renamed
            FOR rec IN
            SELECT c.name old_name, att.attname new_name
            FROM _timescaledb_catalog.hypertable_column c
            LEFT JOIN pg_attribute att ON (attrelid = info.objid AND att.attnum = c.attnum AND NOT attisdropped)
            WHERE hypertable_id = hypertable_row.id AND c.name IS DISTINCT FROM att.attname
                LOOP
                    PERFORM _timescaledb_catalog.alter_table_rename_column(
                        hypertable_row.id,
                        rec.old_name,
                        rec.new_name
                    );
                    found_action = TRUE;
                END LOOP;

            --was a column default changed
            FOR rec IN
            SELECT name, _timescaledb_internal.get_default_value_for_attribute(att) AS new_default_value
            FROM _timescaledb_catalog.hypertable_column c
            LEFT JOIN pg_attribute att ON (attrelid = info.objid AND attname = c.name AND att.attnum = c.attnum AND NOT attisdropped)
            WHERE hypertable_id = hypertable_row.id AND _timescaledb_internal.get_default_value_for_attribute(att) IS DISTINCT FROM c.default_value
                LOOP
                    PERFORM _timescaledb_catalog.alter_column_set_default(
                        hypertable_row.id,
                        rec.name,
                        rec.new_default_value
                    );
                    found_action = TRUE;
            END LOOP;

            --was the not null flag changed?
            FOR rec IN
              SELECT name, attnotnull AS new_not_null
              FROM _timescaledb_catalog.hypertable_column c
              LEFT JOIN pg_attribute att ON (attrelid = info.objid AND attname = c.name AND att.attnum = c.attnum AND NOT attisdropped)
              WHERE hypertable_id = hypertable_row.id AND attnotnull != c.not_null
                LOOP
                    PERFORM _timescaledb_catalog.alter_column_set_not_null(
                        hypertable_row.id,
                        rec.name,
                        rec.new_not_null
                    );
                    found_action = TRUE;
                END LOOP;

            --type changed
            FOR rec IN
            SELECT name
            FROM _timescaledb_catalog.hypertable_column c
            INNER JOIN pg_attribute att ON (attrelid = info.objid AND attname = c.name AND att.attnum = c.attnum AND NOT attisdropped)
            WHERE hypertable_id = hypertable_row.id AND att.atttypid IS DISTINCT FROM c.data_type
                LOOP
                    RAISE EXCEPTION 'ALTER TABLE ... ALTER COLUMN SET DATA TYPE  not supported on hypertable %', info.objid::regclass
                    USING ERRCODE = 'IO101';
                END LOOP;

            IF NOT found_action THEN
              RAISE EXCEPTION 'Unknown alter table action on %', info.objid::regclass
              USING ERRCODE = 'IO101';
            END IF;
        END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.is_hypertable(
    schema_name NAME,
    table_name  NAME
)
    RETURNS BOOLEAN LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
     RETURN EXISTS(
         SELECT 1 FROM _timescaledb_catalog.hypertable h
         WHERE h.schema_name = is_hypertable.schema_name AND h.table_name = is_hypertable.table_name
     );
END
$BODY$;

--Handles drop table command
CREATE OR REPLACE FUNCTION _timescaledb_internal.ddl_process_drop_table()
        RETURNS event_trigger LANGUAGE plpgsql AS $BODY$
DECLARE
    obj record;
BEGIN
    IF current_setting('timescaledb_internal.ignore_ddl') = 'on' THEN
        RETURN;
    END IF;

    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF tg_tag = 'DROP TABLE' AND _timescaledb_internal.is_hypertable(obj.schema_name, obj.object_name) THEN
            PERFORM _timescaledb_catalog.drop_hypertable(obj.schema_name, obj.object_name);
        END IF;
    END LOOP;
END
$BODY$;
