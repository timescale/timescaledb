CREATE OR REPLACE FUNCTION _sysinternal.on_create_hypertable()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    remote_node node;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on namespace table'
        USING ERRCODE = 'IO101';
    END IF;

    PERFORM _sysinternal.create_schema(NEW.main_schema_name);
    PERFORM _sysinternal.create_schema(NEW.associated_schema_name);
    PERFORM _sysinternal.create_schema(NEW.root_schema_name);
    PERFORM _sysinternal.create_schema(NEW.distinct_schema_name);

    PERFORM _sysinternal.create_root_table(NEW.root_schema_name, NEW.root_table_name);
    PERFORM _sysinternal.create_root_distinct_table(NEW.distinct_schema_name, NEW.distinct_table_name);
    PERFORM _sysinternal.create_insert_temp_table(NEW.associated_schema_name, NEW.insert_temp_table_name);

    IF NEW.created_on <> current_database() THEN
       PERFORM _sysinternal.create_main_table(NEW.main_schema_name, NEW.main_table_name);
    END IF;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';
