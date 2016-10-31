CREATE OR REPLACE FUNCTION _sysinternal.create_data_table_index(
    table_oid  REGCLASS,
    field_name NAME,
    index_type field_index_type
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    index_name NAME;
    prefix     BIGINT;
BEGIN
    prefix = nextval('data_table_index_name_prefix');
    IF index_type = 'TIME-VALUE' THEN
        index_name := format('%s-time-%s', prefix, field_name);
    ELSIF index_type = 'VALUE-TIME' THEN
        index_name := format('%s-%s-time', prefix, field_name);
    ELSE
        RAISE EXCEPTION 'Unknown index type %', index_type;
    END IF;

    INSERT INTO data_table_index (table_oid, field_name, index_name, index_type) VALUES
        (table_oid, field_name, index_name, index_type)
    ON CONFLICT DO NOTHING;
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.on_create_data_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    field_row field;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (
               (OLD.start_time IS NULL AND new.start_time IS NOT NULL)
               OR
               (OLD.end_time IS NULL AND new.end_time IS NOT NULL)
           )
           AND (
               OLD.table_oid = NEW.table_oid AND
               OLD.namespace_name = NEW.namespace_name AND
               OLD.partition_number = NEW.partition_number AND
               OLD.total_partitions = NEW.total_partitions AND
               OLD.partitioning_field = NEW.partitioning_field
           ) THEN
            RETURN NEW;
        ELSE
            RAISE EXCEPTION 'This type of update not allowed on data_table';
        END IF;
    END IF;

    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts and updates supported on data_table table';
    END IF;

    FOR field_row IN SELECT f.*
                     FROM field AS f
                     WHERE f.namespace_name = NEW.namespace_name LOOP
        PERFORM _sysinternal.create_data_table_index(NEW.table_oid, field_row.name, index_type)
        FROM unnest(field_row.index_types) AS index_type;
    END LOOP;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_data_table
ON data_table;
CREATE TRIGGER trigger_on_create_data_table AFTER INSERT OR UPDATE OR DELETE ON data_table
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_data_table();
COMMIT;


CREATE OR REPLACE FUNCTION _sysinternal.on_create_data_table_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on namespace table';
    END IF;

    IF NEW.index_type = 'TIME-VALUE' THEN
        EXECUTE format(
            $$
                CREATE INDEX %1$I ON %2$s
                USING BTREE(time DESC NULLS LAST, %3$I)
                WHERE %3$I IS NOT NULL
            $$,
            NEW.index_name, NEW.table_oid, NEW.field_name);
    ELSIF NEW.index_type = 'VALUE-TIME' THEN
        EXECUTE format(
            $$
                CREATE INDEX %1$I ON %2$s
                USING BTREE(%3$I, time DESC NULLS LAST)
                WHERE %3$I IS NOT NULL
            $$,
            NEW.index_name, NEW.table_oid, NEW.field_name);
    ELSE
        RAISE EXCEPTION 'Unknown index type %', index_type;
    END IF;
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_data_table_index
ON data_table_index;
CREATE TRIGGER trigger_on_create_data_table_index AFTER INSERT OR UPDATE OR DELETE ON data_table_index
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_data_table_index();
COMMIT;
