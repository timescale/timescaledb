CREATE OR REPLACE FUNCTION _iobeamdb_internal.on_change_hypertable_replica()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_row _iobeamdb_catalog.hypertable;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT *
        INTO STRICT hypertable_row
        FROM _iobeamdb_catalog.hypertable AS h
        WHERE h.id = NEW.hypertable_id;

        PERFORM _iobeamdb_internal.create_replica_table(NEW.schema_name, NEW.table_name, hypertable_row.root_schema_name,
                                                        hypertable_row.root_table_name);
        PERFORM _iobeamdb_internal.create_replica_table(NEW.distinct_schema_name, NEW.distinct_table_name,
                                                        hypertable_row.distinct_schema_name, hypertable_row.distinct_table_name);
        RETURN NEW;
    END IF;

    PERFORM _iobeamdb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$
SET SEARCH_PATH = 'public';
