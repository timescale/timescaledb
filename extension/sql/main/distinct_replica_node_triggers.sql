/*
  Creates tables for distinct_replica_node rows.
*/
CREATE OR REPLACE FUNCTION _iobeamdb_internal.on_change_distinct_replica_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_replica_row _iobeamdb_catalog.hypertable_replica;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT *
        INTO STRICT hypertable_replica_row
        FROM _iobeamdb_catalog.hypertable_replica AS h
        WHERE h.hypertable_id = NEW.hypertable_id AND
              h.replica_id = NEW.replica_id;

        IF NEW.database_name = current_database() THEN
            PERFORM _iobeamdb_internal.create_local_distinct_table(NEW.schema_name, NEW.table_name,
                                                             hypertable_replica_row.distinct_schema_name,
                                                             hypertable_replica_row.distinct_table_name);
        ELSE
            PERFORM _iobeamdb_internal.create_remote_table(NEW.schema_name, NEW.table_name,
                                                     hypertable_replica_row.distinct_schema_name,
                                                     hypertable_replica_row.distinct_table_name, NEW.database_name);
        END IF;
        RETURN NEW;
    END IF;

    PERFORM _iobeamdb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$
SET SEARCH_PATH = 'public';
