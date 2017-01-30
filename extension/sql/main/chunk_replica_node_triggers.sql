-- Creates/drops tables (and associated indexes) for chunk_replica_node rows.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.on_change_chunk_replica_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    partition_replica_row _iobeamdb_catalog.partition_replica;
    chunk_row             _iobeamdb_catalog.chunk;
    kind                  pg_class.relkind%type;
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT *
        INTO STRICT partition_replica_row
        FROM _iobeamdb_catalog.partition_replica AS p
        WHERE p.id = NEW.partition_replica_id;

        SELECT *
        INTO STRICT chunk_row
        FROM _iobeamdb_catalog.chunk AS c
        WHERE c.id = NEW.chunk_id;

        IF NEW.database_name = current_database() THEN
            PERFORM _iobeamdb_internal.create_local_data_table(NEW.schema_name, NEW.table_name,
                                                         partition_replica_row.schema_name,
                                                         partition_replica_row.table_name);

            PERFORM _iobeamdb_internal.create_chunk_replica_node_index(NEW.schema_name, NEW.table_name,
                                    h.main_schema_name, h.main_index_name, h.definition)
            FROM _iobeamdb_catalog.hypertable_index h
            WHERE h.hypertable_id = partition_replica_row.hypertable_id;
        ELSE
            PERFORM _iobeamdb_internal.create_remote_table(NEW.schema_name, NEW.table_name,
                                                     partition_replica_row.schema_name, partition_replica_row.table_name,
                                                     NEW.database_name);
        END IF;

        PERFORM _iobeamdb_internal.set_time_constraint(NEW.schema_name, NEW.table_name, chunk_row.start_time, chunk_row.end_time);

        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        --when deleting the chunk replica row from the metadata table,
        --also DROP the actual chunk replica table that holds data.
        --Note that the table could already be deleted in case this
        --trigger fires as a result of a DROP TABLE on the hypertable
        --that this chunk belongs to.
        SELECT c.relkind INTO kind
        FROM pg_class c
        WHERE relname = OLD.table_name AND relnamespace = OLD.schema_name::regnamespace;

        IF kind IS NULL THEN
            RETURN OLD;
        END IF;

        IF kind = 'f' THEN
            EXECUTE format(
                $$
                DROP FOREIGN TABLE %I.%I
                $$, OLD.schema_name, OLD.table_name
            );
        ELSE
            EXECUTE format(
                $$
                DROP TABLE %I.%I
                $$, OLD.schema_name, OLD.table_name
            );
        END IF;
        RETURN OLD;
    END IF;

    PERFORM _iobeamdb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
