CREATE OR REPLACE FUNCTION _sysinternal.on_create_chunk_replica_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    partition_replica_row partition_replica;
    chunk_row chunk;
    field_row field;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    SELECT *
    INTO STRICT partition_replica_row
    FROM partition_replica AS p
    WHERE p.id = NEW.partition_replica_id;

    SELECT *
    INTO STRICT chunk_row
    FROM chunk AS c
    WHERE c.id = NEW.chunk_id;

    IF NEW.database_name = current_database() THEN
       PERFORM _sysinternal.create_local_data_table(NEW.schema_name, NEW.table_name, 
        partition_replica_row.schema_name, partition_replica_row.table_name);

        FOR field_row IN SELECT f.*
                     FROM field AS f
                     WHERE f.hypertable_name = partition_replica_row.hypertable_name LOOP
          PERFORM _sysinternal.create_chunk_replica_node_index(NEW.schema_name, NEW.table_name, field_row.name, index_type)
          FROM unnest(field_row.index_types) AS index_type;
        END LOOP;
    ELSE
       PERFORM _sysinternal.create_remote_table(NEW.schema_name, NEW.table_name, 
       partition_replica_row.schema_name, partition_replica_row.table_name, NEW.database_name);
    END IF;

    PERFORM _sysinternal.set_time_constraint(NEW.schema_name, NEW.table_name, chunk_row.start_time, chunk_row.end_time);

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_chunk_replica_node
ON chunk_replica_node;
CREATE TRIGGER  trigger_on_create_chunk_replica_node AFTER INSERT OR UPDATE OR DELETE ON chunk_replica_node
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_chunk_replica_node();
COMMIT;
