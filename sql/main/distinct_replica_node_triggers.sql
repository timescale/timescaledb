CREATE OR REPLACE FUNCTION _sysinternal.on_create_distinct_replica_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_replica_row hypertable_replica;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    SELECT *
    INTO STRICT hypertable_replica_row
    FROM hypertable_replica AS h
    WHERE h.hypertable_name = NEW.hypertable_name AND
          h.replica_id = NEW.replica_id;

    IF NEW.database_name = current_database() THEN
      PERFORM _sysinternal.create_local_distinct_table(NEW.schema_name, NEW.table_name, 
      hypertable_replica_row.distinct_schema_name, hypertable_replica_row.distinct_table_name); 
    ELSE
      PERFORM _sysinternal.create_remote_table(NEW.schema_name, NEW.table_name, 
      hypertable_replica_row.distinct_schema_name, hypertable_replica_row.distinct_table_name, NEW.database_name);
    END IF;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_distinct_replica_node
ON distinct_replica_node;
CREATE TRIGGER  trigger_on_create_distinct_replica_node AFTER INSERT OR UPDATE OR DELETE ON distinct_replica_node
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_distinct_replica_node();
COMMIT;
