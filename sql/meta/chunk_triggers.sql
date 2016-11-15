CREATE OR REPLACE FUNCTION _sysinternal.on_create_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    field_row field;
    schema_name NAME;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (
               (OLD.start_time IS NULL AND new.start_time IS NOT NULL)
               OR
               (OLD.end_time IS NULL AND new.end_time IS NOT NULL)
           )
           AND (
               OLD.id = NEW.id AND
               OLD.partition_id = NEW.partition_id 
           ) THEN
          NULL;
        ELSE
            RAISE EXCEPTION 'This type of update not allowed on % table', TG_TABLE_NAME
            USING ERRCODE = 'IO101';
        END IF;
    ELSIF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts and updates supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    --sync data on insert
    IF TG_OP = 'INSERT' THEN
      FOR schema_name IN
      SELECT n.schema_name
      FROM node AS n
      LOOP
          EXECUTE format(
              $$
                  INSERT INTO %I.%I SELECT $1.*
              $$,
              schema_name,
              TG_TABLE_NAME
          )
          USING NEW;
      END LOOP;

      --do not sync data on update. synced by close_chunk logic.

      --TODO: random node picking broken (should make sure replicas are on different nodes). also stickiness.
      INSERT INTO chunk_replica_node(chunk_id,partition_replica_id, database_name, schema_name, table_name)
      SELECT NEW.id, 
             pr.id, 
             (SELECT database_name FROM node ORDER BY random() LIMIT 1),
             pr.schema_name,
             format('%s_%s_%s_%s_data', h.associated_table_prefix, pr.id, pr.replica_id, NEW.id)
      FROM partition_replica pr
      INNER JOIN hypertable h ON (h.name = pr.hypertable_name)
      WHERE pr.partition_id = NEW.partition_id;
    END IF;

    RETURN NEW;
END
$BODY$;

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_chunk
ON chunk;
CREATE TRIGGER trigger_on_create_chunk AFTER INSERT OR UPDATE OR DELETE ON chunk
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_chunk();
COMMIT;

