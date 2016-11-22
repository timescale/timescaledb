CREATE OR REPLACE FUNCTION _sysinternal.on_create_partition()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % name ', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    INSERT INTO partition_replica (partition_id, hypertable_name, replica_id, schema_name, table_name)
        SELECT
            NEW.id,
            hypertable_name,
            replica_id,
            h.associated_schema_name,
            format('%s_%s_%s_partition', h.associated_table_prefix, NEW.id, replica_id)
        FROM hypertable_replica hr
        INNER JOIN hypertable h ON (h.name = hr.hypertable_name)
        WHERE hypertable_name = (
            SELECT hypertable_name
            FROM partition_epoch
            WHERE id = NEW.epoch_id
        );

    RETURN NEW;
END
$BODY$;

BEGIN;
DROP TRIGGER IF EXISTS trigger_create_partition
ON partition;
CREATE TRIGGER trigger_create_partition AFTER INSERT OR UPDATE OR DELETE ON partition
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_partition();
COMMIT;


