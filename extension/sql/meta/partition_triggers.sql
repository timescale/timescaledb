CREATE OR REPLACE FUNCTION _meta.on_create_partition()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'INSERT' THEN
    
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
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RAISE EXCEPTION 'Only inserts and deletes supported on % name ', TG_TABLE_NAME
         USING ERRCODE = 'IO101';

END
$BODY$;
