CREATE OR REPLACE FUNCTION _sysinternal.sync_only_insert()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

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
    RETURN NEW;
END
$BODY$;

DO
$BODY$
DECLARE
    table_name NAME;
BEGIN
    FOREACH table_name IN ARRAY ARRAY ['cluster_user', 'hypertable', 'hypertable_replica',
    'distinct_replica_node', 'partition_epoch', 'partition', 'partition_replica',
    'chunk_replica_node', 'field', 'meta'] :: NAME [] LOOP
        EXECUTE format(
            $$
                DROP TRIGGER IF EXISTS trigger_0_sync_%1$s ON %1$s
            $$,
            table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER trigger_0_sync_%1$s AFTER INSERT OR UPDATE OR DELETE ON %1$s
                FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_only_insert();
            $$,
            table_name);
    END LOOP;
END
$BODY$;

