-- Initializes a meta node in the cluster
CREATE OR REPLACE FUNCTION setup_meta()
    RETURNS void LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    table_name NAME;
BEGIN

    DROP TRIGGER IF EXISTS trigger_on_create_chunk_replica_node
    ON chunk_replica_node;
    CREATE TRIGGER trigger_on_create_chunk_replica_node AFTER INSERT OR UPDATE OR DELETE ON chunk_replica_node
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_chunk_replica_node_meta();

    DROP TRIGGER IF EXISTS trigger_on_create_chunk
    ON chunk;
    CREATE TRIGGER trigger_on_create_chunk AFTER INSERT OR UPDATE OR DELETE ON chunk
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_chunk();

    DROP TRIGGER IF EXISTS trigger_create_hypertable
    ON hypertable;
    CREATE TRIGGER trigger_create_hypertable AFTER INSERT OR UPDATE OR DELETE ON hypertable
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_hypertable();

    DROP TRIGGER IF EXISTS trigger_create_node
    ON node;
    CREATE TRIGGER trigger_create_node BEFORE INSERT OR UPDATE OR DELETE ON node
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_node();

    DROP TRIGGER IF EXISTS trigger_sync_node
    ON node;
    CREATE TRIGGER trigger_sync_node AFTER INSERT ON node
    FOR EACH ROW EXECUTE PROCEDURE _meta.sync_node();

    DROP TRIGGER IF EXISTS trigger_create_partition
    ON partition;
    CREATE TRIGGER trigger_create_partition AFTER INSERT OR UPDATE OR DELETE ON partition
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_partition();

    FOREACH table_name IN ARRAY ARRAY ['cluster_user', 'hypertable', 'deleted_hypertable', 'hypertable_index', 'deleted_hypertable_index', 'hypertable_replica',
    'distinct_replica_node', 'partition_epoch', 'partition', 'partition_replica',
    'chunk_replica_node', 'field', 'deleted_field', 'meta', 'default_replica_node'] :: NAME [] LOOP
        EXECUTE format(
            $$
                DROP TRIGGER IF EXISTS trigger_0_sync_insert_%1$s ON %1$s;
                DROP TRIGGER IF EXISTS trigger_0_sync_update_%1$s ON %1$s;
                DROP TRIGGER IF EXISTS trigger_0_sync_delete_%1$s ON %1$s;
            $$,
            table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER trigger_0_sync_insert_%1$s AFTER INSERT ON %1$s
                FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_insert();
                CREATE TRIGGER trigger_0_sync_update_%1$s AFTER UPDATE ON %1$s
                FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_update();
                CREATE TRIGGER trigger_0_sync_delete_%1$s AFTER DELETE ON %1$s
                FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_delete();
            $$,
            table_name);
    END LOOP;

    FOREACH table_name IN ARRAY ARRAY ['field', 'hypertable_index', 'hypertable'] :: NAME [] LOOP
        EXECUTE format(
            $$
               DROP TRIGGER IF EXISTS trigger_0_deleted_%1$s ON %1$s
            $$,
            table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER trigger_0_deleted_%1$s AFTER DELETE ON %1$s
                FOR EACH ROW EXECUTE PROCEDURE _meta.log_delete();
            $$,
            table_name);
    END LOOP;

END
$BODY$;
