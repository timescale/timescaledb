-- Initializes a meta node in the cluster
CREATE OR REPLACE FUNCTION setup_meta()
    RETURNS void LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    table_name NAME;
BEGIN

    DROP TRIGGER IF EXISTS trigger_meta_on_create_chunk_replica_node
    ON _iobeamdb_catalog.chunk_replica_node;
    CREATE TRIGGER trigger_meta_on_create_chunk_replica_node AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.chunk_replica_node
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_chunk_replica_node_meta();

    DROP TRIGGER IF EXISTS trigger_meta_on_create_chunk
    ON _iobeamdb_catalog.chunk;
    CREATE TRIGGER trigger_meta_on_create_chunk AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.chunk
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_chunk();

    DROP TRIGGER IF EXISTS trigger_2_meta_create_hypertable
    ON _iobeamdb_catalog.hypertable;
    CREATE TRIGGER trigger_2_meta_create_hypertable AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.hypertable
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_hypertable();

    DROP TRIGGER IF EXISTS trigger_meta_create_node
    ON _iobeamdb_catalog.node;
    CREATE TRIGGER trigger_meta_create_node BEFORE INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.node
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_node();

    DROP TRIGGER IF EXISTS trigger_meta_sync_node
    ON _iobeamdb_catalog.node;
    CREATE TRIGGER trigger_meta_sync_node AFTER INSERT ON _iobeamdb_catalog.node
    FOR EACH ROW EXECUTE PROCEDURE _meta.sync_node();

    DROP TRIGGER IF EXISTS trigger_meta_create_partition
    ON _iobeamdb_catalog.partition;
    CREATE TRIGGER trigger_meta_create_partition AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.partition
    FOR EACH ROW EXECUTE PROCEDURE _meta.on_create_partition();

    --Setup sync triggers for tables that are mirrored on data nodes. Exclude 'chunk' table,
    --because it has its own sync trigger in chunk_triggers.sql
    FOREACH table_name IN ARRAY ARRAY ['cluster_user', 'meta', 'hypertable', 'deleted_hypertable', 'hypertable_index', 'deleted_hypertable_index',
    'hypertable_column', 'deleted_hypertable_column', 'hypertable_replica', 'default_replica_node', 'partition_epoch',
    'partition', 'partition_replica', 'distinct_replica_node', 'chunk_replica_node'] :: NAME [] LOOP
        EXECUTE format(
            $$
                DROP TRIGGER IF EXISTS trigger_0_meta_sync_insert_%1$s ON _iobeamdb_catalog.%1$s;
                DROP TRIGGER IF EXISTS trigger_0_meta_sync_update_%1$s ON _iobeamdb_catalog.%1$s;
                DROP TRIGGER IF EXISTS trigger_0_meta_sync_delete_%1$s ON _iobeamdb_catalog.%1$s;
            $$,
            table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER trigger_0_meta_sync_insert_%1$s AFTER INSERT ON _iobeamdb_catalog.%1$s
                FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_insert();
                CREATE TRIGGER trigger_0_meta_sync_update_%1$s AFTER UPDATE ON _iobeamdb_catalog.%1$s
                FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_update();
                CREATE TRIGGER trigger_0_meta_sync_delete_%1$s AFTER DELETE ON _iobeamdb_catalog.%1$s
                FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_delete();
            $$,
            table_name);
    END LOOP;

    FOREACH table_name IN ARRAY ARRAY ['hypertable_column', 'hypertable_index', 'hypertable'] :: NAME [] LOOP
        EXECUTE format(
            $$
                DROP TRIGGER IF EXISTS trigger_0_meta_deleted_%1$s ON _iobeamdb_catalog.%1$s
            $$,
            table_name);
        EXECUTE format(
            $$
                CREATE TRIGGER trigger_0_meta_deleted_%1$s BEFORE DELETE ON _iobeamdb_catalog.%1$s
                FOR EACH ROW EXECUTE PROCEDURE _meta.log_delete();
            $$,
            table_name);
    END LOOP;

END
$BODY$
SET client_min_messages = WARNING --supress notices for trigger drops
;
