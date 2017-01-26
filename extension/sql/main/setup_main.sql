-- Initializes a data node in the cluster.
CREATE OR REPLACE FUNCTION setup_main()
    RETURNS void LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN

    DROP TRIGGER IF EXISTS trigger_main_on_change_chunk_replica_node_index
    ON _iobeamdb_catalog.chunk_replica_node_index;
    CREATE TRIGGER trigger_main_on_change_chunk_replica_node_index
    AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.chunk_replica_node_index
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_chunk_replica_node_index();

    DROP TRIGGER IF EXISTS trigger_main_on_change_chunk_replica_node
    ON _iobeamdb_catalog.chunk_replica_node;
    CREATE TRIGGER trigger_main_on_change_chunk_replica_node
    AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.chunk_replica_node
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_chunk_replica_node();

    DROP TRIGGER IF EXISTS trigger_main_on_change_chunk
    ON _iobeamdb_catalog.chunk;
    CREATE TRIGGER trigger_main_on_change_chunk
    -- no DELETE/INSERT: they would be no-ops
    AFTER UPDATE ON _iobeamdb_catalog.chunk
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_chunk();

    DROP TRIGGER IF EXISTS trigger_main_on_change_cluster_user
    ON _iobeamdb_catalog.cluster_user;
    CREATE TRIGGER trigger_main_on_change_cluster_user
    AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.cluster_user
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_cluster_user();

    DROP TRIGGER IF EXISTS trigger_main_on_change_distinct_replica_node
    ON _iobeamdb_catalog.distinct_replica_node;
    CREATE TRIGGER trigger_main_on_change_distinct_replica_node
    -- no DELETE: it would be a no-op
    AFTER INSERT OR UPDATE ON _iobeamdb_catalog.distinct_replica_node
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_distinct_replica_node();

    DROP TRIGGER IF EXISTS trigger_main_on_change_column
    ON _iobeamdb_catalog.hypertable_column;
    CREATE TRIGGER trigger_main_on_change_column
    -- no DELETE: it would be a no-op
    AFTER INSERT OR UPDATE ON _iobeamdb_catalog.hypertable_column
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_column();

    DROP TRIGGER IF EXISTS trigger_main_on_change_deleted_column
    ON _iobeamdb_catalog.deleted_hypertable_column;
    CREATE TRIGGER trigger_main_on_change_deleted_column
    AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.deleted_hypertable_column
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_deleted_column();

    -- no DELETE: it would be a no-op
    DROP TRIGGER IF EXISTS trigger_main_on_change_hypertable_replica
    ON _iobeamdb_catalog.hypertable_replica;
    CREATE TRIGGER trigger_main_on_change_hypertable_replica
    AFTER INSERT OR UPDATE ON _iobeamdb_catalog.hypertable_replica
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_hypertable_replica();

    -- no DELETE: it would be a no-op
    DROP TRIGGER IF EXISTS trigger_1_main_on_change_create_hypertable
    ON _iobeamdb_catalog.hypertable;
    CREATE TRIGGER trigger_1_main_on_change_hypertable
    AFTER INSERT OR UPDATE ON _iobeamdb_catalog.hypertable
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_hypertable();

    DROP TRIGGER IF EXISTS trigger_main_on_change_meta
    ON _iobeamdb_catalog.meta;
    CREATE TRIGGER trigger_main_on_change_meta
    AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.meta
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_meta();

    DROP TRIGGER IF EXISTS trigger_main_on_change_node
    ON _iobeamdb_catalog.node;
    CREATE TRIGGER trigger_main_on_change_node
    AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.node
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_node();

    -- no DELETE: it would be a no-op
    DROP TRIGGER IF EXISTS trigger_main_on_change_partition_replica_table
    ON _iobeamdb_catalog.partition_replica;
    CREATE TRIGGER trigger_main_on_change_partition_replica_table
    AFTER INSERT OR UPDATE ON _iobeamdb_catalog.partition_replica
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_partition_replica_table();

    -- no DELETE: it would be a no-op
    DROP TRIGGER IF EXISTS trigger_main_on_change_hypertable_index
    ON _iobeamdb_catalog.hypertable_index;
    CREATE TRIGGER trigger_main_on_change_hypertable_index
    AFTER INSERT OR UPDATE ON _iobeamdb_catalog.hypertable_index
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_hypertable_index();

    DROP TRIGGER IF EXISTS trigger_main_on_change_deleted_hypertable_index
    ON _iobeamdb_catalog.deleted_hypertable_index;
    CREATE TRIGGER trigger_main_on_change_deleted_hypertable_index
    AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.deleted_hypertable_index
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_deleted_hypertable_index();

    DROP TRIGGER IF EXISTS trigger_on_change_deleted_hypertable
    ON _iobeamdb_catalog.deleted_hypertable;
    CREATE TRIGGER trigger_on_change_deleted_hypertable
    AFTER INSERT OR UPDATE OR DELETE ON _iobeamdb_catalog.deleted_hypertable
    FOR EACH ROW EXECUTE PROCEDURE _iobeamdb_internal.on_change_deleted_hypertable();

    CREATE EVENT TRIGGER ddl_create_index ON ddl_command_end
        WHEN tag IN ('create index')
        EXECUTE PROCEDURE _iobeamdb_internal.ddl_process_create_index();

    CREATE EVENT TRIGGER ddl_alter_index ON ddl_command_end
        WHEN tag IN ('alter index')
        EXECUTE PROCEDURE _iobeamdb_internal.ddl_process_alter_index();

    CREATE EVENT TRIGGER ddl_drop_index ON sql_drop
        WHEN tag IN ('drop index')
        EXECUTE PROCEDURE _iobeamdb_internal.ddl_process_drop_index();

    CREATE EVENT TRIGGER ddl_create_column ON ddl_command_end
       WHEN tag IN ('alter table')
       EXECUTE PROCEDURE _iobeamdb_internal.ddl_process_alter_table();

    CREATE EVENT TRIGGER ddl_check_drop_command
       ON sql_drop
       EXECUTE PROCEDURE _iobeamdb_internal.ddl_process_drop_table();

END
$BODY$
SET client_min_messages = WARNING --supress notices for trigger drops
;
