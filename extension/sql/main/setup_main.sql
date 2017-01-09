-- Initializes a data node in the cluster.
CREATE OR REPLACE FUNCTION setup_main()
    RETURNS void LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN

    DROP TRIGGER IF EXISTS trigger_on_create_chunk_replica_node_index
    ON chunk_replica_node_index;
    CREATE TRIGGER trigger_on_create_chunk_replica_node_index AFTER INSERT OR UPDATE OR DELETE ON chunk_replica_node_index
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_chunk_replica_node_index();

    DROP TRIGGER IF EXISTS trigger_on_create_chunk_replica_node
    ON chunk_replica_node;
    CREATE TRIGGER trigger_on_create_chunk_replica_node AFTER INSERT OR UPDATE OR DELETE ON chunk_replica_node
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_chunk_replica_node();

    DROP TRIGGER IF EXISTS trigger_on_create_chunk
    ON chunk;
    CREATE TRIGGER trigger_on_create_chunk AFTER INSERT OR UPDATE OR DELETE ON chunk
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_chunk();

    DROP TRIGGER IF EXISTS trigger_on_create_cluster_user
    ON cluster_user;
    CREATE TRIGGER trigger_on_create_cluster_user AFTER INSERT OR UPDATE OR DELETE ON cluster_user
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_cluster_user();

    DROP TRIGGER IF EXISTS trigger_on_create_distinct_replica_node
    ON distinct_replica_node;
    CREATE TRIGGER trigger_on_create_distinct_replica_node AFTER INSERT OR UPDATE OR DELETE ON distinct_replica_node
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_distinct_replica_node();

    DROP TRIGGER IF EXISTS trigger_on_modify_field
    ON field;
    CREATE TRIGGER trigger_on_modify_field AFTER INSERT OR UPDATE OR DELETE ON field
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_modify_field();

    DROP TRIGGER IF EXISTS trigger_on_deleted_field
    ON deleted_field;
    CREATE TRIGGER trigger_on_deleted_field AFTER INSERT OR UPDATE OR DELETE ON deleted_field
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_deleted_field();

    DROP TRIGGER IF EXISTS trigger_on_create_hypertable_replica
    ON hypertable_replica;
    CREATE TRIGGER trigger_on_create_hypertable_replica AFTER INSERT OR UPDATE OR DELETE ON hypertable_replica
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_hypertable_replica();

    DROP TRIGGER IF EXISTS trigger_on_create_hypertable
    ON hypertable;
    CREATE TRIGGER trigger_on_create_hypertable AFTER INSERT OR UPDATE OR DELETE ON hypertable
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_hypertable();

    DROP TRIGGER IF EXISTS trigger_on_create_meta
    ON meta;
    CREATE TRIGGER trigger_on_create_meta AFTER INSERT OR UPDATE OR DELETE ON meta
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_meta();

    DROP TRIGGER IF EXISTS trigger_on_create_node
    ON node;
    CREATE TRIGGER trigger_on_create_node AFTER INSERT OR UPDATE OR DELETE ON node
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_node();

    DROP TRIGGER IF EXISTS trigger_on_create_partition_replica_table
    ON partition_replica;
    CREATE TRIGGER trigger_on_create_partition_replica_table AFTER INSERT OR UPDATE OR DELETE ON partition_replica
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_partition_replica_table();

    DROP TRIGGER IF EXISTS trigger_on_modify_hypertable_index
    ON hypertable_index;
    CREATE TRIGGER trigger_on_modify_hypertable_index AFTER INSERT OR UPDATE OR DELETE ON hypertable_index
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_modify_hypertable_index();

    DROP TRIGGER IF EXISTS trigger_on_deleted_hypertable_index
    ON deleted_hypertable_index;
    CREATE TRIGGER trigger_on_deleted_hypertable_index AFTER INSERT OR UPDATE OR DELETE ON deleted_hypertable_index
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_deleted_hypertable_index();

    CREATE EVENT TRIGGER ddl_create_index ON ddl_command_end
        WHEN tag IN ('create index')
        EXECUTE PROCEDURE _sysinternal.ddl_process_create_index();

    CREATE EVENT TRIGGER ddl_alter_index ON ddl_command_end
        WHEN tag IN ('alter index')
        EXECUTE PROCEDURE _sysinternal.ddl_process_alter_index();

    CREATE EVENT TRIGGER ddl_drop_index ON sql_drop
        WHEN tag IN ('drop index')
        EXECUTE PROCEDURE _sysinternal.ddl_process_drop_index();

    CREATE EVENT TRIGGER ddl_create_column ON ddl_command_end
       WHEN tag IN ('alter table')
       EXECUTE PROCEDURE _sysinternal.ddl_process_alter_table();

END
$BODY$;
