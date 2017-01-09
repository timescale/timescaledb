-- Initializes kafka-related triggers on data nodes
CREATE OR REPLACE FUNCTION setup_kafka()
    RETURNS void LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN

    DROP TRIGGER IF EXISTS trigger_on_create_node_insert_kafka_offset_node
    ON node;
    CREATE TRIGGER trigger_on_create_node_insert_kafka_offset_node AFTER INSERT OR UPDATE OR DELETE ON node
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_node_insert_kafka_offset_node();

    DROP TRIGGER IF EXISTS trigger_on_create_kafka_offset_node
    ON kafka_offset_node;
    CREATE TRIGGER trigger_on_create_kafka_offset_node AFTER INSERT OR UPDATE OR DELETE ON kafka_offset_node
    FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_kafka_offset_node();

END
$BODY$;
