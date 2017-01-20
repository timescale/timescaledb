CREATE OR REPLACE FUNCTION get_kafka_offset_remote_table_name(
    remote_node _iobeamdb_catalog.node
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('kafka_offset_remote_%s', remote_node.database_name) :: NAME;
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.on_create_node_insert_kafka_offset_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on node table'
        USING ERRCODE = 'IO101';
    END IF;

    INSERT INTO _iobeamdb_catalog.kafka_offset_node (database_name, local_table_name, remote_table_name)
    VALUES (NEW.database_name, 'kafka_offset_local', get_kafka_offset_remote_table_name(NEW));

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

CREATE OR REPLACE FUNCTION create_remote_kafka_offset_table(
    remote_node       _iobeamdb_catalog.node,
    remote_table_name NAME,
    local_table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE FOREIGN TABLE IF NOT EXISTS _iobeamdb_catalog.%1$I ()
            INHERITS(_iobeamdb_catalog.kafka_offset_cluster) SERVER %2$I OPTIONS (schema_name '_iobeamdb_catalog', table_name '%3$I')
        $$,
        remote_table_name, remote_node.server_name, local_table_name);
END
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.on_create_kafka_offset_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    remote_node _iobeamdb_catalog.node;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on kafka offset table'
        USING ERRCODE = 'IO101';
    END IF;

    IF NEW.database_name <> current_database() THEN
        SELECT *
        INTO STRICT remote_node
        FROM _iobeamdb_catalog.node n
        WHERE n.database_name = NEW.database_name;

        PERFORM create_remote_kafka_offset_table(remote_node, NEW.remote_table_name, NEW.local_table_name);
    END IF;
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';
