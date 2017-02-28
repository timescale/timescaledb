-- Allows a node to join a cluster (issued from the node).
CREATE OR REPLACE FUNCTION join_cluster(
    meta_database   NAME,
    meta_hostname   TEXT,
    meta_port       INT     = inet_server_port(),
    node_database   NAME    = current_database(),
    node_hostname   TEXT    = gethostname(),
    node_port       INT     = inet_server_port(),
    username        TEXT    = current_user,
    password        TEXT    = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    node_row        _timescaledb_catalog.node;
BEGIN
    SELECT *
    INTO node_row
    FROM _timescaledb_catalog.node n
    WHERE n.database_name = node_database AND n.hostname = node_hostname AND n.port = node_port
    LIMIT 1;

    IF node_row IS NULL THEN
        PERFORM _timescaledb_internal.setup_main_immmediate(node_database, username, password);
        PERFORM _timescaledb_meta_api.join_cluster(meta_database, meta_hostname, meta_port,
                                                node_database, node_hostname, node_port,
                                                username, password);
    ELSE
        RAISE EXCEPTION 'The node is already part of a cluster'
        USING ERRCODE = 'IO120';
    END IF;
END
$BODY$;
