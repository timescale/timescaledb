CREATE OR REPLACE FUNCTION _timescaledb_data_api.get_chunk_size(
    chunk_id INTEGER
)
    RETURNS BIGINT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_size BIGINT;
BEGIN
    SELECT _timescaledb_internal.node_immediate_commit_exec_with_return(
        n.database_name, n.server_name,
        format('SELECT * FROM _timescaledb_internal.get_local_chunk_size(%L)', get_chunk_size.chunk_id)
    )::BIGINT
    INTO STRICT chunk_size
    FROM _timescaledb_catalog.chunk_replica_node crn
    INNER JOIN _timescaledb_catalog.node n ON (n.database_name = crn.database_name)
    WHERE crn.chunk_id = get_chunk_size.chunk_id
    ORDER BY (crn.database_name = current_database()) DESC --prefer local nodes
    LIMIT 1;

    RETURN chunk_size;
END
$BODY$;
