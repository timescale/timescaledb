CREATE OR REPLACE FUNCTION _timescaledb_data_api.lock_for_chunk_close(
    node_database_name NAME,
    chunk_id           INTEGER
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_meta.node_transaction_exec_with_return(
        n.database_name, n.server_name,
        format('SELECT * FROM _timescaledb_internal.lock_for_chunk_close(%L)', chunk_id)
    )
    FROM _timescaledb_catalog.node n
    WHERE n.database_name = node_database_name;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_data_api.max_time_for_chunk_close(
    node_database_name NAME,
    schema_name        NAME,
    table_name         NAME
)
    RETURNS BIGINT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    max_time BIGINT;
BEGIN
    SELECT _timescaledb_meta.node_transaction_exec_with_return(
        n.database_name, n.server_name,
        format('SELECT * FROM _timescaledb_internal.max_time_for_chunk_close(%L, %L)',
                max_time_for_chunk_close.schema_name,
                max_time_for_chunk_close.table_name
        )
    )::bigint
    INTO max_time
    FROM _timescaledb_catalog.node n
    WHERE n.database_name = node_database_name;

    RETURN max_time;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_data_api.set_end_time_for_chunk_close(
    node_database_name NAME,
    chunk_id           INTEGER,
    max_time           BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _timescaledb_meta.node_transaction_exec_with_return(
        n.database_name, n.server_name,
        format('SELECT * FROM _timescaledb_internal.set_end_time_for_chunk_close(%L, %L)', chunk_id, max_time)
    )
    FROM _timescaledb_catalog.node n
    WHERE n.database_name = node_database_name;
END
$BODY$;

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
