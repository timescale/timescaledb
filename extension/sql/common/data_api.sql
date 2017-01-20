CREATE OR REPLACE FUNCTION _iobeamdb_data_api.lock_for_chunk_close(
    node_database_name NAME,
    chunk_id INTEGER
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
     PERFORM _meta.node_transaction_exec_with_return(n.database_name, n.server_name,
            format('SELECT * FROM _sysinternal.lock_for_chunk_close(%L)', chunk_id)
     )
     FROM _iobeamdb_catalog.node n
     WHERE n.database_name = node_database_name;
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_data_api.max_time_for_chunk_close(
    node_database_name NAME,
    schema_name NAME,
    table_name  NAME
)
RETURNS BIGINT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    max_time BIGINT;
BEGIN
     SELECT _meta.node_transaction_exec_with_return(n.database_name, n.server_name,
            format('SELECT * FROM _sysinternal.max_time_for_chunk_close(%L, %L)',
                max_time_for_chunk_close.schema_name,
                max_time_for_chunk_close.table_name
            )
     )::bigint
     INTO max_time
     FROM _iobeamdb_catalog.node n
     WHERE n.database_name = node_database_name;

     RETURN max_time;
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_data_api.set_end_time_for_chunk_close(
    node_database_name NAME,
    chunk_id INTEGER,
    max_time BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
     PERFORM _meta.node_transaction_exec_with_return(n.database_name, n.server_name,
            format('SELECT * FROM _sysinternal.set_end_time_for_chunk_close(%L, %L)', chunk_id, max_time)
     )
     FROM _iobeamdb_catalog.node n
     WHERE n.database_name = node_database_name;
END
$BODY$;
