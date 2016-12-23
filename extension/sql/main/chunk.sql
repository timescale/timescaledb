
CREATE OR REPLACE FUNCTION _sysinternal.lock_for_chunk_close(
    chunk_id INTEGER
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    --take an update lock on the chunk row
    --this conflicts, by design, with the lock taken when inserting on the node getting the insert command (not the node with the chunk table)
    PERFORM *
    FROM chunk c
    WHERE c.id = chunk_id
    FOR UPDATE;
END
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.max_time_for_chunk_close(
    schema_name NAME,
    table_name  NAME
)
    RETURNS BIGINT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    max_time BIGINT;
BEGIN
    EXECUTE format(
        $$
            SELECT max(%s)
            FROM %I.%I
        $$,
    _sysinternal.extract_time_sql(
        format('%I', _sysinternal.time_col_name_for_crn(schema_name, table_name)),
        _sysinternal.time_col_type_for_crn(schema_name, table_name)
    ), 
    schema_name, table_name)
    INTO max_time;

    RETURN max_time;
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.set_end_time_for_chunk_close(
    chunk_id INTEGER,
    max_time BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    UPDATE chunk
    SET end_time = max_time
    WHERE id = chunk_id;
END
$BODY$;

--closes the given chunk if it is over the size limit set for the hypertable
--it belongs to.
CREATE OR REPLACE FUNCTION _sysinternal.close_chunk_if_needed(
    chunk_row chunk
)
    RETURNS boolean LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_size      BIGINT;
    chunk_max_size  BIGINT;
BEGIN
    chunk_size := _sysinternal.get_chunk_size(chunk_row.id);
    chunk_max_size := _sysinternal.get_chunk_max_size(chunk_row.id);

    IF chunk_row.end_time IS NOT NULL OR (NOT chunk_size >= chunk_max_size) THEN
        RETURN FALSE;
    END IF;

    PERFORM close_chunk_end(chunk_row.id);
    return TRUE;
END
$BODY$;

--gets or creates a chunk on a data node. First tries seeing if chunk exists.
--If not, ask meta server to create one. Local lock obtained by this call.
--NOTE: cannot close chunk after calling this because it locks the chunk locally.
CREATE OR REPLACE FUNCTION get_or_create_chunk(
    partition_id INT,
    time_point   BIGINT,
    lock_chunk   boolean = FALSE
)
    RETURNS chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row chunk;
    meta_row  meta;
BEGIN

    IF lock_chunk THEN
        chunk_row := _sysinternal.get_chunk_locked(partition_id, time_point);
    ELSE
        chunk_row := _sysinternal.get_chunk(partition_id, time_point);
    END IF;

    --Create a new chunk in case no chunk was returned.
    --We need to do this in a retry loop in case the chunk returned by the 
    --meta node RPC changes between the RPC call and the local lock on
    --the chunk. This can happen if someone closes the chunk during that short
    --time window (in which case the local get_chunk_locked might return null).
    WHILE chunk_row IS NULL LOOP
        SELECT *
        INTO STRICT meta_row
        FROM meta;

        SELECT t.*
        INTO chunk_row
        FROM dblink(meta_row.server_name, format('SELECT * FROM _meta.get_or_create_chunk(%L, %L) ', partition_id, time_point))
            AS t(id INTEGER, partition_id INTEGER, start_time BIGINT, end_time BIGINT);
        
        IF lock_chunk THEN
            chunk_row := _sysinternal.get_chunk_locked(partition_id, time_point);
        END IF;
    END LOOP;

    RETURN chunk_row;
END
$BODY$;

CREATE OR REPLACE FUNCTION close_chunk_end(
    chunk_id INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM 1
    FROM meta m,
            dblink(m.server_name,
                   format('SELECT * FROM _meta.close_chunk_end(%L)', chunk_id)) AS t(x TEXT);
END
$BODY$;
