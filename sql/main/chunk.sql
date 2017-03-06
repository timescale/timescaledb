
CREATE OR REPLACE FUNCTION _timescaledb_internal.lock_for_chunk_close(
    chunk_id INTEGER
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    --take an update lock on the chunk row
    --this conflicts, by design, with the lock taken when inserting on the node getting the insert command (not the node with the chunk table)
    PERFORM *
    FROM _timescaledb_catalog.chunk c
    WHERE c.id = chunk_id
    FOR UPDATE;
END
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.max_time_for_chunk_close(
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
        _timescaledb_internal.extract_time_sql(
            format('%I', _timescaledb_internal.time_col_name_for_crn(schema_name, table_name)),
            _timescaledb_internal.time_col_type_for_crn(schema_name, table_name)
        ),
        schema_name,
        table_name
    )
    INTO max_time;

    RETURN max_time;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.set_end_time_for_chunk_close(
    chunk_id INTEGER,
    max_time BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    UPDATE _timescaledb_catalog.chunk
    SET end_time = max_time
    WHERE id = chunk_id;
END
$BODY$;

--closes the given chunk if it is over the size limit set for the hypertable
--it belongs to.
CREATE OR REPLACE FUNCTION _timescaledb_internal.close_chunk_if_needed(
    chunk_id INTEGER
)
    RETURNS boolean LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_size      BIGINT;
    chunk_max_size  BIGINT;
BEGIN
    chunk_size := _timescaledb_data_api.get_chunk_size(chunk_id);
    chunk_max_size := _timescaledb_internal.get_chunk_max_size(chunk_id);

    IF chunk_size >= chunk_max_size THEN
        --This should use the non-transactional rpc because this needs to
        --commit before we can take a lock for writing on the closed chunk.
        --That means this operation is not transactional with the insert
        --and will not be rolled back.
        PERFORM _timescaledb_meta_api.close_chunk_end_immediate(chunk_id);
        RETURN TRUE;
    END IF;

    RETURN FALSE;
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
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN

    IF lock_chunk THEN
        chunk_row := _timescaledb_internal.get_chunk_locked(partition_id, time_point);
    ELSE
        chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);
    END IF;

    --Create a new chunk in case no chunk was returned.
    --We need to do this in a retry loop in case the chunk returned by the
    --meta node RPC changes between the RPC call and the local lock on
    --the chunk. This can happen if someone closes the chunk during that short
    --time window (in which case the local get_chunk_locked might return null).
    WHILE chunk_row IS NULL LOOP
        --this should use the non-transactional (_immediate) rpc because we can't wait for the end
        --of this local transaction to see the new chunk. Indeed we must see the results of get_or_create_chunk just a few
        --lines down. Which means that this operation must be committed. Thus this operation is not transactional wrt this call.
        --A chunk creation will NOT be rolled back if this transaction later aborts. Not ideal, but good enough for now.
        SELECT *
        INTO chunk_row
        FROM _timescaledb_meta_api.get_or_create_chunk_immediate(partition_id, time_point);

        IF lock_chunk THEN
            chunk_row := _timescaledb_internal.get_chunk_locked(partition_id, time_point);
        END IF;
    END LOOP;

    RETURN chunk_row;
END
$BODY$;
