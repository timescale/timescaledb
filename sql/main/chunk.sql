--gets or creates a chunk on a data node. First tries seeing if chunk exists.
CREATE OR REPLACE FUNCTION get_or_create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
     chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);
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
    END LOOP;

    RETURN chunk_row;
END
$BODY$;
