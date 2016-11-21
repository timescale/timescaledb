CREATE OR REPLACE FUNCTION lock_for_chunk_close(
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


CREATE OR REPLACE FUNCTION max_time_for_chunk_close(
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
            SELECT max("time")
            FROM %I.%I
        $$,
        schema_name, table_name)
    INTO max_time;

    RETURN max_time;
END
$BODY$;

CREATE OR REPLACE FUNCTION set_end_time_for_chunk_close(
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

--gets or creates a chunk on a data node. First tries seeing if chunk exists.
--If not, ask meta server to create one. Local lock obtained by this call.
--NOTE: cannot close chunk after calling this because it locks the chunk locally.
CREATE OR REPLACE FUNCTION get_or_create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row chunk;
    meta_row  meta;
BEGIN
    chunk_row := _sysinternal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        SELECT *
        INTO STRICT meta_row
        FROM meta;

        RAISE WARNING 'testing % %', partition_id, time_point;

        SELECT t.*
        INTO chunk_row
        FROM dblink(meta_row.server_name,
                    format('SELECT * FROM get_or_create_chunk(%L, %L) ', partition_id, time_point))
            AS t(id INTEGER, partition_id INTEGER, start_time BIGINT, end_time BIGINT);

        IF chunk_row IS NULL THEN
            RAISE EXCEPTION 'Should never happen: chunk not found in remote meta call on database %', current_database()
            USING ERRCODE = 'IO501';
        END IF;

        --need to fetch locally to get lock.
        chunk_row := _sysinternal.get_chunk(partition_id, time_point);

        IF chunk_row IS NULL THEN
            RAISE EXCEPTION 'Should never happen: chunk not found after creation on database %', current_database()
            USING ERRCODE = 'IO501';
        END IF;
    END IF;

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
                   format('SELECT * FROM close_chunk_end(%L)', chunk_id)) AS t(x TEXT);
END
$BODY$;
