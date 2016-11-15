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
    table_name NAME
)
    RETURNS BIGINT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    max_time       BIGINT;
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
    UPDATE chunk SET end_time = max_time WHERE id = chunk_id;
END
$BODY$;


