CREATE OR REPLACE FUNCTION kafka_get_start_and_next_offset(
        topic                TEXT,
        partition_number     SMALLINT,
        default_start_offset INTEGER,
    OUT start_offset         INTEGER,
    OUT next_offset          INTEGER
)
LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    SELECT clust.next_offset
    INTO next_offset
    FROM kafka_offset_cluster clust
    WHERE clust.topic = kafka_get_start_and_next_offset.topic AND
          clust.partition_number = kafka_get_start_and_next_offset.partition_number
    ORDER BY next_offset DESC
    LIMIT 1;

    IF next_offset IS NULL THEN
        next_offset := default_start_offset;
    ELSE
        SELECT clust.start_offset
        INTO start_offset
        FROM kafka_offset_cluster clust
        WHERE clust.topic = kafka_get_start_and_next_offset.topic AND
              clust.partition_number = kafka_get_start_and_next_offset.partition_number AND
              clust.next_offset = kafka_get_start_and_next_offset.next_offset AND
              clust.database_name = current_database();
        IF start_offset IS NOT NULL THEN
            RETURN;
        END IF;
    END IF;

    start_offset := next_offset;
    INSERT INTO kafka_offset_local (topic, partition_number, start_offset, next_offset, database_name)
    VALUES (topic, partition_number, start_offset, next_offset, current_database());
    RETURN;
END
$BODY$;

CREATE OR REPLACE FUNCTION kafka_set_next_offset(
    topic            TEXT,
    partition_number SMALLINT,
    start_offset     INTEGER,
    next_offset      INTEGER
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    affected INTEGER;
BEGIN
    UPDATE kafka_offset_local AS loc
    SET next_offset = kafka_set_next_offset.next_offset
    WHERE loc.topic = kafka_set_next_offset.topic AND
          loc.partition_number = kafka_set_next_offset.partition_number AND
          loc.start_offset = kafka_set_next_offset.start_offset;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> 1 THEN
        RAISE EXCEPTION 'Rows affected not = 1. Affected: %', affected
        USING ERRCODE = 'IO501';
    END IF;
END
$BODY$;