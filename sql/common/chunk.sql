CREATE OR REPLACE FUNCTION _sysinternal.get_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS chunk LANGUAGE SQL VOLATILE AS
$BODY$
SELECT *
FROM chunk c
WHERE c.partition_id = get_chunk.partition_id AND
      (c.start_time <= time_point OR c.start_time IS NULL) AND
      (c.end_time >= time_point OR c.end_time IS NULL)
FOR SHARE;
$BODY$;


