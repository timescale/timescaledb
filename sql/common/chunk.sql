--get the chunk for a given partition and time.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _iobeamdb_catalog.chunk LANGUAGE SQL STABLE AS
$BODY$
SELECT *
FROM _iobeamdb_catalog.chunk c
WHERE c.partition_id = get_chunk.partition_id AND
      (c.start_time <= time_point OR c.start_time IS NULL) AND
      (c.end_time >= time_point OR c.end_time IS NULL);
$BODY$;


--get the chunk for a given partition and time. The function takes a (shared) row lock
--on the chunk and might therefore block.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_chunk_locked(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _iobeamdb_catalog.chunk LANGUAGE SQL VOLATILE AS
$BODY$
SELECT *
FROM _iobeamdb_catalog.chunk c
WHERE c.partition_id = get_chunk_locked.partition_id AND
      (c.start_time <= time_point OR c.start_time IS NULL) AND
      (c.end_time >= time_point OR c.end_time IS NULL)
FOR SHARE;
$BODY$;

--returns the current size of a chunk (in bytes) given its ID.
--The size is typically aligned with the page size in Postgres.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_local_chunk_size(
    chunk_id INT
)
    RETURNS BIGINT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    chunk_replica_row _iobeamdb_catalog.chunk_replica_node;
    chunk_table_name  TEXT;
BEGIN
    SELECT *
    INTO STRICT chunk_replica_row
    FROM _iobeamdb_catalog.chunk_replica_node crn
    WHERE crn.chunk_id = get_local_chunk_size.chunk_id;

    IF chunk_replica_row.database_name != current_database() THEN
        RAISE EXCEPTION 'get_local_chunk_size should only be called locally'
        USING ERRCODE = 'IO501';
    END IF;

    chunk_table_name := format('%I.%I', chunk_replica_row.schema_name, chunk_replica_row.table_name);
    RETURN pg_table_size(chunk_table_name :: REGCLASS);
END
$BODY$;

--returns the max size (in bytes) that a chunk is allowed to grow to.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_chunk_max_size(
    chunk_id INT
)
    RETURNS BIGINT LANGUAGE SQL STABLE AS
$BODY$
    SELECT h.chunk_size_bytes
    FROM _iobeamdb_catalog.chunk_replica_node crn
    INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.id = crn.partition_replica_id)
    INNER JOIN _iobeamdb_catalog.hypertable h ON (h.id = pr.hypertable_id)
    WHERE (crn.chunk_id = get_chunk_max_size.chunk_id);
$BODY$;
