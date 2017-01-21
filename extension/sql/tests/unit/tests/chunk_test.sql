CREATE OR REPLACE FUNCTION unit_tests.test_chunk_size()
RETURNS test_result
AS
$$
DECLARE
chunk_size BIGINT;
message test_result;
result boolean;
BEGIN
    SELECT _iobeamdb_data_api.get_chunk_size(1) INTO chunk_size;
    SELECT * FROM assert.is_greater_than(chunk_size, 0::BIGINT) INTO message, result;

    IF result = false THEN
        RETURN message;
    END IF;

	SELECT assert.ok('End of test.') INTO message;
	RETURN message;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION unit_tests.test_create_new_chunk()
RETURNS test_result
AS
$$
DECLARE
chunk_size      BIGINT;
chunk_row       _iobeamdb_catalog.chunk;
chunk_id        _iobeamdb_catalog.chunk.id%type;
chunk_end_time  _iobeamdb_catalog.chunk.end_time%type;
message         test_result;
result          boolean;
BEGIN
    SELECT * FROM _iobeamdb_catalog.chunk c
    INTO chunk_row
    LEFT JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
    LEFT JOIN _iobeamdb_catalog.partition_replica pr ON (crn.partition_replica_id = pr.id)
    WHERE hypertable_name = 'public.chunk_test';

    SELECT * FROM assert.is_not_null(chunk_row.id) INTO message, result;

    IF result = false THEN
        RETURN message;
    END IF;

    -- Check that the chunk is open-ended in both directions
    SELECT * FROM assert.is_null(chunk_row.start_time) INTO message, result;

    IF result = false THEN
        RETURN message;
    END IF;

    SELECT * FROM assert.is_null(chunk_row.end_time) INTO message, result;

    IF result = false THEN
        RETURN message;
    END IF;

    -- Remember this chunk's ID
    chunk_id := chunk_row.id;

    SELECT _sysinternal.get_local_chunk_size(chunk_row.id) INTO chunk_size;

    -- Insert one row. Should trigger the creation of a new chunk
    INSERT INTO chunk_test VALUES(2, 2, 'dev2');

    SELECT * FROM _iobeamdb_catalog.chunk c
    INTO STRICT chunk_row WHERE (c.id = chunk_id);

    -- Check that start time is still NULL on the old chunk
    SELECT * FROM assert.is_null(chunk_row.start_time) INTO message, result;

    IF result = false THEN
        RETURN message;
    END IF;

    -- The end time should now be set on the old chunk.
    SELECT * FROM assert.is_equal(chunk_row.end_time, 1::BIGINT) INTO message, result;

    IF result = false THEN
        RETURN message;
    END IF;

    chunk_end_time := chunk_row.end_time;

    -- Query for the new chunk
    SELECT * FROM _iobeamdb_catalog.chunk c
    INTO STRICT chunk_row
    LEFT JOIN _iobeamdb_catalog.chunk_replica_node crn ON (c.id = crn.chunk_id)
    LEFT JOIN _iobeamdb_catalog.partition_replica pr ON (crn.partition_replica_id = pr.id)
    WHERE hypertable_name = 'public.chunk_test' AND c.end_time IS NULL;

    -- Check that start time is the end time of the previous chunk plus one
    SELECT * FROM assert.is_equal(chunk_row.start_time, chunk_end_time + 1::BIGINT) INTO message, result;

    IF result = false THEN
        RETURN message;
    END IF;

    -- The end time should be open on the new chunk
    SELECT * FROM assert.is_null(chunk_row.end_time) INTO message, result;

    IF result = false THEN
        RETURN message;
    END IF;

	SELECT assert.ok('End of test.') INTO message;
	RETURN message;
END
$$
LANGUAGE plpgsql;
