
DROP FUNCTION IF EXISTS unit_tests.test_ioql_queries_empty();
CREATE FUNCTION unit_tests.test_ioql_queries_empty()
RETURNS test_result
AS
$$
DECLARE
message test_result;
diffcount integer;
result json[];
expected json[];
expected_row json;
cursor REFCURSOR;
rowvar record;
expected_record record;
result_jsonb jsonb[];
expected_cursor REFCURSOR;
BEGIN

    PERFORM insert_data_one_partition('test_input_data.batch1_dev1', get_partition_for_key('dev1'::text, 10 :: SMALLINT), 10 :: SMALLINT);

    -- Test json output from query (q1_response_json.csv)
    SELECT Array (select * FROM ioql_exec_query(new_ioql_query(namespace_name => '33_testNs'))) into result;
    SELECT Array (select * FROM test_outputs.q1_response_json) into expected;

    IF to_jsonb(result) != to_jsonb(expected) THEN
		SELECT assert.fail('Bad json return from rows from query.') INTO message;
		RETURN message;
	END IF;
    
    -- Test cursor based queries, compare to json table (q1_response_json.csv)
    SELECT ioql_exec_query_record_cursor(new_ioql_query(namespace_name => '33_testNs'), 'cursor') into cursor;

    FOREACH expected_row IN ARRAY expected
    LOOP
        FETCH cursor into rowvar;
        IF FOUND = FALSE THEN 
            raise exception 'Expected row: %v got nothing', to_jsonb(expected_row);
            EXIT;
        END IF;
        
        IF to_jsonb(expected_row) != to_jsonb(rowvar) THEN
            raise exception 'Expected row: %v got: %v', to_jsonb(expected_row), to_jsonb(rowvar);
        END IF;
    END LOOP;

    -- Test cursor based queries, compare to records table (q1_response_fields.csv)

    CLOSE cursor;

    SELECT ioql_exec_query_record_cursor(new_ioql_query(namespace_name => '33_testNs'), 'cursor') into cursor;

    FOR expected_record in SELECT * FROM test_outputs.q1_response_fields 
    LOOP
        FETCH cursor into rowvar;
        IF FOUND = FALSE THEN 
            raise exception 'Expected row: %v got nothing', to_jsonb(expected_record);
            EXIT;
        END IF;

        -- Record comparison fails on different types of columns
        IF expected_record != rowvar THEN
            raise exception 'Expected row: %v got: %v', to_jsonb(expected_record), to_jsonb(rowvar);
        END IF;
    END LOOP;

    SELECT assert.ok('End of test.') INTO message;
	RETURN message;
END
$$
LANGUAGE plpgsql;