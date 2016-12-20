CREATE OR REPLACE FUNCTION unit_tests.test_unknown_hyper_table()
RETURNS test_result
AS
$$
DECLARE
message test_result;
success boolean;
BEGIN
    
    BEGIN 
      PERFORM test_utils.test_query(new_ioql_query(namespace_name=>'UNKNOWN_HT'), 'test_outputs', 'empty_result');
    EXCEPTION WHEN SQLSTATE 'IO001' THEN
      SELECT assert.ok('End of test.') INTO message;
      RETURN message;
    END;

    SELECT assert.fail('Expected raise.') INTO message;
    RETURN message;
END
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION unit_tests.test_unknown_column()
RETURNS test_result
AS
$$
DECLARE
message test_result;
success boolean;
BEGIN
    
    BEGIN 
      PERFORM test_utils.test_query(new_ioql_query(namespace_name=>'33_testNs',
      select_items=>ARRAY[new_select_item('field_does_not_exist'::text,NULL::aggregate_function_type)]), 'test_outputs', 'empty_result');
    EXCEPTION WHEN SQLSTATE 'IO002' THEN
      SELECT assert.ok('End of test.') INTO message;
      RETURN message;
    END;

    SELECT assert.fail('Expected raise.') INTO message;
    RETURN message;
END
$$
LANGUAGE plpgsql;
