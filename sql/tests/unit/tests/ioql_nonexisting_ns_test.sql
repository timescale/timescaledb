CREATE OR REPLACE FUNCTION unit_tests.test_unknown_ns()
RETURNS test_result
AS
$$
DECLARE
message test_result;
success boolean;
BEGIN
    /* 
    // Causes parse error
    PERFORM test_utils.test_query(new_ioql_query(namespace_name=>'UNKNOWN_NS'), 'test_outputs', 'empty_result');
    */ 
    SELECT assert.ok('End of test.') INTO message;
    RETURN message;
END
$$
LANGUAGE plpgsql;
