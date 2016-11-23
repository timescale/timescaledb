
CREATE OR REPLACE FUNCTION unit_tests.test_ioql_queries()
RETURNS test_result
AS
$$
DECLARE
message test_result;
success boolean;
BEGIN
    PERFORM test_utils.test_query(new_ioql_query(namespace_name=>'33_testNs'), 'test_outputs', 'output_0');
    SELECT assert.ok('End of test.') INTO message;
    RETURN message;
END
$$
LANGUAGE plpgsql;

