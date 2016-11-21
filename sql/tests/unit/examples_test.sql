DROP FUNCTION IF EXISTS unit_tests.example_test();
CREATE FUNCTION unit_tests.names_tests_start_set()
RETURNS test_result
AS
$$
DECLARE
message test_result;
ret_name NAME;
BEGIN
	SELECT assert.ok('End of test.') INTO message;
	RETURN message;
END
$$
LANGUAGE plpgsql;


