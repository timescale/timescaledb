\ir ../../main/names.sql

DROP FUNCTION IF EXISTS unit_tests.names_tests_start_set();
CREATE FUNCTION unit_tests.names_tests_start_set()
RETURNS test_result
AS
$$
DECLARE
message test_result;
ret_name NAME;
BEGIN
    ret_name := get_data_table_name('ns':: NAME,0 :: SMALLINT,10 :: SMALLINT,'field' :: NAME,1e9::BIGINT,0::BIGINT);

	IF ret_name != 'data_0_10_1' THEN
		SELECT assert.fail('Bad table name when table start is > 0.') INTO message;
		RETURN message;
	END IF;

	SELECT assert.ok('End of test.') INTO message;
	RETURN message;
END
$$
LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS unit_tests.names_tests_stop_set();
CREATE FUNCTION unit_tests.names_tests_stop_set()
RETURNS test_result
AS
$$
DECLARE
message test_result;
ret_name NAME;
debug text;
BEGIN
    ret_name := get_data_table_name('ns':: NAME,0 :: SMALLINT,10 :: SMALLINT,'field' :: NAME,NULL::BIGINT,1e9::BIGINT);

	IF ret_name != 'data_0_10_1' THEN
		SELECT assert.fail('Bad table name when table start is NULL.') INTO message;
		RETURN message;
	END IF;

	SELECT assert.ok('End of test.') INTO message;
	RETURN message;
END
$$
LANGUAGE plpgsql;