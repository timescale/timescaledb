CREATE OR REPLACE FUNCTION unit_tests.hash_function_test()
RETURNS test_result
AS
$$
DECLARE
message test_result;
success boolean;
hash_value integer;
result boolean;
BEGIN

SELECT _timescaledb_catalog.get_partition_for_key('', 16:: INT4) INTO hash_value;
SELECT * FROM assert.is_equal(hash_value, 13) INTO message, result;

IF result = false THEN
    RETURN message;
END IF;

SELECT _timescaledb_catalog.get_partition_for_key('dev1', 16:: INT4) INTO hash_value;
SELECT * FROM assert.is_equal(hash_value, 4) INTO message, result;

IF result = false THEN
    RETURN message;
END IF;

SELECT _timescaledb_catalog.get_partition_for_key('longlonglonglongpartitionkey', 16:: INT4) INTO hash_value;
SELECT * FROM assert.is_equal(hash_value, 6) INTO message, result;

IF result = false THEN
    RETURN message;
END IF;

--Test passed.
SELECT assert.ok('End of test.') INTO message;
RETURN message;

END
$$
LANGUAGE plpgsql;
