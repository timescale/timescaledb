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

/*
    Ensure same output as the hashlib extension.
*/
SELECT _timescaledb_internal.murmur3_hash_string('', 1 :: INT4) INTO hash_value;
SELECT * FROM assert.is_equal(hash_value, 1364076727) INTO message, result;

IF result = false THEN
    RETURN message;
END IF;

SELECT _timescaledb_internal.murmur3_hash_string('dev1', 1 :: INT4) INTO hash_value;
SELECT * FROM assert.is_equal(hash_value, 1398815044) INTO message, result;

IF result = false THEN
    RETURN message;
END IF;

SELECT _timescaledb_internal.murmur3_hash_string('longlonglonglongpartitionkey', 1 :: INT4) INTO hash_value;
SELECT * FROM assert.is_equal(hash_value, -1320242451) INTO message, result;

IF result = false THEN
    RETURN message;
END IF;

--Test passed.
SELECT assert.ok('End of test.') INTO message;
RETURN message;

END
$$
LANGUAGE plpgsql;
