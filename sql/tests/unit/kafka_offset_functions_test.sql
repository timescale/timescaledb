
DROP FUNCTION IF EXISTS unit_tests.kafka_get_start_and_next_offset_test();
CREATE FUNCTION unit_tests.kafka_get_start_and_next_offset_test()
RETURNS test_result
AS
$$
DECLARE 
message test_result;
start_offset_var integer;
next_offset_var integer;
DEFAULT_START_OFFSET integer;
BEGIN
	DEFAULT_START_OFFSET := 42;
	SELECT start_offset, next_offset FROM kafka_get_start_and_next_offset('topic'::text, 0::SMALLINT, DEFAULT_START_OFFSET) 
	INTO start_offset_var, next_offset_var;

	IF start_offset_var != DEFAULT_START_OFFSET THEN
		SELECT assert.fail('Bad default start offset.') INTO message;
		RETURN message;
	END IF;

	IF next_offset_var != DEFAULT_START_OFFSET THEN
		SELECT assert.fail('Bad initial next_offset.') INTO message;
		RETURN message;
	END IF;

	PERFORM kafka_set_next_offset(
		topic => 'topic'::text,
		partition_number => 0::SMALLINT,
		start_offset => DEFAULT_START_OFFSET,
		next_offset => DEFAULT_START_OFFSET + 1
	);

	SELECT start_offset, next_offset FROM kafka_get_start_and_next_offset('topic'::text, 0::SMALLINT, DEFAULT_START_OFFSET) 
	INTO start_offset_var, next_offset_var;

	IF start_offset_var != DEFAULT_START_OFFSET THEN
		SELECT assert.fail('Bad start offset after update.') INTO message;
		RETURN message;
	END IF;

	IF next_offset_var != DEFAULT_START_OFFSET + 1 THEN
		SELECT assert.fail('Bad next offset after update.') INTO message;
		RETURN message;
	END IF;

	BEGIN
		PERFORM kafka_set_next_offset(
		topic => 'newtopic'::text,
		partition_number => 0::SMALLINT,
		start_offset => DEFAULT_START_OFFSET,
		next_offset => DEFAULT_START_OFFSET + 1
	);
	EXCEPTION
		WHEN sqlstate 'IO501' THEN
			RAISE NOTICE 'right exception thrown';
	END;

	SELECT assert.ok('End of test.') INTO message;	
	RETURN message;
END
$$
LANGUAGE plpgsql;
