CREATE EXTENSION IF NOT EXISTS "dblink";

CREATE OR REPLACE FUNCTION unit_tests.test_ioql_exec_query()
RETURNS test_result
AS
$BODY$
DECLARE
  tc record;
  count int;
  message test_result;
BEGIN
  PERFORM unit_tests.load_data_1();
  FOR tc IN SELECT * FROM cases LOOP
    DECLARE
      em1 text;
      em2 text;
      em3 text;
      em4 text;
    BEGIN 
      EXECUTE format('SELECT count(*) FROM ioql_exec_query(%L) as t', tc.query) into count;
    EXCEPTION 
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS 
          em1 = MESSAGE_TEXT,
          em2 = PG_EXCEPTION_DETAIL,
          em3 = PG_EXCEPTION_HINT,
          em4 = PG_EXCEPTION_CONTEXT 
          ;

        SELECT assert.fail(format('query failed with(%s; %s; %s; %s)', em1, em2, em3, em4)) INTO message;
        RETURN message;
    END;
    IF count IS DISTINCT FROM tc.expected THEN
      SELECT assert.fail(format('result %L not equal expected %L; query: %L', count, tc.expected, tc.query)) INTO message;
      RETURN message;
    END IF; 
  END LOOP;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$BODY$
LANGUAGE plpgsql;


