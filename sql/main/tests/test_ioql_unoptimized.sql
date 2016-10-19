CREATE OR REPLACE FUNCTION unit_tests.test_get_cluster_table_name()
RETURNS test_result
AS
$BODY$
DECLARE
  tc record;
  res regclass;
  message test_result;
  expected regclass;
BEGIN
  DROP TABLE IF EXISTS cases;
  CREATE TEMP TABLE "cases"  (
    project_id BIGINT,
    name text,
    replica_no smallint,
    expected text
  ) ON COMMIT DROP;
 
  INSERT INTO cases VALUES
    (12, 'test', 1, '12_test_1'), 
    (13, 'two', 2, '13_two_2');
  
  FOR tc IN SELECT * FROM cases LOOP
    EXECUTE(format($$ CREATE TABLE cluster."%s" (id int) $$, tc.expected ));
    expected := (format($$cluster."%s"$$, tc.expected));
    
    SELECT * INTO res FROM get_cluster_table(ROW(tc.project_id, tc.name, tc.replica_no));
    
    IF res IS DISTINCT FROM expected THEN
      SELECT assert.fail(format('result %L not equal expected %L', res, expected)) INTO message;
      RETURN message;
    END IF;
    
    EXECUTE(format($$ DROP TABLE cluster."%s" $$, tc.expected ));
  END LOOP;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION unit_tests.test_get_value_clause()
RETURNS test_result
AS
$BODY$
DECLARE
  tc record;
  res TEXT;
  message test_result;
BEGIN
  DROP TABLE IF EXISTS cases;
  CREATE TEMP TABLE "cases"  (
    ns namespace_type,
    field text,
    op text,
    expected text
  ) ON COMMIT DROP;
 
  INSERT INTO cases VALUES
    (ROW(22, 'testns', 1), '*', NULL, 'value'), 
    (ROW(22, 'testns', 1), 'field_1', NULL, $$jsonb_build_object('field_1', ((value->>'field_1')::DOUBLE PRECISION))$$), 
    (ROW(22, 'testns', 1), 'field_2', NULL, $$jsonb_build_object('field_2', ((value->>'field_2')::TEXT))$$), 
    (ROW(22, 'testns', 1), 'field_1', 'max', $$max(((value->>'field_1')::DOUBLE PRECISION))$$);
  
  FOR tc IN SELECT * FROM cases LOOP
    SELECT * INTO res FROM get_value_clause(tc.ns, tc.field, tc.op);
    
    IF lower(res) IS DISTINCT FROM lower(tc.expected) THEN
      SELECT assert.fail(format('result %L not equal expected %L', res, tc.expected)) INTO message;
      RETURN message;
    END IF; 
  END LOOP;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION unit_tests.test_get_time_predicate()
RETURNS test_result
AS
$BODY$
DECLARE
  tc record;
  res TEXT;
  message test_result;
BEGIN
  DROP TABLE IF EXISTS cases;
  CREATE TEMP TABLE "cases"  (
    from_time BIGINT,
    to_time BIGINT,
    expected text
  ) ON COMMIT DROP;
 
  INSERT INTO cases VALUES
    (1::bigint, 2::bigint, 'time>=1 and time<2'), 
    (NULL::bigint, 2::bigint, 'time<2'), 
    (1::bigint, NULL::bigint, 'time>=1');
  
  FOR tc IN SELECT * FROM cases LOOP
    SELECT * INTO res FROM get_time_predicate(new_time_condition(tc.from_time, tc.to_time));
    
    IF lower(res) IS DISTINCT FROM lower(tc.expected) THEN
      SELECT assert.fail(format('result %L not equal expected %L', res, tc.expected)) INTO message;
      RETURN message;
    END IF; 
  END LOOP;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION unit_tests.test_get_partitioning_field_value()
RETURNS test_result
AS
$BODY$
DECLARE
  tc record;
  res TEXT;
  message test_result;
  expected  TEXT;
BEGIN
  DROP TABLE IF EXISTS cases;
  CREATE TEMP TABLE "cases"  (
    ns namespace_type,
    conj predicate_conjunctive,
    pred field_predicate[],
    expected TEXT
  ) ON COMMIT DROP;
 
  INSERT INTO cases VALUES
     (ROW(22, 'testns', 1), 'AND', ARRAY[ROW('field_2', '=', '1')::field_predicate],  NULL),
     (ROW(22, 'testns', 1), 'AND', ARRAY[ROW('field_1', '=', '1')::field_predicate], '1'),
     (ROW(22, 'testns', 1), 'AND', ARRAY[ROW('field_1', '!=', '1')::field_predicate], NULL),
     (ROW(22, 'testns', 1), 'OR', ARRAY[ROW('field_1', '=', '1')::field_predicate], NULL),
      (ROW(22, 'testns', 1), 'AND', ARRAY[ROW('field_2', '=', '1')::field_predicate, ROW('field_3', '=', '1')::field_predicate], NULL),
      (ROW(22, 'testns', 1), 'AND', ARRAY[ROW('field_2', '=', '1')::field_predicate, ROW('field_1', '=', '2')::field_predicate], '2'),
      (ROW(22, 'testns', 1), 'OR', ARRAY[ROW('field_2', '=', '1')::field_predicate, ROW('field_3', '=', '1')::field_predicate],  NULL);

  FOR tc IN SELECT * FROM cases LOOP
    SELECT * INTO res FROM get_partitioning_field_value(tc.ns, new_field_condition(tc.conj, tc.pred));
    SELECT tc.expected INTO expected;
    
    IF lower(res) IS DISTINCT FROM lower(expected) THEN
      SELECT assert.fail(format('result %L not equal expected %L', lower(res), lower(expected))) INTO message;
      RETURN message;
    END IF; 
  END LOOP;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION unit_tests.test_unoptimized_query()
RETURNS test_result
AS
$BODY$
DECLARE
  tc record;
  res text;
  count int;
  message test_result;
BEGIN
  PERFORM unit_tests.load_data_1();
  FOR tc IN SELECT * FROM cases LOOP
    SELECT * INTO res FROM full_query(tc.query, '22_testns_1_8_partition'::regclass);
   
    DECLARE
      em1 text;
      em2 text;
      em3 text;
    BEGIN 
      EXECUTE format('SELECT count(*) FROM (%s) as t', res) into count;
    EXCEPTION 
      WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS 
          em1 = MESSAGE_TEXT,
          em2 = PG_EXCEPTION_DETAIL,
          em3 = PG_EXCEPTION_HINT;

        SELECT assert.fail(format('query failed with(%s; %s; %s) %s %L', em1, em2, em3, res, tc.query)) INTO message;
        RETURN message;
    END;
    IF count IS DISTINCT FROM tc.expected THEN
      SELECT assert.fail(format('result %L not equal expected %L; query_sql %s query: %L', count, tc.expected, res, tc.query)) INTO message;
      RETURN message;
    END IF; 
  END LOOP;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$BODY$
LANGUAGE plpgsql;


