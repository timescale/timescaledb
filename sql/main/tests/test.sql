DROP FUNCTION IF EXISTS unit_tests.test_create_data_table();
DROP FUNCTION IF EXISTS unit_tests.test_create_field();
--DROP FUNCTION IF EXISTS unit_tests.test_insert_new_data();
create extension if not exists "uuid-ossp";

CREATE OR REPLACE FUNCTION unit_tests.test_create_data_table()
RETURNS test_result
AS
$$
DECLARE 
  message test_result;
  cnt int;
  dt data_tables%ROWTYPE;
BEGIN
  CREATE TABLE "22_testns_1_6_partition" (
    time BIGINT,
    partition_key VARCHAR,
    value jsonb,
    uuid UUID,
    row_id smallint,
    PRIMARY KEY(time, partition_key, uuid, row_id)
  );

  -- reference time: Mon Jan _2 15:04:05 2006
  -- unix time 1136239445

  SELECT * into dt FROM get_data_table(1136239445 * 1e9::bigint, 22, 'testns', 1::smallint, 6::smallint, 10::smallint, 
    'public."22_testns_1_6_partition"'::regclass, NULL, NULL);

  if dt.table_name IS NULL THEN
      SELECT assert.fail('data table name is null') INTO message;
      RETURN message;
  END IF;

  IF to_regclass('public."22_testns_1_6_1136160000"') IS NULL THEN
        SELECT assert.fail('table does not exist 1.') INTO message;
        RETURN message;
  END IF;

  SELECT count(*) INTO cnt FROM data_tables 
    WHERE project_id = 22 
    and namespace = 'testns'
    and replica_no = 1
    and partition = 6;
  IF cnt != 1 THEN
      SELECT assert.fail('data table row cnt wrong') INTO message;
      RETURN message;
  END IF;
  
  PERFORM get_data_table(1136160000 * 1e9::bigint, 22, 'testns', 1::smallint, 6::smallint, 10::smallint,
  'public."22_testns_1_6_partition"'::regclass, NULL, NULL); --tests use of existing table on inclusive match

  SELECT count(*) INTO cnt FROM data_tables 
    WHERE project_id = 22 
    and namespace = 'testns'
    and replica_no = 1
    and partition = 6;
  IF cnt != 1 THEN
      SELECT assert.fail('data table row cnt wrong.') INTO message;
      RETURN message;
  END IF;

  PERFORM get_data_table(1136159999 * 1e9::bigint, 22, 'testns', 1::smallint, 6::smallint, 10::smallint,
  'public."22_testns_1_6_partition"'::regclass, NULL, NULL); --create new table before

  IF to_regclass('public."22_testns_1_6_1136159999"') IS NULL THEN
        SELECT assert.fail('table does not exist 2.') INTO message;
        RETURN message;
  END IF;

  SELECT count(*) INTO cnt FROM data_tables 
    WHERE project_id = 22 
    and namespace = 'testns'
    and replica_no = 1
    and partition = 6;
  IF cnt != 2 THEN
      SELECT assert.fail('data table row cnt wrong.') INTO message;
      RETURN message;
  END IF;

  PERFORM close_data_table_end(dt);

  SELECT * into dt FROM data_tables WHERE table_name = '22_testns_1_6_1136160000'::regclass;
  if dt.end_time IS DISTINCT FROM (1136246400*1e9::bigint)-1 THEN
    SELECT assert.fail('data table name end time is null') INTO message;
      RETURN message;
  END IF;


  PERFORM get_data_table((1136246400*1e9::bigint)-1, 22, 'testns', 1::smallint, 6::smallint, 10::smallint,
   'public."22_testns_1_6_partition"'::regclass, NULL, NULL ); --tests use of existing table on inclusive match (upper)

  SELECT count(*) INTO cnt FROM data_tables 
    WHERE project_id = 22 
    and namespace = 'testns'
    and replica_no = 1
    and partition = 6;
  IF cnt != 2 THEN
      SELECT assert.fail('data table row cnt wrong.') INTO message;
      RETURN message;
  END IF;

  PERFORM get_data_table(1136246401 * 1e9::bigint, 22, 'testns', 1::smallint, 6::smallint, 10::smallint,
    'public."22_testns_1_6_partition"'::regclass, NULL, NULL); --create new table after

  IF to_regclass('public."22_testns_1_6_1136246400"') IS NULL THEN
      SELECT assert.fail('table does not exist 3.') INTO message;
      RETURN message;
  END IF;

  SELECT count(*) INTO cnt FROM data_tables 
    WHERE project_id = 22 
    and namespace = 'testns'
    and replica_no = 1
    and partition = 6;
  IF cnt != 3 THEN
      SELECT assert.fail('data table row cnt wrong.') INTO message;
      RETURN message;
  END IF;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION unit_tests.test_create_field()
RETURNS test_result
AS
$BODY$
DECLARE 
  message test_result;
  cnt int;
  dt data_tables%ROWTYPE;
  ns system.namespaces%ROWTYPE;
  df data_fields%ROWTYPE;
BEGIN
  raise log 'test';
  
  CREATE TABLE IF NOT EXISTS "22_testns_1_6_partition" (
    time BIGINT,
    partition_key VARCHAR,
    value jsonb,
    uuid UUID,
    row_id smallint,
    PRIMARY KEY(time, partition_key, uuid, row_id)
  );

  SELECT * INTO dt FROM get_data_table(1136239445 * 1e9::bigint, 22, 'testns', 1::smallint, 6::smallint, 10::smallint,
    'public."22_testns_1_6_partition"'::regclass, NULL, NULL);
 
  IF to_regclass('public."22_testns_1_6_1136160000-field_1"') IS NOT NULL THEN
        SELECT assert.fail('table does not exist. 2-1') INTO message;
        RETURN message;
  END IF;

  raise log 'field 1';
  PERFORM create_field(22, 'testns', dt, 'field_1', 'public."22_testns_1_6_partition"'::regclass, NULL, NULL);
  raise log 'field 2';
  PERFORM create_field(22, 'testns',dt, 'field_2', 'public."22_testns_1_6_partition"'::regclass, NULL, NULL);
  raise log 'field 3';
  PERFORM create_field(22, 'testns',dt, 'field_3', 'public."22_testns_1_6_partition"'::regclass, NULL, NULL);
  PERFORM create_field(22, 'testns',dt, 'Field_4', 'public."22_testns_1_6_partition"'::regclass, NULL, NULL);

  SELECT count(*) INTO cnt FROM data_fields 
    WHERE table_name = dt.table_name;
  IF cnt != 4 THEN
      SELECT assert.fail('data fields row cnt wrong.') INTO message;
      RETURN message;
  END IF;

  SELECT * INTO STRICT df FROM data_fields WHERE  table_name = 'public."22_testns_1_6_1136160000"'::regclass
    AND field_name = 'field_1' 
    AND field_type = 'double precision'::regtype
    AND is_distinct=true;
  
    IF to_regclass(df.idx::cstring) IS NULL THEN
        SELECT assert.fail('table does not exist. 2-2') INTO message;
        RETURN message;
  END IF;

  --the following are strict and will throw errors on non-match



  SELECT * INTO STRICT df FROM data_fields WHERE  table_name = 'public."22_testns_1_6_1136160000"'::regclass
    AND field_name = 'field_2' 
    AND field_type = 'text'::regtype
    AND is_distinct=true;

    SELECT * INTO STRICT df FROM data_fields WHERE  table_name = 'public."22_testns_1_6_1136160000"'::regclass
    AND field_name = 'field_3' 
    AND field_type = 'double precision'::regtype
    AND is_distinct=false;

  SELECT * INTO STRICT df FROM data_fields WHERE  table_name = 'public."22_testns_1_6_1136160000"'::regclass
    AND field_name = 'Field_4' 
    AND field_type = 'text'::regtype
    AND is_distinct=false;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION unit_tests.test_insert_new_data()
RETURNS test_result
AS
$BODY$
DECLARE 
  message test_result;
  cnt int;
  ns system.namespaces%ROWTYPE;
  df data_fields%ROWTYPE;
BEGIN
  CREATE TABLE IF NOT EXISTS "23_testns_1_8_partition" (
    time BIGINT,
    partition_key VARCHAR,
    value jsonb,
    uuid UUID,
    row_id smallint,
    PRIMARY KEY(time, partition_key, uuid, row_id)
  );

  CREATE TEMP TABLE "temp_table" (LIKE "23_testns_1_8_partition" INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP;

  -- reference time: Mon Jan _2 15:04:05 2006
  -- unix time 1136239445
  INSERT INTO temp_table (time, partition_key, value, uuid, row_id) VALUES
  (1136239445 * 1e9::bigint, '1', '{"field_1":1, "field_2":"one"}', uuid_generate_v4(), 1),
  (1136239445 * 1e9::bigint, '1', '{"field_1":1, "field_2":"two"}', uuid_generate_v4(), 1);
  
  PERFORM  insert_copy_table('temp_table'::regclass, 23, 'testns', 1::smallint, get_partition_for_key('1', 10)::smallint, 10::smallint,
         'public."23_testns_1_8_partition"'::regclass, NULL, NULL);

 
  SELECT * INTO STRICT df FROM data_fields WHERE  table_name = 'public."23_testns_1_8_1136160000"'::regclass
    AND field_name = 'field_1' 
    AND field_type = 'double precision'::regtype
    AND is_distinct=true;

  SELECT count(*) INTO cnt FROM public."23_testns_1_8_1136160000" ;
  IF cnt != 2 THEN
      SELECT assert.fail('data row cnt wrong.') INTO message;
      RETURN message;
  END IF;

  SELECT count(*) INTO cnt FROM public."23_testns_1_distinct" ;
  IF cnt != 3 THEN
      SELECT assert.fail(format('distinct cnt wrong. %L', cnt)) INTO message;
      RETURN message;
  END IF;

  -- enter more rows
  TRUNCATE TABLE temp_table;
  INSERT INTO temp_table (time, partition_key, value, uuid, row_id) VALUES
  (1136239445 * 1e9::bigint, '1', '{"field_1":1, "field_2":"one"}', uuid_generate_v4(), 1),
  (1136239445 * 1e9::bigint, '1', '{"field_1":1, "field_2":"three"}', uuid_generate_v4(), 1);
  PERFORM  insert_copy_table('temp_table'::regclass, 23, 'testns', 1::smallint, get_partition_for_key('1', 10)::smallint, 10::smallint, 
         'public."23_testns_1_8_partition"'::regclass, NULL, NULL);
  
  SELECT * INTO STRICT df FROM data_fields WHERE  table_name = 'public."23_testns_1_8_1136160000"'::regclass
    AND field_name = 'field_2' 
    AND field_type = 'text'::regtype
    AND is_distinct=true;


  SELECT count(*) INTO cnt FROM data_fields where table_name = '"23_testns_1_8_1136160000"'::regclass ;
  IF cnt != 2 THEN
      SELECT assert.fail('data field cnt wrong.') INTO message;
      RETURN message;
  END IF;


  SELECT count(*) INTO cnt FROM public."23_testns_1_8_1136160000" ;
  IF cnt != 4 THEN
      SELECT assert.fail('data row cnt wrong.') INTO message;
      RETURN message;
  END IF;

  SELECT count(*) INTO cnt FROM public."23_testns_1_distinct" ;
  IF cnt != 4 THEN
      SELECT assert.fail(format('distinct cnt wrong. %L', cnt)) INTO message;
      RETURN message;
  END IF;


  --add a field
  TRUNCATE TABLE temp_table;
  INSERT INTO temp_table (time, partition_key, value, uuid, row_id) VALUES
  (1136239445 * 1e9::bigint, '1', '{"field_1":1, "field_2":"one", "field_3":"2.345"}', uuid_generate_v4(), 1);
  PERFORM  insert_copy_table('temp_table'::regclass, 23, 'testns', 1::smallint, get_partition_for_key('1', 10)::smallint, 10::smallint, 
         'public."23_testns_1_8_partition"'::regclass, NULL, NULL);

  SELECT * INTO STRICT df FROM data_fields WHERE  table_name = 'public."23_testns_1_8_1136160000"'::regclass
    AND field_name = 'field_3' 
    AND field_type = 'double precision'::regtype
    AND is_distinct=false;

  SELECT count(*) INTO cnt FROM data_fields where table_name = '"23_testns_1_8_1136160000"'::regclass ;
  IF cnt != 3 THEN
      SELECT assert.fail('data field cnt wrong.') INTO message;
      RETURN message;
  END IF;


  SELECT count(*) INTO cnt FROM public."23_testns_1_8_1136160000" ;
  IF cnt != 5 THEN
      SELECT assert.fail('data row cnt wrong.') INTO message;
      RETURN message;
  END IF;

  SELECT count(*) INTO cnt FROM public."23_testns_1_distinct" ;
  IF cnt != 4 THEN
      SELECT assert.fail(format('distinct cnt wrong. %L', cnt)) INTO message;
      RETURN message;
  END IF;

  SELECT assert.ok('End of test.') INTO message;  
  RETURN message; 
END
$BODY$
LANGUAGE plpgsql;



