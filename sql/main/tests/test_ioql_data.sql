CREATE OR REPLACE FUNCTION unit_tests.load_data_1()
RETURNS VOID
AS
$BODY$
BEGIN
  TRUNCATE data_tables cascade;
  
  DROP TABLE IF EXISTS cluster."22_testns_1" cascade; 
  CREATE TABLE cluster."22_testns_1" (
    time BIGINT,
    partition_key VARCHAR,
    value jsonb,
    uuid UUID,
    row_id smallint,
    PRIMARY KEY(time, partition_key, uuid, row_id)
  );
  DROP TABLE IF EXISTS "22_testns_1_8_partition" cascade; 
  CREATE TABLE "22_testns_1_8_partition" () INHERITS (cluster."22_testns_1");

  DROP TABLE IF EXISTS temp_table;
  CREATE TEMP TABLE "temp_table" (LIKE "22_testns_1_8_partition" INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP;
  INSERT INTO temp_table (time, partition_key, value, uuid, row_id) VALUES
  (1136239445 * 1e9::bigint, '1', '{"field_1":1, "field_2":"one", "field_3":3, "Field_4":1}', uuid_generate_v4(), 1),
  (1136239446 * 1e9::bigint, '1', '{"field_1":1, "field_2":"two", "Field_4":2}', uuid_generate_v4(), 1),
  (1136239447 * 1e9::bigint, '1', '{"field_1":1, "field_2":"two", "Field_4":3}', uuid_generate_v4(), 1);
  PERFORM insert_copy_table('temp_table'::regclass, 22, 'testns', 1::smallint, 8::smallint, 10::smallint, 
         'public."22_testns_1_8_partition"'::regclass, NULL, NULL);
  

  DROP TABLE IF EXISTS cases;
  CREATE TEMP TABLE "cases"  (
    query ioql_query,
    expected int 
  ) ON COMMIT DROP;
 
  INSERT INTO cases VALUES
  (new_ioql_query(22, 'testns', '*'      ), 3),
  (new_ioql_query(22, 'testns', 'field_1'), 3),
  (new_ioql_query(22, 'testns', '*',       limit_rows => 1), 1),
  (new_ioql_query(22, 'testns', 'field_1', limit_rows => 1), 1),
  (new_ioql_query(22, 'testns', 'field_1', aggregate=> new_aggregate('MAX', 5e9::bigint)), 1),
  (new_ioql_query(22, 'testns', 'field_1', aggregate=> new_aggregate('MAX', 5*1e9::bigint, 'field_2')), 2),
  (new_ioql_query(22, 'testns', 'field_1', aggregate=> new_aggregate('MAX', 5*1e9::bigint, 'field_2'), 
      time_condition=> new_time_condition(from_time => 1136239447 * 1e9::bigint),
      limit_time_periods => 1), 2), --still returns 2 rows even if no data group_bys  with limit_time_periods are sparse
  (new_ioql_query(22, 'testns', 'field_1', aggregate=>  new_aggregate('MAX', 1*1e9::bigint, 'field_2'),
      limit_time_periods => 2), 3), --returns 3 rows; group by over times 1136239447  and 1136239446 for "two", null for "one"
  (new_ioql_query(22, 'testns', 'field_1', aggregate=>  new_aggregate('MAX',  1*1e9::bigint),
      time_condition => new_time_condition (to_time => 1136239447 * 1e9::bigint),
      limit_time_periods => 2), 2),--returns 2 rows;  over times 1136239446  and 1136239445 
 (new_ioql_query(22, 'testns', 'field_1',  aggregate=> new_aggregate('MAX', 1*1e9::bigint, 'field_2'),
      limit_time_periods => 1), 2), --returns 2 rows; group by over times 1136239447 for "two", null for "one"
 (new_ioql_query(22, 'testns', 'field_1',  aggregate=>  new_aggregate('MAX', 1*1e9::bigint, 'field_2'),
      limit_time_periods => 1, limit_rows => 1), 1), --returns 1 rows; due to limit_rows
 (new_ioql_query(22, 'testns', 'field_1',  aggregate=>  new_aggregate('MAX', 5*1e9::bigint, 'field_2'), 
    time_condition => new_time_condition(from_time => 1136239448 * 1e9::bigint))
  , 2), -- returns 2 rows even if no data, both null. group_bys with field are sparse

  (new_ioql_query(22, 'testns', '*', time_condition=> new_time_condition(from_time => 1136239448 * 1e9::bigint)), 0),
  (new_ioql_query(22, 'testns', '*', time_condition=> new_time_condition(from_time => 1136239447 * 1e9::bigint)), 1), --inclusive from_time
  (new_ioql_query(22, 'testns', '*', time_condition=> new_time_condition(to_time => 1136239444  * 1e9::bigint)), 0),
  (new_ioql_query(22, 'testns', '*', time_condition=> new_time_condition(to_time => 1136239445  * 1e9::bigint)), 0),  --exclusive to_time
  (new_ioql_query(22, 'testns', '*', time_condition=> new_time_condition(to_time => 1136239446  * 1e9::bigint)), 1),
  (new_ioql_query(22, 'testns', '*', time_condition=> new_time_condition(from_time=> 1136239445 * 1e9::bigint, to_time => 1136239448  * 1e9::bigint)), 3),
  

  (new_ioql_query(22, 'testns', 'field_1', 
      field_condition => new_field_condition(conjunctive=>'AND', predicates=> ARRAY[new_field_predicate('field_1', '=', '1')])
     ), 3),
  (new_ioql_query(22, 'testns', 'field_1', 
      field_condition => new_field_condition(conjunctive=>'AND', predicates=> ARRAY[new_field_predicate('field_1', '=', '2')])
     ), 0),
 
  (new_ioql_query(22, 'testns', '*', limit_by_field => new_limit_by_field('field_2', 1)), 2),
  (new_ioql_query(22, 'testns', '*', limit_by_field => new_limit_by_field('field_2', 1), limit_rows=>1), 1),
  (new_ioql_query(22, 'testns', '*', 
      time_condition => new_time_condition(from_time=>1136239448 * 1e9::bigint), 
      limit_by_field =>  new_limit_by_field('field_2', 1)),  2),--by every is sparse
  (new_ioql_query(22, 'testns', '*', limit_by_field =>  new_limit_by_field('field_1',1)), 1),
  (new_ioql_query(22, 'testns', '*', limit_by_field =>  new_limit_by_field('field_3',1)), 
    0), --TODO: should probably throw an error; field_3 not a distinct field.
  
  (new_ioql_query(22, 'testns', '*', limit_rows=>1), 1);

END
$BODY$
LANGUAGE plpgsql;
