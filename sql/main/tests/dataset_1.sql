CREATE TABLE IF NOT EXISTS "22_testns_1_6_partition" (
  time BIGINT,
  partition_key VARCHAR,
  value jsonb,
  uuid UUID,
  row_id smallint,
  PRIMARY KEY(time, partition_key, uuid, row_id)
);


CREATE TEMP TABLE "temp_table" (LIKE "22_testns_1_6_partition" INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP;

-- reference time: Mon Jan _2 15:04:05 2006
-- unix time 1136239445
INSERT INTO temp_table (time, partition_key, value, uuid, row_id) VALUES
(1136239445 * 1e9::bigint, '1', '{"field_1":1, "field_2":"one", "field_4":1}', uuid_generate_v4(), 1),
(1136239446 * 1e9::bigint, '1', '{"field_1":1, "field_2":"two", "field_4":2}', uuid_generate_v4(), 1);
(1136239447 * 1e9::bigint, '1', '{"field_1":1, "field_2":"two", "field_4":3}', uuid_generate_v4(), 1);
  
 SELECT insert_copy_table('temp_table'::regclass, 22, 'testns', 1::smallint, 6::smallint); 
  
