\ir include/insert_two_partitions.sql

SELECT * FROM hypertable_relation_size('"public"."two_Partitions"');
SELECT * FROM chunk_relation_size('"public"."two_Partitions"');
SELECT * FROM indexes_relation_size('"public"."two_Partitions"');

