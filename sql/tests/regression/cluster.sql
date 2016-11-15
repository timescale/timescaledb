\set ON_ERROR_STOP 1

\ir create_clustered_db.sql

\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT add_node('Test1' :: NAME, 'localhost');
SELECT add_node('test2' :: NAME, 'localhost');

SELECT add_hypertable('testNs' :: NAME, 'Device_id');
SELECT * 
FROM  partition_replica;


SELECT add_field('testNs' :: NAME, 'Device_id', 'text', TRUE, TRUE, ARRAY['TIME-VALUE'] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'temp', 'double precision', FALSE, FALSE, ARRAY['VALUE-TIME'] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'occupied', 'boolean', FALSE, FALSE, ARRAY[] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'latitude', 'bigint', FALSE, FALSE, ARRAY[] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'really_long_field_goes_on_and_on_and_on_and_on_and_on_and_on_and_on_and_on', 'bigint', FALSE, FALSE, ARRAY['TIME-VALUE','VALUE-TIME'] :: field_index_type []);

SELECT * FROM get_or_create_chunk(1,1257894000000000000::bigint);

SELECT *
FROM node;
SELECT *
FROM hypertable;
SELECT *
FROM hypertable_replica;
SELECT * 
FROM  distinct_replica_node;
SELECT * 
FROM  partition_epoch;
SELECT * 
FROM  partition;
SELECT * 
FROM  partition_replica;
SELECT *
FROM chunk;
SELECT *
FROM chunk_replica_node;
SELECT *
FROM field;

\echo *********************************************************************************************************ß
\c Test1

SELECT *
FROM node;
SELECT *
FROM hypertable;
SELECT *
FROM hypertable_replica;
SELECT * 
FROM  distinct_replica_node;
SELECT * 
FROM  partition_epoch;
SELECT * 
FROM  partition;
SELECT * 
FROM  partition_replica;
SELECT *
FROM chunk;
SELECT *
FROM chunk_replica_node;
SELECT *
FROM field;
\d+ "_sys_1_testNs".*
--\d+ "_sys_1_testNs"."_sys_1_testNs_1_0_partition"
--\d+ "_sys_1_testNs"."_sys_1_testNs_2_0_partition"
--\det "_sys_1_testNs".*
--\d+ "testNs".distinct
--test idempotence
\c meta

SELECT add_hypertable('testNs' :: NAME, 'Device_id');

SELECT add_field('testNs' :: NAME, 'Device_id', 'text', TRUE, TRUE, ARRAY['TIME-VALUE'] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'temp', 'double precision', FALSE, FALSE, ARRAY['VALUE-TIME'] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'occupied', 'boolean', FALSE, FALSE, ARRAY[] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'latitude', 'bigint', FALSE, FALSE, ARRAY[] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'really_long_field_goes_on_and_on_and_on_and_on_and_on_and_on_and_on_and_on', 'bigint', FALSE, FALSE, ARRAY['TIME-VALUE','VALUE-TIME'] :: field_index_type []);

SELECT * FROM get_or_create_chunk(1,1257894000000000000::bigint);
\c Test1
\d+ "_sys_1_testNs".*

\c meta
SELECT close_chunk_end(1);
SELECT *
FROM chunk;

SELECT * FROM get_or_create_chunk(1,10::bigint);
SELECT * FROM get_or_create_chunk(1,1257894000000000000::bigint);
SELECT *
FROM chunk;

\c Test1
\d+ "_sys_1_testNs".*


