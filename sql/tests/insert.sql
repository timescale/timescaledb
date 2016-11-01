\set ON_ERROR_STOP 1

\ir create_clustered_db.sql

\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT add_node('Test1' :: NAME, 'localhost');
SELECT add_node('test2' :: NAME, 'localhost');

SELECT add_namespace('testNs' :: NAME);
SELECT add_field('testNs' :: NAME, 'device_id', 'text', TRUE, TRUE, ARRAY ['VALUE-TIME'] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'series_0', 'double precision', FALSE, FALSE,
                 ARRAY ['TIME-VALUE'] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'series_1', 'double precision', FALSE, FALSE,
                 ARRAY ['TIME-VALUE'] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'series_2', 'double precision', FALSE, FALSE,
                 ARRAY ['TIME-VALUE'] :: field_index_type []);
SELECT add_field('testNs' :: NAME, 'series_bool', 'boolean', FALSE, FALSE, ARRAY ['TIME-VALUE'] :: field_index_type []);

\c Test1
BEGIN;
SELECT *
FROM create_temp_copy_table_one_partition('copy_t', get_partition_for_key('dev1', 10 :: SMALLINT), 10 :: SMALLINT);
\COPY copy_t FROM 'data/ds1_dev1_1.tsv';
SELECT *
FROM insert_data_one_partition('copy_t', get_partition_for_key('dev1', 10 :: SMALLINT), 10 :: SMALLINT);
COMMIT;

SELECT close_data_table_end(dt.table_oid)
FROM data_table dt
WHERE dt.namespace_name = 'testNs';
BEGIN;
SELECT *
FROM create_temp_copy_table_one_partition('copy_t', get_partition_for_key('dev1', 10 :: SMALLINT), 10 :: SMALLINT);
\COPY copy_t FROM 'data/ds1_dev1_2.tsv';
SELECT *
FROM insert_data_one_partition('copy_t', get_partition_for_key('dev1', 10 :: SMALLINT), 10 :: SMALLINT);
COMMIT;

\c test2

BEGIN;
SELECT *
FROM create_temp_copy_table_one_partition('copy_t', get_partition_for_key('dev2', 10 :: SMALLINT), 10 :: SMALLINT);
\COPY copy_t FROM 'data/ds1_dev2_1.tsv';
SELECT *
FROM insert_data_one_partition('copy_t', get_partition_for_key('dev2', 10 :: SMALLINT), 10 :: SMALLINT);
COMMIT;

\c Test1
\dt "testNs".*

\c test2
\dt "testNs".*
SELECT *
FROM "testNs".cluster;
SELECT *
FROM "testNs".distinct;

SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs'));
SELECT *
FROM ioql_exec_query(new_ioql_query(namespace_name => 'testNs',
                                    select_items => ARRAY [new_select_item('series_0', 'SUM')],
                                    aggregate=> new_aggregate(10e9::BIGINT)
                     ));
