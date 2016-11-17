\set ON_ERROR_STOP 1

\ir create_clustered_db.sql

\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('meta' :: NAME, 'localhost');
SELECT add_node('Test1' :: NAME, 'localhost');
SELECT add_node('test2' :: NAME, 'localhost');

SELECT add_hypertable('testNs' :: NAME, 'device_id', 'testNs', 'testNs' );
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
FROM create_temp_copy_table('copy_t');
\COPY copy_t FROM 'data/ds1_dev1_1.tsv';
SELECT *
FROM insert_data('copy_t');
COMMIT;

SELECT close_chunk_end(c.id)
FROM get_open_partition_for_key('testNs','dev1') part
INNER JOIN chunk c ON (c.partition_id = part.id);

\c Test1
BEGIN;
SELECT *
FROM create_temp_copy_table('copy_t');
\COPY copy_t FROM 'data/ds1_dev1_2.tsv';
SELECT *
FROM insert_data('copy_t');
COMMIT;

\c test2
BEGIN;
SELECT *
FROM create_temp_copy_table('copy_t');
\COPY copy_t FROM 'data/ds1_dev2_1.tsv';
SELECT *
FROM insert_data('copy_t');
COMMIT;

\c Test1
\d+ "testNs".*

\c test2
\d+ "testNs".*
SELECT *
FROM "testNs"._hyper_1_0_replica;
SELECT *
FROM "testNs"._hyper_1_0_distinct;


