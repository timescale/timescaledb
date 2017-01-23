\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\ir include/create_clustered_db.sql

\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('meta' :: NAME, 'localhost');
SELECT add_node('Test1' :: NAME, 'localhost');

SELECT add_node('test2' :: NAME, 'localhost');

\c Test1

create schema test_schema;
create table test_schema.test_table(time bigint, temp float8, device_id text);
\dt "test_schema".*
select * from create_hypertable('test_schema.test_table', 'time', 'device_id');

\C test2
\dt "test_schema".*
