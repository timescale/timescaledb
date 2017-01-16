\set ON_ERROR_STOP 1

\o /dev/null
\ir include/create_single_db.sql

\o
\set ECHO ALL
\c single
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('single' :: NAME, 'fakehost');
SELECT add_node('single' :: NAME, 'fakehost');

\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN SELECT * FROM PUBLIC."Hypertable_1";
SELECT * FROM PUBLIC.default_replica_node;

\ir include/ddl_ops_2.sql

\d+ PUBLIC."Hypertable_1"
\d+ "_sysinternal"."_hyper_1_root"
\d+ _sysinternal._hyper_1_1_0_1_data
SELECT * FROM _sysinternal._hyper_1_0_1_distinct_data;

SELECT * FROM PUBLIC."Hypertable_1";
