\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\o /dev/null
\ir include/create_clustered_db.sql

\o
\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('meta' :: NAME, 'localhost');
SELECT add_node('Test1' :: NAME, 'localhost');
SELECT add_node('test2' :: NAME, 'localhost');

\c Test1


\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN SELECT * FROM PUBLIC."Hypertable_1";
SELECT * FROM ONLY PUBLIC."Hypertable_1";
EXPLAIN SELECT * FROM ONLY PUBLIC."Hypertable_1";

\d+ PUBLIC."Hypertable_1"
\d+ "_sysinternal"."_hyper_1_root"
\d+ _sysinternal._hyper_1_1_0_1_data
SELECT * FROM PUBLIC.default_replica_node;


\c test2

\d+ PUBLIC."Hypertable_1"
\d+ "_sysinternal"."_hyper_1_root"

\ir include/ddl_ops_2.sql

\d+ PUBLIC."Hypertable_1"
\d+ "_sysinternal"."_hyper_1_root"
SELECT * FROM _sysinternal._hyper_1_0_1_distinct_data;


\c Test1
\d+ PUBLIC."Hypertable_1"
\d+ "_sysinternal"."_hyper_1_root"
\d+ _sysinternal._hyper_1_1_0_1_data

SELECT * FROM PUBLIC."Hypertable_1";
