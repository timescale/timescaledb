\c single :ROLE_SUPERUSER
CREATE SCHEMA IF NOT EXISTS "customSchema" AUTHORIZATION :ROLE_DEFAULT_PERM_USER;
\c single :ROLE_DEFAULT_PERM_USER

\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM PUBLIC."Hypertable_1";
SELECT * FROM ONLY PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM ONLY PUBLIC."Hypertable_1";

SELECT * FROM test.show_columns('PUBLIC."Hypertable_1"');
SELECT * FROM test.show_columns('_timescaledb_internal._hyper_1_1_chunk');

\ir include/ddl_ops_2.sql

SELECT * FROM test.show_columns('PUBLIC."Hypertable_1"');

SELECT * FROM PUBLIC."Hypertable_1";
