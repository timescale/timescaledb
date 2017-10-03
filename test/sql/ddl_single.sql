\o /dev/null
\ir include/create_single_db.sql
\o
\c single postgres
CREATE SCHEMA IF NOT EXISTS "customSchema" AUTHORIZATION alt_usr;
\c single alt_usr

\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM PUBLIC."Hypertable_1";

\ir include/ddl_ops_2.sql

\d+ PUBLIC."Hypertable_1"
\d+ _timescaledb_internal._hyper_1_1_chunk

SELECT * FROM PUBLIC."Hypertable_1";
