\o /dev/null
\ir include/create_single_db.sql
\o

\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM PUBLIC."Hypertable_1";
SELECT * FROM ONLY PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM ONLY PUBLIC."Hypertable_1";

\d+ PUBLIC."Hypertable_1"
\d+ _timescaledb_internal._hyper_1_1_chunk

\ir include/ddl_ops_2.sql

\d+ PUBLIC."Hypertable_1"

SELECT * FROM PUBLIC."Hypertable_1";
