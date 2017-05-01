\o /dev/null
\ir include/create_single_db.sql
\o
\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM PUBLIC."Hypertable_1";
SELECT * FROM _timescaledb_catalog.default_replica_node;

\ir include/ddl_ops_2.sql

\d+ PUBLIC."Hypertable_1"
\d+ "_timescaledb_internal"."_hyper_1_root"
\d+ _timescaledb_internal._hyper_1_1_0_1_data

SELECT * FROM PUBLIC."Hypertable_1";
