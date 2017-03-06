\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\o /dev/null
\ir include/create_single_db.sql

\o
\set ECHO ALL

\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM PUBLIC."Hypertable_1";
SELECT * FROM ONLY PUBLIC."Hypertable_1";
EXPLAIN (costs off) SELECT * FROM ONLY PUBLIC."Hypertable_1";

\d+ PUBLIC."Hypertable_1"
\d+ "_timescaledb_internal"."_hyper_1_root"
\d+ _timescaledb_internal._hyper_1_1_0_1_data
SELECT * FROM _timescaledb_catalog.default_replica_node;

\ir include/ddl_ops_2.sql

\d+ PUBLIC."Hypertable_1"
\d+ "_timescaledb_internal"."_hyper_1_root"

SELECT * FROM PUBLIC."Hypertable_1";
