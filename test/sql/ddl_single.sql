\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\o /dev/null
\ir include/create_single_db.sql

\o
\set ECHO ALL
\c single

\ir include/ddl_ops_1.sql

SELECT * FROM PUBLIC."Hypertable_1";
EXPLAIN SELECT * FROM PUBLIC."Hypertable_1";
SELECT * FROM _iobeamdb_catalog.default_replica_node;

\ir include/ddl_ops_2.sql

\d+ PUBLIC."Hypertable_1"
\d+ "_iobeamdb_internal"."_hyper_1_root"
\d+ _iobeamdb_internal._hyper_1_1_0_1_data

SELECT * FROM PUBLIC."Hypertable_1";
