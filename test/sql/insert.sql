\set ON_ERROR_STOP 1

\set ECHO ALL
\ir include/insert_two_partitions.sql

\d+ "_timescaledb_internal".*
SELECT *
FROM "_timescaledb_internal"._hyper_1_0_replica;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.chunk_replica_node;
SELECT * FROM _timescaledb_catalog.partition_replica;
SELECT * FROM chunk_closing_test;
SELECT * FROM ONLY chunk_closing_test;
SELECT * FROM "testNs";
SELECT * FROM ONLY "testNs";
