\set ON_ERROR_STOP 1

\set ECHO ALL
\ir include/insert.sql

\c Test1
\d+ "_iobeamdb_internal".*

\c test2
\d+ "_iobeamdb_internal".*
SELECT *
FROM "_iobeamdb_internal"._hyper_1_0_replica;
SELECT *
FROM "_iobeamdb_internal"._hyper_1_0_distinct;
SELECT * FROM _iobeamdb_catalog.chunk;
SELECT * FROM _iobeamdb_catalog.chunk_replica_node;
SELECT * FROM _iobeamdb_catalog.partition_replica;
select * from public.chunk_closing_test;
