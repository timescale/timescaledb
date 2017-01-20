\set ON_ERROR_STOP 1
\o /dev/null
\ir include/insert.sql
\o
\set ECHO ALL

\c Test1
\d+ "_sysinternal".*

\c test2
SELECT *
FROM "_sysinternal"._hyper_1_0_replica;
SELECT *
FROM "_sysinternal"._hyper_1_0_distinct;

SELECT * FROM _iobeamdb_catalog.hypertable;
DROP TABLE "testNs";

SELECT * FROM _iobeamdb_catalog.hypertable;
\dt  "public".*
\dt  "_iobeamdb_catalog".*
\dt+ "_sysinternal".*

\c Test1
SELECT * FROM _iobeamdb_catalog.hypertable;
\dt  "public".*
\dt  "_iobeamdb_catalog".*
\dt+ "_sysinternal".*
