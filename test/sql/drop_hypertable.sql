\set ON_ERROR_STOP 1
\o /dev/null
\ir include/insert_single.sql
\o
\set ECHO ALL

\c single
\d+ "_iobeamdb_internal".*

SELECT * FROM _iobeamdb_catalog.hypertable;
DROP TABLE "testNs";

SELECT * FROM _iobeamdb_catalog.hypertable;
\dt  "public".*
\dt  "_iobeamdb_catalog".*
\dt+ "_iobeamdb_internal".*
