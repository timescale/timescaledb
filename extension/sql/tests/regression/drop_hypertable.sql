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

SELECT * FROM hypertable;
DROP TABLE "testNs";
SELECT * FROM hypertable;
\dt+ "_sysinternal".*

\c Test1

\dt+ "_sysinternal".*