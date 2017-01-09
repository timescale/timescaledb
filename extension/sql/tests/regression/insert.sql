\set ON_ERROR_STOP 1

\set ECHO ALL
\ir include/insert.sql


\c Test1
\d+ "testNs".*

\c test2
\d+ "testNs".*
SELECT *
FROM "testNs"._hyper_1_0_replica;
SELECT *
FROM "testNs"._hyper_1_0_distinct;
