\set ON_ERROR_STOP 1

\set ECHO ALL
\ir include/insert_single.sql

\c single
\d+ "testNs".*
SELECT *
FROM "testNs"._hyper_1_0_distinct;
SELECT * FROM "testNs";
