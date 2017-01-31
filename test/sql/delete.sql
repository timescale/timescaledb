\set ON_ERROR_STOP 1

\o /dev/null
\ir include/insert.sql

\o
\set ECHO ALL
\c test2

SELECT * FROM "testNs";

DELETE FROM "testNs" WHERE series_0 = 1.5;
DELETE FROM "testNs" WHERE series_0 = 100;
SELECT * FROM "testNs";

