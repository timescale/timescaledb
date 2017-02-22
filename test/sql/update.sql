\set ON_ERROR_STOP 1

\o /dev/null
\ir include/insert_single.sql

\o
\set ECHO ALL
\c single

SELECT * FROM "testNs";

UPDATE "testNs" SET series_1 = 47;
UPDATE "testNs" SET series_bool = true;
SELECT * FROM "testNs";

