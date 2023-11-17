-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- We are testing different repair functions here to make sure that
-- they work as expected.

\c :TEST_DBNAME :ROLE_SUPERUSER

\set TMP_USER :TEST_DBNAME _wizard

CREATE USER :TMP_USER;
CREATE USER "Random L User";

CREATE TABLE test_table_1(time timestamptz not null, temp float);
SELECT create_hypertable('test_table_1', by_range('time', '1 day'::interval));

INSERT INTO test_table_1(time,temp)
SELECT time, 100 * random()
  FROM generate_series(
           '2000-01-01'::timestamptz,
           '2000-01-05'::timestamptz,
           '1min'::interval
       ) time;

CREATE TABLE test_table_2(time timestamptz not null, temp float);
SELECT create_hypertable('test_table_2', by_range('time', '1 day'::interval));

INSERT INTO test_table_2(time,temp)
SELECT time, 100 * random()
  FROM generate_series(
           '2000-01-01'::timestamptz,
           '2000-01-05'::timestamptz,
           '1min'::interval
       ) time;

GRANT ALL ON test_table_1 TO :TMP_USER;
GRANT ALL ON test_table_2 TO :TMP_USER;
GRANT SELECT, INSERT ON test_table_1 TO "Random L User";
GRANT INSERT ON test_table_2 TO "Random L User";

-- Break the relacl of the table by deleting users
DELETE FROM pg_authid WHERE rolname IN (:'TMP_USER', 'Random L User');

CREATE TABLE saved (LIKE pg_class);
INSERT INTO saved SELECT * FROM pg_class;

CALL _timescaledb_functions.repair_relation_acls();

-- The only thing we should see here are the relations we broke and
-- the privileges we added for that user. No other relations should be
-- touched.
WITH
   lhs AS (SELECT oid, aclexplode(relacl) FROM pg_class),
   rhs AS (SELECT oid, aclexplode(relacl) FROM saved)
SELECT rhs.oid::regclass
FROM lhs FULL OUTER JOIN rhs ON row(lhs) = row(rhs)
WHERE lhs.oid IS NULL AND rhs.oid IS NOT NULL
GROUP BY rhs.oid;

DROP TABLE saved;
DROP TABLE test_table_1;
DROP TABLE test_table_2;
