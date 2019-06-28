-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Set this variable to avoid using a hard-coded path each time query
-- results are compared
\set QUERY_RESULT_TEST_EQUAL_RELPATH 'include/query_result_test_equal.sql'

\c postgres :ROLE_SUPERUSER
DROP DATABASE :TEST_DBNAME;
CREATE DATABASE :TEST_DBNAME;

\c :TEST_DBNAME
CREATE SCHEMA "testSchema0";
SET client_min_messages=error;
CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA "testSchema0";
RESET client_min_messages;
SET timescaledb.disable_optimizations = :DISABLE_OPTIMIZATIONS;


CREATE TABLE test_ts(time timestamp, temp float8, device text);
CREATE TABLE test_tz(time timestamptz, temp float8, device text);
CREATE TABLE test_dt(time date, temp float8, device text);


SELECT "testSchema0".create_hypertable('test_ts', 'time', 'device', 2);
SELECT "testSchema0".create_hypertable('test_tz', 'time', 'device', 2);
SELECT "testSchema0".create_hypertable('test_dt', 'time', 'device', 2);

SELECT * FROM _timescaledb_catalog.hypertable;

INSERT INTO test_ts VALUES('Mon Mar 20 09:17:00.936242 2017', 23.4, 'dev1');
INSERT INTO test_ts VALUES('Mon Mar 20 09:27:00.936242 2017', 22, 'dev2');
INSERT INTO test_ts VALUES('Mon Mar 20 09:28:00.936242 2017', 21.2, 'dev1');
INSERT INTO test_ts VALUES('Mon Mar 20 09:37:00.936242 2017', 30, 'dev3');
SELECT * FROM test_ts ORDER BY time;

INSERT INTO test_tz VALUES('Mon Mar 20 09:17:00.936242 2017', 23.4, 'dev1');
INSERT INTO test_tz VALUES('Mon Mar 20 09:27:00.936242 2017', 22, 'dev2');
INSERT INTO test_tz VALUES('Mon Mar 20 09:28:00.936242 2017', 21.2, 'dev1');
INSERT INTO test_tz VALUES('Mon Mar 20 09:37:00.936242 2017', 30, 'dev3');
SELECT * FROM test_tz ORDER BY time;

INSERT INTO test_dt VALUES('Mon Mar 20 09:17:00.936242 2017', 23.4, 'dev1');
INSERT INTO test_dt VALUES('Mon Mar 21 09:27:00.936242 2017', 22, 'dev2');
INSERT INTO test_dt VALUES('Mon Mar 22 09:28:00.936242 2017', 21.2, 'dev1');
INSERT INTO test_dt VALUES('Mon Mar 23 09:37:00.936242 2017', 30, 'dev3');
SELECT * FROM test_dt ORDER BY time;
-- testing time_bucket START
SELECT AVG(temp) AS avg_tmp, "testSchema0".time_bucket('5 minutes', time, INTERVAL '1 minutes') AS ten_min FROM test_ts GROUP BY ten_min ORDER BY avg_tmp;
SELECT AVG(temp) AS avg_tmp, "testSchema0".time_bucket('5 minutes', time, INTERVAL '1 minutes') AS ten_min FROM test_tz GROUP BY ten_min ORDER BY avg_tmp;
SELECT AVG(temp) AS avg_tmp, "testSchema0".time_bucket('1 day', time, INTERVAL '-0.5 day') AS ten_min FROM test_dt GROUP BY ten_min ORDER BY avg_tmp;
-- testing time_bucket END

-- testing drop_chunks START
-- show_chunks and drop_chunks output should be the same
\set QUERY1 'SELECT "testSchema0".show_chunks(older_than => \'2017-03-01\'::timestamp, hypertable => \'test_ts\')::REGCLASS::TEXT'
\set QUERY2 'SELECT "testSchema0".drop_chunks(\'2017-03-01\'::timestamp, \'test_ts\')::TEXT'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test_ts ORDER BY time;
\set QUERY1 'SELECT "testSchema0".show_chunks(older_than => interval \'1 minutes\', hypertable => \'test_tz\')::REGCLASS::TEXT'
\set QUERY2 'SELECT "testSchema0".drop_chunks(interval \'1 minutes\', \'test_tz\')::TEXT'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test_tz ORDER BY time;
\set QUERY1 'SELECT "testSchema0".show_chunks(older_than => interval \'1 minutes\', hypertable => \'test_dt\')::REGCLASS::TEXT'
\set QUERY2 'SELECT "testSchema0".drop_chunks(interval \'1 minutes\', \'test_dt\')::TEXT'
\set ECHO errors
\ir  :QUERY_RESULT_TEST_EQUAL_RELPATH
\set ECHO all
SELECT * FROM test_dt ORDER BY time;
-- testing drop_chunks END

-- testing hypertable_relation_size_pretty START
SELECT * FROM "testSchema0".hypertable_relation_size_pretty('test_ts');
-- testing hypertable_relation_size_pretty END

-- testing indexes_relation_size_pretty START
SELECT * FROM "testSchema0".indexes_relation_size_pretty('test_ts') ORDER BY index_name;
-- testing indexes_relation_size_pretty END

CREATE SCHEMA "testSchema";

\set ON_ERROR_STOP 0
ALTER EXTENSION timescaledb SET SCHEMA "testSchema";
\set ON_ERROR_STOP 1
