\c postgres :ROLE_SUPERUSER
DROP DATABASE single;
CREATE DATABASE single;

\c single
CREATE SCHEMA "testSchema0";
CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA "testSchema0";
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
SELECT AVG(temp) AS avg_tmp, "testSchema0".time_bucket('5 minutes', time, '1 minutes') AS ten_min FROM test_ts GROUP BY ten_min ORDER BY avg_tmp;
SELECT AVG(temp) AS avg_tmp, "testSchema0".time_bucket('5 minutes', time, '1 minutes') AS ten_min FROM test_tz GROUP BY ten_min ORDER BY avg_tmp;
SELECT AVG(temp) AS avg_tmp, "testSchema0".time_bucket('1 day', time, '-0.5 day') AS ten_min FROM test_dt GROUP BY ten_min ORDER BY avg_tmp;
-- testing time_bucket END

-- testing drop_chunks START
SELECT "testSchema0".drop_chunks(interval '2 years', 'test_ts');
SELECT * FROM test_ts ORDER BY time;
SELECT "testSchema0".drop_chunks(interval '1 minutes', 'test_tz');
SELECT * FROM test_tz ORDER BY time;
SELECT "testSchema0".drop_chunks(interval '1 minutes', 'test_dt');
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
