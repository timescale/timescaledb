SET client_min_messages = WARNING;
DROP DATABASE IF EXISTS single;
SET client_min_messages = NOTICE;
CREATE DATABASE single;

\c single
CREATE SCHEMA "testSchema0";
CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA "testSchema0";
SET timescaledb.disable_optimizations = :DISABLE_OPTIMIZATIONS;


CREATE TABLE test(time timestamp, temp float8, device text);

SELECT "testSchema0".create_hypertable('test', 'time', 'device', 2);
SELECT * FROM _timescaledb_catalog.hypertable;
INSERT INTO test VALUES('Mon Mar 20 09:17:00.936242 2017', 23.4, 'dev1');
SELECT * FROM test;

CREATE SCHEMA "testSchema";

\set ON_ERROR_STOP 0
ALTER EXTENSION timescaledb SET SCHEMA "testSchema";
\set ON_ERROR_STOP 1
