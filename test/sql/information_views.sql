-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT * FROM timescaledb_information.hypertables;

-- create simple hypertable with 1 chunk
CREATE TABLE ht1(time TIMESTAMPTZ NOT NULL);
SELECT create_hypertable('ht1','time');
INSERT INTO ht1 SELECT '2000-01-01'::TIMESTAMPTZ;

-- create simple hypertable with 1 chunk and toasted data
CREATE TABLE ht2(time TIMESTAMPTZ NOT NULL, data TEXT);
SELECT create_hypertable('ht2','time');
INSERT INTO ht2 SELECT '2000-01-01'::TIMESTAMPTZ, repeat('8k',4096);

SELECT * FROM timescaledb_information.hypertables
ORDER BY hypertable_schema, hypertable_name;

\c :TEST_DBNAME :ROLE_SUPERUSER

-- create schema open and hypertable with 3 chunks
CREATE SCHEMA open;
GRANT USAGE ON SCHEMA open TO :ROLE_DEFAULT_PERM_USER;
CREATE TABLE open.open_ht(time TIMESTAMPTZ NOT NULL);
SELECT create_hypertable('open.open_ht','time');
INSERT INTO open.open_ht SELECT '2000-01-01'::TIMESTAMPTZ;
INSERT INTO open.open_ht SELECT '2001-01-01'::TIMESTAMPTZ;
INSERT INTO open.open_ht SELECT '2002-01-01'::TIMESTAMPTZ;

-- create schema closed and hypertable
CREATE SCHEMA closed;
CREATE TABLE closed.closed_ht(time TIMESTAMPTZ NOT NULL);
SELECT create_hypertable('closed.closed_ht','time');
INSERT INTO closed.closed_ht SELECT '2000-01-01'::TIMESTAMPTZ;

SELECT * FROM timescaledb_information.hypertables
ORDER BY hypertable_schema, hypertable_name;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
\x
SELECT * FROM timescaledb_information.hypertables
ORDER BY hypertable_schema, hypertable_name;

-- filter by schema
SELECT * FROM timescaledb_information.hypertables
WHERE hypertable_schema = 'closed'
ORDER BY hypertable_schema, hypertable_name;

-- filter by table name
SELECT * FROM timescaledb_information.hypertables
WHERE hypertable_name = 'ht1'
ORDER BY hypertable_schema, hypertable_name;

-- filter by owner
SELECT * FROM timescaledb_information.hypertables
WHERE owner = 'super_user'
ORDER BY hypertable_schema, hypertable_name;
\x

---Add integer table --
CREATE TABLE test_table_int(time bigint, junk int);
SELECT create_hypertable('test_table_int', 'time', chunk_time_interval => 10);
CREATE OR REPLACE function table_int_now() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 1::BIGINT';
SELECT set_integer_now_func('test_table_int', 'table_int_now');
INSERT into test_table_int SELECT generate_series( 1, 20), 100;

 SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = 'ht1' ORDER BY chunk_name;
 SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = 'test_table_int' ORDER BY chunk_name;

\x
SELECT * FROM timescaledb_information.dimensions ORDER BY hypertable_name, dimension_number;
\x
