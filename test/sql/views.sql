-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT * FROM timescaledb_information.hypertable;

-- create simple hypertable with 1 chunk
CREATE TABLE ht1(time TIMESTAMPTZ NOT NULL);
SELECT create_hypertable('ht1','time');
INSERT INTO ht1 SELECT '2000-01-01'::TIMESTAMPTZ;

-- create simple hypertable with 1 chunk and toasted data
CREATE TABLE ht2(time TIMESTAMPTZ NOT NULL, data TEXT);
SELECT create_hypertable('ht2','time');
INSERT INTO ht2 SELECT '2000-01-01'::TIMESTAMPTZ, repeat('8k',4096);

SELECT * FROM timescaledb_information.hypertable ORDER BY table_schema, table_name;

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

SELECT * FROM timescaledb_information.hypertable ORDER BY table_schema, table_name;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
SELECT * FROM timescaledb_information.hypertable ORDER BY table_schema,table_name;

-- filter by schema
SELECT * FROM timescaledb_information.hypertable WHERE table_schema = 'closed' ORDER BY table_schema, table_name;

-- filter by table name
SELECT * FROM timescaledb_information.hypertable WHERE table_name = 'ht1' ORDER BY table_schema, table_name;

-- filter by owner
SELECT * FROM timescaledb_information.hypertable WHERE table_owner = 'super_user' ORDER BY table_schema,table_name;
