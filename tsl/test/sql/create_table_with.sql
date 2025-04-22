-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- our user needs permission to create schema for the schema tests
\c :TEST_DBNAME :ROLE_SUPERUSER
GRANT CREATE ON DATABASE :TEST_DBNAME TO :ROLE_DEFAULT_PERM_USER;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- create table with non-tsdb option should not be affected
CREATE TABLE t1(time timestamptz, device text, value float) WITH (autovacuum_enabled);
DROP TABLE t1;

-- test error cases
\set ON_ERROR_STOP 0
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.hypertable);
CREATE TABLE t2(time timestamptz, device text, value float) WITH (timescaledb.hypertable);
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.hypertable,tsdb.time_column=NULL);
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='');
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='foo');
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.time_column='time');
CREATE TABLE t2(time timestamptz, device text, value float) WITH (timescaledb.time_column='time');
CREATE TABLE t2(time timestamptz , device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.chunk_time_interval='foo');
CREATE TABLE t2(time int2 NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.chunk_time_interval='3 months');
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.create_default_indexes='time');
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.create_default_indexes=2);
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.create_default_indexes=-1);
\set ON_ERROR_STOP 1


BEGIN;
CREATE TABLE t3(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
CREATE TABLE t4(time timestamp, device text, value float) WITH (tsdb.hypertable,timescaledb.time_column='time');
CREATE TABLE t5(time date, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',autovacuum_enabled);
CREATE TABLE t6(time timestamptz NOT NULL, device text, value float) WITH (timescaledb.hypertable,tsdb.time_column='time');

SELECT hypertable_name FROM timescaledb_information.hypertables ORDER BY 1;
ROLLBACK;

-- IF NOT EXISTS
BEGIN;
CREATE TABLE IF NOT EXISTS t7(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
CREATE TABLE IF NOT EXISTS t7(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
CREATE TABLE IF NOT EXISTS t7(time timestamptz NOT NULL, device text, value float);

SELECT hypertable_name FROM timescaledb_information.hypertables ORDER BY 1;
ROLLBACK;

-- table won't be converted to hypertable unless it is in the initial CREATE TABLE
BEGIN;
CREATE TABLE IF NOT EXISTS t8(time timestamptz NOT NULL, device text, value float);
CREATE TABLE IF NOT EXISTS t8(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
CREATE TABLE IF NOT EXISTS t8(time timestamptz NOT NULL, device text, value float);

SELECT hypertable_name FROM timescaledb_information.hypertables ORDER BY 1;
ROLLBACK;

-- chunk_time_interval
BEGIN;
CREATE TABLE IF NOT EXISTS t9(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.chunk_time_interval='8weeks');
SELECT hypertable_name, column_name, column_type, time_interval FROM timescaledb_information.dimensions;
ROLLBACK;

BEGIN;
CREATE TABLE IF NOT EXISTS t9(time timestamp NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.chunk_time_interval='23 days');
SELECT hypertable_name, column_name, column_type, time_interval FROM timescaledb_information.dimensions;
ROLLBACK;

BEGIN;
CREATE TABLE IF NOT EXISTS t9(time date NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.chunk_time_interval='3 months');
SELECT hypertable_name, column_name, column_type, time_interval FROM timescaledb_information.dimensions;
ROLLBACK;

BEGIN;
CREATE TABLE IF NOT EXISTS t9(time int2 NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.chunk_time_interval=12);
SELECT hypertable_name, column_name, column_type, integer_interval FROM timescaledb_information.dimensions;
ROLLBACK;

BEGIN;
CREATE TABLE IF NOT EXISTS t9(time int4 NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.chunk_time_interval=3453);
SELECT hypertable_name, column_name, column_type, integer_interval FROM timescaledb_information.dimensions;
ROLLBACK;

BEGIN;
CREATE TABLE IF NOT EXISTS t9(time int8 NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.chunk_time_interval=32768);
SELECT hypertable_name, column_name, column_type, integer_interval FROM timescaledb_information.dimensions;
ROLLBACK;

-- create_default_indexes
BEGIN;
CREATE TABLE t10(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
SELECT indexrelid::regclass from pg_index where indrelid='t10'::regclass ORDER BY indexrelid::regclass::text;
ROLLBACK;

BEGIN;
CREATE TABLE t10(time timestamptz NOT NULL PRIMARY KEY, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
SELECT indexrelid::regclass from pg_index where indrelid='t10'::regclass ORDER BY indexrelid::regclass::text;
ROLLBACK;

BEGIN;
CREATE TABLE t10(time timestamptz NOT NULL UNIQUE, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
SELECT indexrelid::regclass from pg_index where indrelid='t10'::regclass ORDER BY indexrelid::regclass::text;
ROLLBACK;

BEGIN;
CREATE TABLE t10(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.create_default_indexes=true);
SELECT indexrelid::regclass from pg_index where indrelid='t10'::regclass ORDER BY indexrelid::regclass::text;
ROLLBACK;

BEGIN;
CREATE TABLE t10(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',tsdb.create_default_indexes=false);
SELECT indexrelid::regclass from pg_index where indrelid='t10'::regclass ORDER BY indexrelid::regclass::text;
ROLLBACK;

-- associated_schema
BEGIN;
CREATE TABLE t11(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
SELECT associated_schema_name FROM _timescaledb_catalog.hypertable WHERE table_name = 't11';
ROLLBACK;

BEGIN;
CREATE TABLE t11(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time', tsdb.associated_schema='abc');
SELECT associated_schema_name FROM _timescaledb_catalog.hypertable WHERE table_name = 't11';
INSERT INTO t11 SELECT '2025-01-01', 'd1', 0.1;
SELECT relname from pg_class where relnamespace = 'abc'::regnamespace ORDER BY 1;
ROLLBACK;

BEGIN;
CREATE SCHEMA abc2;
CREATE TABLE t11(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time', tsdb.associated_schema='abc2');
SELECT associated_schema_name FROM _timescaledb_catalog.hypertable WHERE table_name = 't11';
INSERT INTO t11 SELECT '2025-01-01', 'd1', 0.1;
SELECT relname from pg_class where relnamespace = 'abc2'::regnamespace ORDER BY 1;
ROLLBACK;

-- associated_table_prefix
BEGIN;
CREATE TABLE t12(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
SELECT associated_table_prefix FROM _timescaledb_catalog.hypertable WHERE table_name = 't12';
ROLLBACK;

BEGIN;
CREATE TABLE t12(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time', tsdb.associated_schema='abc', tsdb.associated_table_prefix='tbl_prefix');
SELECT associated_table_prefix FROM _timescaledb_catalog.hypertable WHERE table_name = 't12';
INSERT INTO t12 SELECT '2025-01-01', 'd1', 0.1;
SELECT relname from pg_class where relnamespace = 'abc'::regnamespace ORDER BY 1;
ROLLBACK;

-- verify compression
BEGIN;
CREATE TABLE t13(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
SELECT hypertable_name, compression_enabled FROM timescaledb_information.hypertables;

INSERT INTO t13 SELECT '2025-01-01','d1',0.1;
SELECT compress_chunk(show_chunks('t13'));
ROLLBACK;

BEGIN;
CREATE TABLE t13(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
SELECT hypertable_name, compression_enabled FROM timescaledb_information.hypertables;
ALTER TABLE t13 SET (tsdb.compress_segmentby='device',tsdb.compress_orderby='time DESC');

INSERT INTO t13 SELECT '2025-01-01','d1',0.1;
SELECT compress_chunk(show_chunks('t13'));
ROLLBACK;

