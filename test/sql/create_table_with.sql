-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

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

