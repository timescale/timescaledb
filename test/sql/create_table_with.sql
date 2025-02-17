-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create table with non-tsdb option should not be affected
CREATE TABLE t1(time timestamptz, device text, value float) WITH (autovacuum_enabled);

-- test error cases
\set ON_ERROR_STOP 0
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.hypertable);
CREATE TABLE t2(time timestamptz, device text, value float) WITH (timescaledb.hypertable);
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.hypertable,tsdb.time_column=NULL);
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='');
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='foo');
CREATE TABLE t2(time timestamptz, device text, value float) WITH (tsdb.time_column='time');
CREATE TABLE t2(time timestamptz, device text, value float) WITH (timescaledb.time_column='time');
\set ON_ERROR_STOP 1


CREATE TABLE t3(time timestamptz NOT NULL, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time');
CREATE TABLE t4(time timestamp, device text, value float) WITH (tsdb.hypertable,timescaledb.time_column='time');
CREATE TABLE t5(time date, device text, value float) WITH (tsdb.hypertable,tsdb.time_column='time',autovacuum_enabled);
CREATE TABLE t6(time timestamptz NOT NULL, device text, value float) WITH (timescaledb.hypertable,tsdb.time_column='time');

SELECT hypertable_name FROM timescaledb_information.hypertables ORDER BY 1;

