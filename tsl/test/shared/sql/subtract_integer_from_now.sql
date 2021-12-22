-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test on normal table
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.subtract_integer_from_now('pg_class', 1);
\set ON_ERROR_STOP 1

-- test on hypertable with non-int time dimension
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.subtract_integer_from_now('metrics', 1);
\set ON_ERROR_STOP 1

SELECT
  format('%I.%I', ht.schema_name, ht.table_name) AS "TABLENAME"
FROM
  _timescaledb_catalog.hypertable ht
  INNER JOIN _timescaledb_catalog.hypertable uncompress ON (ht.id = uncompress.compressed_hypertable_id
      AND uncompress.table_name = 'metrics_compressed') \gset

-- test on hypertable without dimensions
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.subtract_integer_from_now(:'TABLENAME', 1);
\set ON_ERROR_STOP 1

-- test on hypertable without now function
CREATE TABLE subtract_int_no_func(time int NOT NULL);
SELECT table_name FROM create_hypertable('subtract_int_no_func','time',chunk_time_interval:=10);
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.subtract_integer_from_now('subtract_int_no_func', 1);
\set ON_ERROR_STOP 1

CREATE OR REPLACE FUNCTION sub_int2_now() RETURNS int2 AS $$ SELECT 2::int2; $$ LANGUAGE SQL IMMUTABLE;
CREATE OR REPLACE FUNCTION sub_int4_now() RETURNS int4 AS $$ SELECT 4::int4; $$ LANGUAGE SQL IMMUTABLE;
CREATE OR REPLACE FUNCTION sub_int8_now() RETURNS int8 AS $$ SELECT 8::int8; $$ LANGUAGE SQL IMMUTABLE;

CREATE TABLE subtract_int2(time int2 NOT NULL);
CREATE TABLE subtract_int4(time int4 NOT NULL);
CREATE TABLE subtract_int8(time int8 NOT NULL);

SELECT table_name FROM create_hypertable('subtract_int2', 'time', chunk_time_interval:=10);
SELECT table_name FROM create_hypertable('subtract_int4', 'time', chunk_time_interval:=10);
SELECT table_name FROM create_hypertable('subtract_int8', 'time', chunk_time_interval:=10);

SELECT set_integer_now_func('subtract_int2', 'sub_int2_now');
SELECT set_integer_now_func('subtract_int4', 'sub_int4_now');
SELECT set_integer_now_func('subtract_int8', 'sub_int8_now');

SELECT _timescaledb_internal.subtract_integer_from_now('subtract_int2', lag) AS sub FROM (VALUES (-10),(0),(2),(4),(8)) v(lag);
SELECT _timescaledb_internal.subtract_integer_from_now('subtract_int4', lag) AS sub FROM (VALUES (-10),(0),(2),(4),(8)) v(lag);
SELECT _timescaledb_internal.subtract_integer_from_now('subtract_int8', lag) AS sub FROM (VALUES (-10),(0),(2),(4),(8)) v(lag);

-- test set_integer_now_func on internal table
\set ON_ERROR_STOP 0
SELECT set_integer_now_func(:'TABLENAME', 'sub_int2_now');
\set ON_ERROR_STOP 1

-- cleanup
DROP TABLE subtract_int_no_func;
DROP TABLE subtract_int2;
DROP TABLE subtract_int4;
DROP TABLE subtract_int8;
