-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/rand_generator.sql
\c :TEST_DBNAME :ROLE_SUPERUSER
\ir include/compression_utils.sql
CREATE TYPE customtype;

CREATE OR REPLACE FUNCTION customtype_in(cstring) RETURNS customtype
AS :TSL_MODULE_PATHNAME, 'ts_compression_custom_type_in'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION customtype_out(customtype) RETURNS cstring
AS :TSL_MODULE_PATHNAME, 'ts_compression_custom_type_out'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION customtype_eq(customtype, customtype) RETURNS BOOL
AS :TSL_MODULE_PATHNAME, 'ts_compression_custom_type_eq'
LANGUAGE C IMMUTABLE STRICT;

-- for testing purposes we need a fixed length pass-by-ref type, and one whose
-- alignment is greater than it's size. This type serves both purposes.
CREATE TYPE customtype (
  INPUT = customtype_in,
  OUTPUT = customtype_out,
  INTERNALLENGTH = 2,
  ALIGNMENT = double,
  STORAGE = plain
);

create operator = (
  leftarg = customtype,
  rightarg = customtype,
  procedure = customtype_eq,
  commutator = =
);

CREATE OPERATOR CLASS customtype_ops
  DEFAULT
  FOR TYPE customtype
  USING hash AS OPERATOR 1 =;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE test1 ("Time" timestamptz, i integer, b bigint, t text);
SELECT table_name from create_hypertable('test1', 'Time', chunk_time_interval=> INTERVAL '1 day');

INSERT INTO test1 SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') t;

ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = '"Time" DESC');

\set QUERY 'SELECT * FROM test1'
\set QUERY_ORDER 'ORDER BY "Time"'
\set HYPERTABLE_NAME 'test1'

\ir include/compression_test_hypertable.sql
\set TYPE timestamptz
\set ORDER_BY_COL_NAME Time
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

-- check stats
SELECT DISTINCT attname, attstattarget
  FROM pg_attribute
 WHERE attrelid = '_timescaledb_internal._compressed_hypertable_2'::REGCLASS
   AND attnum > 0
 ORDER BY attname;

SELECT DISTINCT attname, attstattarget
  FROM pg_attribute
  WHERE attrelid in (SELECT "Child" FROM test.show_subtables('_timescaledb_internal._compressed_hypertable_2'))
    AND attnum > 0
  ORDER BY attname;


-- Test that the GUC to disable bulk decompression works.
explain (analyze, verbose, timing off, costs off, summary off)
select * from _timescaledb_internal._hyper_1_10_chunk;

set timescaledb.enable_bulk_decompression to false;

explain (analyze, verbose, timing off, costs off, summary off)
select * from _timescaledb_internal._hyper_1_10_chunk;

reset timescaledb.enable_bulk_decompression;


TRUNCATE test1;
/* should be no data in table */
SELECT * FROM test1;
/* nor compressed table */
SELECT * FROM _timescaledb_internal._compressed_hypertable_2;
/* the compressed table should have not chunks */
EXPLAIN (costs off) SELECT * FROM _timescaledb_internal._compressed_hypertable_2;

--add test for altered hypertable
CREATE TABLE test2 ("Time" timestamptz, i integer, b bigint, t text);
SELECT table_name from create_hypertable('test2', 'Time', chunk_time_interval=> INTERVAL '1 day');

--create some chunks with the old column numbers
INSERT INTO test2 SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-04 1:00', '1 hour') t;

ALTER TABLE test2 DROP COLUMN b;
--add default a value
ALTER TABLE test2 ADD COLUMN c INT DEFAULT -15;
--add default NULL
ALTER TABLE test2 ADD COLUMN d INT;

--write to both old chunks and new chunks with different column #s
INSERT INTO test2 SELECT t, gen_rand_minstd(), gen_rand_minstd()::text, gen_rand_minstd(), gen_rand_minstd() FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-06 1:00', '1 hour') t;

ALTER TABLE test2 set (timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'c, "Time" DESC');

\set QUERY 'SELECT * FROM test2'
\set QUERY_ORDER 'ORDER BY c,"Time"'
\set HYPERTABLE_NAME 'test2'

\ir include/compression_test_hypertable.sql

\set TYPE int
\set ORDER_BY_COL_NAME c
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

\set TYPE timestamptz
\set ORDER_BY_COL_NAME Time
\set SEGMENT_META_COL_MIN _ts_meta_min_2
\set SEGMENT_META_COL_MAX _ts_meta_max_2
\ir include/compression_test_hypertable_segment_meta.sql

--TEST4 create segments with > 1000 rows.
CREATE TABLE test4 (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      location2   char(10)          NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );
--we want all the data to go into 1 chunk. so use 1 year chunk interval
select create_hypertable( 'test4', 'timec', chunk_time_interval=> '1 year'::interval);
alter table test4 set (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'timec');
insert into test4
select generate_series('2018-01-01 00:00'::timestamp, '2018-01-31 00:00'::timestamp, '1 day'), 'NYC', 'klick', 55, 75;
insert into test4
select generate_series('2018-02-01 00:00'::timestamp, '2018-02-14 00:00'::timestamp, '1 min'), 'POR', 'klick', 55, 75;
select hypertable_name, num_chunks
from timescaledb_information.hypertables
where hypertable_name like 'test4';

select location, count(*)
from test4
group by location ORDER BY location;

\set QUERY 'SELECT * FROM test4'
\set QUERY_ORDER 'ORDER BY timec'
\set HYPERTABLE_NAME 'test4'

\ir include/compression_test_hypertable.sql
\set TYPE TIMESTAMPTZ
\set ORDER_BY_COL_NAME timec
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

-- check stats with segmentby
SELECT DISTINCT attname, attstattarget
  FROM pg_attribute
 WHERE attrelid = '_timescaledb_internal._compressed_hypertable_6'::REGCLASS
   AND attnum > 0
 ORDER BY attname;

SELECT DISTINCT attname, attstattarget
  FROM pg_attribute
  WHERE attrelid in (SELECT "Child" FROM test.show_subtables('_timescaledb_internal._compressed_hypertable_6'))
    AND attnum > 0
  ORDER BY attname;

--add hypertable with order by a non by-val type with NULLs

CREATE TABLE test5 (
      time      TIMESTAMPTZ       NOT NULL,
      device_id   TEXT              NULL,
      temperature DOUBLE PRECISION  NULL
    );
--we want all the data to go into 1 chunk. so use 1 year chunk interval
select create_hypertable( 'test5', 'time', chunk_time_interval=> '1 day'::interval);
alter table test5 set (timescaledb.compress, timescaledb.compress_orderby = 'device_id, time');

insert into test5
select generate_series('2018-01-01 00:00'::timestamp, '2018-01-10 00:00'::timestamp, '2 hour'), 'device_1', gen_rand_minstd();
insert into test5
select generate_series('2018-01-01 00:00'::timestamp, '2018-01-10 00:00'::timestamp, '2 hour'), 'device_2', gen_rand_minstd();
insert into test5
select generate_series('2018-01-01 00:00'::timestamp, '2018-01-10 00:00'::timestamp, '2 hour'), NULL, gen_rand_minstd();

\set QUERY 'SELECT * FROM test5'
\set QUERY_ORDER 'ORDER BY device_id, time'
\set HYPERTABLE_NAME 'test5'

\ir include/compression_test_hypertable.sql
\set TYPE TEXT
\set ORDER_BY_COL_NAME device_id
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

TRUNCATE test5;

SELECT * FROM test5;

-- test 6, test with custom type, and NULLs in the segmentby
CREATE table test6(
  time INT NOT NULL,
  device_id INT,
  data customtype
);

SELECT create_hypertable('test6', 'time', chunk_time_interval=> 50);
ALTER TABLE test6 SET
  (timescaledb.compress, timescaledb.compress_segmentby='device_id', timescaledb.compress_orderby = 'time DESC');

INSERT INTO test6 SELECT t, d, customtype_in((t + d)::TEXT::cstring)
  FROM generate_series(1, 200) t, generate_series(1, 3) d;
INSERT INTO test6 SELECT t, NULL, customtype_in(t::TEXT::cstring)
  FROM generate_series(1, 200) t;

\set QUERY 'SELECT * FROM test6'
\set QUERY_ORDER 'ORDER BY device_id, time'
\set HYPERTABLE_NAME 'test6'

\ir include/compression_test_hypertable.sql
\set TYPE INT
\set ORDER_BY_COL_NAME time
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

DROP TABLE test6;

-- test 7, compress misc types, and NULLs in dictionaries
CREATE TABLE test7(time INT, c1 DATE, c2 TIMESTAMP, c3 FLOAT, n TEXT);
SELECT create_hypertable('test7', 'time', chunk_time_interval=> 200);
ALTER TABLE test7 SET
  (timescaledb.compress, timescaledb.compress_orderby = 'time DESC, c1 DESC');

INSERT INTO test7
  SELECT t, d, '2019/07/07 01:00', gen_rand_minstd(), 'a'
  FROM generate_series(1, 10) t,
       generate_series('2019/02/01'::DATE, '2019/02/10', '1d') d;
INSERT INTO test7
  SELECT t, d, '2019/07/07 01:30', gen_rand_minstd(), NULL
  FROM generate_series(10, 20) t,
       generate_series('2019/03/01'::DATE, '2019/03/10', '1d') d;

\set QUERY 'SELECT * FROM test7'
\set QUERY_ORDER 'ORDER BY time, c1'
\set HYPERTABLE_NAME 'test7'

\ir include/compression_test_hypertable.sql
\set TYPE INT
\set ORDER_BY_COL_NAME time
\set SEGMENT_META_COL_MIN _ts_meta_min_1
\set SEGMENT_META_COL_MAX _ts_meta_max_1
\ir include/compression_test_hypertable_segment_meta.sql

DROP TABLE test7;

-- helper function: float -> pseudorandom float [0..1].
create or replace function mix(x float4) returns float4 as $$ select ((hashfloat4(x) / (pow(2., 31) - 1) + 1) / 2)::float4 $$ language sql;

-- test gorilla for float4 and deltadelta for int32 and other types
create table test8(ts date, id int, value float4, valueint2 int2, valuebool bool);

select create_hypertable('test8', 'ts');

alter table test8 set (timescaledb.compress,
    timescaledb.compress_segmentby = 'id',
    timescaledb.compress_orderby = 'ts desc')
;

insert into test8
    select '2022-02-02 02:02:02+03'::timestamptz + interval '1 month' * mix(x),
        mix(x + 1.) * 20,
        mix(x + 2.) * 50,
        mix(x + 3.) * 100,
        mix(x + 4.) > 0.1
    from generate_series(1, 10000) x(x)
;

select compress_chunk(x) from show_chunks('test8') x;

select distinct on (id) * from test8
order by id, ts desc, value
;

drop table test8;