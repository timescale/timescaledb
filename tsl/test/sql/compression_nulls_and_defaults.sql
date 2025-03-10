-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- test case #1: altering a compressed hypertable by adding a new column
-- with a default value
--
-- The point of this test is to verify the behaviour of the default value if
-- the hypertable is compressed before the new column is added.
-- It adds rows before and after the column is added to make sure that the
-- default value is returned correctly in both cases. This is to make sure
-- that changing the code related to the default value does not break the
-- behaviour of the default values.
--
set timescaledb.enable_segmentwise_recompression to off;
drop table if exists t;
create table t(ts int, c1 int);
select create_hypertable('t','ts');
alter table t set (timescaledb.compress, timescaledb.compress_segmentby = 'ts');
insert into t (ts,c1) values (6,6);
select compress_chunk(show_chunks('t'));
alter table t add column a double precision default 7.1;
select * from t;
select compress_chunk(show_chunks('t'));
select * from t;
insert into t (ts,c1) values (7,7);
select compress_chunk(show_chunks('t'));
select * from t;
set timescaledb.enable_segmentwise_recompression to on;


-- test case #2: altering an uncompressed hypertable by adding a new column
-- with a default value
--
-- This is another test case to check that the correct behaviour is preserved
-- after changing the code related to the default value.
--
drop table if exists t;
create table t(ts int, c1 int);
select create_hypertable('t','ts');
alter table t set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
alter table t add column a double precision default 7.1;
insert into t (ts,c1) values (6,6);
select compress_chunk(show_chunks('t'));
update t set a = null;
select compress_chunk(show_chunks('t'));
select * from t;
insert into t (ts,c1) values (7,7);
update t set a = null;
select compress_chunk(show_chunks('t'));
select * from t;


-- test case #3: altering an uncompressed hypertable by adding a new column
-- with a default value
--
-- This is one more testcase for establishing the correct behaviour of the
-- default value after changing the code related to the default value.
--
set timescaledb.enable_segmentwise_recompression to off;
drop table if exists t;
create table t(ts int, c1 int);
select create_hypertable('t','ts');
alter table t set (timescaledb.compress, timescaledb.compress_segmentby = 'ts');
alter table t add column a double precision default 7.1;
insert into t (ts,c1) values (4,4);
insert into t (ts,c1) values (5,5);
insert into t (ts,c1) values (6,6);
select compress_chunk(show_chunks('t'));
update t set a = null;
select * from t;
select compress_chunk(show_chunks('t'));
select * from t;
set timescaledb.enable_segmentwise_recompression to on;


-- test case #4: altering a compressed hypertable by adding a new column
-- with a default value
--
-- This is one more testcase for establishing the correct behaviour of the
-- default value after changing the code.
--
drop table if exists t;
create table t(ts int, c1 int);
select create_hypertable('t','ts');
alter table t set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
alter table t add column a double precision default 7.1;
insert into t (ts,c1) values (6,6);
select compress_chunk(show_chunks('t'));
select * from t;
update t set a = null;
insert into t (ts,c1) values (7,7);
update t set a = null;
select compress_chunk(show_chunks('t'));
select * from t;


-- test case #5: altering an uncompressed hypertable by adding a new column
-- with a default value
--
-- This is the first testcase to reproduce the problem found by Sven.
-- Before the fix, the default value was not returned correctly after
-- compressing the hypertable.
--
set timescaledb.enable_segmentwise_recompression to off;
drop table if exists t;
create table t (ts int);
select create_hypertable('t', 'ts');
alter table t set(timescaledb.compress, timescaledb.compress_segmentby = 'ts');
insert into t (ts) values (1);
alter table t add column c1 double precision default 42.99;
update t set c1 = null;
select * from t;
select compress_chunk(show_chunks('t'));
select * from t;
set timescaledb.enable_segmentwise_recompression to on;

-- test case #6: altering a compressed hypertable by adding a new column
-- with a default value
--
-- This is the second testcase to reproduce the problem, by Alex.
-- Before the fix, the default value was not returned correctly after
-- compressing the hypertable the second time.
--
drop table if exists t;
create table t(ts int);
select create_hypertable('t', 'ts');
insert into t values (1);
alter table t set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
select compress_chunk(show_chunks('t'));
alter table t add column a double precision default 7.987;
insert into t values (2, null);
select * from t;
select compress_chunk(show_chunks('t'));
select * from t;


-- test case #7: a variation of #1 where
-- the default value is changed after the column is added
--
-- This is a variation of the first test case where the default value is
-- changed after the column is added. This is to make sure that changing the
-- default value does not break the behaviour of the default values.
--
set timescaledb.enable_segmentwise_recompression to off;
drop table if exists t;
create table t(ts int, c1 int);
select create_hypertable('t','ts');
alter table t set (timescaledb.compress, timescaledb.compress_segmentby = 'ts');
insert into t (ts,c1) values (6,6);
select compress_chunk(show_chunks('t'));
alter table t add column a double precision default 7.1;
select * from t;
select compress_chunk(show_chunks('t'));
select * from t;
alter table t alter column a set default 7.2;
insert into t (ts,c1) values (7,7);
select compress_chunk(show_chunks('t'));
select * from t;
set timescaledb.enable_segmentwise_recompression to on;


-- test case #8: a variation of #5 and #7 where
-- I change the default value multiple times
--
-- This is a variation of the previous test cases with changing the default
-- values. Before the fix the first default value was returned for all rows
-- even if it was changed twice afterwards and the value was set to null.
--
drop table if exists t;
create table t(ts int, c1 int);
select create_hypertable('t','ts');
alter table t set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
insert into t (ts,c1) values (6,6);
select compress_chunk(show_chunks('t'));
alter table t add column a double precision default 7.1;
alter table t alter column a set default 8.2;
insert into t (ts,c1) values (7,7);
select * from t;
select compress_chunk(show_chunks('t'));
select * from t;
alter table t alter column a set default 9.3;
insert into t (ts,c1) values (8,8);
select * from t;
update t set a = null;
select * from t;
select compress_chunk(show_chunks('t'));
select * from t;


-- test case #9: a variation of the previous ones with the twist
-- of updating another column which triggers decompression and
-- risk of re-applying the default value for the other column.
--
drop table if exists t;
create table t(ts int, c1 int);
select create_hypertable('t','ts');
alter table t set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
insert into t (ts,c1) values (6,6);
select compress_chunk(show_chunks('t'));
alter table t add column a double precision default 7.1;
select compress_chunk(show_chunks('t'));
select * from t;
update t set a = null;
select compress_chunk(show_chunks('t'));
select * from t;
update t set c1 = 99, a = null;
select * from t;
select compress_chunk(show_chunks('t'));
select * from t;
update t set c1 = 98;
select * from t;


-- test case #10: adding a few columns to a compressed hypertable
-- and then updating them to null and dropping them
--
drop table if exists t;
create table t(ts int, c1 int);
select create_hypertable('t','ts');
alter table t set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
insert into t (ts,c1) values (1,1);
select compress_chunk(show_chunks('t'));

alter table t add column a double precision default 3.3;
insert into t (ts,c1) values (2,2);
select compress_chunk(show_chunks('t'));
select * from t;

alter table t add column b double precision default 4.4;
insert into t (ts,c1) values (3,3);
select compress_chunk(show_chunks('t'));
select * from t;

update t set a = null;
select compress_chunk(show_chunks('t'));
select * from t;

alter table t add column c double precision;
insert into t (ts,c1) values (4,4);
select compress_chunk(show_chunks('t'));
select * from t;

update t set b = null;
select compress_chunk(show_chunks('t'));
select * from t;

alter table t drop column a;
update t set b = null;
select compress_chunk(show_chunks('t'));
select * from t;

alter table t drop column b;
update t set c = null;
select compress_chunk(show_chunks('t'));
select * from t;

-- this is to make codecove happy so we exercise some
-- code paths that are hard to do through the unit tests
drop table if exists codecov;
create table codecov(ts int, c1 int);
select create_hypertable('codecov','ts');
alter table codecov set (timescaledb.compress, timescaledb.compress_orderby = 'ts');
insert into codecov (ts,c1) values (1,NULL);
select compress_chunk(show_chunks('codecov'));

DO $$
DECLARE
	comp_regclass REGCLASS;
	rec RECORD;
BEGIN
	FOR comp_regclass IN
		SELECT
			format('%I.%I', comp.schema_name, comp.table_name)::regclass as comp_regclass
		FROM
			_timescaledb_catalog.chunk uncomp,
			_timescaledb_catalog.chunk comp,
			(SELECT show_chunks('codecov') as c) as x
		WHERE
			uncomp.dropped IS FALSE AND uncomp.compressed_chunk_id IS NOT NULL AND
			comp.id = uncomp.compressed_chunk_id AND
			x.c = format('%I.%I', uncomp.schema_name, uncomp.table_name)::regclass
	LOOP
		-- codecov to record coverage of 'tsl_compressed_data_info'
		FOR rec IN
			EXECUTE format('SELECT c1, _timescaledb_functions.compressed_data_info(c1) FROM %s', comp_regclass)
		LOOP
			RAISE NOTICE 'Compressed info results: %', rec;
		END LOOP;

		-- codecov to record coverage of 'tsl_compressed_data_has_nulls'
		FOR rec IN
			EXECUTE format('SELECT c1, _timescaledb_functions.compressed_data_has_nulls(c1) FROM %s', comp_regclass)
		LOOP
			RAISE NOTICE 'Has nulls results: %', rec;
		END LOOP;

	END LOOP;
END;
$$;
