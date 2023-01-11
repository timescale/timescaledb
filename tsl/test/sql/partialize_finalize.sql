-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- TEST1 count with integers
SHOW enable_partitionwise_aggregate;
SET enable_partitionwise_aggregate = on;

create table foo (a integer, b integer, c integer);
insert into foo values( 1 , 10 , 20);
insert into foo values( 1 , 11 , 20);
insert into foo values( 1 , 12 , 20);
insert into foo values( 1 , 13 , 20);
insert into foo values( 1 , 14 , 20);
insert into foo values( 2 , 14 , 20);
insert into foo values( 2 , 15 , 20);
insert into foo values( 2 , 16 , 20);
insert into foo values( 3 , 16 , 20);


create or replace view v1(a , partial)
as
 SELECT a, _timescaledb_internal.partialize_agg( count(b)) from foo group by a;

create table t1 as select * from v1;

select a, _timescaledb_internal.finalize_agg( 'count("any")', null, null, null, partial, cast('1' as int8) ) from t1
group by a order by a ;

insert into t1 select * from t1;
select a, _timescaledb_internal.finalize_agg( 'count("any")', null, null, null, partial, cast('1' as int8) ) from t1
group by a order by a ;

--TEST2 sum numeric and min on float--
drop table t1;
drop view v1;
drop table foo;

create table foo (a integer, b numeric , c float);
insert into foo values( 1 , 10 , 20);
insert into foo values( 1 , 20 , 19);
insert into foo values( 1 , 30 , 11.0);
insert into foo values( 1 , 40 , 200);
insert into foo values( 1 , 50 , -10);
insert into foo values( 2 , 10 , 20);
insert into foo values( 2 , 20 , 20);
insert into foo values( 2 , 30 , 20);
insert into foo values( 3 , 40 , 0);


create or replace view v1(a , partialb, partialminc)
as
 SELECT a,  _timescaledb_internal.partialize_agg( sum(b)) , _timescaledb_internal.partialize_agg( min(c)) from foo group by a;

create table t1 as select * from v1;

select a, _timescaledb_internal.finalize_agg( 'sum(numeric)', null, null, null, partialb, cast('1' as numeric) ) sumb, _timescaledb_internal.finalize_agg( 'min(double precision)', null, null, null, partialminc, cast('1' as float8) ) minc from t1 group by a order by a ;

insert into foo values( 3, 0, -1);
insert into foo values( 5, 40, 10);
insert into foo values( 5, 40, 0);
--note that rows for 3 get added all over again + new row
--sum aggfnoid 2114, min aggfnoid is 2136 oid  numeric is 1700
insert into t1 select * from v1 where ( a = 3 ) or a = 5;
select a, _timescaledb_internal.finalize_agg( 'sum(numeric)', null, null, null, partialb, cast('1' as numeric) ) sumb, _timescaledb_internal.finalize_agg( 'min(double precision)', null, null, null, partialminc, cast('1' as float8) ) minc from t1 group by a order by a ;

SET enable_partitionwise_aggregate = off;

--TEST3 sum with expressions
drop table t1;
drop view v1;
drop table foo;

create table foo (a integer, b numeric , c float);
insert into foo values( 1 , 10 , 20);
insert into foo values( 1 , 20 , 19);
insert into foo values( 1 , 30 , 11.0);
insert into foo values( 1 , 40 , 200);
insert into foo values( 1 , 50 , -10);
insert into foo values( 2 , 10 , 20);
insert into foo values( 2 , 20 , 20);
insert into foo values( 2 , 30 , 20);
insert into foo values( 3 , 40 , 0);
insert into foo values(10, NULL, NULL);
insert into foo values(11, NULL, NULL);
insert into foo values(11, NULL, NULL);
insert into foo values(12, NULL, NULL);

create or replace view v1(a , b, partialb, partialminc)
as
 SELECT a, b, _timescaledb_internal.partialize_agg( sum(b+c)) , _timescaledb_internal.partialize_agg( min(c)) from foo group by a, b ;

create table t1 as select * from v1;

insert into foo values( 3, 0, -1);
insert into foo values( 5, 40, 10);
insert into foo values( 5, 40, 0);
insert into foo values(12, 10, 20);
insert into t1 select * from v1 where ( a = 3 and b = 0 ) or a = 5 or (a = 12 and b = 10) ;

--results should match query: select a, sum(b+c), min(c) from foo group by a order by a;
--sum aggfnoid 2111 for float8, min aggfnoid is 2136 oid  numeric is 1700
select a, _timescaledb_internal.finalize_agg( 'sum(double precision)', null, null, null, partialb, null::float8 ) sumcd, _timescaledb_internal.finalize_agg( 'min(double precision)', null, null, null, partialminc, cast('1' as float8) ) minc from t1 group by a order by a ;

insert into t1 select * from v1;
select a, _timescaledb_internal.finalize_agg( 'sum(double precision)', null, null, null, partialb, null::float8 ) sumcd, _timescaledb_internal.finalize_agg( 'min(double precision)', null, null, null, partialminc, cast('1' as float8) ) minc from t1 group by a order by a ;

-- TEST4 with collation (text), NULLS and timestamp --
drop table t1;
drop view v1;
drop table foo;

create table foo (a integer, b numeric , c text, d timestamptz, e bigint);
insert into foo values( 1 , 10 , 'hello', '2010-01-01 09:00:00-08', 10);
insert into foo values( 1 , 20 , 'abc', '2010-01-02 09:00:00-08', 20);
insert into foo values( 1 , 30 , 'abcd',  '2010-01-03 09:00:00-08', 30);
insert into foo values( 1 , 40 , 'abcde', NULL, 40);
insert into foo values( 1 , 50 , NULL,  '2010-01-01 09:00:00-08', 50);
--group with all values for c and d same
insert into foo values( 2 , 10 ,  'hello', '2010-01-01 09:00:00-08', 10);
insert into foo values( 2 , 20 , 'hello', '2010-01-01 09:00:00-08', 20);
insert into foo values( 2 , 30 , 'hello', '2010-01-01 09:00:00-08', 30);
--group with all values for c and d NULL
insert into foo values( 3 , 40 , NULL, NULL, 40);
insert into foo values( 3 , 50 , NULL, NULL, 50);
insert into foo values(11, NULL, NULL, NULL, NULL);
insert into foo values(11, NULL, 'hello', '2010-01-02 09:00:00-05', NULL);
--group with all values for c and d NULL and later add non-null.
insert into foo values(12, NULL, NULL, NULL, NULL);


create or replace view v1(a , b, partialb, partialc, partiald, partiale, partialf)
as
 SELECT a, b, _timescaledb_internal.partialize_agg(sum(b))
 , _timescaledb_internal.partialize_agg(min(c))
 , _timescaledb_internal.partialize_agg(max(d))
 , _timescaledb_internal.partialize_agg(stddev(b))
 , _timescaledb_internal.partialize_agg(stddev(e)) from foo group by a, b ;

create table t1 as select * from v1;

--sum 2114, collid 0, min(text) 2145, collid 100, max(ts) 2127
insert into foo values(12, 10, 'hello', '2010-01-02 09:00:00-05', 10);
insert into t1 select * from v1 where  (a = 12 and b = 10) ;

--select a, sum(b), min(c) , max(d), stddev(b), stddev(e) from foo group by a order by a;
--results should match above query
CREATE OR REPLACE VIEW vfinal(a , sumb, minc, maxd, stddevb, stddeve)
AS
select a, _timescaledb_internal.finalize_agg( 'sum(numeric)', null, null, null, partialb, null::numeric ) sumb
, _timescaledb_internal.finalize_agg( 'min(text)', 'pg_catalog', 'default', null, partialc, null::text ) minc
, _timescaledb_internal.finalize_agg( 'max(timestamp with time zone)', null, null, null, partiald, null::timestamptz ) maxd
, _timescaledb_internal.finalize_agg( 'stddev(numeric)', null, null, null, partiale, null::numeric ) stddevb
, _timescaledb_internal.finalize_agg( 'stddev(int8)', null, null, null, partialf, null::numeric ) stddeve
from t1 group by a order by a ;

SELECT * FROM vfinal;
CREATE TABLE vfinal_res AS SELECT * FROM vfinal;

-- overwrite partials with dumped binary values from PostrgeSQL 13 --
TRUNCATE TABLE t1;
\COPY t1 FROM data/partialize_finalize_data.csv WITH CSV HEADER

--repeat query to verify partial serialization sanitization works for versions PG >= 14
CREATE TABLE vfinal_dump_res AS SELECT * FROM vfinal;

-- compare results to verify there is no difference
(SELECT * FROM vfinal_res) EXCEPT (SELECT * FROM vfinal_dump_res);

--with having clause --
select a, b ,  _timescaledb_internal.finalize_agg( 'min(text)', 'pg_catalog', 'default', null, partialc, null::text ) minc, _timescaledb_internal.finalize_agg( 'max(timestamp with time zone)', null, null, null, partiald, null::timestamptz ) maxd from t1  where b is not null group by a, b having _timescaledb_internal.finalize_agg( 'max(timestamp with time zone)', null, null, null, partiald, null::timestamptz ) is not null order by a, b;

--TEST5 test with TOAST data

drop view vfinal;
drop table t1;
drop view v1;
drop table foo;
create table foo( a integer, b timestamptz, toastval TEXT);
-- Set storage type to EXTERNAL to prevent PostgreSQL from compressing my
-- easily compressable string and instead store it with TOAST
ALTER TABLE foo ALTER COLUMN toastval SET STORAGE EXTERNAL;
SELECT count(*) FROM create_hypertable('foo', 'b');

INSERT INTO foo VALUES( 1,  '2004-10-19 10:23:54', repeat('this must be over 2k. ', 1100));
INSERT INTO foo VALUES(1,  '2005-10-19 10:23:54', repeat('I am a tall big giraffe in the zoo.  ', 1100));
INSERT INTO foo values( 1, '2005-01-01 00:00:00+00', NULL);
INSERT INTO foo values( 2, '2005-01-01 00:00:00+00', NULL);

create or replace  view v1(a, partialb, partialtv) as select a, _timescaledb_internal.partialize_agg( max(b) ), _timescaledb_internal.partialize_agg( min(toastval)) from foo group by a;

EXPLAIN (VERBOSE, COSTS OFF)
create table t1 as select * from v1;

create table t1 as select * from v1;

insert into t1 select * from v1;
select a, _timescaledb_internal.finalize_agg( 'max(timestamp with time zone)', null, null, null, partialb, null::timestamptz ) maxb,
_timescaledb_internal.finalize_agg( 'min(text)', 'pg_catalog', 'default', null, partialtv, null::text ) = repeat('I am a tall big giraffe in the zoo.  ', 1100) mintv_equal
from t1 group by a order by a;

--non top-level partials
with cte as (
   select a, _timescaledb_internal.partialize_agg(min(toastval)) tp from foo group by a
)
select length(tp) from cte;

select length(_timescaledb_internal.partialize_agg( min(toastval))) from foo group by a;

select length(_timescaledb_internal.partialize_agg(min(a+1))) from foo;

\set ON_ERROR_STOP 0
select length(_timescaledb_internal.partialize_agg(1+min(a))) from foo;
select length(_timescaledb_internal.partialize_agg(min(a)+min(a))) from foo;
--non-trivial HAVING clause not allowed with partialize_agg
select time_bucket('1 hour', b) as b, _timescaledb_internal.partialize_agg(avg(a))
from foo
group by 1
having avg(a) > 3;
--mixing partialized and non-partialized aggs is not allowed
select time_bucket('1 hour', b) as b, _timescaledb_internal.partialize_agg(avg(a)), sum(a)
from foo
group by 1;
\set ON_ERROR_STOP 1

--partializing works with HAVING when the planner can effectively
--reduce it. In this case to a simple filter.
select time_bucket('1 hour', b) as b, toastval, _timescaledb_internal.partialize_agg(avg(a))
from foo
group by b, toastval
having toastval LIKE 'does not exist';

--
-- TEST FINALIZEFUNC_EXTRA
--

-- create special aggregate to test ffunc_extra
-- Raise warning with the actual type being passed in
CREATE OR REPLACE FUNCTION fake_ffunc(a int8, b int, x anyelement)
RETURNS anyelement AS $$
BEGIN
 RAISE WARNING 'type %', pg_typeof(x);
 RETURN x;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fake_sfunc(a int8, b int, x anyelement)
RETURNS int8 AS $$
BEGIN
 RETURN b;
END; $$
LANGUAGE plpgsql;


CREATE AGGREGATE aggregate_to_test_ffunc_extra(int, anyelement) (
    SFUNC = fake_sfunc,
    STYPE = int8,
    COMBINEFUNC = int8pl,
    FINALFUNC = fake_ffunc,
    PARALLEL = SAFE,
    FINALFUNC_EXTRA
);

select aggregate_to_test_ffunc_extra(8, 'name'::text);

\set ON_ERROR_STOP 0
--errors on wrong input type array
with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 'name'::text)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, null, part, null::text) from cte;

with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 'name'::text)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, array[array['a'::name, 'b'::name, 'c'::name]], part, null::text) from cte;

with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 'name'::text)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, array[array[]::name[]]::name[], part, null::text) from cte;

with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 'name'::text)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, array[]::name[], part, null::text) from cte;

with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 'name'::text)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, array[array['public'::name, 'int'::name], array['public', 'text']], part, null::text) from cte;

with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 'name'::text)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, array[array['public'::name, 'int4'::name], array['public', 'text']], part, null::text) from cte;

with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 'name'::text)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, array[array['pg_catalog'::name, 'int4'::name], array['pg_catalog', 'text'], array['pg_catalog', 'text']], part, null::text) from cte;

select _timescaledb_internal.finalize_agg(NULL::text,NULL::name,NULL::name,NULL::_name,NULL::bytea,a) over () from foo;
\set ON_ERROR_STOP 1

--make sure right type in warning and is null returns true
with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 'name'::text)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, array[array['pg_catalog'::name, 'int4'::name], array['pg_catalog', 'text']], part, null::text) is null from cte;

with cte as (SELECT  _timescaledb_internal.partialize_agg(aggregate_to_test_ffunc_extra(8, 1::bigint)) as part)
select _timescaledb_internal.finalize_agg( 'aggregate_to_test_ffunc_extra(int, anyelement)', null, null, array[array['pg_catalog'::name, 'int4'::name], array['pg_catalog', 'int8']], part, null::text) is null from cte;


-- Issue 4922
CREATE TABLE issue4922 (
  time  TIMESTAMPTZ NOT NULL,
  value INTEGER
);

SELECT create_hypertable('issue4922', 'time');

-- helper function: integer -> pseudorandom integer [0..100].
CREATE OR REPLACE FUNCTION mix(x INTEGER) RETURNS INTEGER AS $$ SELECT (((hashint4(x) / (pow(2, 31) - 1) + 1) / 2) * 100)::INTEGER $$ LANGUAGE SQL;

INSERT INTO issue4922 (time, value)
SELECT '2022-01-01 00:00:00-03'::timestamptz + interval '1 year' * mix(x), mix(x)
FROM generate_series(1, 100000) x(x);

SET force_parallel_mode = 'on';
SET parallel_setup_cost = 0;

SELECT
  sum(value),
  avg(value),
  min(value),
  max(value),
  count(*)
FROM issue4922;

-- The results should be the EQUAL TO the previous query
SELECT
  _timescaledb_internal.finalize_agg('pg_catalog.sum(integer)'::text, NULL::name, NULL::name, '{{pg_catalog,int4}}'::name[], partial_sum, NULL::bigint) AS sum,
  _timescaledb_internal.finalize_agg('pg_catalog.avg(integer)'::text, NULL::name, NULL::name, '{{pg_catalog,int4}}'::name[], partial_avg, NULL::numeric) AS avg,
  _timescaledb_internal.finalize_agg('pg_catalog.min(integer)'::text, NULL::name, NULL::name, '{{pg_catalog,int4}}'::name[], partial_min, NULL::integer) AS min,
  _timescaledb_internal.finalize_agg('pg_catalog.max(integer)'::text, NULL::name, NULL::name, '{{pg_catalog,int4}}'::name[], partial_max, NULL::integer) AS max,
  _timescaledb_internal.finalize_agg('pg_catalog.count()'::text, NULL::name, NULL::name, '{}'::name[], partial_count, NULL::bigint) AS count
FROM (
  SELECT
    _timescaledb_internal.partialize_agg(sum(value)) AS partial_sum,
    _timescaledb_internal.partialize_agg(avg(value)) AS partial_avg,
    _timescaledb_internal.partialize_agg(min(value)) AS partial_min,
    _timescaledb_internal.partialize_agg(max(value)) AS partial_max,
    _timescaledb_internal.partialize_agg(count(*)) AS partial_count
  FROM public.issue4922) AS a;

-- Check for parallel planning
EXPLAIN (COSTS OFF)
SELECT
  sum(value),
  avg(value),
  min(value),
  max(value),
  count(*)
FROM issue4922;

-- Make sure even forcing the parallel mode those functions are not safe for parallel
EXPLAIN (COSTS OFF)
SELECT
  _timescaledb_internal.finalize_agg('pg_catalog.sum(integer)'::text, NULL::name, NULL::name, '{{pg_catalog,int4}}'::name[], partial_sum, NULL::bigint) AS sum,
  _timescaledb_internal.finalize_agg('pg_catalog.avg(integer)'::text, NULL::name, NULL::name, '{{pg_catalog,int4}}'::name[], partial_avg, NULL::numeric) AS avg,
  _timescaledb_internal.finalize_agg('pg_catalog.min(integer)'::text, NULL::name, NULL::name, '{{pg_catalog,int4}}'::name[], partial_min, NULL::integer) AS min,
  _timescaledb_internal.finalize_agg('pg_catalog.max(integer)'::text, NULL::name, NULL::name, '{{pg_catalog,int4}}'::name[], partial_max, NULL::integer) AS max,
  _timescaledb_internal.finalize_agg('pg_catalog.count()'::text, NULL::name, NULL::name, '{}'::name[], partial_count, NULL::bigint) AS count
FROM (
  SELECT
    _timescaledb_internal.partialize_agg(sum(value)) AS partial_sum,
    _timescaledb_internal.partialize_agg(avg(value)) AS partial_avg,
    _timescaledb_internal.partialize_agg(min(value)) AS partial_min,
    _timescaledb_internal.partialize_agg(max(value)) AS partial_max,
    _timescaledb_internal.partialize_agg(count(*)) AS partial_count
  FROM public.issue4922) AS a;
