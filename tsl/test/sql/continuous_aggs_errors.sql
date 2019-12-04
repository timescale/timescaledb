-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0

--negative tests for query validation
create table mat_t1( a integer, b integer,c TEXT);

CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature integer  NULL,
      humidity    DOUBLE PRECISION  NULL,
	  timemeasure TIMESTAMPTZ,
      timeinterval INTERVAL
    );
select table_name from create_hypertable( 'conditions', 'timec');

create  view mat_m1 WITH ( timescaledb.continuous, timescaledb.myfill = 1)
as
select location , min(temperature)
from conditions
group by time_bucket('1d', timec), location;

--valid PG option
create  view mat_m1 WITH ( timescaledb.continuous, check_option = LOCAL )
as
select * from conditions , matt1;

-- join multiple tables
create  view mat_m1 WITH ( timescaledb.continuous)
as
select location, count(*) from conditions , mat_t1
where conditions.location = mat_t1.c
group by location;

-- join multiple tables WITH explicit JOIN
create  view mat_m1 WITH ( timescaledb.continuous)
as
select location, count(*) from conditions JOIN mat_t1 ON true
where conditions.location = mat_t1.c
group by location;

-- LATERAL multiple tables
create  view mat_m1 WITH ( timescaledb.continuous)
as
select location, count(*) from conditions,
LATERAL (Select * from mat_t1 where c = conditions.location) q
group by location;


--non-hypertable
create  view mat_m1 WITH ( timescaledb.continuous)
as
select a, count(*) from mat_t1
group by a;


-- no group by
create  view mat_m1 WITH ( timescaledb.continuous)
as
select count(*) from conditions ;

-- no time_bucket in group by
create  view mat_m1 WITH ( timescaledb.continuous)
as
select count(*) from conditions group by location;

-- with valid query in a CTE
create  view mat_m1 WITH ( timescaledb.continuous)
AS
with m1 as (
Select location, count(*) from conditions
 group by time_bucket('1week', timec) , location)
select * from m1;

--with DISTINCT ON
create  view mat_m1 WITH ( timescaledb.continuous)
as
 select distinct on ( location ) count(*)  from conditions group by location, time_bucket('1week', timec) ;

--aggregate with DISTINCT
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select time_bucket('1week', timec),
 count(location) , sum(distinct temperature) from conditions
 group by time_bucket('1week', timec) , location;

--aggregate with FILTER
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select time_bucket('1week', timec),
 sum(temperature) filter ( where humidity > 20 ) from conditions
 group by time_bucket('1week', timec) , location;

-- aggregate with filter in having clause
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select time_bucket('1week', timec), max(temperature)
from conditions
 group by time_bucket('1week', timec) , location
 having sum(temperature) filter ( where humidity > 20 ) > 50;

-- time_bucket on non partitioning column of hypertable
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timemeasure) , location;

--time_bucket on expression
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timec+ '10 minutes'::interval) , location;

--multiple time_bucket functions
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timec) , time_bucket('1month', timec), location;

--time_bucket using additional args
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket( INTERVAL '5 minutes', timec, INTERVAL '-2.5 minutes') , location;

--time_bucket using non-const for first argument
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select max(temperature)
from conditions
 group by time_bucket( timeinterval, timec) , location;

-- ordered set aggr
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select mode() within group( order by humidity)
from conditions
 group by time_bucket('1week', timec) ;

--window function
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select avg(temperature) over( order by humidity)
from conditions
;

--aggregate without combine function
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select json_agg(location)
from conditions
 group by time_bucket('1week', timec) , location;
;

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature), array_agg(location)
from conditions
 group by time_bucket('1week', timec) , location;
;

-- userdefined aggregate without combine function
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), newavg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location;
;

-- using subqueries
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from
( select humidity, temperature, location, timec
from conditions ) q
 group by time_bucket('1week', timec) , location ;

create  view mat_m1 WITH ( timescaledb.continuous)
AS
select * from
( Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location )  q;

--using limit /limit offset
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
limit 10 ;

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
offset 10;

--using ORDER BY in view defintion
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
ORDER BY 1;

--using FETCH
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
fetch first 10 rows only;

--using locking clauses FOR clause
--all should be disabled. we cannot guarntee locks on the hypertable
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR KEY SHARE;

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR SHARE;


create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR UPDATE;

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR NO KEY UPDATE;

--tablesample clause
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions tablesample bernoulli(0.2)
 group by time_bucket('1week', timec) , location
;

-- ONLY in from clause
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from ONLY conditions
 group by time_bucket('1week', timec) , location ;

--grouping sets and variants
create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by grouping sets(time_bucket('1week', timec) , location ) ;

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
group by rollup(time_bucket('1week', timec) , location ) ;

--NO immutable functions -- check all clauses
CREATE FUNCTION test_stablefunc(int) RETURNS int LANGUAGE 'sql'
       STABLE AS 'SELECT $1 + 10';

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), max(timec + INTERVAL '1h')
from conditions
group by time_bucket('1week', timec) , location  ;

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum(humidity), min(location)
from conditions
group by time_bucket('1week', timec)
having  max(timec + INTERVAL '1h') > '2010-01-01 09:00:00-08';

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum( test_stablefunc(humidity::int) ), min(location)
from conditions
group by time_bucket('1week', timec);

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum( temperature ), min(location)
from conditions
group by time_bucket('1week', timec), test_stablefunc(humidity::int);

-- row security on table
create table rowsec_tab( a bigint, b integer, c integer);
select table_name from create_hypertable( 'rowsec_tab', 'a', chunk_time_interval=>10);
CREATE OR REPLACE FUNCTION integer_now_test() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(a), 0)::bigint FROM rowsec_tab $$;
SELECT set_integer_now_func('rowsec_tab', 'integer_now_test');
alter table rowsec_tab ENABLE ROW LEVEL SECURITY;
create policy rowsec_tab_allview ON rowsec_tab FOR SELECT USING(true);

create  view mat_m1 WITH ( timescaledb.continuous)
AS
Select sum( b), min(c)
from rowsec_tab
group by time_bucket('1', a);

drop table conditions cascade;

--negative tests for WITH options
CREATE TABLE conditions (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec');

\set ON_ERROR_STOP 0
create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '5 joules', timescaledb.refresh_interval = '1h')
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec);

create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '5h', timescaledb.refresh_interval = '1 joule')
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec);
\set ON_ERROR_STOP 1

create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '5 hours', timescaledb.refresh_interval = '1h')
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec);

create or replace view mat_with_test_no_inval( timec, minl, sumt , sumh)
        WITH ( timescaledb.continuous, timescaledb.refresh_lag = '5 hours', timescaledb.refresh_interval = '1h',
               timescaledb.ignore_invalidation_older_than='0')
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec);

SELECT  h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_with_test'
\gset

\set ON_ERROR_STOP 0
ALTER VIEW mat_with_test SET(timescaledb.create_group_indexes = 'false');
ALTER VIEW mat_with_test SET(timescaledb.create_group_indexes = 'true');
ALTER VIEW mat_with_test SET(timescaledb.refresh_lag = '1 joule');
ALTER VIEW mat_with_test RESET(timescaledb.refresh_lag);
ALTER VIEW mat_with_test ALTER timec DROP default;
ALTER VIEW :"PART_VIEW_SCHEMA".:"PART_VIEW_NAME" SET(timescaledb.refresh_lag = '1 hour');
\set ON_ERROR_STOP 1

DROP TABLE conditions CASCADE;


--test WITH using a hypertable with an integer time dimension
CREATE TABLE conditions (
      timec       SMALLINT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec', chunk_time_interval=> 100);
CREATE OR REPLACE FUNCTION integer_now_test_s() returns smallint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0)::smallint FROM conditions $$;
SELECT set_integer_now_func('conditions', 'integer_now_test_s');

\set ON_ERROR_STOP 0
create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '1 hour', timescaledb.refresh_interval = '1h')
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec);

create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '32768', timescaledb.refresh_interval = '1h')
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec);

ALTER TABLE conditions ALTER timec type int;

create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '2147483648', timescaledb.refresh_interval = '1h')
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec);

-- max_interval_per_job must be at least time_bucket
create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.max_interval_per_job='10')
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec);

--ignore_invalidation_older_than must be positive
create or replace view mat_with_test( timec, minl, sumt , sumh)
        WITH ( timescaledb.continuous, timescaledb.ignore_invalidation_older_than='-10')
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec);

create or replace view mat_with_test( timec, minl, sumt , sumh)
        WITH ( timescaledb.continuous, timescaledb.ignore_invalidation_older_than='1 hour')
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec);

\set ON_ERROR_STOP 1

create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '2147483647', timescaledb.refresh_interval = '2h')
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec);

\set ON_ERROR_STOP 0
ALTER VIEW mat_with_test SET(timescaledb.refresh_lag = '1h');
ALTER VIEW mat_with_test SET(timescaledb.refresh_lag = '2147483648');
ALTER TABLE conditions ALTER timec type bigint;
\set ON_ERROR_STOP
drop view mat_with_test CASCADE;

ALTER TABLE conditions ALTER timec type bigint;
create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '2147483647', timescaledb.refresh_interval = '2h')
as
select time_bucket(BIGINT '100', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by 1;

-- custom time partition functions are not supported with invalidations
CREATE FUNCTION text_part_func(TEXT) RETURNS BIGINT
    AS $$ SELECT length($1)::BIGINT $$
    LANGUAGE SQL IMMUTABLE;

CREATE TABLE text_time(time TEXT);
    SELECT create_hypertable('text_time', 'time', chunk_time_interval => 10, time_partitioning_func => 'text_part_func');

\set ON_ERROR_STOP 0
CREATE VIEW text_view
    WITH ( timescaledb.continuous, timescaledb.refresh_interval='72 hours')
    AS SELECT time_bucket('5', text_part_func(time)), COUNT(time)
        FROM text_time
        GROUP BY 1;
\set ON_ERROR_STOP 1
DROP TABLE text_time CASCADE;
