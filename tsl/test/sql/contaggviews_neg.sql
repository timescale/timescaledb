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

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start', timescaledb.myfill = 1)
as
select location , min(temperature)
from conditions 
group by time_bucket('1d', timec), location;

--valid PG option
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start', check_option = LOCAL )
as
select * from conditions , matt1;

-- join multiple tables
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
as
select location, count(*) from conditions , mat_t1
where conditions.location = mat_t1.c
group by location;

-- LATERAL multiple tables
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
as
select location, count(*) from conditions,
LATERAL (Select * from mat_t1 where c = conditions.location) q
group by location;


--non-hypertable
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
as
select a, count(*) from mat_t1
group by a;


-- no group by
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
as
select count(*) from conditions ;

-- no time_bucket in group by
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
as
select count(*) from conditions group by location;

-- with valid query in a CTE
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
with m1 as (
Select location, count(*) from conditions
 group by time_bucket('1week', timec) , location)  
select * from m1;

--with DISTINCT ON
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
as
 select distinct on ( location ) count(*)  from conditions group by location, time_bucket('1week', timec) ;

--aggregate with DISTINCT
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select time_bucket('1week', timec),
 count(location) , sum(distinct temperature) from conditions
 group by time_bucket('1week', timec) , location;

--aggregate with FILTER
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select time_bucket('1week', timec),
 sum(temperature) filter ( where humidity > 20 ) from conditions
 group by time_bucket('1week', timec) , location;

-- aggregate with filter in having clause
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select time_bucket('1week', timec), max(temperature)
from conditions
 group by time_bucket('1week', timec) , location
 having sum(temperature) filter ( where humidity > 20 ) > 50;

-- time_bucket on non partitioning column of hypertable
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timemeasure) , location;

--time_bucket on expression
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timec+ '10 minutes'::interval) , location;

--multiple time_bucket functions
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timec) , time_bucket('1month', timec), location;

--time_bucket using additional args
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select max(temperature)
from conditions
 group by time_bucket( INTERVAL '5 minutes', timec, INTERVAL '-2.5 minutes') , location;

--time_bucket using non-const for first argument
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select max(temperature)
from conditions
 group by time_bucket( timeinterval, timec) , location;

-- ordered set aggr
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select mode() within group( order by humidity)
from conditions
 group by time_bucket('1week', timec) ;

--window function
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select avg(temperature) over( order by humidity)
from conditions
;

--aggregate without combine function
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select json_agg(location) 
from conditions
 group by time_bucket('1week', timec) , location;
;

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
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
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), newavg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location;
;

-- using subqueries
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4)  
from
( select humidity, temperature, location, timec     
from conditions ) q
 group by time_bucket('1week', timec) , location ;

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
select * from
( Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location )  q;

--using limit /limit offset
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location
limit 10 ;

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location
offset 10;

--using ORDER BY in view defintion
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location
ORDER BY 1;

--using FETCH
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location
fetch first 10 rows only;

--using locking clauses FOR clause
--all should be disabled. we cannot guarntee locks on the hypertable 
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location
FOR KEY SHARE;

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location
FOR SHARE;


create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location
FOR UPDATE;

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by time_bucket('1week', timec) , location
FOR NO KEY UPDATE;

--tablesample clause
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions tablesample bernoulli(0.2)
 group by time_bucket('1week', timec) , location
;

-- ONLY in from clause
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from ONLY conditions
 group by time_bucket('1week', timec) , location ;

--grouping sets and variants
create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
 group by grouping sets(time_bucket('1week', timec) , location ) ;

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), avg(temperature::int4) 
from conditions
group by rollup(time_bucket('1week', timec) , location ) ;

--NO immutable functions -- check all clauses
CREATE FUNCTION test_stablefunc(int) RETURNS int LANGUAGE 'sql'
       STABLE AS 'SELECT $1 + 10';

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), max(timec + INTERVAL '1h')
from conditions
group by time_bucket('1week', timec) , location  ;

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum(humidity), min(location) 
from conditions
group by time_bucket('1week', timec) 
having  max(timec + INTERVAL '1h') > '2010-01-01 09:00:00-08';

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum( test_stablefunc(humidity::int) ), min(location) 
from conditions
group by time_bucket('1week', timec);

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum( temperature ), min(location) 
from conditions
group by time_bucket('1week', timec), test_stablefunc(humidity::int);

-- row security on table
create table rowsec_tab( a bigint, b integer, c integer);
select table_name from create_hypertable( 'rowsec_tab', 'a', chunk_time_interval=>10); 
alter table rowsec_tab ENABLE ROW LEVEL SECURITY;
create policy rowsec_tab_allview ON rowsec_tab FOR SELECT USING(true);

create  view mat_m1 WITH ( timescaledb.continuous_agg = 'start')
AS
Select sum( b), min(c)
from rowsec_tab
group by time_bucket('1', a);
