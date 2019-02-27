-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--TEST1 ---
--basic test with count
create table foo (a integer, b integer, c integer);
select table_name from create_hypertable('foo', 'a', chunk_time_interval=> 10);

insert into foo values( 3 , 16 , 20);
insert into foo values( 1 , 10 , 20);
insert into foo values( 1 , 11 , 20);
insert into foo values( 1 , 12 , 20);
insert into foo values( 1 , 13 , 20);
insert into foo values( 1 , 14 , 20);
insert into foo values( 2 , 14 , 20);
insert into foo values( 2 , 15 , 20);
insert into foo values( 2 , 16 , 20);

create or replace view mat_m1( a, countb )
WITH ( timescaledb.continuous_agg = 'start')
as
select a, count(b)
from foo
group by time_bucket(1, a), a;

insert into  _timescaledb_internal.ts_internal_mat_m1tab
select a, _timescaledb_internal.partialize_agg(count(b)),
time_bucket(1, a)
,1
from foo
group by time_bucket(1, a) , a ;

select * from mat_m1 order by a ;

-- TEST2 ---
drop view mat_m1 cascade;


CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );

select table_name from create_hypertable( 'conditions', 'timec');

insert into conditions values ( '2010-01-01 09:00:00-08', 'SFO', 55, 45);
insert into conditions values ( '2010-01-02 09:00:00-08', 'por', 100, 100);
insert into conditions values ( '2010-01-02 09:00:00-08', 'SFO', 65, 45);
insert into conditions values ( '2010-01-02 09:00:00-08', 'NYC', 65, 45);
insert into conditions values ( '2018-11-01 09:00:00-08', 'NYC', 45, 35);
insert into conditions values ( '2018-11-02 09:00:00-08', 'NYC', 35, 15);


create or replace view mat_m1( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec);

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m1tab
select
 time_bucket('1day', timec), _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity))
,1
from conditions
group by time_bucket('1day', timec) ;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
--should have same results --
select timec, minl, sumt, sumh
from mat_m1
order by timec;

select time_bucket('1day', timec), min(location), sum(temperature), sum(humidity)
from conditions
group by time_bucket('1day', timec)
order by 1;

-- TEST3 --
-- drop on table conditions should cascade to materialized mat_v1
-- Oid used in timescaledb_catalog table from catalog table -- TODO

drop table conditions cascade;

CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );

select table_name from create_hypertable( 'conditions', 'timec');

insert into conditions values ( '2010-01-01 09:00:00-08', 'SFO', 55, 45);
insert into conditions values ( '2010-01-02 09:00:00-08', 'por', 100, 100);
insert into conditions values ( '2010-01-02 09:00:00-08', 'NYC', 65, 45);
insert into conditions values ( '2010-01-02 09:00:00-08', 'SFO', 65, 45);
insert into conditions values ( '2010-01-03 09:00:00-08', 'NYC', 45, 55);
insert into conditions values ( '2010-01-05 09:00:00-08', 'SFO', 75, 100);
insert into conditions values ( '2018-11-01 09:00:00-08', 'NYC', 45, 35);
insert into conditions values ( '2018-11-02 09:00:00-08', 'NYC', 35, 15);
insert into conditions values ( '2018-11-03 09:00:00-08', 'NYC', 35, 25);


create or replace view mat_m1( timec, minl, sumth, stddevh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by time_bucket('1week', timec) ;

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m1tab
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions
group by time_bucket('1week', timec) ;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--should have same results --
select timec, minl, sumth, stddevh
from mat_m1
order by timec;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+ sum(humidity), stddev(humidity)
from conditions
group by time_bucket('1week', timec)
order by time_bucket('1week', timec);

-- TEST4 --
--materialized view with group by clause + expression in SELECT
-- use previous data from conditions
--drop only the view.
-- TODO catalog entry should get deleted?

-- apply where clause on result of mat_m1 --
drop view mat_m1 cascade;
create or replace view mat_m1( timec, minl, sumth, stddevh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
where location = 'NYC'
group by time_bucket('1week', timec)
;

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m1tab
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions
where location = 'NYC'
group by time_bucket('1week', timec) ;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--should have same results --
select timec, minl, sumth, stddevh
from mat_m1
where stddevh is not null
order by timec;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+ sum(humidity), stddev(humidity)
from conditions
where location = 'NYC'
group by time_bucket('1week', timec)
order by time_bucket('1week', timec);

-- TEST5 --
---------test with having clause ----------------------
drop view mat_m1 cascade;
create or replace view mat_m1( timec, minl, sumth, stddevh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by time_bucket('1week', timec)
having stddev(humidity) is not null;
;

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m1tab
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions
group by time_bucket('1week', timec) ;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- should have same results --
select * from mat_m1
order by sumth;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by time_bucket('1week', timec)
having stddev(humidity) is not null
order by sum(temperature)+sum(humidity);

-- TEST6 --
--group by with more than 1 group column
-- having clause with a mix of columns from select list + others

drop table conditions cascade;

CREATE TABLE conditions (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec');

insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70;
insert into conditions
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40;
insert into conditions
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, 71, 28;

--drop view mat_m1 cascade;
create or replace view mat_m1( timec, minl, sumth, stddevh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by  time_bucket('1week', timec)
having min(location) >= 'NYC' and avg(temperature) > 20
;

select attnum , attname from pg_attribute
where attnum > 0 and attrelid =
(Select oid from pg_class where relname like 'ts_internal_mat_m1tab')
order by attnum, attname;

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m1tab
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,_timescaledb_internal.partialize_agg( avg(temperature))
,1
from conditions
group by time_bucket('1week', timec) ;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--should have same results --
select timec, minl, sumth, stddevh
from mat_m1
order by timec, minl;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by  time_bucket('1week', timec)
having min(location) >= 'NYC' and avg(temperature) > 20
order by time_bucket('1week', timec), min(location);

--TEST6 -- catalog entries and select from internal view
--check the entry in the catalog tables --
--TODO wrong results
select partial_view_name from _timescaledb_catalog.continuous_agg where user_view_name like 'mat_m1';

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into _timescaledb_internal.ts_internal_mat_m1tab
select * from _timescaledb_internal.ts_internal_mat_m1view;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--lets drop the view and check
drop view mat_m1 cascade;
--TODO does not work --
--select partial_view_name from _timescaledb_catalog.continuous_agg where user_view_name like 'mat_m1';

drop table conditions;
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

insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70, NULL;
insert into conditions
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40, NULL;
insert into conditions
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, NULL, 28, NULL;


SELECT
  $$
  select time_bucket('1week', timec) ,
  min(location) as col1, sum(temperature)+sum(humidity) as col2, stddev(humidity) as col3, min(allnull) as col4
  from conditions
  group by  time_bucket('1week', timec)
  having min(location) >= 'NYC' and avg(temperature) > 20
  $$ AS "QUERY"
\gset


\set ECHO errors
\ir include/cont_agg_equal.sql
\set ECHO all

SELECT
  $$
  select time_bucket('1week', timec), location,
  sum(temperature)+sum(humidity) as col2, stddev(humidity) as col3, min(allnull) as col4
  from conditions
  group by location, time_bucket('1week', timec)
  $$ AS "QUERY"
\gset

\set ECHO errors
\ir include/cont_agg_equal.sql
\set ECHO all
