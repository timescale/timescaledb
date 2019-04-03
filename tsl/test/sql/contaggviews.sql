-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- initialize the bgw mock state to prevent the materialization workers from running
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION ts_bgw_params_create() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

\set WAIT_ON_JOB 0
\set IMMEDIATELY_SET_UNTIL 1
\set WAIT_FOR_OTHER_TO_ADVANCE 2

-- stop the background workers from locking up the tables,
-- and remove any default jobs, e.g., telemetry so bgw_job isn't polluted
SELECT _timescaledb_internal.stop_background_workers();
DELETE FROM _timescaledb_config.bgw_job WHERE TRUE;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT * FROM _timescaledb_config.bgw_job;

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

SELECT * FROM _timescaledb_config.bgw_job;

insert into  _timescaledb_internal.ts_internal_mat_m1tab
select a, _timescaledb_internal.partialize_agg(count(b)),
time_bucket(1, a)
,1
from foo
group by time_bucket(1, a) , a ;

select * from mat_m1 order by a ;


drop view mat_m1 cascade;
SELECT * FROM _timescaledb_config.bgw_job;

-- TEST2 ---
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


create or replace view mat_m2( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec);

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m2tab
select
 time_bucket('1day', timec), _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity))
,1
from conditions
group by time_bucket('1day', timec) ;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
--should have same results --
select timec, minl, sumt, sumh
from mat_m2
order by timec;

select time_bucket('1day', timec), min(location), sum(temperature), sum(humidity)
from conditions
group by time_bucket('1day', timec)
order by 1;

-- TEST3 --
-- drop on table conditions should cascade to materialized mat_v1
-- Oid used in timescaledb_catalog table from catalog table -- TODO

drop table conditions cascade;

CREATE TABLE conditions2 (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );

select table_name from create_hypertable( 'conditions2', 'timec');

insert into conditions2 values ( '2010-01-01 09:00:00-08', 'SFO', 55, 45);
insert into conditions2 values ( '2010-01-02 09:00:00-08', 'por', 100, 100);
insert into conditions2 values ( '2010-01-02 09:00:00-08', 'NYC', 65, 45);
insert into conditions2 values ( '2010-01-02 09:00:00-08', 'SFO', 65, 45);
insert into conditions2 values ( '2010-01-03 09:00:00-08', 'NYC', 45, 55);
insert into conditions2 values ( '2010-01-05 09:00:00-08', 'SFO', 75, 100);
insert into conditions2 values ( '2018-11-01 09:00:00-08', 'NYC', 45, 35);
insert into conditions2 values ( '2018-11-02 09:00:00-08', 'NYC', 35, 15);
insert into conditions2 values ( '2018-11-03 09:00:00-08', 'NYC', 35, 25);


create or replace view mat_m3( timec, minl, sumth, stddevh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions2
group by time_bucket('1week', timec) ;

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m3tab
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions2
group by time_bucket('1week', timec) ;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--should have same results --
select timec, minl, sumth, stddevh
from mat_m3
order by timec;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+ sum(humidity), stddev(humidity)
from conditions2
group by time_bucket('1week', timec)
order by time_bucket('1week', timec);

-- TEST4 --
--materialized view with group by clause + expression in SELECT
-- use previous data from conditions
--drop only the view.
-- TODO catalog entry should get deleted?

-- apply where clause on result of mat_m1 --
drop view mat_m3 cascade;
create or replace view mat_m4( timec, minl, sumth, stddevh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions2
where location = 'NYC'
group by time_bucket('1week', timec)
;

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m4tab
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions2
where location = 'NYC'
group by time_bucket('1week', timec) ;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--should have same results --
select timec, minl, sumth, stddevh
from mat_m4
where stddevh is not null
order by timec;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+ sum(humidity), stddev(humidity)
from conditions2
where location = 'NYC'
group by time_bucket('1week', timec)
order by time_bucket('1week', timec);

-- TEST5 --
---------test with having clause ----------------------
drop view mat_m4 cascade;
create or replace view mat_m5( timec, minl, sumth, stddevh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions2
group by time_bucket('1week', timec)
having stddev(humidity) is not null;
;

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m5tab
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions2
group by time_bucket('1week', timec) ;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- should have same results --
select * from mat_m5
order by sumth;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions2
group by time_bucket('1week', timec)
having stddev(humidity) is not null
order by sum(temperature)+sum(humidity);

-- TEST6 --
--group by with more than 1 group column
-- having clause with a mix of columns from select list + others

drop table conditions2 cascade;

CREATE TABLE conditions3 (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        numeric NULL,
      highp       numeric null
    );

select table_name from create_hypertable( 'conditions3', 'timec');

insert into conditions3
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70;
insert into conditions3
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40;
insert into conditions3
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, 71, 28;

create or replace view mat_m6( timec, minl, sumth, stddevh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions3
group by  time_bucket('1week', timec)
having min(location) >= 'NYC' and avg(temperature) > 20
;

select attnum , attname from pg_attribute
where attnum > 0 and attrelid =
(Select oid from pg_class where relname like 'ts_internal_mat_m6tab')
order by attnum, attname;

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into  _timescaledb_internal.ts_internal_mat_m6tab
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,_timescaledb_internal.partialize_agg( avg(temperature))
,1
from conditions3
group by time_bucket('1week', timec) ;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--should have same results --
select timec, minl, sumth, stddevh
from mat_m6
order by timec, minl;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions3
group by  time_bucket('1week', timec)
having min(location) >= 'NYC' and avg(temperature) > 20 and avg(lowp) > 10
order by time_bucket('1week', timec), min(location);

--TEST6 -- catalog entries and select from internal view
--check the entry in the catalog tables --
select partial_view_name from _timescaledb_catalog.continuous_agg where user_view_name like 'mat_m6';

\c :TEST_DBNAME :ROLE_SUPERUSER
insert into _timescaledb_internal.ts_internal_mat_m6tab
select * from _timescaledb_internal.ts_internal_mat_m6view;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

--lets drop the view and check
drop view mat_m6 cascade;
--TODO does not work --
--select partial_view_name from _timescaledb_catalog.continuous_agg where user_view_name like 'mat_m6';

drop table conditions3;
CREATE TABLE conditions4 (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions4', 'timec');

insert into conditions4
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70, NULL;
insert into conditions4
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40, NULL;
insert into conditions4
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, NULL, 28, NULL;


SELECT
  $$
  select time_bucket('1week', timec) ,
  min(location) as col1, sum(temperature)+sum(humidity) as col2, stddev(humidity) as col3, min(allnull) as col4
  from conditions4
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
  from conditions4
  group by location, time_bucket('1week', timec)
  $$ AS "QUERY"
\gset

\set ECHO errors
\ir include/cont_agg_equal.sql
\set ECHO all

--DROP tests
\set ON_ERROR_STOP 0
SELECT  h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_test'
\gset

DROP TABLE :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME";
DROP VIEW :"PART_VIEW_SCHEMA".:"PART_VIEW_NAME";
DROP VIEW mat_test;
\set ON_ERROR_STOP 1

--catalog entry still there;
SELECT count(*)
FROM _timescaledb_catalog.continuous_agg ca
WHERE user_view_name = 'mat_test';

--mat table, user_view, and partial view all there
select count(*) from pg_class where relname = :'PART_VIEW_NAME';
select count(*) from pg_class where relname = :'MAT_TABLE_NAME';
select count(*) from pg_class where relname = 'mat_test';

DROP VIEW mat_test CASCADE;

--catalog entry should be gone
SELECT count(*)
FROM _timescaledb_catalog.continuous_agg ca
WHERE user_view_name = 'mat_test';

--mat table, user_view, and partial view all gone
select count(*) from pg_class where relname = :'PART_VIEW_NAME';
select count(*) from pg_class where relname = :'MAT_TABLE_NAME';
select count(*) from pg_class where relname = 'mat_test';


--test dropping raw table
DROP TABLE conditions4;
CREATE TABLE conditions5 (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions5', 'timec');

--no data in hyper table on purpose so that CASCADE is not required because of chunks

create or replace view mat_drop_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous_agg = 'start')
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions5
group by time_bucket('1day', timec);

SELECT * FROM _timescaledb_config.bgw_job;

\set ON_ERROR_STOP 0
DROP TABLE conditions5;
\set ON_ERROR_STOP 1

SELECT  h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_drop_test'
\gset

DROP TABLE conditions5 CASCADE;

--catalog entry should be gone
SELECT count(*)
FROM _timescaledb_catalog.continuous_agg ca
WHERE user_view_name = 'mat_drop_test';

SELECT * FROM _timescaledb_config.bgw_job;

--mat table, user_view, and partial view all gone
select count(*) from pg_class where relname = :'PART_VIEW_NAME';
select count(*) from pg_class where relname = :'MAT_TABLE_NAME';
select count(*) from pg_class where relname = 'mat_drop_test';
