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

-- remove any default jobs, e.g., telemetry so bgw_job isn't polluted
DELETE FROM _timescaledb_config.bgw_job WHERE TRUE;

SET ROLE :ROLE_DEFAULT_PERM_USER;

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

CREATE OR REPLACE FUNCTION integer_now_foo() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(a), 0) FROM foo $$;
SELECT set_integer_now_func('foo', 'integer_now_foo');


create or replace view mat_m1( a, countb )
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select a, count(b)
from foo
group by time_bucket(1, a), a;

SELECT * FROM _timescaledb_config.bgw_job;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_m1'
\gset

insert into :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
select a, _timescaledb_internal.partialize_agg(count(b)),
time_bucket(1, a)
,1
from foo
group by time_bucket(1, a) , a ;

select * from mat_m1 order by a ;

--check triggers on user hypertable --
SET ROLE :ROLE_SUPERUSER;
select tgname, tgtype, tgenabled , relname from pg_trigger, pg_class
where tgrelid = pg_class.oid and pg_class.relname like 'foo'
order by tgname;

SET ROLE :ROLE_DEFAULT_PERM_USER;

-- TEST2 ---
drop view mat_m1 cascade;

SELECT * FROM _timescaledb_config.bgw_job;

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
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec);

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_m1'
\gset

SET ROLE :ROLE_SUPERUSER;
insert into  :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
select
 time_bucket('1day', timec), _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity))
,1
from conditions
group by time_bucket('1day', timec) ;

SET ROLE :ROLE_DEFAULT_PERM_USER;
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
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by time_bucket('1week', timec) ;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_m1'
\gset

SET ROLE :ROLE_SUPERUSER;
insert into  :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions
group by time_bucket('1week', timec) ;
SET ROLE :ROLE_DEFAULT_PERM_USER;

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

-- apply where clause on result of mat_m1 --
drop view mat_m1 cascade;
create or replace view mat_m1( timec, minl, sumth, stddevh)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
where location = 'NYC'
group by time_bucket('1week', timec)
;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_m1'
\gset

SET ROLE :ROLE_SUPERUSER;
insert into  :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions
where location = 'NYC'
group by time_bucket('1week', timec) ;
SET ROLE :ROLE_DEFAULT_PERM_USER;

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
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by time_bucket('1week', timec)
having stddev(humidity) is not null;
;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_m1'
\gset

SET ROLE :ROLE_SUPERUSER;
insert into  :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,1
from conditions
group by time_bucket('1week', timec) ;
SET ROLE :ROLE_DEFAULT_PERM_USER;

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
      lowp        numeric NULL,
      highp       numeric null
    );

select table_name from create_hypertable( 'conditions', 'timec');

insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70;
insert into conditions
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40;
insert into conditions
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, 71, 28;

--drop view mat_m1 cascade;
--naming with AS clauses
create or replace view mat_naming
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1week', timec) as bucket, location as loc, sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by bucket, loc
having min(location) >= 'NYC' and avg(temperature) > 20;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_naming'
\gset

select attnum , attname from pg_attribute
where attnum > 0 and attrelid =
(Select oid from pg_class where relname like :'MAT_TABLE_NAME')
order by attnum, attname;

DROP VIEW mat_naming CASCADE;

--naming with default names
create or replace view mat_naming
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1week', timec), location, sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by 1,2
having min(location) >= 'NYC' and avg(temperature) > 20;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_naming'
\gset

select attnum , attname from pg_attribute
where attnum > 0 and attrelid =
(Select oid from pg_class where relname like :'MAT_TABLE_NAME')
order by attnum, attname;

DROP VIEW mat_naming CASCADE;

--naming with view col names
create or replace view mat_naming (bucket, loc, sum_t_h, stdd)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1week', timec), location, sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by 1,2
having min(location) >= 'NYC' and avg(temperature) > 20;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_naming'
\gset

select attnum , attname from pg_attribute
where attnum > 0 and attrelid =
(Select oid from pg_class where relname like :'MAT_TABLE_NAME')
order by attnum, attname;

DROP VIEW mat_naming CASCADE;

create or replace view mat_m1( timec, minl, sumth, stddevh)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by  time_bucket('1week', timec)
having min(location) >= 'NYC' and avg(temperature) > 20;

SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_m1'
\gset

select attnum , attname from pg_attribute
where attnum > 0 and attrelid =
(Select oid from pg_class where relname like :'MAT_TABLE_NAME')
order by attnum, attname;

SET ROLE :ROLE_SUPERUSER;
insert into  :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
select
 time_bucket('1week', timec),  _timescaledb_internal.partialize_agg( min(location)), _timescaledb_internal.partialize_agg( sum(temperature)) , _timescaledb_internal.partialize_agg( sum(humidity)), _timescaledb_internal.partialize_agg(stddev(humidity))
,_timescaledb_internal.partialize_agg( avg(temperature))
,1
from conditions
group by time_bucket('1week', timec) ;
SET ROLE :ROLE_DEFAULT_PERM_USER;

--should have same results --
select timec, minl, sumth, stddevh
from mat_m1
order by timec, minl;

select time_bucket('1week', timec) ,
min(location), sum(temperature)+sum(humidity), stddev(humidity)
from conditions
group by  time_bucket('1week', timec)
having min(location) >= 'NYC' and avg(temperature) > 20 and avg(lowp) > 10
order by time_bucket('1week', timec), min(location);

--check view defintion in information views
select view_name, view_definition from timescaledb_information.continuous_aggregates
where view_name::text like 'mat_m1';

--TEST6 -- select from internal view

SET ROLE :ROLE_SUPERUSER;
insert into :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
select * from :"PART_VIEW_SCHEMA".:"PART_VIEW_NAME";
SET ROLE :ROLE_DEFAULT_PERM_USER;

--lets drop the view and check
drop view mat_m1 cascade;

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

--TEST7 -- drop tests for view and hypertable
--DROP tests
\set ON_ERROR_STOP 0
SELECT  h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA",
       direct_view_name as "DIR_VIEW_NAME",
       direct_view_schema as "DIR_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_test'
\gset

DROP TABLE :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME";
DROP VIEW :"PART_VIEW_SCHEMA".:"PART_VIEW_NAME";
DROP VIEW :"DIR_VIEW_SCHEMA".:"DIR_VIEW_NAME";
DROP VIEW mat_test;
\set ON_ERROR_STOP 1

--catalog entry still there;
SELECT count(*)
FROM _timescaledb_catalog.continuous_agg ca
WHERE user_view_name = 'mat_test';

--mat table, user_view, direct view and partial view all there
select count(*) from pg_class where relname = :'PART_VIEW_NAME';
select count(*) from pg_class where relname = :'MAT_TABLE_NAME';
select count(*) from pg_class where relname = :'DIR_VIEW_NAME';
select count(*) from pg_class where relname = 'mat_test';

DROP VIEW mat_test CASCADE;

--catalog entry should be gone
SELECT count(*)
FROM _timescaledb_catalog.continuous_agg ca
WHERE user_view_name = 'mat_test';

--mat table, user_view, direct view and partial view all gone
select count(*) from pg_class where relname = :'PART_VIEW_NAME';
select count(*) from pg_class where relname = :'MAT_TABLE_NAME';
select count(*) from pg_class where relname = :'DIR_VIEW_NAME';
select count(*) from pg_class where relname = 'mat_test';


--test dropping raw table
DROP TABLE conditions;
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

--no data in hyper table on purpose so that CASCADE is not required because of chunks

create or replace view mat_drop_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec);

\set ON_ERROR_STOP 0
DROP TABLE conditions;
\set ON_ERROR_STOP 1

--insert data now

insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70, NULL;
insert into conditions
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40, NULL;
insert into conditions
select generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, NULL, 28, NULL;


SELECT ca.raw_hypertable_id as "RAW_HYPERTABLE_ID",
       h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_drop_test'
\gset

SET client_min_messages TO LOG;
SET timescaledb.current_timestamp_mock = '2018-12-31 00:00';
REFRESH MATERIALIZED VIEW mat_drop_test;

--force invalidation
insert into conditions
select generate_series('2017-11-01 00:00'::timestamp, '2017-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, NULL, 28, NULL;

select count(*) from _timescaledb_catalog.continuous_aggs_invalidation_threshold;
select count(*) from _timescaledb_catalog.continuous_aggs_completed_threshold;
select count(*) from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

DROP TABLE conditions CASCADE;

--catalog entry should be gone
SELECT count(*)
FROM _timescaledb_catalog.continuous_agg ca
WHERE user_view_name = 'mat_drop_test';
select count(*) from _timescaledb_catalog.continuous_aggs_invalidation_threshold;
select count(*) from _timescaledb_catalog.continuous_aggs_completed_threshold;
select count(*) from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
select count(*) from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

SELECT * FROM _timescaledb_config.bgw_job;

--mat table, user_view, and partial view all gone
select count(*) from pg_class where relname = :'PART_VIEW_NAME';
select count(*) from pg_class where relname = :'MAT_TABLE_NAME';
select count(*) from pg_class where relname = 'mat_drop_test';

--TEST With options

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

create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '5 hours', timescaledb.refresh_interval = '1h')
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec), location, humidity, temperature;

SELECT schedule_interval FROM _timescaledb_config.bgw_job;
SELECT _timescaledb_internal.to_interval(refresh_lag) FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'mat_with_test';

ALTER VIEW mat_with_test SET(timescaledb.refresh_lag = '6 h', timescaledb.refresh_interval = '2h');
SELECT _timescaledb_internal.to_interval(refresh_lag) FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'mat_with_test';
SELECT schedule_interval FROM _timescaledb_config.bgw_job;

select indexname, indexdef from pg_indexes where tablename =
(SELECT h.table_name
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_with_test')
order by indexname;

drop view mat_with_test cascade;
--no additional indexes
create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '5 hours', timescaledb.refresh_interval = '1h', timescaledb.create_group_indexes=false)
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec), location, humidity, temperature;

select indexname, indexdef from pg_indexes where tablename =
(SELECT h.table_name
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_with_test');

DROP TABLE conditions CASCADE;

--test WITH using a hypertable with an integer time dimension
CREATE TABLE conditions (
      timec       INT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec', chunk_time_interval=> 100);

CREATE OR REPLACE FUNCTION integer_now_conditions() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0) FROM conditions $$;
SELECT set_integer_now_func('conditions', 'integer_now_conditions');

create or replace view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '500', timescaledb.refresh_interval = '2h')
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec);

SELECT schedule_interval FROM _timescaledb_config.bgw_job;
SELECT refresh_lag FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'mat_with_test';

ALTER VIEW mat_with_test SET(timescaledb.refresh_lag = '100');
SELECT refresh_lag FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'mat_with_test';

-- we can SET multiple options in one commad
ALTER VIEW mat_with_test SET (timescaledb.refresh_lag = '100', timescaledb.max_interval_per_job='400');
SELECT refresh_lag, max_interval_per_job FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'mat_with_test';

DROP TABLE conditions CASCADE;


--test space partitions
CREATE TABLE space_table (
    time BIGINT,
    dev  BIGINT,
    data BIGINT
);

SELECT create_hypertable(
    'space_table',
    'time',
    chunk_time_interval => 10,
    partitioning_column => 'dev',
    number_partitions => 3);

CREATE OR REPLACE FUNCTION integer_now_space_table() returns BIGINT LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), BIGINT '0') FROM space_table $$;
SELECT set_integer_now_func('space_table', 'integer_now_space_table');

CREATE VIEW space_view
WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '-2', timescaledb.refresh_interval = '72h')
AS SELECT time_bucket('4', time), COUNT(data)
   FROM space_table
   GROUP BY 1;

INSERT INTO space_table VALUES
  (0, 1, 1), (0, 2, 1), (1, 1, 1), (1, 2, 1),
  (10, 1, 1), (10, 2, 1), (11, 1, 1), (11, 2, 1);

SELECT  h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA",
       direct_view_name as "DIR_VIEW_NAME",
       direct_view_schema as "DIR_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'space_view'
\gset

SELECT * FROM :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
  ORDER BY time_bucket, chunk_id;


REFRESH MATERIALIZED VIEW space_view;

SELECT * FROM space_view ORDER BY 1;

SELECT * FROM :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
  ORDER BY time_bucket, chunk_id;


INSERT INTO space_table VALUES (3, 2, 1);

REFRESH MATERIALIZED VIEW space_view;

SELECT * FROM space_view ORDER BY 1;

SELECT * FROM :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
  ORDER BY time_bucket, chunk_id;


INSERT INTO space_table VALUES (2, 3, 1);

REFRESH MATERIALIZED VIEW space_view;

SELECT * FROM space_view ORDER BY 1;

SELECT * FROM :"MAT_SCHEMA_NAME".:"MAT_TABLE_NAME"
  ORDER BY time_bucket, chunk_id;


DROP TABLE space_table CASCADE;

--
-- TEST FINALIZEFUNC_EXTRA
--

-- create special aggregate to test ffunc_extra
-- Raise warning with the actual type being passed in
CREATE OR REPLACE FUNCTION fake_ffunc(a int8, b int, c int, d int, x anyelement)
RETURNS anyelement AS $$
BEGIN
 RAISE WARNING 'type % %', pg_typeof(d), pg_typeof(x);
 RETURN x;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fake_sfunc(a int8, b int, c int, d int, x anyelement)
RETURNS int8 AS $$
BEGIN
 RETURN b;
END; $$
LANGUAGE plpgsql;


CREATE AGGREGATE aggregate_to_test_ffunc_extra(int, int, int, anyelement) (
    SFUNC = fake_sfunc,
    STYPE = int8,
    COMBINEFUNC = int8pl,
    FINALFUNC = fake_ffunc,
    PARALLEL = SAFE,
    FINALFUNC_EXTRA
);

CREATE TABLE conditions (
      timec       INT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec', chunk_time_interval=> 100);

CREATE OR REPLACE FUNCTION integer_now_conditions() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0) FROM conditions $$;
SELECT set_integer_now_func('conditions', 'integer_now_conditions');

insert into conditions
select generate_series(0, 200, 10), 'POR', 55, 75, 40, 70, NULL;


create or replace view mat_ffunc_test
WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '-200')
as
select time_bucket(100, timec), aggregate_to_test_ffunc_extra(timec, 1, 3, 'test'::text)
from conditions
group by time_bucket(100, timec);

REFRESH MATERIALIZED VIEW mat_ffunc_test;

SELECT * FROM mat_ffunc_test;

DROP view mat_ffunc_test cascade;

create or replace view mat_ffunc_test
WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '-200')
as
select time_bucket(100, timec), aggregate_to_test_ffunc_extra(timec, 4, 5, bigint '123')
from conditions
group by time_bucket(100, timec);

REFRESH MATERIALIZED VIEW mat_ffunc_test;

SELECT * FROM mat_ffunc_test;

--refresh mat view test when time_bucket is not projected --
drop view mat_ffunc_test cascade;
create or replace view mat_refresh_test
WITH (timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag = '-200')
as
select location, max(humidity)
from conditions
group by time_bucket(100, timec), location;

insert into conditions
select generate_series(0, 50, 10), 'NYC', 55, 75, 40, 70, NULL;

REFRESH MATERIALIZED VIEW mat_refresh_test;
SELECT * FROM mat_refresh_test order by 1,2 ;

-- test for bug when group by is not in project list
create view conditions_grpby_view with (timescaledb.continuous, timescaledb.refresh_lag = '-200') as 
select time_bucket(100, timec),  sum(humidity) 
from conditions 
group by time_bucket(100, timec), location;
REFRESH MATERIALIZED VIEW conditions_grpby_view;
select * from conditions_grpby_view order by 1, 2;

create view conditions_grpby_view2 with (timescaledb.continuous, timescaledb.refresh_lag = '-200') as 
select time_bucket(100, timec), sum(humidity)
from conditions
group by time_bucket(100, timec), location  
having avg(temperature) > 0
;
REFRESH MATERIALIZED VIEW conditions_grpby_view2;
select * from conditions_grpby_view2 order by 1, 2;


--test ignore_invalidation_older_than on timestamps-based ht

DROP TABLE conditions CASCADE;
CREATE TABLE conditions (
                            time       TIMESTAMPTZ       NOT NULL,
                            temperature DOUBLE PRECISION  NULL
);

select table_name from create_hypertable( 'conditions', 'time');

create or replace view mat_test5( timec, maxt)
        WITH ( timescaledb.continuous, timescaledb.ignore_invalidation_older_than='30 day', timescaledb.refresh_lag='-1day',
               timescaledb.max_interval_per_job='260 day')
as
select time_bucket('1day', time), max(temperature)
from conditions
group by time_bucket('1day', time);

SET timescaledb.current_timestamp_mock = '2001-03-11';
insert into conditions values('2001-03-10', '1');
insert into conditions values('2001-03-10', '2');

REFRESH MATERIALIZED VIEW mat_test5;
SELECT * FROM mat_test5;

insert into conditions values('2001-02-15', '1');
insert into conditions values('2001-01-15', '1');

REFRESH MATERIALIZED VIEW mat_test5;
--will see the feb but not the january change
SELECT * FROM mat_test5;

--what matters is the time at insertion not materialization
SET timescaledb.current_timestamp_mock = '2001-03-11';
--will see this change
insert into conditions values('2001-02-15', '3');
SET timescaledb.current_timestamp_mock = '2001-05-11';
--but not this one
insert into conditions values('2001-02-20', '4');
REFRESH MATERIALIZED VIEW mat_test5;
SELECT * FROM mat_test5;
--verify that watermark is limited by max value and not by
-- the current time (now value)--
SET timescaledb.current_timestamp_mock = '2018-05-11';
SELECT view_name, completed_threshold, invalidation_threshold 
FROM timescaledb_information.continuous_aggregate_stats
where view_name::text like 'mat_test5';
REFRESH MATERIALIZED VIEW mat_test5;
SELECT view_name, completed_threshold, invalidation_threshold 
FROM timescaledb_information.continuous_aggregate_stats
where view_name::text like 'mat_test5';
