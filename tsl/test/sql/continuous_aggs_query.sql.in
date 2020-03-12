-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set TEST_BASE_NAME continuous_aggs_query
SELECT
       format('%s/results/%s_results_view.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_VIEW",
       format('%s/results/%s_results_view_hashagg.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_VIEW_HASHAGG",
       format('%s/results/%s_results_table.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_TABLE"
\gset
SELECT format('\! diff %s %s', :'TEST_RESULTS_VIEW', :'TEST_RESULTS_TABLE') as "DIFF_CMD",
      format('\! diff %s %s', :'TEST_RESULTS_VIEW_HASHAGG', :'TEST_RESULTS_TABLE') as "DIFF_CMD2"
\gset


\set EXPLAIN 'EXPLAIN (VERBOSE, COSTS OFF)'

SET client_min_messages TO LOG;

CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );

select table_name from create_hypertable( 'conditions', 'timec');

insert into conditions values ( '2018-01-01 09:00:00-08', 'SFO', 55, 45);
insert into conditions values ( '2018-01-02 09:00:00-08', 'por', 100, 100);
insert into conditions values ( '2018-01-02 09:00:00-08', 'SFO', 65, 45);
insert into conditions values ( '2018-01-02 09:00:00-08', 'NYC', 65, 45);
insert into conditions values ( '2018-11-01 09:00:00-08', 'NYC', 45, 30);
insert into conditions values ( '2018-11-01 10:00:00-08', 'NYC', 55, 35);
insert into conditions values ( '2018-11-01 11:00:00-08', 'NYC', 65, 40);
insert into conditions values ( '2018-11-01 12:00:00-08', 'NYC', 75, 45);
insert into conditions values ( '2018-11-01 13:00:00-08', 'NYC', 85, 50);
insert into conditions values ( '2018-11-02 09:00:00-08', 'NYC', 10, 10);
insert into conditions values ( '2018-11-02 10:00:00-08', 'NYC', 20, 15);
insert into conditions values ( '2018-11-02 11:00:00-08', 'NYC', null, null);
insert into conditions values ( '2018-11-03 09:00:00-08', 'NYC', null, null);

create table location_tab( locid integer, locname text );
insert into location_tab values( 1, 'SFO');
insert into location_tab values( 2, 'NYC');
insert into location_tab values( 3, 'por');

create or replace view mat_m1( location, timec, minl, sumt , sumh)
WITH ( timescaledb.continuous, timescaledb.max_interval_per_job='365 days')
as
select location, time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec), location;

SET timescaledb.current_timestamp_mock = '2018-12-31 00:00';
--compute time_bucketted max+bucket_width for the materialized view
SELECT time_bucket('1day' , q.timeval+ '1day'::interval)
FROM ( select max(timec)as timeval from conditions ) as q; 
REFRESH MATERIALIZED VIEW mat_m1;

--test first/last
create or replace view mat_m2(location, timec, firsth, lasth, maxtemp, mintemp)
WITH ( timescaledb.continuous, timescaledb.max_interval_per_job='365 days')
as
select location, time_bucket('1day', timec), first(humidity, timec), last(humidity, timec), max(temperature), min(temperature)
from conditions
group by time_bucket('1day', timec), location;
--time that refresh assumes as now() for repeatability
SET timescaledb.current_timestamp_mock = '2018-12-31 00:00';
SELECT time_bucket('1day' , q.timeval+ '1day'::interval)
FROM ( select max(timec)as timeval from conditions ) as q; 
REFRESH MATERIALIZED VIEW mat_m2;

--normal view --
create or replace view regview( location, timec, minl, sumt , sumh)
as
select location, time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by location, time_bucket('1day', timec);

set enable_hashagg = false;

-- NO pushdown cases ---
--when we have addl. attrs in order by that are not in the
-- group by, we will still need a sort
:EXPLAIN
select * from mat_m1 order by sumh, sumt, minl, timec ;
:EXPLAIN
select * from regview order by timec desc;

-- PUSHDOWN cases --
-- all group by elts in order by , reorder group by elts to match
-- group by order
-- This should prevent an additional sort after GroupAggregate
:EXPLAIN
select * from mat_m1 order by timec desc, location;

:EXPLAIN
select * from mat_m1 order by location, timec desc;

:EXPLAIN
select * from mat_m1 order by location, timec asc;
:EXPLAIN
select * from mat_m1 where timec > '2018-10-01' order by timec desc;
-- outer sort is used by mat_m1 for grouping. But doesn't avoid a sort after the join ---
:EXPLAIN
select l.locid, mat_m1.* from mat_m1 , location_tab l where timec > '2018-10-01' and l.locname = mat_m1.location order by timec desc;

:EXPLAIN
select * from mat_m2 where timec > '2018-10-01' order by timec desc;

:EXPLAIN
select * from (select * from mat_m2 where timec > '2018-10-01' order by timec desc ) as q limit 1;

:EXPLAIN
select * from (select * from mat_m2 where timec > '2018-10-01' order by timec desc , location asc nulls first) as q limit 1;

--plans with CTE
:EXPLAIN
with m1 as (
Select * from mat_m2 where timec > '2018-10-01' order by timec desc ) 
select * from m1;

-- should reorder mat_m1 group by only based on mat_m1 order-by
:EXPLAIN
select * from mat_m1, mat_m2 where mat_m1.timec > '2018-10-01' and mat_m1.timec = mat_m2.timec order by mat_m1.timec desc;
--should reorder only for mat_m1.
:EXPLAIN
select * from mat_m1, regview where mat_m1.timec > '2018-10-01' and mat_m1.timec = regview.timec order by mat_m1.timec desc;

select l.locid, mat_m1.* from mat_m1 , location_tab l where timec > '2018-10-01' and l.locname = mat_m1.location order by timec desc;

\set ECHO none
SET client_min_messages TO error;
\o :TEST_RESULTS_VIEW
select * from mat_m1 order by timec desc, location;
select * from mat_m1 order by location, timec desc;
select * from mat_m1 order by location, timec asc;
select * from mat_m1 where timec > '2018-10-01' order by timec desc;
select * from mat_m2 where timec > '2018-10-01' order by timec desc;
\o
RESET client_min_messages;
\set ECHO all

---- Run the same queries with hash agg enabled now 
set enable_hashagg = true;
\set ECHO none
SET client_min_messages TO error;
\o :TEST_RESULTS_VIEW_HASHAGG
select * from mat_m1 order by timec desc, location;
select * from mat_m1 order by location, timec desc;
select * from mat_m1 order by location, timec asc;
select * from mat_m1 where timec > '2018-10-01' order by timec desc;
select * from mat_m2 where timec > '2018-10-01' order by timec desc;
\o
RESET client_min_messages;
\set ECHO all

--- Run the queries directly on the table now
set enable_hashagg = true;
\set ECHO none
SET client_min_messages TO error;
\o :TEST_RESULTS_TABLE
SELECT location, time_bucket('1day', timec) as timec, min(location) as minl, sum(temperature) as sumt, sum(humidity) as sumh from conditions group by time_bucket('1day', timec) , location
order by timec desc, location;
SELECT location, time_bucket('1day', timec) as timec, min(location) as minl, sum(temperature) as sumt, sum(humidity) as sumh from conditions group by time_bucket('1day', timec) , location
order by location, timec desc;
SELECT location, time_bucket('1day', timec) as timec, min(location) as minl, sum(temperature) as sumt, sum(humidity) as sumh from conditions group by time_bucket('1day', timec) , location
order by location, timec asc;
select * from (SELECT location, time_bucket('1day', timec) as timec, min(location) as minl, sum(temperature) as sumt, sum(humidity) as sumh from conditions 
group by time_bucket('1day', timec) , location ) as q
where timec > '2018-10-01' order by timec desc;
--comparison for mat_m2 queries
select * from (
select location, time_bucket('1day', timec) as timec, first(humidity, timec) firsth, last(humidity, timec) lasth, max(temperature) maxtemp, min(temperature) mintemp
from conditions
group by time_bucket('1day', timec), location) as q
where timec > '2018-10-01' order by timec desc limit 10;
\o
RESET client_min_messages;
\set ECHO all

-- diff results view select and table select
:DIFF_CMD
:DIFF_CMD2

--check if the guc works , reordering will not work
set timescaledb.enable_cagg_reorder_groupby = false;
set enable_hashagg = false;
:EXPLAIN
select * from mat_m1 order by timec desc, location;
