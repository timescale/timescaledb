-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0
\set VERBOSITY default

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

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false, timescaledb.myfill = 1)
as
select location , min(temperature)
from conditions
group by time_bucket('1d', timec), location WITH NO DATA;

--valid PG option
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false, check_option = LOCAL )
as
select * from conditions , mat_t1 WITH NO DATA;

-- join multiple tables
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select location, count(*) from conditions , mat_t1
where conditions.location = mat_t1.c
group by location WITH NO DATA;

-- join multiple tables WITH explicit JOIN
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select location, count(*) from conditions JOIN mat_t1 ON true
where conditions.location = mat_t1.c
group by location WITH NO DATA;

-- LATERAL multiple tables
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select location, count(*) from conditions,
LATERAL (Select * from mat_t1 where c = conditions.location) q
group by location WITH NO DATA;

--non-hypertable
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select a, count(*) from mat_t1
group by a WITH NO DATA;

-- no group by
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select count(*) from conditions  WITH NO DATA;

-- no time_bucket in group by
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select count(*) from conditions group by location WITH NO DATA;

-- with valid query in a CTE
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
with m1 as (
Select location, count(*) from conditions
 group by time_bucket('1week', timec) , location)
select * from m1 WITH NO DATA;

--with DISTINCT ON
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
as
 select distinct on ( location ) count(*)  from conditions group by location, time_bucket('1week', timec)  WITH NO DATA;

--aggregate with DISTINCT
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select time_bucket('1week', timec),
 count(location) , sum(distinct temperature) from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;

--aggregate with FILTER
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select time_bucket('1week', timec),
 sum(temperature) filter ( where humidity > 20 ) from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;

-- aggregate with filter in having clause
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select time_bucket('1week', timec), max(temperature)
from conditions
 group by time_bucket('1week', timec) , location
 having sum(temperature) filter ( where humidity > 20 ) > 50 WITH NO DATA;

-- time_bucket on non partitioning column of hypertable
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timemeasure) , location WITH NO DATA;

--time_bucket on expression
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timec+ '10 minutes'::interval) , location WITH NO DATA;

--multiple time_bucket functions
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select max(temperature)
from conditions
 group by time_bucket('1week', timec) , time_bucket('1month', timec), location WITH NO DATA;

--time_bucket using additional args
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select max(temperature)
from conditions
 group by time_bucket( INTERVAL '5 minutes', timec, INTERVAL '-2.5 minutes') , location WITH NO DATA;

--time_bucket using non-const for first argument
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select max(temperature)
from conditions
 group by time_bucket( timeinterval, timec) , location WITH NO DATA;

-- ordered set aggr
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select mode() within group( order by humidity)
from conditions
 group by time_bucket('1week', timec)  WITH NO DATA;

--window function
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select avg(temperature) over( order by humidity)
from conditions
 WITH NO DATA;

--aggregate without combine function
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select json_agg(location)
from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;
;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature), array_agg(location)
from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;
;

-- userdefined aggregate without combine function
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), newavg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location WITH NO DATA;
;

-- using subqueries
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from
( select humidity, temperature, location, timec
from conditions ) q
 group by time_bucket('1week', timec) , location  WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
select * from
( Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location )  q WITH NO DATA;

--using limit /limit offset
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
limit 10  WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
offset 10 WITH NO DATA;

--using ORDER BY in view defintion
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
ORDER BY 1 WITH NO DATA;

--using FETCH
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
fetch first 10 rows only WITH NO DATA;

--using locking clauses FOR clause
--all should be disabled. we cannot guarntee locks on the hypertable
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR KEY SHARE WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR SHARE WITH NO DATA;


CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR UPDATE WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by time_bucket('1week', timec) , location
FOR NO KEY UPDATE WITH NO DATA;

--tablesample clause
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions tablesample bernoulli(0.2)
 group by time_bucket('1week', timec) , location
 WITH NO DATA;

-- ONLY in from clause
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from ONLY conditions
 group by time_bucket('1week', timec) , location  WITH NO DATA;

--grouping sets and variants
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
 group by grouping sets(time_bucket('1week', timec) , location )  WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), avg(temperature::int4)
from conditions
group by rollup(time_bucket('1week', timec) , location )  WITH NO DATA;

--NO immutable functions -- check all clauses
CREATE FUNCTION test_stablefunc(int) RETURNS int LANGUAGE 'sql'
       STABLE AS 'SELECT $1 + 10';

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), max(timec + INTERVAL '1h')
from conditions
group by time_bucket('1week', timec) , location   WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum(humidity), min(location)
from conditions
group by time_bucket('1week', timec)
having  max(timec + INTERVAL '1h') > '2010-01-01 09:00:00-08' WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum( test_stablefunc(humidity::int) ), min(location)
from conditions
group by time_bucket('1week', timec) WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum( temperature ), min(location)
from conditions
group by time_bucket('1week', timec), test_stablefunc(humidity::int) WITH NO DATA;

-- Should use CREATE MATERIALIZED VIEW to create continuous aggregates
CREATE VIEW continuous_aggs_errors_tbl1 WITH (timescaledb.continuous, timescaledb.finalized = false) AS
SELECT time_bucket('1 week', timec)
  FROM conditions
GROUP BY time_bucket('1 week', timec);

-- row security on table
create table rowsec_tab( a bigint, b integer, c integer);
select table_name from create_hypertable( 'rowsec_tab', 'a', chunk_time_interval=>10);
CREATE OR REPLACE FUNCTION integer_now_test() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(a), 0)::bigint FROM rowsec_tab $$;
SELECT set_integer_now_func('rowsec_tab', 'integer_now_test');
alter table rowsec_tab ENABLE ROW LEVEL SECURITY;
create policy rowsec_tab_allview ON rowsec_tab FOR SELECT USING(true);

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
Select sum( b), min(c)
from rowsec_tab
group by time_bucket('1', a) WITH NO DATA;

-- cagg on cagg not allowed
CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.finalized = false)
AS
SELECT time_bucket('1 day', timec) AS bucket
  FROM conditions
GROUP BY time_bucket('1 day', timec);

CREATE MATERIALIZED VIEW mat_m2_on_mat_m1 WITH (timescaledb.continuous)
AS
SELECT time_bucket('1 week', bucket) AS bucket
  FROM mat_m1
GROUP BY time_bucket('1 week', bucket);

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

create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec) WITH NO DATA;

SELECT  h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME",
       partial_view_name as "PART_VIEW_NAME",
       partial_view_schema as "PART_VIEW_SCHEMA"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'mat_with_test'
\gset

\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW mat_with_test SET(timescaledb.create_group_indexes = 'false');
ALTER MATERIALIZED VIEW mat_with_test SET(timescaledb.create_group_indexes = 'true');
ALTER MATERIALIZED VIEW mat_with_test ALTER timec DROP default;
\set ON_ERROR_STOP 1
\set VERBOSITY terse

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
create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec) WITH NO DATA;

create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec) WITH NO DATA;

ALTER TABLE conditions ALTER timec type int;

create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select time_bucket(100, timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket(100, timec) WITH NO DATA;

\set ON_ERROR_STOP 1
DROP TABLE conditions cascade;

CREATE TABLE conditions (
      timec       BIGINT       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null
    );

select table_name from create_hypertable( 'conditions', 'timec', chunk_time_interval=> 100);
CREATE OR REPLACE FUNCTION integer_now_test_b() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timec), 0)::bigint FROM conditions $$;
SELECT set_integer_now_func('conditions', 'integer_now_test_b');

create materialized view mat_with_test( timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.finalized = false)
as
select time_bucket(BIGINT '100', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by 1 WITH NO DATA;

-- custom time partition functions are not supported with invalidations
CREATE FUNCTION text_part_func(TEXT) RETURNS BIGINT
    AS $$ SELECT length($1)::BIGINT $$
    LANGUAGE SQL IMMUTABLE;

CREATE TABLE text_time(time TEXT);
    SELECT create_hypertable('text_time', 'time', chunk_time_interval => 10, time_partitioning_func => 'text_part_func');

\set VERBOSITY default
\set ON_ERROR_STOP 0
CREATE MATERIALIZED VIEW text_view
    WITH (timescaledb.continuous, timescaledb.finalized = false)
    AS SELECT time_bucket('5', text_part_func(time)), COUNT(time)
        FROM text_time
        GROUP BY 1 WITH NO DATA;
\set ON_ERROR_STOP 1

-- Check that we get an error when mixing normal materialized views
-- and continuous aggregates.
CREATE MATERIALIZED VIEW normal_mat_view AS
SELECT time_bucket('5', text_part_func(time)), COUNT(time)
  FROM text_time
GROUP BY 1 WITH NO DATA;
\set VERBOSITY terse

\set ON_ERROR_STOP 0
DROP MATERIALIZED VIEW normal_mat_view, mat_with_test;
\set ON_ERROR_STOP 1

DROP TABLE text_time CASCADE;

CREATE TABLE measurements (time TIMESTAMPTZ NOT NULL, device INT, value FLOAT);
SELECT create_hypertable('measurements', 'time');

INSERT INTO measurements VALUES ('2019-03-04 13:30', 1, 1.3);

-- Add a continuous aggregate on the measurements table and a policy
-- to be able to test error cases for the add_job function.
CREATE MATERIALIZED VIEW measurements_summary WITH (timescaledb.continuous, timescaledb.finalized = false) AS
SELECT time_bucket('1 day', time), COUNT(time)
  FROM measurements
GROUP BY 1 WITH NO DATA;

-- First test that add_job checks the config. It is currently possible
-- to add non-custom jobs using the add_job function so we need to
-- test that the function actually checks the config parameters. These
-- should all generate errors, for different reasons...
\set ON_ERROR_STOP 0
-- ... this one because it is missing a field.
SELECT add_job(
       '_timescaledb_internal.policy_refresh_continuous_aggregate'::regproc,
       '1 hour'::interval,
       check_config => '_timescaledb_internal.policy_refresh_continuous_aggregate_check'::regproc,
       config => '{"end_offset": null, "start_offset": null}');
-- ... this one because it has a bad value for start_offset
SELECT add_job(
       '_timescaledb_internal.policy_refresh_continuous_aggregate'::regproc,
       '1 hour'::interval,
       check_config => '_timescaledb_internal.policy_refresh_continuous_aggregate_check'::regproc,
       config => '{"end_offset": null, "start_offset": "1 fortnight", "mat_hypertable_id": 11}');
-- ... this one because it has a bad value for end_offset
SELECT add_job(
       '_timescaledb_internal.policy_refresh_continuous_aggregate'::regproc,
       '1 hour'::interval,
       check_config => '_timescaledb_internal.policy_refresh_continuous_aggregate_check'::regproc,
       config => '{"end_offset": "chicken", "start_offset": null, "mat_hypertable_id": 11}');
\set ON_ERROR_STOP 1

SELECT add_continuous_aggregate_policy('measurements_summary', NULL, NULL, '1 h'::interval) AS job_id
\gset

\x on
SELECT * FROM _timescaledb_config.bgw_job WHERE id = :job_id;
\x off

-- These are all weird values for the parameters for the continuous
-- aggregate jobs and should generate an error. Since the config will
-- be replaced, we will also generate error for missing arguments.
\set ON_ERROR_STOP 0
SELECT alter_job(:job_id, config => '{"end_offset": "1 week", "start_offset": "2 fortnights"}');
SELECT alter_job(:job_id,
       config => '{"mat_hypertable_id": 11, "end_offset": "chicken", "start_offset": "1 fortnights"}');
SELECT alter_job(:job_id,
       config => '{"mat_hypertable_id": 11, "end_offset": "chicken", "start_offset": "1 week"}');
\set ON_ERROR_STOP 1

DROP TABLE measurements CASCADE;
DROP TABLE conditions CASCADE;

-- test handling of invalid mat_hypertable_id
create table i2980(time timestamptz not null);
select create_hypertable('i2980','time');
create materialized view i2980_cagg with (timescaledb.continuous, timescaledb.finalized = false) AS SELECT time_bucket('1h',time), avg(7) FROM i2980 GROUP BY 1;
select add_continuous_aggregate_policy('i2980_cagg',NULL,NULL,'4h') AS job_id \gset
\set ON_ERROR_STOP 0
select alter_job(:job_id,config:='{"end_offset": null, "start_offset": null, "mat_hypertable_id": 1000}');

--test creating continuous aggregate with compression enabled --
CREATE MATERIALIZED VIEW  i2980_cagg2 with (timescaledb.continuous, timescaledb.compress, timescaledb.finalized = false)
AS SELECT time_bucket('1h',time), avg(7) FROM i2980 GROUP BY 1;

--this one succeeds
CREATE MATERIALIZED VIEW  i2980_cagg2 with (timescaledb.continuous, timescaledb.finalized = false)
AS SELECT time_bucket('1h',time) as bucket, avg(7) FROM i2980 GROUP BY 1;

--now enable compression with invalid parameters
ALTER MATERIALIZED VIEW i2980_cagg2 SET ( timescaledb.compress,
timescaledb.compress_segmentby = 'bucket');

ALTER MATERIALIZED VIEW i2980_cagg2 SET ( timescaledb.compress,
timescaledb.compress_orderby = 'bucket');

--enable compression and test re-enabling compression
ALTER MATERIALIZED VIEW i2980_cagg2 SET ( timescaledb.compress);
insert into i2980 select now();
call refresh_continuous_aggregate('i2980_cagg2', NULL, NULL);
SELECT compress_chunk(ch) FROM show_chunks('i2980_cagg2') ch;
ALTER MATERIALIZED VIEW i2980_cagg2 SET ( timescaledb.compress = 'false');
ALTER MATERIALIZED VIEW i2980_cagg2 SET ( timescaledb.compress = 'true');
ALTER MATERIALIZED VIEW i2980_cagg2 SET ( timescaledb.compress, timescaledb.compress_segmentby = 'bucket');

--Errors with compression policy on caggs--
select add_continuous_aggregate_policy('i2980_cagg2', interval '10 day', interval '2 day' ,'4h') AS job_id ;
SELECT add_compression_policy('i2980_cagg', '8 day'::interval);
ALTER MATERIALIZED VIEW i2980_cagg SET ( timescaledb.compress );
SELECT add_compression_policy('i2980_cagg', '8 day'::interval);

SELECT add_continuous_aggregate_policy('i2980_cagg2', '10 day'::interval, '6 day'::interval);
SELECT add_compression_policy('i2980_cagg2', '3 day'::interval);
SELECT add_compression_policy('i2980_cagg2', '1 day'::interval);
SELECT add_compression_policy('i2980_cagg2', '3'::integer);
SELECT add_compression_policy('i2980_cagg2', 13::integer);

SELECT materialization_hypertable_schema || '.' || materialization_hypertable_name AS "MAT_TABLE_NAME"
FROM timescaledb_information.continuous_aggregates
WHERE view_name = 'i2980_cagg2'
\gset
SELECT add_compression_policy( :'MAT_TABLE_NAME', 13::integer);

--TEST compressing cagg chunks without enabling compression
SELECT count(*) FROM (select decompress_chunk(ch) FROM show_chunks('i2980_cagg2') ch ) q;
ALTER MATERIALIZED VIEW i2980_cagg2 SET (timescaledb.compress = 'false');
SELECT compress_chunk(ch) FROM show_chunks('i2980_cagg2') ch;

-- test error handling when trying to create cagg on internal hypertable
CREATE TABLE comp_ht_test(time timestamptz NOT NULL);
SELECT table_name FROM create_hypertable('comp_ht_test','time');
ALTER TABLE comp_ht_test SET (timescaledb.compress);

SELECT
  format('%I.%I', ht.schema_name, ht.table_name) AS "INTERNALTABLE"
FROM
  _timescaledb_catalog.hypertable ht
  INNER JOIN _timescaledb_catalog.hypertable uncompress ON (ht.id = uncompress.compressed_hypertable_id
      AND uncompress.table_name = 'comp_ht_test') \gset

CREATE MATERIALIZED VIEW cagg1 WITH(timescaledb.continuous, timescaledb.finalized = false) AS SELECT time_bucket('1h',_ts_meta_min_1) FROM :INTERNALTABLE GROUP BY 1;
