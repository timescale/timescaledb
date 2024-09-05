-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT
       format('%s/results/%s_results_view.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_VIEW",
       format('%s/results/%s_results_view_hashagg.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_VIEW_HASHAGG",
       format('%s/results/%s_results_table.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_TABLE"
\gset
SELECT format('\! diff %s %s', :'TEST_RESULTS_VIEW', :'TEST_RESULTS_TABLE') as "DIFF_CMD",
      format('\! diff %s %s', :'TEST_RESULTS_VIEW_HASHAGG', :'TEST_RESULTS_TABLE') as "DIFF_CMD2"
\gset


\set EXPLAIN 'EXPLAIN (VERBOSE, COSTS OFF)'

SET client_min_messages TO NOTICE;

CREATE TABLE conditions (
      timec        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );

select table_name from create_hypertable( 'conditions', 'timec');

insert into conditions values ( '2018-01-01 09:20:00-08', 'SFO', 55, 45);
insert into conditions values ( '2018-01-02 09:30:00-08', 'por', 100, 100);
insert into conditions values ( '2018-01-02 09:20:00-08', 'SFO', 65, 45);
insert into conditions values ( '2018-01-02 09:10:00-08', 'NYC', 65, 45);
insert into conditions values ( '2018-11-01 09:20:00-08', 'NYC', 45, 30);
insert into conditions values ( '2018-11-01 10:40:00-08', 'NYC', 55, 35);
insert into conditions values ( '2018-11-01 11:50:00-08', 'NYC', 65, 40);
insert into conditions values ( '2018-11-01 12:10:00-08', 'NYC', 75, 45);
insert into conditions values ( '2018-11-01 13:10:00-08', 'NYC', 85, 50);
insert into conditions values ( '2018-11-02 09:20:00-08', 'NYC', 10, 10);
insert into conditions values ( '2018-11-02 10:30:00-08', 'NYC', 20, 15);
insert into conditions values ( '2018-11-02 11:40:00-08', 'NYC', null, null);
insert into conditions values ( '2018-11-03 09:50:00-08', 'NYC', null, null);

create table location_tab( locid integer, locname text );
insert into location_tab values( 1, 'SFO');
insert into location_tab values( 2, 'NYC');
insert into location_tab values( 3, 'por');

create materialized view mat_m1( location, timec, minl, sumt , sumh)
WITH (timescaledb.continuous, timescaledb.materialized_only=false)
as
select location, time_bucket('1day', timec), min(location), sum(temperature),sum(humidity)
from conditions
group by time_bucket('1day', timec), location WITH NO DATA;

--compute time_bucketted max+bucket_width for the materialized view
SELECT time_bucket('1day' , q.timeval+ '1day'::interval)
FROM ( select max(timec)as timeval from conditions ) as q;
CALL refresh_continuous_aggregate('mat_m1', NULL, NULL);

--test first/last
create materialized view mat_m2(location, timec, firsth, lasth, maxtemp, mintemp)
WITH (timescaledb.continuous, timescaledb.materialized_only=false)
as
select location, time_bucket('1day', timec), first(humidity, timec), last(humidity, timec), max(temperature), min(temperature)
from conditions
group by time_bucket('1day', timec), location WITH NO DATA;
--time that refresh assumes as now() for repeatability
SELECT time_bucket('1day' , q.timeval+ '1day'::interval)
FROM ( select max(timec)as timeval from conditions ) as q;
CALL refresh_continuous_aggregate('mat_m2', NULL, NULL);

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

-----------------------------------------------------------------------
-- Test the cagg_watermark function. The watermark gives the point
-- where to UNION raw and materialized data in real-time
-- aggregation. Specifically, test that the watermark caching works as
-- expected.
-----------------------------------------------------------------------

-- Insert some more data so that there is something to UNION in
-- real-time aggregation.

insert into conditions values ( '2018-12-02 20:10:00-08', 'SFO', 55, 45);
insert into conditions values ( '2018-12-02 21:20:00-08', 'SFO', 65, 45);
insert into conditions values ( '2018-12-02 20:30:00-08', 'NYC', 65, 45);
insert into conditions values ( '2018-12-02 21:50:00-08', 'NYC', 45, 30);

-- Test join of two caggs. Joining two caggs will force the cache to
-- reset every time the watermark function is invoked on a different
-- cagg in the same query.
SELECT mat_hypertable_id AS mat_id,
	   raw_hypertable_id AS raw_id,
	   schema_name AS mat_schema,
	   table_name AS mat_name,
	   format('%I.%I', schema_name, table_name) AS mat_table
FROM _timescaledb_catalog.continuous_agg ca, _timescaledb_catalog.hypertable h
WHERE user_view_name='mat_m1'
AND h.id = ca.mat_hypertable_id \gset

BEGIN;

-- Query without join
SELECT m1.location, m1.timec, sumt, sumh
FROM mat_m1 m1
ORDER BY m1.location COLLATE "C", m1.timec DESC
LIMIT 10;

-- Query that joins two caggs. This should force the watermark cache
-- to reset when the materialized hypertable ID changes. A hash join
-- could potentially read all values from mat_m1 then all values from
-- mat_m2. This would be the optimal situation for cagg_watermark
-- caching. We want to avoid it in tests to see that caching doesn't
-- do anything wrong in worse situations (e.g., a nested loop join).
SET enable_hashjoin=false;

SELECT m1.location, m1.timec, sumt, sumh, firsth, lasth, maxtemp, mintemp
FROM mat_m1 m1 RIGHT JOIN mat_m2 m2
ON (m1.location = m2.location
AND m1.timec = m2.timec)
ORDER BY m1.location COLLATE "C", m1.timec DESC
LIMIT 10;

-- Show the current watermark
SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_id));

-- The watermark should, in this case, be the same as the invalidation
-- threshold
SELECT _timescaledb_functions.to_timestamp(watermark)
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :raw_id;

-- The watermark is the end of materialization (end of last bucket)
-- while the MAX is the start of the last bucket
SELECT max(timec) FROM :mat_table;

-- Drop the most recent chunk
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = :'mat_name';

SELECT drop_chunks('mat_m1', newer_than=>'2018-01-01'::timestamptz);

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = :'mat_name';

-- The watermark should be updated to reflect the dropped data (i.e.,
-- the cache should be reset)
SELECT _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(:mat_id));

-- Since we removed the last chunk, the invalidation threshold doesn't
-- move back, while the watermark does.
SELECT _timescaledb_functions.to_timestamp(watermark)
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :raw_id;

-- Compare the new watermark to the MAX time in the table
SELECT max(timec) FROM :mat_table;

-- Try a subtransaction
SAVEPOINT clear_cagg;

SELECT m1.location, m1.timec, sumt, sumh, firsth, lasth, maxtemp, mintemp
FROM mat_m1 m1 RIGHT JOIN mat_m2 m2
ON (m1.location = m2.location
AND m1.timec = m2.timec)
ORDER BY m1.location COLLATE "C", m1.timec DESC
LIMIT 10;

ALTER MATERIALIZED VIEW mat_m1 SET (timescaledb.materialized_only=true);

SELECT m1.location, m1.timec, sumt, sumh, firsth, lasth, maxtemp, mintemp
FROM mat_m1 m1 RIGHT JOIN mat_m2 m2
ON (m1.location = m2.location
AND m1.timec = m2.timec)
ORDER BY m1.location COLLATE "C" NULLS LAST, m1.timec DESC NULLS LAST, firsth NULLS LAST,
         lasth NULLS LAST, mintemp NULLS LAST, maxtemp NULLS LAST
LIMIT 10;

ROLLBACK;

-----
-- Tests with time_bucket and offset/origin
-----
CREATE TABLE temperature (
  time timestamptz NOT NULL,
  value float
);

SELECT create_hypertable('temperature', 'time');

INSERT INTO temperature VALUES ('2000-01-01 01:00:00'::timestamptz, 5);

CREATE TABLE temperature_wo_tz (
  time timestamp NOT NULL,
  value float
);

SELECT create_hypertable('temperature_wo_tz', 'time');

INSERT INTO temperature_wo_tz VALUES ('2000-01-01 01:00:00'::timestamp, 5);

CREATE TABLE temperature_date (
  time date NOT NULL,
  value float
);

SELECT create_hypertable('temperature_date', 'time');

INSERT INTO temperature_date VALUES ('2000-01-01 01:00:00'::timestamp, 5);

-- Integer based tables
CREATE TABLE table_smallint (
  time smallint,
  data smallint
);

CREATE TABLE table_int (
  time int,
  data int
);

CREATE TABLE table_bigint (
  time bigint,
  data bigint
);

SELECT create_hypertable('table_smallint', 'time', chunk_time_interval => 10);
SELECT create_hypertable('table_int', 'time', chunk_time_interval => 10);
SELECT create_hypertable('table_bigint', 'time', chunk_time_interval => 10);

CREATE OR REPLACE FUNCTION integer_now_smallint() returns smallint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM table_smallint $$;
CREATE OR REPLACE FUNCTION integer_now_int() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM table_int $$;
CREATE OR REPLACE FUNCTION integer_now_bigint() returns bigint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM table_bigint $$;

SELECT set_integer_now_func('table_smallint', 'integer_now_smallint');
SELECT set_integer_now_func('table_int', 'integer_now_int');
SELECT set_integer_now_func('table_bigint', 'integer_now_bigint');

INSERT INTO table_smallint VALUES(1,2);
INSERT INTO table_int VALUES(1,2);
INSERT INTO table_bigint VALUES(1,2);

CREATE VIEW caggs_info AS
SELECT user_view_schema, user_view_name, bucket_func, bucket_width, bucket_origin, bucket_offset, bucket_timezone, bucket_fixed_width
FROM _timescaledb_catalog.continuous_aggs_bucket_function NATURAL JOIN _timescaledb_catalog.continuous_agg;

---
-- Tests with CAgg creation
---
CREATE MATERIALIZED VIEW cagg_4_hours
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours';
DROP MATERIALIZED VIEW cagg_4_hours;

CREATE MATERIALIZED VIEW cagg_4_hours_offset
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, '30m'::interval), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_offset';
DROP MATERIALIZED VIEW cagg_4_hours_offset;

CREATE MATERIALIZED VIEW cagg_4_hours_offset2
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, "offset"=>'30m'::interval), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_offset2';
DROP MATERIALIZED VIEW cagg_4_hours_offset2;

-- Variable buckets (timezone is provided) with offset
CREATE MATERIALIZED VIEW cagg_4_hours_offset_ts
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, "offset"=>'30m'::interval, timezone=>'UTC'), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_offset_ts';
DROP MATERIALIZED VIEW cagg_4_hours_offset_ts;

CREATE MATERIALIZED VIEW cagg_4_hours_origin
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, '2000-01-01 01:00:00 PST'::timestamptz), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_origin';
DROP MATERIALIZED VIEW cagg_4_hours_origin;

-- Using named parameter
CREATE MATERIALIZED VIEW cagg_4_hours_origin2
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, origin=>'2000-01-01 01:00:00 PST'::timestamptz), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_origin2';
DROP MATERIALIZED VIEW cagg_4_hours_origin2;

-- Variable buckets (timezone is provided) with origin
CREATE MATERIALIZED VIEW cagg_4_hours_origin_ts
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, origin=>'2000-01-01 01:00:00 PST'::timestamptz, timezone=>'UTC'), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_origin_ts';
DROP MATERIALIZED VIEW cagg_4_hours_origin_ts;

-- Without named parameter
CREATE MATERIALIZED VIEW cagg_4_hours_origin_ts2
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, 'UTC', '2000-01-01 01:00:00 PST'::timestamptz), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_origin_ts2';
DROP MATERIALIZED VIEW cagg_4_hours_origin_ts2;

-- Timestamp based CAggs
CREATE MATERIALIZED VIEW cagg_4_hours_wo_tz
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time), max(value)
    FROM temperature_wo_tz
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_wo_tz';

CREATE MATERIALIZED VIEW cagg_4_hours_origin_ts_wo_tz
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, '2000-01-01 01:00:00'::timestamp), max(value)
    FROM temperature_wo_tz
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_origin_ts_wo_tz';
DROP MATERIALIZED VIEW cagg_4_hours_origin_ts_wo_tz;

-- Variable buckets (timezone is provided) with origin
CREATE MATERIALIZED VIEW cagg_4_hours_origin_ts_wo_tz2
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, origin=>'2000-01-01 01:00:00'::timestamp), max(value)
    FROM temperature_wo_tz
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_origin_ts_wo_tz2';
DROP MATERIALIZED VIEW cagg_4_hours_origin_ts_wo_tz2;

CREATE MATERIALIZED VIEW cagg_4_hours_offset_wo_tz
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, "offset"=>'30m'::interval), max(value)
    FROM temperature_wo_tz
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_offset_wo_tz';
DROP MATERIALIZED VIEW cagg_4_hours_offset_wo_tz;
DROP MATERIALIZED VIEW cagg_4_hours_wo_tz;

-- Date based CAggs
CREATE MATERIALIZED VIEW cagg_4_hours_date
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 days', time), max(value)
    FROM temperature_date
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_date';
DROP MATERIALIZED VIEW cagg_4_hours_date;

CREATE MATERIALIZED VIEW cagg_4_hours_date_origin
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 days', time, '2000-01-01'::date), max(value)
    FROM temperature_date
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_date_origin';
DROP MATERIALIZED VIEW cagg_4_hours_date_origin;

CREATE MATERIALIZED VIEW cagg_4_hours_date_origin2
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 days', time, origin=>'2000-01-01'::date), max(value)
    FROM temperature_date
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_date_origin2';
DROP MATERIALIZED VIEW cagg_4_hours_date_origin2;

CREATE MATERIALIZED VIEW cagg_4_hours_date_offset
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 days', time, "offset"=>'30m'::interval), max(value)
    FROM temperature_date
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_4_hours_date_offset';
DROP MATERIALIZED VIEW cagg_4_hours_date_offset;

-- Integer based CAggs
CREATE MATERIALIZED VIEW cagg_smallint
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2', time), SUM(data) as value
        FROM table_smallint
        GROUP BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_smallint';
DROP MATERIALIZED VIEW cagg_smallint;

CREATE MATERIALIZED VIEW cagg_smallint_offset
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2', time, "offset"=>1::smallint), SUM(data) as value
        FROM table_smallint
        GROUP BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_smallint_offset';
DROP MATERIALIZED VIEW cagg_smallint_offset;

CREATE MATERIALIZED VIEW cagg_int
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2', time), SUM(data) as value
        FROM table_int
        GROUP BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_int';
DROP MATERIALIZED VIEW cagg_int;

CREATE MATERIALIZED VIEW cagg_int_offset
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2', time, "offset"=>1::int), SUM(data) as value
        FROM table_int
        GROUP BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_int_offset';
DROP MATERIALIZED VIEW cagg_int_offset;

CREATE MATERIALIZED VIEW cagg_bigint
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2', time), SUM(data) as value
        FROM table_bigint
        GROUP BY 1 WITH NO DATA;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_bigint';
DROP MATERIALIZED VIEW cagg_bigint;

CREATE MATERIALIZED VIEW cagg_bigint_offset
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2', time, "offset"=>1::bigint), SUM(data) as value
        FROM table_bigint
        GROUP BY 1 WITH NO DATA;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_bigint_offset';
DROP MATERIALIZED VIEW cagg_bigint_offset;

-- Without named parameter
CREATE MATERIALIZED VIEW cagg_bigint_offset2
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('2', time, 1::bigint), SUM(data) as value
        FROM table_bigint
        GROUP BY 1 WITH NO DATA;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_bigint_offset2';

-- mess with the bucket_func signature to make sure it will raise an exception
SET ROLE :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
BEGIN;
UPDATE _timescaledb_catalog.continuous_aggs_bucket_function SET bucket_func = 'func_does_not_exist()';
-- should error because function does not exist
CALL refresh_continuous_aggregate('cagg_bigint_offset2', NULL, NULL);
ROLLBACK;
\set ON_ERROR_STOP 1
SET ROLE :ROLE_DEFAULT_PERM_USER;

DROP MATERIALIZED VIEW cagg_bigint_offset2;

-- Test invalid bucket definitions
\set ON_ERROR_STOP 0
-- Offset and origin at the same time is not allowed (function does not exists)
CREATE MATERIALIZED VIEW cagg_4_hours_offset_and_origin
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, "offset"=>'30m'::interval, origin=>'2000-01-01 01:00:00 PST'::timestamptz), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;

-- Offset and origin at the same time is not allowed (function does exists but invalid parameter combination)
CREATE MATERIALIZED VIEW cagg_4_hours_offset_and_origin
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, "offset"=>'30m'::interval, origin=>'2000-01-01 01:00:00 PST'::timestamptz, timezone=>'UTC'), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;
\set ON_ERROR_STOP 1

---
-- Tests with CAgg processing
---

-- Check used timezone
SHOW timezone;

-- Populate it
INSERT INTO temperature
  SELECT time, 5
    FROM generate_series('2000-01-01 01:00:00 PST'::timestamptz,
                         '2000-01-01 23:59:59 PST','1m') time;

INSERT INTO temperature
  SELECT time, 6
    FROM generate_series('2020-01-01 00:00:00 PST'::timestamptz,
                         '2020-01-01 23:59:59 PST','1m') time;

-- Create CAggs
CREATE MATERIALIZED VIEW cagg_4_hours
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;

CREATE MATERIALIZED VIEW cagg_4_hours_offset
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, '30m'::interval), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;

-- Align origin with first value
CREATE MATERIALIZED VIEW cagg_4_hours_origin
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('4 hour', time, '2000-01-01 01:00:00 PST'::timestamptz), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;

-- Query the CAggs and check that all buckets are materialized
SELECT time_bucket('4 hour', time), max(value) FROM temperature GROUP BY 1 ORDER BY 1;
SELECT * FROM cagg_4_hours;
ALTER MATERIALIZED VIEW cagg_4_hours SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours;

SELECT time_bucket('4 hour', time, '30m'::interval), max(value) FROM temperature GROUP BY 1 ORDER BY 1;
SELECT * FROM cagg_4_hours_offset;
ALTER MATERIALIZED VIEW cagg_4_hours_offset SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours_offset;

SELECT time_bucket('4 hour', time, '2000-01-01 01:00:00 PST'::timestamptz), max(value) FROM temperature GROUP BY 1 ORDER BY 1;
SELECT * FROM cagg_4_hours_origin;
ALTER MATERIALIZED VIEW cagg_4_hours_origin SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours_origin;

-- Update the last bucket and re-materialize
INSERT INTO temperature values('2020-01-01 23:55:00 PST', 10);

CALL refresh_continuous_aggregate('cagg_4_hours', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_4_hours_offset', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_4_hours_origin', NULL, NULL);

SELECT * FROM cagg_4_hours;
SELECT * FROM cagg_4_hours_offset;
SELECT * FROM cagg_4_hours_origin;

-- Check the real-time functionality
ALTER MATERIALIZED VIEW cagg_4_hours SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW cagg_4_hours_offset SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW cagg_4_hours_origin SET (timescaledb.materialized_only=false);

-- Check watermarks
SELECT continuous_agg.user_view_name, continuous_aggs_watermark.watermark, _timescaledb_functions.to_timestamp(watermark)
  FROM _timescaledb_catalog.continuous_aggs_watermark
  JOIN _timescaledb_catalog.continuous_agg USING (mat_hypertable_id)
WHERE user_view_name LIKE 'cagg_4_hours%'
ORDER BY mat_hypertable_id, watermark;

-- Insert new data
INSERT INTO temperature values('2020-01-02 00:10:00 PST', 2222);
INSERT INTO temperature values('2020-01-02 05:35:00 PST', 5555);
INSERT INTO temperature values('2020-01-02 09:05:00 PST', 8888);

-- Watermark is at Thu Jan 02 00:00:00 2020 PST - all inserted tuples should be seen
SELECT * FROM cagg_4_hours;

-- Watermark is at Thu Jan 02 00:30:00 2020 PST - only two inserted tuples should be seen
SELECT * FROM cagg_4_hours_offset;

-- Watermark is at Thu Jan 02 01:00:00 2020 PST - only two inserted tuples should be seen
SELECT * FROM cagg_4_hours_origin;

-- Update materialized data
SET client_min_messages TO DEBUG1;
CALL refresh_continuous_aggregate('cagg_4_hours', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_4_hours_offset', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_4_hours_origin', NULL, NULL);
RESET client_min_messages;

-- Query the CAggs and check that all buckets are materialized
SELECT * FROM cagg_4_hours;
ALTER MATERIALIZED VIEW cagg_4_hours SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours;
SELECT time_bucket('4 hour', time), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

SELECT * FROM cagg_4_hours_offset;
ALTER MATERIALIZED VIEW cagg_4_hours_offset SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours_offset;
SELECT time_bucket('4 hour', time, '30m'::interval), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

SELECT * FROM cagg_4_hours_origin;
ALTER MATERIALIZED VIEW cagg_4_hours_origin SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours_origin;
SELECT time_bucket('4 hour', time, '2000-01-01 01:00:00 PST'::timestamptz), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

-- Test invalidations
TRUNCATE temperature;
CALL refresh_continuous_aggregate('cagg_4_hours', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_4_hours_offset', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_4_hours_origin', NULL, NULL);

INSERT INTO temperature
  SELECT time, 5
    FROM generate_series('2000-01-01 01:00:00 PST'::timestamptz,
                         '2000-01-01 23:59:59 PST','1m') time;

INSERT INTO temperature
  SELECT time, 6
    FROM generate_series('2020-01-01 00:00:00 PST'::timestamptz,
                         '2020-01-01 23:59:59 PST','1m') time;

INSERT INTO temperature values('2020-01-02 01:05:00+01', 2222);
INSERT INTO temperature values('2020-01-02 01:35:00+01', 5555);
INSERT INTO temperature values('2020-01-02 05:05:00+01', 8888);

SET client_min_messages TO DEBUG1;
CALL refresh_continuous_aggregate('cagg_4_hours', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_4_hours_offset', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_4_hours_origin', NULL, NULL);
RESET client_min_messages;

ALTER MATERIALIZED VIEW cagg_4_hours SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours;
ALTER MATERIALIZED VIEW cagg_4_hours SET (timescaledb.materialized_only=false);
SELECT * FROM cagg_4_hours;
SELECT time_bucket('4 hour', time), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

ALTER MATERIALIZED VIEW cagg_4_hours_offset SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours_offset;
ALTER MATERIALIZED VIEW cagg_4_hours_offset SET (timescaledb.materialized_only=false);
SELECT * FROM cagg_4_hours_offset;
SELECT time_bucket('4 hour', time, '30m'::interval), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

ALTER MATERIALIZED VIEW cagg_4_hours_origin SET (timescaledb.materialized_only=true);
SELECT * FROM cagg_4_hours_origin;
ALTER MATERIALIZED VIEW cagg_4_hours_origin SET (timescaledb.materialized_only=false);
SELECT * FROM cagg_4_hours_origin;
SELECT time_bucket('4 hour', time, '2000-01-01 01:00:00 PST'::timestamptz), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

--- Test with variable width buckets (use February, since hourly origins are not supported with variable sized buckets)
TRUNCATE temperature;
INSERT INTO temperature
  SELECT time, 5
    FROM generate_series('2000-02-01 01:00:00 PST'::timestamptz,
                         '2000-02-01 23:59:59 PST','1m') time;

INSERT INTO temperature
  SELECT time, 6
    FROM generate_series('2020-02-01 01:00:00 PST'::timestamptz,
                         '2020-02-01 23:59:59 PST','1m') time;

SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log ORDER BY 1, 2, 3;

CREATE MATERIALIZED VIEW cagg_1_year
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
  SELECT time_bucket('1 year', time), max(value)
    FROM temperature
    GROUP BY 1 ORDER BY 1;

SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log ORDER BY 1, 2, 3;

---
-- Tests with integer based hypertables
---
TRUNCATE table_int;

INSERT INTO table_int
  SELECT time, 5
    FROM generate_series(-50, 50) time;

CREATE MATERIALIZED VIEW cagg_int
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('10', time), SUM(data) as value
        FROM table_int
        GROUP BY 1 ORDER BY 1;

CREATE MATERIALIZED VIEW cagg_int_offset
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('10', time, "offset"=>5), SUM(data) as value
        FROM table_int
        GROUP BY 1 ORDER BY 1;

-- Compare bucketing results
SELECT time_bucket('10', time), SUM(data) FROM table_int GROUP BY 1 ORDER BY 1;
SELECT * FROM cagg_int;

SELECT time_bucket('10', time, "offset"=>5), SUM(data) FROM table_int GROUP BY 1 ORDER BY 1;
SELECT * FROM cagg_int_offset;

-- Update table
INSERT INTO table_int VALUES(51, 100);
INSERT INTO table_int VALUES(100, 555);

-- Compare bucketing results
SELECT time_bucket('10', time), SUM(data) FROM table_int GROUP BY 1 ORDER BY 1;
SELECT * FROM cagg_int;
CALL refresh_continuous_aggregate('cagg_int', NULL, NULL);
SELECT * FROM cagg_int;

SELECT time_bucket('10', time, "offset"=>5), SUM(data) FROM table_int GROUP BY 1 ORDER BY 1;
SELECT * FROM cagg_int_offset;  -- the value 100 is part of the already serialized bucket, so it should not be visible
CALL refresh_continuous_aggregate('cagg_int_offset', NULL, NULL);
SELECT * FROM cagg_int_offset;

-- Ensure everything was materialized
ALTER MATERIALIZED VIEW cagg_int SET (timescaledb.materialized_only=true);
ALTER MATERIALIZED VIEW cagg_int_offset SET (timescaledb.materialized_only=true);

SELECT * FROM cagg_int;
SELECT * FROM cagg_int_offset;

-- Check that the refresh is properly aligned
INSERT INTO table_int VALUES(114, 0);

SET client_min_messages TO DEBUG1;
CALL refresh_continuous_aggregate('cagg_int_offset', 110, 130);
RESET client_min_messages;

SELECT * FROM cagg_int_offset;
SELECT time_bucket('10', time, "offset"=>5), SUM(data) FROM table_int GROUP BY 1 ORDER BY 1;

-- Variable sized buckets with origin
CREATE MATERIALIZED VIEW cagg_1_hour_variable_bucket_fixed_origin
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 year', time, origin=>'2000-01-01 01:05:00 UTC'::timestamptz, timezone=>'UTC') AS hour_bucket, max(value) AS max_value
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_1_hour_variable_bucket_fixed_origin';
DROP MATERIALIZED VIEW cagg_1_hour_variable_bucket_fixed_origin;

-- Variable due to the used timezone
CREATE MATERIALIZED VIEW cagg_1_hour_variable_bucket_fixed_origin2
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 hour', time, origin=>'2000-01-01 01:05:00 UTC'::timestamptz, timezone=>'UTC') AS hour_bucket, max(value) AS max_value
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_1_hour_variable_bucket_fixed_origin2';
DROP MATERIALIZED VIEW cagg_1_hour_variable_bucket_fixed_origin2;

-- Variable with offset
CREATE MATERIALIZED VIEW cagg_1_hour_variable_bucket_fixed_origin3
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 year', time, "offset"=>'5 minutes'::interval) AS hour_bucket, max(value) AS max_value
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_1_hour_variable_bucket_fixed_origin3';
DROP MATERIALIZED VIEW cagg_1_hour_variable_bucket_fixed_origin3;

---
-- Test with blocking a few broken configurations
---
\set ON_ERROR_STOP 0

-- Unfortunately '\set VERBOSITY verbose' cannot be used here to check the error details
-- since it also prints the line number of the location, which is depended on the build

-- Different time origin
CREATE MATERIALIZED VIEW cagg_1_hour_origin
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 hour', time, origin=>'2000-01-02 01:00:00 PST'::timestamptz) AS hour_bucket, max(value) AS max_value
    FROM temperature
    GROUP BY 1 ORDER BY 1;

CREATE MATERIALIZED VIEW cagg_1_week_origin
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 week', hour_bucket, origin=>'2022-01-02 01:00:00 PST'::timestamptz) AS week_bucket, max(max_value) AS max_value
    FROM cagg_1_hour_origin
    GROUP BY 1 ORDER BY 1;

-- Different time offset
CREATE MATERIALIZED VIEW cagg_1_hour_offset
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 hour', time, "offset"=>'30m'::interval) AS hour_bucket, max(value) AS max_value
    FROM temperature
    GROUP BY 1 ORDER BY 1;

CREATE MATERIALIZED VIEW cagg_1_week_offset
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 week', hour_bucket, "offset"=>'35m'::interval) AS week_bucket, max(max_value) AS max_value
    FROM cagg_1_hour_offset
    GROUP BY 1 ORDER BY 1;

-- Different integer offset
CREATE MATERIALIZED VIEW cagg_int_offset_5
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('10', time, "offset"=>5) AS time, SUM(data) AS value
        FROM table_int
        GROUP BY 1 ORDER BY 1;

CREATE MATERIALIZED VIEW cagg_int_offset_10
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS SELECT time_bucket('10', time, "offset"=>10) AS time, SUM(value) AS value
        FROM cagg_int_offset_5
        GROUP BY 1 ORDER BY 1;

\set ON_ERROR_STOP 1

DROP MATERIALIZED VIEW cagg_1_hour_origin;
DROP MATERIALIZED VIEW cagg_1_hour_offset;
DROP MATERIALIZED VIEW cagg_int_offset_5;

---
-- CAGGs on CAGGs tests
---
CREATE MATERIALIZED VIEW cagg_1_hour_offset
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 hour', time, origin=>'2000-01-02 01:00:00 PST'::timestamptz) AS hour_bucket, max(value) AS max_value
    FROM temperature
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_1_hour_offset';

CREATE MATERIALIZED VIEW cagg_1_week_offset
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 week', hour_bucket, origin=>'2000-01-02 01:00:00 PST'::timestamptz) AS week_bucket, max(max_value) AS max_value
    FROM cagg_1_hour_offset
    GROUP BY 1 ORDER BY 1;
SELECT * FROM caggs_info WHERE user_view_name = 'cagg_1_week_offset';

-- Compare output
SELECT * FROM cagg_1_week_offset;
SELECT time_bucket('1 week', time, origin=>'2000-01-02 01:00:00 PST'::timestamptz), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

INSERT INTO temperature values('2030-01-01 05:05:00 PST', 22222);
INSERT INTO temperature values('2030-01-03 05:05:00 PST', 55555);

-- Compare real-time functionality
ALTER MATERIALIZED VIEW cagg_1_hour_offset SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW cagg_1_week_offset SET (timescaledb.materialized_only=false);

SELECT * FROM cagg_1_week_offset;
SELECT time_bucket('1 week', time, origin=>'2000-01-02 01:00:00 PST'::timestamptz), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

-- Test refresh
CALL refresh_continuous_aggregate('cagg_1_hour_offset', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_1_week_offset', NULL, NULL);

-- Everything should be now materailized
ALTER MATERIALIZED VIEW cagg_1_hour_offset SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW cagg_1_week_offset SET (timescaledb.materialized_only=false);

SELECT * FROM cagg_1_week_offset;
SELECT time_bucket('1 week', time, origin=>'2000-01-02 01:00:00 PST'::timestamptz), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

TRUNCATE temperature;

SELECT * FROM cagg_1_week_offset;
SELECT time_bucket('1 week', time, origin=>'2000-01-02 01:00:00 PST'::timestamptz), max(value) FROM temperature GROUP BY 1 ORDER BY 1;

DROP VIEW caggs_info;
