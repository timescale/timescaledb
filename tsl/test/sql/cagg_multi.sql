-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_PERM_USER;
SET client_min_messages TO LOG;

CREATE TABLE continuous_agg_test(timeval integer, col1 integer, col2 integer);
select create_hypertable('continuous_agg_test', 'timeval', chunk_time_interval=> 2);
CREATE OR REPLACE FUNCTION integer_now_test1() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timeval), 0) FROM continuous_agg_test $$;
SELECT set_integer_now_func('continuous_agg_test', 'integer_now_test1');

INSERT INTO continuous_agg_test VALUES
    (10, - 4, 1), (11, - 3, 5), (12, - 3, 7), (13, - 3, 9), (14,-4, 11),
    (15, -4, 22), (16, -4, 23);

-- TEST for multiple continuous aggs
--- invalidations are picked up by both caggs
CREATE MATERIALIZED VIEW cagg_1( timed, cnt )
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
    SELECT time_bucket( 2, timeval), COUNT(col1)
    FROM continuous_agg_test
    GROUP BY 1 WITH NO DATA;

CREATE MATERIALIZED VIEW cagg_2( timed, grp, maxval)
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
    SELECT time_bucket(2, timeval), col1, max(col2)
    FROM continuous_agg_test
    GROUP BY 1, 2 WITH NO DATA;

select view_name, view_owner, materialization_hypertable_name
from timescaledb_information.continuous_aggregates ORDER BY 1;

--TEST1: cagg_1 is materialized, not cagg_2.
CALL refresh_continuous_aggregate('cagg_1', NULL, NULL);
select * from cagg_1 order by 1;
SELECT time_bucket(2, timeval), COUNT(col1) as value
FROM continuous_agg_test
GROUP BY 1 order by 1;

-- check that cagg_2 not materialized
select * from cagg_2 order by 1,2;
CALL refresh_continuous_aggregate('cagg_2', NULL, NULL);
select * from cagg_2 order by 1,2;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

--TEST2: cagg_2 gets invalidations after cagg_1's refresh
--will trigger invalidations
INSERT INTO continuous_agg_test VALUES
    (10, -4, 10), (11, - 3, 50), (11, - 3, 70), (10, - 4, 10);

SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
CALL refresh_continuous_aggregate('cagg_1', NULL, NULL);
select * from cagg_1 order by 1;
SELECT time_bucket(2, timeval), COUNT(col1) as value
FROM continuous_agg_test
GROUP BY 1 order by 1;

-- are the invalidations picked up here?
select * from cagg_2 order by 1, 2;
SELECT time_bucket(2, timeval), col1, max(col2)
FROM continuous_agg_test
GROUP BY 1, 2
order by 1,2 ;
CALL refresh_continuous_aggregate('cagg_2', NULL, NULL);
select * from cagg_2 order by 1, 2;

--TEST3: invalidations left over by cagg_1 are dropped
--trigger another invalidation
INSERT INTO continuous_agg_test VALUES
    (10, -4, 1000);
select count(*) from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
select count(*) from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
CALL refresh_continuous_aggregate('cagg_1', NULL, NULL);
select count(*) from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
select count(*) from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

--now drop cagg_1, should still have materialization_invalidation_log
DROP MATERIALIZED VIEW cagg_1;
select count(*) from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
--cagg_2 still exists
select view_name from timescaledb_information.continuous_aggregates;
drop table continuous_agg_test cascade;
select count(*) from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
select view_name from timescaledb_information.continuous_aggregates;

--TEST4: invalidations that are copied over by cagg1 are not deleted by cagg2 refresh if
-- they do not meet materialization invalidation threshold for cagg2.
CREATE TABLE continuous_agg_test(timeval integer, col1 integer, col2 integer);
select create_hypertable('continuous_agg_test', 'timeval', chunk_time_interval=> 2);
CREATE OR REPLACE FUNCTION integer_now_test1() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timeval), 0) FROM continuous_agg_test $$;
SELECT set_integer_now_func('continuous_agg_test', 'integer_now_test1');

INSERT INTO continuous_agg_test VALUES
    (10, -4, 1), (11, -3, 5), (12, -3, 7), (13, -3, 9), (14, -4, 11),
    (15, -4, 22), (16, -4, 23);

CREATE MATERIALIZED VIEW cagg_1( timed, cnt )
WITH (timescaledb.continuous,
      timescaledb.materialized_only = true)
AS
    SELECT time_bucket( 2, timeval), COUNT(col1)
    FROM continuous_agg_test
    GROUP BY 1 WITH DATA;
CREATE MATERIALIZED VIEW cagg_2( timed, maxval)
WITH (timescaledb.continuous,
      timescaledb.materialized_only = true)
AS
    SELECT time_bucket(2, timeval), max(col2)
    FROM continuous_agg_test
    GROUP BY 1 WITH NO DATA;

--cagg_1 already refreshed on creation
select * from cagg_1 order by 1;
--partially refresh cagg_2 to 14 to test inserts that won't be seen in
--one of the caggs
CALL refresh_continuous_aggregate('cagg_2', NULL, 14);
select * from cagg_2 order by 1;
--this insert will be processed only by cagg_1 and copied over to cagg_2
insert into continuous_agg_test values( 14, -2, 100);
CALL refresh_continuous_aggregate('cagg_1', NULL, NULL);
select * from cagg_1 order by 1;
CALL refresh_continuous_aggregate('cagg_2', NULL, 14);
select * from cagg_2 order by 1;
SET ROLE :ROLE_SUPERUSER;
select * from _timescaledb_catalog.continuous_aggs_invalidation_threshold order by 1;
select * from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log order by 1;
SET ROLE :ROLE_DEFAULT_PERM_USER;
--this insert will be processed only by cagg_1 and cagg_2 will process the previous
--one
insert into continuous_agg_test values( 18, -2, 200);
CALL refresh_continuous_aggregate('cagg_1', NULL, NULL);
select * from cagg_1 order by 1;
-- Now make changes visible up to 16
CALL refresh_continuous_aggregate('cagg_2', NULL, 16);
select * from cagg_2 order by 1;

--TEST5 2 inserts with the same value can be copied over to materialization invalidation log
insert into continuous_agg_test values( 18, -2, 100);
insert into continuous_agg_test values( 18, -2, 100);
select * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log order by 1;
CALL refresh_continuous_aggregate('cagg_1', NULL, NULL);
select * from cagg_1 where timed = 18 ;
--copied over for cagg_2 to process later?
select * from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log order by 1;
DROP MATERIALIZED VIEW cagg_1;
DROP MATERIALIZED VIEW cagg_2;

----TEST7 multiple continuous aggregates with real time aggregates test----
create table foo (a integer, b integer, c integer);
select table_name FROM create_hypertable('foo', 'a', chunk_time_interval=> 10);

INSERT into foo values( 1 , 10 , 20);
INSERT into foo values( 1 , 11 , 20);
INSERT into foo values( 1 , 12 , 20);
INSERT into foo values( 1 , 13 , 20);
INSERT into foo values( 1 , 14 , 20);
INSERT into foo values( 5 , 14 , 20);
INSERT into foo values( 5 , 15 , 20);
INSERT into foo values( 5 , 16 , 20);
INSERT into foo values( 20 , 16 , 20);
INSERT into foo values( 20 , 26 , 20);
INSERT into foo values( 20 , 16 , 20);
INSERT into foo values( 21 , 15 , 30);
INSERT into foo values( 21 , 15 , 30);
INSERT into foo values( 21 , 15 , 30);
INSERT into foo values( 45 , 14 , 70);

CREATE OR REPLACE FUNCTION integer_now_foo() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(a), 0) FROM foo $$;
SELECT set_integer_now_func('foo', 'integer_now_foo');

CREATE MATERIALIZED VIEW mat_m1(a, countb)
WITH (timescaledb.continuous)
AS
SELECT time_bucket(10, a), count(*)
FROM foo
GROUP BY time_bucket(10, a) WITH NO DATA;

CREATE MATERIALIZED VIEW mat_m2(a, countb)
WITH (timescaledb.continuous)
AS
SELECT time_bucket(5, a), count(*)
FROM foo
GROUP BY time_bucket(5, a) WITH NO DATA;

select view_name, materialized_only from timescaledb_information.continuous_aggregates
WHERE view_name::text like 'mat_m%'
order by view_name;

CALL refresh_continuous_aggregate('mat_m1', NULL, 35);
CALL refresh_continuous_aggregate('mat_m2', NULL, NULL);

-- the results from the view should match the direct query
SELECT * from mat_m1 order by 1;
SELECT time_bucket(5, a), count(*)
FROM foo
GROUP BY time_bucket(5, a)
ORDER BY 1;

SELECT * from mat_m2 order by 1;
SELECT time_bucket(10, a), count(*)
FROM foo
GROUP BY time_bucket(10, a)
ORDER BY 1;
