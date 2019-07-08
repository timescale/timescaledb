-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


-- stop the continous aggregate background workers from interfering
\c :TEST_DBNAME :ROLE_SUPERUSER

-- stop the continous aggregate background workers from interfering
SELECT _timescaledb_internal.stop_background_workers();
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER


CREATE TABLE continuous_agg_test(timeval integer, col1 integer, col2 integer);
select create_hypertable('continuous_agg_test', 'timeval', chunk_time_interval=> 2);


INSERT INTO continuous_agg_test VALUES
    (10, - 4, 1), (11, - 3, 5), (12, - 3, 7), (13, - 3, 9), (14,-4, 11),
    (15, -4, 22), (16, -4, 23);

-- TEST for multiple continuous aggs 
--- invalidations are picked up by both caggs
CREATE VIEW cagg_1( timed, cnt ) 
WITH ( timescaledb.continuous , timescaledb.refresh_lag = '-2')
AS
    SELECT time_bucket( 2, timeval), COUNT(col1) 
    FROM continuous_agg_test
    GROUP BY 1;

CREATE VIEW cagg_2( timed, grp, maxval) 
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '-2' )
AS
    SELECT time_bucket(2, timeval), col1, max(col2) 
    FROM continuous_agg_test
    GROUP BY 1, 2;

select view_name, view_owner, refresh_lag, refresh_interval, max_interval_per_job , materialization_hypertable 
from timescaledb_information.continuous_aggregates;

--TEST1: cagg_1 is materialized, not cagg_2.
refresh materialized view cagg_1;
select * from cagg_1 order by 1;
SELECT time_bucket(2, timeval), COUNT(col1) as value
FROM continuous_agg_test
GROUP BY 1 order by 1;

-- check that cagg_2 not materialized 
select * from cagg_2 order by 1,2;

refresh materialized view cagg_2;
select * from cagg_2 order by 1,2;
SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;

--TEST2: cagg_2 gets invalidations after cagg_1's refresh
--will trigger invalidations 
INSERT INTO continuous_agg_test VALUES
    (10, -4, 10), (11, - 3, 50), (11, - 3, 70), (10, - 4, 10);

SELECT * FROM _timescaledb_catalog.continuous_aggs_completed_threshold;
SELECT * FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold;
refresh materialized view cagg_1;
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
refresh materialized view cagg_2;
select * from cagg_2 order by 1, 2;

--TEST3: invalidations left over by cagg_1 are dropped
--trigger another invalidation
INSERT INTO continuous_agg_test VALUES
    (10, -4, 1000);
select count(*) from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
select count(*) from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
refresh materialized view cagg_1;
select count(*) from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
select count(*) from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

--now drop cagg_1, should still have materialization_invalidation_log
drop view cagg_1 cascade;
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

INSERT INTO continuous_agg_test VALUES
    (10, - 4, 1), (11, - 3, 5), (12, - 3, 7), (13, - 3, 9), (14,-4, 11),
    (15, -4, 22), (16, -4, 23);

CREATE VIEW cagg_1( timed, cnt ) 
WITH ( timescaledb.continuous , timescaledb.refresh_lag = '-2')
AS
    SELECT time_bucket( 2, timeval), COUNT(col1) 
    FROM continuous_agg_test
    GROUP BY 1;
CREATE VIEW cagg_2( timed, maxval) 
WITH ( timescaledb.continuous, timescaledb.refresh_lag = '2' )
AS
    SELECT time_bucket(2, timeval), max(col2) 
    FROM continuous_agg_test
    GROUP BY 1;
refresh materialized view cagg_1;
select * from cagg_1 order by 1;
refresh materialized view cagg_2;
select * from cagg_2 order by 1;
--this insert will be processed only by cagg_1 and copied over to cagg_2
insert into continuous_agg_test values( 14, -2, 100); 
refresh materialized view cagg_1;
select * from cagg_1 order by 1;
refresh materialized view cagg_2;
select * from cagg_2 order by 1;
\c :TEST_DBNAME :ROLE_SUPERUSER
select * from _timescaledb_catalog.continuous_aggs_invalidation_threshold order by 1;
select * from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log order by 1;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
--this insert will be processed only by cagg_1 and cagg_2 will process the previous
--one
insert into continuous_agg_test values( 18, -2, 200); 
refresh materialized view cagg_1;
select * from cagg_1 order by 1;
refresh materialized view cagg_2;
select * from cagg_2 order by 1;

--TEST5 2 inserts with the same value can be copied over to materialization invalidation log
insert into continuous_agg_test values( 18, -2, 100); 
insert into continuous_agg_test values( 18, -2, 100); 
select * from _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log order by 1;
refresh materialized view cagg_1;
select * from cagg_1 where timed = 18 ;
--copied over for cagg_2 to process later?
select * from _timescaledb_catalog.continuous_aggs_materialization_invalidation_log order by 1;
 
