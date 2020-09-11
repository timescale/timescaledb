-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test add and remove refresh policy apis

SET ROLE :ROLE_DEFAULT_PERM_USER;


--TEST1 ---
--basic test with count
CREATE TABLE int_tab (a integer, b integer, c integer);
SELECT table_name FROM create_hypertable('int_tab', 'a', chunk_time_interval=> 10);

INSERT INTO int_tab VALUES( 3 , 16 , 20);
INSERT INTO int_tab VALUES( 1 , 10 , 20);
INSERT INTO int_tab VALUES( 1 , 11 , 20);
INSERT INTO int_tab VALUES( 1 , 12 , 20);
INSERT INTO int_tab VALUES( 1 , 13 , 20);
INSERT INTO int_tab VALUES( 1 , 14 , 20);
INSERT INTO int_tab VALUES( 2 , 14 , 20);
INSERT INTO int_tab VALUES( 2 , 15 , 20);
INSERT INTO int_tab VALUES( 2 , 16 , 20);

CREATE OR REPLACE FUNCTION integer_now_int_tab() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(a), 0) FROM int_tab $$;
SELECT set_integer_now_func('int_tab', 'integer_now_int_tab');


CREATE MATERIALIZED VIEW mat_m1( a, countb )
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
SELECT a, count(b)
FROM int_tab
GROUP BY time_bucket(1, a), a WITH NO DATA;

\c :TEST_DBNAME :ROLE_SUPERUSER
DELETE FROM _timescaledb_config.bgw_job WHERE TRUE;

SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT count(*) FROM _timescaledb_config.bgw_job;

\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('int_tab', '1 day'::interval, 10 , '1 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '1 day'::interval, 10 , '1 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '1 day'::interval, 10 );
SELECT add_continuous_aggregate_policy('mat_m1', 10, '1 day'::interval, '1 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 20, 10, '1h'::interval) as job_id \gset

--adding again should warn/error
SELECT add_continuous_aggregate_policy('mat_m1', 20, 10, '1h'::interval, if_not_exists=>false);
SELECT add_continuous_aggregate_policy('mat_m1', 10, 20, '1h'::interval, if_not_exists=>true);
SELECT add_continuous_aggregate_policy('mat_m1', 20, 10, '1h'::interval, if_not_exists=>true);

-- modify config and try to add, should error out
SELECT config FROM _timescaledb_config.bgw_job where id = :job_id;
SELECT hypertable_id as mat_id FROM _timescaledb_config.bgw_job where id = :job_id \gset
\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_config.bgw_job
SET config = jsonb_build_object('mat_hypertable_id', :mat_id)
WHERE id = :job_id;
SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT config FROM _timescaledb_config.bgw_job where id = :job_id;
SELECT add_continuous_aggregate_policy('mat_m1', 20, 10, '1h'::interval, if_not_exists=>true);

SELECT remove_continuous_aggregate_policy('int_tab');
SELECT remove_continuous_aggregate_policy('mat_m1');
--this one will fail
SELECT remove_continuous_aggregate_policy('mat_m1');
SELECT remove_continuous_aggregate_policy('mat_m1', if_not_exists=>true);

--now try to add a policy as a different user than the one that created the cagg
--should fail
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT add_continuous_aggregate_policy('mat_m1', 20, 10, '1h'::interval) as job_id ;

SET ROLE :ROLE_DEFAULT_PERM_USER;
DROP MATERIALIZED VIEW mat_m1;

--- code coverage tests : add policy for timestamp and date based table ---
CREATE TABLE continuous_agg_max_mat_date(time DATE);
SELECT create_hypertable('continuous_agg_max_mat_date', 'time');
CREATE MATERIALIZED VIEW max_mat_view_date
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('7 days', time)
        FROM continuous_agg_max_mat_date
        GROUP BY 1 WITH NO DATA;

\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('max_mat_view_date', '2 days'::interval, 10 , '1 day'::interval);
\set ON_ERROR_STOP 1
SELECT add_continuous_aggregate_policy('max_mat_view_date', '2 day'::interval, '1 day'::interval , '1 day'::interval) as job_id \gset
SELECT config FROM _timescaledb_config.bgw_job
WHERE id = :job_id;

INSERT INTO continuous_agg_max_mat_date
    SELECT generate_series('2019-09-01'::date, '2019-09-10'::date, '1 day');
CALL run_job(:job_id);
DROP MATERIALIZED VIEW max_mat_view_date;

CREATE TABLE continuous_agg_timestamp(time TIMESTAMP);
SELECT create_hypertable('continuous_agg_timestamp', 'time');

CREATE MATERIALIZED VIEW max_mat_view_timestamp
    WITH (timescaledb.continuous, timescaledb.materialized_only=true)
    AS SELECT time_bucket('7 days', time)
        FROM continuous_agg_timestamp
        GROUP BY 1 WITH NO DATA;

SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', '10 day'::interval, '1 h'::interval , '1 h'::interval) as job_id \gset
CALL run_job(:job_id);

SELECT config FROM _timescaledb_config.bgw_job
WHERE id = :job_id;

\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_config.bgw_job
SET config = jsonb_build_object('mat_hypertable_id', :mat_id)
WHERE id = :job_id;

SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT config FROM _timescaledb_config.bgw_job where id = :job_id;
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', '10 day'::interval, '1 day'::interval, '1h'::interval, if_not_exists=>true);
\set ON_ERROR_STOP 1

DROP MATERIALIZED VIEW max_mat_view_timestamp;

--smallint table
CREATE TABLE smallint_tab (a smallint);
SELECT table_name FROM create_hypertable('smallint_tab', 'a', chunk_time_interval=> 10);
CREATE OR REPLACE FUNCTION integer_now_smallint_tab() returns smallint LANGUAGE SQL STABLE as $$ SELECT 20::smallint $$;
SELECT set_integer_now_func('smallint_tab', 'integer_now_smallint_tab');

CREATE MATERIALIZED VIEW mat_smallint( a, countb )
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
SELECT time_bucket( SMALLINT '1', a) , count(*)
FROM smallint_tab
GROUP BY 1 WITH NO DATA;
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_smallint', 15, 0 , '1 h'::interval);
SELECT add_continuous_aggregate_policy('mat_smallint', 98898::smallint , 0::smallint, '1 h'::interval);
\set ON_ERROR_STOP 1
SELECT add_continuous_aggregate_policy('mat_smallint', 15::smallint, 0::smallint , '1 h'::interval) as job_id \gset
INSERT INTO smallint_tab VALUES(5);
INSERT INTO smallint_tab VALUES(10);
INSERT INTO smallint_tab VALUES(20);
CALL run_job(:job_id);
SELECT * FROM mat_smallint;

\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_smallint', 15, 10, '1h'::interval, if_not_exists=>true);
\set ON_ERROR_STOP 1
DROP MATERIALIZED VIEW mat_smallint;

-- end of coverage tests
