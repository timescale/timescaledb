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
\set VERBOSITY default
SELECT add_continuous_aggregate_policy('int_tab', '1 day'::interval, 10 , '1 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '1 day'::interval, 10 , '1 h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', '1 day'::interval, 10 );
SELECT add_continuous_aggregate_policy('mat_m1', 10, '1 day'::interval, '1 h'::interval);
--start_interval < end_interval
SELECT add_continuous_aggregate_policy('mat_m1', 5, 10, '1h'::interval);
--refresh window less than two buckets
SELECT add_continuous_aggregate_policy('mat_m1', 11, 10, '1h'::interval);
SELECT add_continuous_aggregate_policy('mat_m1', 20, 10, '1h'::interval) as job_id \gset

--adding again should warn/error
SELECT add_continuous_aggregate_policy('mat_m1', 20, 10, '1h'::interval, if_not_exists=>false);
SELECT add_continuous_aggregate_policy('mat_m1', 20, 15, '1h'::interval, if_not_exists=>true);
SELECT add_continuous_aggregate_policy('mat_m1', 20, 10, '1h'::interval, if_not_exists=>true);

-- modify config and try to add, should error out
SELECT config FROM _timescaledb_config.bgw_job where id = :job_id;
SELECT hypertable_id as mat_id FROM _timescaledb_config.bgw_job where id = :job_id \gset
\set VERBOSITY terse
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_config.bgw_job
SET config = jsonb_build_object('mat_hypertable_id', :mat_id)
WHERE id = :job_id;
SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT config FROM _timescaledb_config.bgw_job where id = :job_id;

\set ON_ERROR_STOP 0
\set VERBOSITY default
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
\set VERBOSITY terse
\set ON_ERROR_STOP 1

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
\set VERBOSITY default
SELECT add_continuous_aggregate_policy('max_mat_view_date', '2 days', 10, '1 day'::interval);
--start_interval < end_interval
SELECT add_continuous_aggregate_policy('max_mat_view_date', '1 day'::interval, '2 days'::interval , '1 day'::interval) ;
--interval less than two buckets
SELECT add_continuous_aggregate_policy('max_mat_view_date', '7 days', '1 day', '1 day'::interval);
SELECT add_continuous_aggregate_policy('max_mat_view_date', '14 days', '1 day', '1 day'::interval);
SELECT add_continuous_aggregate_policy('max_mat_view_date', '13 days', '-10 hours', '1 day'::interval);
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-- Negative start offset gives two bucket window:
SELECT add_continuous_aggregate_policy('max_mat_view_date', '13 days', '-1 day', '1 day'::interval);
SELECT remove_continuous_aggregate_policy('max_mat_view_date');
-- Both offsets NULL:
SELECT add_continuous_aggregate_policy('max_mat_view_date', NULL, NULL, '1 day'::interval);
SELECT remove_continuous_aggregate_policy('max_mat_view_date');

SELECT add_continuous_aggregate_policy('max_mat_view_date', '15 days', '1 day', '1 day'::interval) as job_id \gset
SELECT config FROM _timescaledb_config.bgw_job
WHERE id = :job_id;

INSERT INTO continuous_agg_max_mat_date
	SELECT generate_series('2019-09-01'::date, '2019-09-10'::date, '1 day');
--- to prevent NOTICES set message level to warning
SET client_min_messages TO warning;
CALL run_job(:job_id);
RESET client_min_messages;
DROP MATERIALIZED VIEW max_mat_view_date;

CREATE TABLE continuous_agg_timestamp(time TIMESTAMP);
SELECT create_hypertable('continuous_agg_timestamp', 'time');

CREATE MATERIALIZED VIEW max_mat_view_timestamp
	WITH (timescaledb.continuous, timescaledb.materialized_only=true)
	AS SELECT time_bucket('7 days', time)
		FROM continuous_agg_timestamp
		GROUP BY 1 WITH NO DATA;

--the start offset overflows the smallest time value, but is capped at
--the min value
SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', '1000000 years', '1 day' , '1 h'::interval);
SELECT remove_continuous_aggregate_policy('max_mat_view_timestamp');

\set ON_ERROR_STOP 0
\set VERBOSITY default
--start and end offset capped at the lowest time value, which means
--zero size window
SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', '1000000 years', '900000 years' , '1 h'::interval);
SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', '301 days', '10 months' , '1 h'::interval);
\set VERBOSITY terse
\set ON_ERROR_STOP 1

SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', '15 days', '1 h'::interval , '1 h'::interval) as job_id \gset

--- to prevent NOTICES set message level to warning
SET client_min_messages TO warning;
CALL run_job(:job_id);
RESET client_min_messages ;

SELECT config FROM _timescaledb_config.bgw_job
WHERE id = :job_id;

\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_config.bgw_job
SET config = jsonb_build_object('mat_hypertable_id', :mat_id)
WHERE id = :job_id;

SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT config FROM _timescaledb_config.bgw_job where id = :job_id;
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', '15 day', '1 day', '1h'::interval, if_not_exists=>true);
SELECT add_continuous_aggregate_policy('max_mat_view_timestamp', 'xyz', '1 day', '1h'::interval, if_not_exists=>true);
\set ON_ERROR_STOP 1

DROP MATERIALIZED VIEW max_mat_view_timestamp;

--smallint table
CREATE TABLE smallint_tab (a smallint);
SELECT table_name FROM create_hypertable('smallint_tab', 'a', chunk_time_interval=> 10);
CREATE OR REPLACE FUNCTION integer_now_smallint_tab() returns smallint LANGUAGE SQL STABLE as $$ SELECT coalesce(max(a)::smallint, 0::smallint) FROM smallint_tab ; $$;
SELECT set_integer_now_func('smallint_tab', 'integer_now_smallint_tab');

CREATE MATERIALIZED VIEW mat_smallint( a, countb )
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
SELECT time_bucket( SMALLINT '1', a) , count(*)
FROM smallint_tab
GROUP BY 1 WITH NO DATA;
\set ON_ERROR_STOP 0
\set VERBOSITY default
SELECT add_continuous_aggregate_policy('mat_smallint', 15, 0 , '1 h'::interval);
SELECT add_continuous_aggregate_policy('mat_smallint', 98898::smallint , 0::smallint, '1 h'::interval);
SELECT add_continuous_aggregate_policy('mat_smallint', 5::smallint, 10::smallint , '1 h'::interval) as job_id \gset
\set VERBOSITY terse
\set ON_ERROR_STOP 1
SELECT add_continuous_aggregate_policy('mat_smallint', 15::smallint, 0::smallint , '1 h'::interval) as job_id \gset
INSERT INTO smallint_tab VALUES(5);
INSERT INTO smallint_tab VALUES(10);
INSERT INTO smallint_tab VALUES(20);
CALL run_job(:job_id);
SELECT * FROM mat_smallint ORDER BY 1;

--remove all the data--
TRUNCATE table smallint_tab;
CALL refresh_continuous_aggregate('mat_smallint', NULL, NULL);
SELECT * FROM mat_smallint ORDER BY 1;

-- Case 1: overflow by subtracting from PG_INT16_MIN
--overflow start_interval, end_interval [-32768, -32768)
SELECT remove_continuous_aggregate_policy('mat_smallint');
INSERT INTO smallint_tab VALUES( -32768 );
SELECT integer_now_smallint_tab();
SELECT add_continuous_aggregate_policy('mat_smallint', 10::smallint, 5::smallint , '1 h'::interval) as job_id \gset

\set ON_ERROR_STOP 0
CALL run_job(:job_id);
\set ON_ERROR_STOP 1
SELECT * FROM mat_smallint ORDER BY 1;

-- overflow start_interval. now this runs as range is capped [-32768, -32765)
INSERT INTO smallint_tab VALUES( -32760 );
SELECT maxval, maxval - 10, maxval -5 FROM integer_now_smallint_tab() as maxval;
CALL run_job(:job_id);
SELECT * FROM mat_smallint ORDER BY 1;

--remove all the data--
TRUNCATE table smallint_tab;
CALL refresh_continuous_aggregate('mat_smallint', NULL, NULL);
SELECT * FROM mat_smallint ORDER BY 1;

-- Case 2: overflow by subtracting from PG_INT16_MAX
--overflow start and end . will fail as range is [32767, 32767]
SELECT remove_continuous_aggregate_policy('mat_smallint');
INSERT INTO smallint_tab VALUES( 32766 );
INSERT INTO smallint_tab VALUES( 32767 );
SELECT maxval, maxval - (-1), maxval - (-2) FROM integer_now_smallint_tab() as maxval;
SELECT add_continuous_aggregate_policy('mat_smallint', -1::smallint, -3::smallint , '1 h'::interval) as job_id \gset
\set ON_ERROR_STOP 0
CALL run_job(:job_id);
\set ON_ERROR_STOP 1
SELECT * FROM mat_smallint ORDER BY 1;

SELECT remove_continuous_aggregate_policy('mat_smallint');
--overflow end . will work range is [32765, 32767)
SELECT maxval, maxval - (1), maxval - (-2) FROM integer_now_smallint_tab() as maxval;
SELECT add_continuous_aggregate_policy('mat_smallint', 1::smallint, -3::smallint , '1 h'::interval) as job_id \gset
\set ON_ERROR_STOP 0
CALL run_job(:job_id);
SELECT * FROM mat_smallint ORDER BY 1;

-- tests for interval argument conversions
--
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_smallint', 15, 10, '1h'::interval, if_not_exists=>true);
SELECT add_continuous_aggregate_policy('mat_smallint', '15', 10, '1h'::interval, if_not_exists=>true);
SELECT add_continuous_aggregate_policy('mat_smallint', '15', '10', '1h'::interval, if_not_exists=>true);
\set ON_ERROR_STOP 1

DROP MATERIALIZED VIEW mat_smallint;
DROP TABLE smallint_tab CASCADE;

--bigint table
CREATE TABLE bigint_tab (a bigint);
SELECT table_name FROM create_hypertable('bigint_tab', 'a', chunk_time_interval=> 10);
CREATE OR REPLACE FUNCTION integer_now_bigint_tab() returns bigint LANGUAGE SQL STABLE as $$ SELECT 20::bigint $$;
SELECT set_integer_now_func('bigint_tab', 'integer_now_bigint_tab');

CREATE MATERIALIZED VIEW mat_bigint( a, countb )
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
SELECT time_bucket( BIGINT '1', a) , count(*)
FROM bigint_tab
GROUP BY 1 WITH NO DATA;
\set ON_ERROR_STOP 0
SELECT add_continuous_aggregate_policy('mat_bigint', 5::bigint, 10::bigint , '1 h'::interval) ;
\set ON_ERROR_STOP 1
SELECT add_continuous_aggregate_policy('mat_bigint', 15::bigint, 0::bigint , '1 h'::interval) as job_mid \gset
INSERT INTO bigint_tab VALUES(5);
INSERT INTO bigint_tab VALUES(10);
INSERT INTO bigint_tab VALUES(20);
CALL run_job(:job_mid);
SELECT * FROM mat_bigint;

-- test NULL for end
SELECT remove_continuous_aggregate_policy('mat_bigint');
SELECT add_continuous_aggregate_policy('mat_bigint', 1::smallint, NULL , '1 h'::interval) as job_id \gset
INSERT INTO bigint_tab VALUES(500);
CALL run_job(:job_id);
SELECT * FROM mat_bigint WHERE a>100 ORDER BY 1;

-- end of coverage tests
