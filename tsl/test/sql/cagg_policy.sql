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

-- Test 1 step policy for integer type buckets
ALTER materialized view mat_m1 set (timescaledb.compress = true);
-- No policy is added if one errors out
SELECT timescaledb_experimental.add_policies('mat_m1', refresh_start_offset => 1, refresh_end_offset => 10, compress_after => 11, drop_after => 20);
SELECT timescaledb_experimental.show_policies('mat_m1');

-- All policies are added in one step
SELECT timescaledb_experimental.add_policies('mat_m1', refresh_start_offset => 10, refresh_end_offset => 1, compress_after => 11, drop_after => 20);
SELECT timescaledb_experimental.show_policies('mat_m1');

--Test coverage: new view for policies on CAggs
SELECT * FROM timescaledb_experimental.policies ORDER BY relation_name, proc_name;

--Test coverage: new view for policies only shows the policies for  CAggs
SELECT add_retention_policy('int_tab', 20);
SELECT * FROM timescaledb_experimental.policies ORDER BY relation_name, proc_name;
SELECT remove_retention_policy('int_tab');

-- Test for duplicated policies (issue #5492)
CREATE MATERIALIZED VIEW mat_m2( a, sumb )
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
as
SELECT a, sum(b)
FROM int_tab
GROUP BY time_bucket(1, a), a WITH NO DATA;

-- add refresh policy
SELECT timescaledb_experimental.add_policies('mat_m2', refresh_start_offset => 10, refresh_end_offset => 1);
SELECT timescaledb_experimental.show_policies('mat_m2');
-- check for only one refresh policy for each cagg
SELECT * FROM timescaledb_experimental.policies WHERE proc_name ~ 'refresh' ORDER BY relation_name, proc_name;

SELECT timescaledb_experimental.remove_all_policies('mat_m2');
DROP MATERIALIZED VIEW mat_m2;

-- Alter policies
SELECT timescaledb_experimental.alter_policies('mat_m1',  refresh_start_offset => 11, compress_after=>13, drop_after => 25);
SELECT timescaledb_experimental.show_policies('mat_m1');

-- Remove one or more policy
SELECT timescaledb_experimental.remove_policies('mat_m1', false, 'policy_refresh_continuous_aggregate', 'policy_compression');
SELECT timescaledb_experimental.show_policies('mat_m1');

-- Add one policy
SELECT timescaledb_experimental.add_policies('mat_m1', refresh_start_offset => 10, refresh_end_offset => 1);
SELECT timescaledb_experimental.show_policies('mat_m1');

-- Remove all policies
SELECT timescaledb_experimental.remove_policies('mat_m1', false, 'policy_refresh_continuous_aggregate', 'policy_retention');
SELECT timescaledb_experimental.show_policies('mat_m1');

--Cross policy checks
--refresh and compression policy overlap
SELECT timescaledb_experimental.add_policies('mat_m1', refresh_start_offset => 12, refresh_end_offset => 1, compress_after=>11);

--refresh and retention policy overlap
SELECT timescaledb_experimental.add_policies('mat_m1', refresh_start_offset => 12, refresh_end_offset => 1, drop_after=>11);

--compression and retention policy overlap
SELECT timescaledb_experimental.add_policies('mat_m1', compress_after => 10, drop_after => 10);

-- Alter non existent policies
SELECT timescaledb_experimental.alter_policies('mat_m1', refresh_start_offset => 12, compress_after=>11, drop_after => 15);

ALTER materialized view mat_m1 set (timescaledb.compress = false);

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

-- Test 1 step policy for timestamp type buckets
ALTER materialized view max_mat_view_date set (timescaledb.compress = true);
-- Only works for cagg
SELECT timescaledb_experimental.add_policies('continuous_agg_max_mat_date', refresh_start_offset => '1 day'::interval, refresh_end_offset => '2 day'::interval, compress_after => '20 days'::interval, drop_after => '25 days'::interval);
SELECT timescaledb_experimental.show_policies('continuous_agg_max_mat_date');
SELECT timescaledb_experimental.alter_policies('continuous_agg_max_mat_date', compress_after=>'16 days'::interval);
SELECT timescaledb_experimental.remove_policies('continuous_agg_max_mat_date', false, 'policy_refresh_continuous_aggregate');

-- No policy is added if one errors out
SELECT timescaledb_experimental.add_policies('max_mat_view_date', refresh_start_offset => '1 day'::interval, refresh_end_offset => '2 day'::interval, compress_after => '20 days'::interval, drop_after => '25 days'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

-- Create open ended refresh_policy
SELECT timescaledb_experimental.add_policies('max_mat_view_date', refresh_end_offset => '2 day'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');
SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false, 'policy_refresh_continuous_aggregate');

SELECT timescaledb_experimental.add_policies('max_mat_view_date', refresh_end_offset => '2 day'::interval, refresh_start_offset=>'-infinity');
SELECT timescaledb_experimental.show_policies('max_mat_view_date');
SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false, 'policy_refresh_continuous_aggregate');

SELECT timescaledb_experimental.add_policies('max_mat_view_date', refresh_start_offset => '2 day'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');
SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false, 'policy_refresh_continuous_aggregate');

SELECT timescaledb_experimental.add_policies('max_mat_view_date', refresh_start_offset => '2 day'::interval, refresh_end_offset=>'infinity');
SELECT timescaledb_experimental.show_policies('max_mat_view_date');
SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false, 'policy_refresh_continuous_aggregate');

-- Open ended at both sides, for code coverage
SELECT timescaledb_experimental.add_policies('max_mat_view_date', refresh_end_offset => 'infinity', refresh_start_offset => '-infinity');
SELECT timescaledb_experimental.show_policies('max_mat_view_date');
SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false, 'policy_refresh_continuous_aggregate');

-- All policies are added in one step
SELECT timescaledb_experimental.add_policies('max_mat_view_date', refresh_start_offset => '15 days'::interval, refresh_end_offset => '1 day'::interval, compress_after => '20 days'::interval, drop_after => '25 days'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

-- Alter policies
SELECT timescaledb_experimental.alter_policies('max_mat_view_date', refresh_start_offset => '16 days'::interval, compress_after=>'26 days'::interval, drop_after => '40 days'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

--Alter refresh_policy to make it open ended
SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false, 'policy_retention', 'policy_compression');
SELECT timescaledb_experimental.alter_policies('max_mat_view_date', refresh_start_offset =>'-infinity');
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

SELECT timescaledb_experimental.alter_policies('max_mat_view_date', refresh_end_offset =>'infinity', refresh_start_offset =>'5 days'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

--Cross policy checks
-- Refresh and compression policies overlap
SELECT timescaledb_experimental.add_policies('max_mat_view_date', compress_after => '20 days'::interval, drop_after => '25 days'::interval);
SELECT timescaledb_experimental.alter_policies('max_mat_view_date', compress_after=> '4 days'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

-- Refresh and retention policies overlap
SELECT timescaledb_experimental.alter_policies('max_mat_view_date', refresh_start_offset =>'5 days'::interval, drop_after=> '4 days'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

--Do not allow refreshed data to be deleted
SELECT add_retention_policy('continuous_agg_max_mat_date', '25 days'::interval);
SELECT timescaledb_experimental.alter_policies('max_mat_view_date', refresh_start_offset =>'25 days'::interval);
SELECT remove_retention_policy('continuous_agg_max_mat_date');

-- Remove one or more policy
-- Code coverage: no policy names provided
SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false);

-- Code coverage: incorrect name of policy
SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false, 'refresh_policy');

SELECT timescaledb_experimental.remove_policies('max_mat_view_date', false, 'policy_refresh_continuous_aggregate', 'policy_compression');
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

-- Add one policy
SELECT timescaledb_experimental.add_policies('max_mat_view_date', refresh_start_offset => '15 day'::interval, refresh_end_offset => '1 day'::interval);
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

-- Remove all policies
SELECT * FROM timescaledb_experimental.policies ORDER BY relation_name, proc_name;
SELECT timescaledb_experimental.remove_all_policies(NULL); -- should fail
SELECT timescaledb_experimental.remove_all_policies('continuous_agg_max_mat_date'); -- should fail
SELECT timescaledb_experimental.remove_all_policies('max_mat_view_date', false);
SELECT timescaledb_experimental.remove_all_policies('max_mat_view_date', false); -- should fail
CREATE OR REPLACE FUNCTION custom_func(jobid int, args jsonb) RETURNS RECORD LANGUAGE SQL AS
$$
  VALUES($1, $2, 'custom_func');
$$;
 -- inject custom job
SELECT add_job('custom_func','1h', config:='{"type":"function"}'::jsonb, initial_start => '2000-01-01 00:00:00+00'::timestamptz) AS job_id \gset
SELECT _timescaledb_internal.alter_job_set_hypertable_id( :job_id, 'max_mat_view_date'::regclass);
SELECT * FROM timescaledb_information.jobs WHERE job_id != 1 ORDER BY 1;
SELECT timescaledb_experimental.remove_all_policies('max_mat_view_date', true); -- ignore custom job
SELECT delete_job(:job_id);
DROP FUNCTION custom_func;
SELECT timescaledb_experimental.show_policies('max_mat_view_date');

ALTER materialized view max_mat_view_date set (timescaledb.compress = false);

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

-- Test 1 step policy for smallint type buckets
ALTER materialized view mat_smallint set (timescaledb.compress = true);

-- All policies are added in one step
SELECT timescaledb_experimental.add_policies('mat_smallint', refresh_start_offset => 10::smallint, refresh_end_offset => 1::smallint, compress_after => 11::smallint, drop_after => 20::smallint);
SELECT timescaledb_experimental.show_policies('mat_smallint');

-- Alter policies
SELECT timescaledb_experimental.alter_policies('mat_smallint',  refresh_start_offset => 11::smallint, compress_after=>13::smallint, drop_after => 25::smallint);
SELECT timescaledb_experimental.show_policies('mat_smallint');

SELECT timescaledb_experimental.remove_all_policies('mat_smallint', false);
ALTER materialized view mat_smallint set (timescaledb.compress = false);

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

-- Test 1 step policy for bigint type buckets
ALTER materialized view mat_bigint set (timescaledb.compress = true);

-- All policies are added in one step
SELECT timescaledb_experimental.add_policies('mat_bigint', refresh_start_offset => 10::bigint, refresh_end_offset => 1::bigint, compress_after => 11::bigint, drop_after => 20::bigint);
SELECT timescaledb_experimental.show_policies('mat_bigint');

-- Alter policies
SELECT timescaledb_experimental.alter_policies('mat_bigint',  refresh_start_offset => 11::bigint, compress_after=>13::bigint, drop_after => 25::bigint);
SELECT timescaledb_experimental.show_policies('mat_bigint');

SELECT timescaledb_experimental.remove_all_policies('mat_bigint', false);
ALTER materialized view mat_bigint set (timescaledb.compress = false);

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

ALTER MATERIALIZED VIEW mat_bigint SET (timescaledb.compress);
ALTER MATERIALIZED VIEW mat_smallint SET (timescaledb.compress);
\set ON_ERROR_STOP 0
SELECT add_compression_policy('mat_smallint', 0::smallint);
SELECT add_compression_policy('mat_smallint', -4::smallint);
SELECT add_compression_policy('mat_bigint', 0::bigint);
\set ON_ERROR_STOP 1
SELECT add_compression_policy('mat_smallint', 5::smallint);
SELECT add_compression_policy('mat_bigint', 20::bigint);

-- end of coverage tests

--TEST continuous aggregate + compression policy on caggs
CREATE TABLE metrics (
    time timestamptz NOT NULL,
    device_id int,
    device_id_peer int,
    v0 int,
    v1 int,
    v2 float,
    v3 float
);

SELECT create_hypertable('metrics', 'time');

INSERT INTO metrics (time, device_id, device_id_peer, v0, v1, v2, v3)
SELECT time,
    device_id,
    0,
    device_id + 1,
    device_id + 2,
    0.5,
    NULL
FROM generate_series('2000-01-01 0:00:00+0'::timestamptz, '2000-01-02 23:55:00+0', '20m') gtime (time),
    generate_series(1, 2, 1) gdevice (device_id);

ALTER TABLE metrics SET ( timescaledb.compress );
SELECT compress_chunk(ch) FROM show_chunks('metrics') ch;

CREATE MATERIALIZED VIEW metrics_cagg WITH (timescaledb.continuous,
  timescaledb.materialized_only = true)
AS
SELECT time_bucket('1 day', time) as dayb, device_id,
       sum(v0), avg(v3)
FROM metrics
GROUP BY 1, 2
WITH NO DATA;

--can set compression policy only after setting up refresh policy --
\set ON_ERROR_STOP 0
SELECT add_compression_policy('metrics_cagg', '1 day'::interval);

--can set compression policy only after enabling compression --
SELECT add_continuous_aggregate_policy('metrics_cagg', '7 day'::interval, '1 day'::interval, '1 h'::interval) as "REFRESH_JOB" \gset
SELECT add_compression_policy('metrics_cagg', '8 day'::interval) AS "COMP_JOB" ;
\set ON_ERROR_STOP 1

ALTER MATERIALIZED VIEW metrics_cagg SET (timescaledb.compress);

SELECT add_compression_policy('metrics_cagg', '8 day'::interval) AS "COMP_JOB" ;
SELECT remove_compression_policy('metrics_cagg');
SELECT add_compression_policy('metrics_cagg', '8 day'::interval) AS "COMP_JOB" \gset

--verify that jobs were added for the policies ---
SELECT materialization_hypertable_schema AS "MAT_SCHEMA_NAME",
       materialization_hypertable_name AS "MAT_TABLE_NAME",
       materialization_hypertable_schema || '.' || materialization_hypertable_name AS "MAT_NAME"
FROM timescaledb_information.continuous_aggregates
WHERE view_name = 'metrics_cagg' \gset

SELECT count(*) FROM timescaledb_information.jobs
WHERE hypertable_name = :'MAT_TABLE_NAME';

--exec the cagg compression job --
CALL refresh_continuous_aggregate('metrics_cagg', NULL, '2001-02-01 00:00:00+0');
CALL run_job(:COMP_JOB);
SELECT count(*), count(*) FILTER ( WHERE is_compressed is TRUE  )
FROM timescaledb_information.chunks
WHERE hypertable_name = :'MAT_TABLE_NAME' ORDER BY 1;

--add some new data into metrics_cagg so that cagg policy job has something to do
INSERT INTO metrics (time, device_id, device_id_peer, v0, v1, v2, v3)
SELECT now() - '5 day'::interval, 102, 0, 10, 10, 10, 10;
CALL run_job(:REFRESH_JOB);
--now we have a new chunk and it is not compressed
SELECT count(*), count(*) FILTER ( WHERE is_compressed is TRUE  )
FROM timescaledb_information.chunks
WHERE hypertable_name = :'MAT_TABLE_NAME' ORDER BY 1;

--verify that both jobs are dropped when view is dropped
DROP MATERIALIZED VIEW metrics_cagg;

SELECT count(*) FROM timescaledb_information.jobs
WHERE hypertable_name = :'MAT_TABLE_NAME';

-- add test case for issue 4252
CREATE TABLE IF NOT EXISTS sensor_data(
time TIMESTAMPTZ NOT NULL,
sensor_id INTEGER,
temperature DOUBLE PRECISION,
cpu DOUBLE PRECISION);

SELECT create_hypertable('sensor_data','time');

INSERT INTO sensor_data(time, sensor_id, cpu, temperature)
SELECT
time,
sensor_id,
extract(dow from time) AS cpu,
extract(doy from time) AS temperature
FROM
generate_series('2022-05-05'::timestamp at time zone 'UTC' - interval '6 weeks', '2022-05-05'::timestamp at time zone 'UTC', interval '5 hours') as g1(time),
generate_series(1,1000,1) as g2(sensor_id);

CREATE materialized view deals_best_weekly
WITH (timescaledb.continuous) AS
SELECT
time_bucket('7 days', "time") AS bucket,
avg(temperature) AS avg_temp,
max(cpu) AS max_rating
FROM sensor_data
GROUP BY bucket
WITH NO DATA;

CREATE materialized view deals_best_daily
WITH (timescaledb.continuous) AS
SELECT
time_bucket('1 day', "time") AS bucket,
avg(temperature) AS avg_temp,
max(cpu) AS max_rating
FROM sensor_data
GROUP BY bucket
WITH NO DATA;

ALTER materialized view deals_best_weekly set (timescaledb.materialized_only=true);
ALTER materialized view deals_best_daily set (timescaledb.materialized_only=true);

-- we have data from 6 weeks before to May 5 2022 (Thu)
CALL refresh_continuous_aggregate('deals_best_weekly', '2022-04-24', '2022-05-03');
SELECT * FROM deals_best_weekly;
CALL refresh_continuous_aggregate('deals_best_daily', '2022-04-20', '2022-05-04');
SELECT * FROM deals_best_daily ORDER BY bucket LIMIT 2;
-- expect to get an up-to-date notice
CALL refresh_continuous_aggregate('deals_best_weekly', '2022-04-24', '2022-05-05');
SELECT * FROM deals_best_weekly;

-- github issue 5907: segfault when creating 1-step policies on cagg
-- whose underlying hypertable has a retention policy setup
CREATE TABLE t(a integer NOT NULL, b integer);
SELECT create_hypertable('t', 'a', chunk_time_interval=> 10);

CREATE OR REPLACE FUNCTION unix_now() returns int LANGUAGE SQL IMMUTABLE as $$ SELECT extract(epoch from now())::INT $$;
SELECT set_integer_now_func('t', 'unix_now');

SELECT add_retention_policy('t', 20);

CREATE MATERIALIZED VIEW cagg(a, sumb) WITH (timescaledb.continuous)
AS SELECT time_bucket(1, a), sum(b)
   FROM t GROUP BY time_bucket(1, a);

SELECT timescaledb_experimental.add_policies('cagg');
