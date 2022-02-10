-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Collect information about different features so that we can pick
-- the right usage. Some of these are changed in the same version, but
-- we keep them separate anyway so that we can do additional checking
-- if necessary.
SELECT
	extversion < '2.0.0' AS has_refresh_mat_view,
	extversion < '2.0.0' AS has_drop_chunks_old_interface,
	extversion < '2.0.0' AS has_ignore_invalidations_older_than,
	extversion < '2.0.0' AS has_max_interval_per_job,
	extversion >= '2.0.0' AS has_create_mat_view,
	extversion >= '2.0.0' AS has_continuous_aggs_policy
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

-- disable background workers to prevent deadlocks between background processes
-- on timescaledb 1.7.x
CALL _timescaledb_testing.stop_workers();

CREATE TYPE custom_type AS (high int, low int);

CREATE TABLE conditions_before (
      timec       TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL,
      lowp        double precision NULL,
      highp       double precision null,
      allnull     double precision null,
      highlow     custom_type null,
      bit_int     smallint,
      good_life   boolean
    );

SELECT table_name FROM create_hypertable( 'conditions_before', 'timec');

INSERT INTO conditions_before
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 55, 75, 40, 70, NULL, (1,2)::custom_type, 2, true;
INSERT INTO conditions_before
SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'NYC', 35, 45, 50, 40, NULL, (3,4)::custom_type, 4, false;
INSERT INTO conditions_before
SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 'LA', 73, 55, NULL, 28, NULL, NULL, 8, true;

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';
  IF ts_version < '2.0.0' THEN
    CREATE VIEW rename_cols
    WITH (timescaledb.continuous, timescaledb.materialized_only = false, timescaledb.refresh_lag='14 days') AS
    SELECT time_bucket('1 week', timec) AS bucket, location, avg(humidity)
    FROM conditions_before
    GROUP BY bucket, location;

    CREATE VIEW mat_before
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag='-30 day', timescaledb.max_interval_per_job ='1000 day')
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity) AS avg_humidity,
	stddev(humidity),
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	corr(temperature, humidity),
	covar_pop(temperature, humidity),
	covar_samp(temperature, humidity),
	regr_avgx(temperature, humidity),
	regr_avgy(temperature, humidity),
	regr_count(temperature, humidity),
	regr_intercept(temperature, humidity),
	regr_r2(temperature, humidity),
	regr_slope(temperature, humidity),
	regr_sxx(temperature, humidity),
	regr_sxy(temperature, humidity),
	regr_syy(temperature, humidity),
	stddev(temperature) as stddev_temp,
	stddev_pop(temperature),
	stddev_samp(temperature),
	variance(temperature),
	var_pop(temperature),
	var_samp(temperature),
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2;

    -- ALTER VIEW cannot rename columns before PG13, but ALTER TABLE
    -- works for views.
    ALTER TABLE rename_cols RENAME COLUMN bucket to "time";
  ELSE
    -- rename_cols cagg view is also used for another test: if we can enable
    -- compression on a cagg after an upgrade
    -- This view has 3 cols which is fewer than the number of cols on the table
    -- we had a bug related to that and need to verify if compression can be
    -- enabled on such a view
    CREATE MATERIALIZED VIEW rename_cols
    WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
    SELECT time_bucket('1 week', timec) AS bucket, location, avg(humidity)
    FROM conditions_before
    GROUP BY bucket, location
    WITH NO DATA;
    PERFORM add_continuous_aggregate_policy('rename_cols', NULL, '14 days'::interval, '336 h');

    CREATE MATERIALIZED VIEW IF NOT EXISTS mat_before
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true)
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity) AS avg_humidity,
	stddev(humidity),
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	corr(temperature, humidity),
	covar_pop(temperature, humidity),
	covar_samp(temperature, humidity),
	regr_avgx(temperature, humidity),
	regr_avgy(temperature, humidity),
	regr_count(temperature, humidity),
	regr_intercept(temperature, humidity),
	regr_r2(temperature, humidity),
	regr_slope(temperature, humidity),
	regr_sxx(temperature, humidity),
	regr_sxy(temperature, humidity),
	regr_syy(temperature, humidity),
	stddev(temperature) as stddev_temp,
	stddev_pop(temperature),
	stddev_samp(temperature),
	variance(temperature),
	var_pop(temperature),
	var_samp(temperature),
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2 WITH NO DATA;
    PERFORM add_continuous_aggregate_policy('mat_before', NULL, '-30 days'::interval, '336 h');

    ALTER MATERIALIZED VIEW rename_cols RENAME COLUMN bucket to "time";
  END IF;
END $$;

\if :WITH_SUPERUSER
GRANT SELECT ON mat_before TO cagg_user WITH GRANT OPTION;
\endif

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW rename_cols;
REFRESH MATERIALIZED VIEW mat_before;
\else
CALL refresh_continuous_aggregate('rename_cols',NULL,NULL);
CALL refresh_continuous_aggregate('mat_before',NULL,NULL);
\endif

-- we create separate schema for realtime agg since we dump all view definitions in public schema
-- but realtime agg view definition is not stable across versions
CREATE SCHEMA cagg;

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW cagg.realtime_mat
    WITH ( timescaledb.continuous, timescaledb.materialized_only=false, timescaledb.refresh_lag='-30 day', timescaledb.max_interval_per_job ='1000 day')
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity) AS avg_humidity,
	stddev(humidity),
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	corr(temperature, humidity),
	covar_pop(temperature, humidity),
	covar_samp(temperature, humidity),
	regr_avgx(temperature, humidity),
	regr_avgy(temperature, humidity),
	regr_count(temperature, humidity),
	regr_intercept(temperature, humidity),
	regr_r2(temperature, humidity),
	regr_slope(temperature, humidity),
	regr_sxx(temperature, humidity),
	regr_sxy(temperature, humidity),
	regr_syy(temperature, humidity),
	stddev(temperature) as stddev_temp,
	stddev_pop(temperature),
	stddev_samp(temperature),
	variance(temperature),
	var_pop(temperature),
	var_samp(temperature),
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2;
  ELSE
    CREATE MATERIALIZED VIEW IF NOT EXISTS cagg.realtime_mat
    WITH ( timescaledb.continuous, timescaledb.materialized_only=false)
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity) AS avg_humidity,
	stddev(humidity),
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	corr(temperature, humidity),
	covar_pop(temperature, humidity),
	covar_samp(temperature, humidity),
	regr_avgx(temperature, humidity),
	regr_avgy(temperature, humidity),
	regr_count(temperature, humidity),
	regr_intercept(temperature, humidity),
	regr_r2(temperature, humidity),
	regr_slope(temperature, humidity),
	regr_sxx(temperature, humidity),
	regr_sxy(temperature, humidity),
	regr_syy(temperature, humidity),
	stddev(temperature) as stddev_temp,
	stddev_pop(temperature),
	stddev_samp(temperature),
	variance(temperature),
	var_pop(temperature),
	var_samp(temperature),
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2 WITH NO DATA;
    PERFORM add_continuous_aggregate_policy('cagg.realtime_mat', NULL, '-30 days'::interval, '336 h');
  END IF;
END $$;

\if :WITH_SUPERUSER
GRANT SELECT ON cagg.realtime_mat TO cagg_user;
\endif

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW cagg.realtime_mat;
\else
CALL refresh_continuous_aggregate('cagg.realtime_mat',NULL,NULL);
\endif

-- test ignore_invalidation_older_than migration --
DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW mat_ignoreinval
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.refresh_lag='-30 day',
           timescaledb.ignore_invalidation_older_than='30 days',
           timescaledb.max_interval_per_job = '100000 days')
    AS
      SELECT time_bucket('1 week', timec) as bucket,
    max(temperature) as maxtemp
      FROM conditions_before
      GROUP BY bucket;
  ELSE
    CREATE MATERIALIZED VIEW IF NOT EXISTS  mat_ignoreinval
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true)
    AS
      SELECT time_bucket('1 week', timec) as bucket,
    max(temperature) as maxtemp
      FROM conditions_before
      GROUP BY bucket WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('mat_ignoreinval', '30 days'::interval, '-30 days'::interval, '336 h');
  END IF;
END $$;

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_ignoreinval;
\else
CALL refresh_continuous_aggregate('mat_ignoreinval',NULL,NULL);
\endif

-- test new data beyond the invalidation threshold is properly handled --
CREATE TABLE inval_test (time TIMESTAMPTZ NOT NULL, location TEXT, temperature DOUBLE PRECISION);
SELECT create_hypertable('inval_test', 'time', chunk_time_interval => INTERVAL '1 week');

INSERT INTO inval_test
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-20 00:00'::timestamp, '1 day'), 'POR', generate_series(40.5, 50.0, 0.5);
INSERT INTO inval_test
SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-20 00:00'::timestamp, '1 day'), 'NYC', generate_series(31.0, 50.0, 1.0);

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW mat_inval
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.refresh_lag='-20 days',
           timescaledb.refresh_interval='12 hours',
           timescaledb.max_interval_per_job='100000 days' )
    AS
      SELECT time_bucket('10 minute', time) as bucket, location, min(temperature) as min_temp,
        max(temperature) as max_temp, avg(temperature) as avg_temp
      FROM inval_test
      GROUP BY bucket, location;

  ELSE
    CREATE MATERIALIZED VIEW mat_inval
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true )
    AS
      SELECT time_bucket('10 minute', time) as bucket, location, min(temperature) as min_temp,
        max(temperature) as max_temp, avg(temperature) as avg_temp
      FROM inval_test
      GROUP BY bucket, location WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('mat_inval', NULL, '-20 days'::interval, '12 hours');
  END IF;
END $$;

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_inval;
\else
CALL refresh_continuous_aggregate('mat_inval',NULL,NULL);
\endif

INSERT INTO inval_test
SELECT generate_series('2118-12-01 00:00'::timestamp, '2118-12-20 00:00'::timestamp, '1 day'), 'POR', generate_series(135.25, 140.0, 0.25);
INSERT INTO inval_test
SELECT generate_series('2118-12-01 00:00'::timestamp, '2118-12-20 00:00'::timestamp, '1 day'), 'NYC', generate_series(131.0, 150.0, 1.0);

-- Add an integer base table to ensure we handle it correctly
CREATE TABLE int_time_test(timeval integer not null, col1 integer, col2 integer);
select create_hypertable('int_time_test', 'timeval', chunk_time_interval=> 2);

CREATE OR REPLACE FUNCTION integer_now_test() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(timeval), 0) FROM public.int_time_test $$;
SELECT set_integer_now_func('int_time_test', 'integer_now_test');

INSERT INTO int_time_test VALUES
(10, - 4, 1), (11, - 3, 5), (12, - 3, 7), (13, - 3, 9), (14,-4, 11),
(15, -4, 22), (16, -4, 23);

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW mat_inttime
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.ignore_invalidation_older_than = 6,
           timescaledb.refresh_lag = 2,
           timescaledb.refresh_interval='12 hours')
    AS
      SELECT time_bucket( 2, timeval), COUNT(col1)
      FROM int_time_test
      GROUP BY 1;

    CREATE VIEW mat_inttime2
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.refresh_lag = 2,
           timescaledb.refresh_interval='12 hours')
    AS
      SELECT time_bucket( 2, timeval), COUNT(col1)
      FROM int_time_test
      GROUP BY 1;

  ELSE
    CREATE MATERIALIZED VIEW mat_inttime
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true )
    AS
      SELECT time_bucket( 2, timeval), COUNT(col1)
      FROM int_time_test
      GROUP BY 1 WITH NO DATA;

    CREATE MATERIALIZED VIEW mat_inttime2
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true )
    AS
      SELECT time_bucket( 2, timeval), COUNT(col1)
      FROM int_time_test
      GROUP BY 1 WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('mat_inttime', 6, 2, '12 hours');
    PERFORM add_continuous_aggregate_policy('mat_inttime2', NULL, 2, '12 hours');
  END IF;
END $$;


\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_inttime;
REFRESH MATERIALIZED VIEW mat_inttime2;
\else
CALL refresh_continuous_aggregate('mat_inttime',NULL,NULL);
CALL refresh_continuous_aggregate('mat_inttime2',NULL,NULL);
\endif

-- Test that retention policies that conflict with continuous aggs are disabled --
CREATE TABLE conflict_test (time TIMESTAMPTZ NOT NULL, location TEXT, temperature DOUBLE PRECISION);
SELECT create_hypertable('conflict_test', 'time', chunk_time_interval => INTERVAL '1 week');

DO LANGUAGE PLPGSQL $$
DECLARE
  ts_version TEXT;
  retention_jobid INTEGER;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

  IF ts_version < '2.0.0' THEN
    CREATE VIEW mat_conflict
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true,
           timescaledb.refresh_lag='1 day',
           timescaledb.ignore_invalidation_older_than = '28 days',
           timescaledb.refresh_interval='12 hours' )
    AS
      SELECT time_bucket('10 minute', time) as bucket, location, min(temperature) as min_temp,
        max(temperature) as max_temp, avg(temperature) as avg_temp
      FROM conflict_test
      GROUP BY bucket, location;

    PERFORM add_drop_chunks_policy('conflict_test', '14 days'::interval);
  ELSE
    CREATE MATERIALIZED VIEW mat_conflict
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true )
    AS
      SELECT time_bucket('10 minute', time) as bucket, location, min(temperature) as min_temp,
        max(temperature) as max_temp, avg(temperature) as avg_temp
      FROM conflict_test
      GROUP BY bucket, location WITH NO DATA;

    PERFORM add_continuous_aggregate_policy('mat_conflict', '28 days', '1 day', '12 hours');
    SELECT add_retention_policy('conflict_test', '14 days'::interval) INTO retention_jobid;
    PERFORM alter_job(retention_jobid, scheduled=>false);
  END IF;
END $$;

\if :WITH_SUPERUSER
GRANT SELECT, TRIGGER, UPDATE
ON mat_conflict TO cagg_user
WITH GRANT OPTION;
\endif

-- Test that calling drop chunks on the hypertable does not break the
-- update process when chunks are marked as dropped rather than
-- removed. This happens when a continuous aggregate is defined on the
-- hypertable, so we create a hypertable and a continuous aggregate
-- here and then drop chunks from the hypertable and make sure that
-- the update from 1.7 to 2.0 works as expected.
CREATE TABLE drop_test (
    time timestamptz not null,
    location INT,
    temperature double PRECISION
);

SELECT create_hypertable ('drop_test', 'time', chunk_time_interval => interval '1 week');

INSERT INTO drop_test
SELECT
    time,
    (random() * 3 + 1)::int,
    random() * 100.0
FROM
    generate_series(now() - interval '28 days', now(), '1 hour') AS time;

\if :has_create_mat_view
CREATE MATERIALIZED VIEW mat_drop
\else
CREATE VIEW mat_drop
\endif
WITH (
     timescaledb.materialized_only = TRUE,
\if :has_ignore_invalidations_older_than
     timescaledb.ignore_invalidation_older_than = '7 days',
\endif
\if :has_max_interval_per_job
     timescaledb.refresh_lag='-30 day',
     timescaledb.max_interval_per_job ='1000 day',
\endif
     timescaledb.continuous
) AS
SELECT
    time_bucket ('10 minute',time) AS bucket,
    LOCATION,
    min(temperature) AS min_temp,
    max(temperature) AS max_temp,
    avg(temperature) AS avg_temp
FROM
    drop_test
GROUP BY
    bucket,
    LOCATION;

\if :has_continuous_aggs_policy
SELECT add_continuous_aggregate_policy('mat_drop', '7 days', '-30 days'::interval, '20 min');
\endif

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_drop;
\else
CALL refresh_continuous_aggregate('mat_drop',NULL,NULL);
\endif

\if :has_drop_chunks_old_interface
SELECT drop_chunks(NOW() - INTERVAL '7 days', table_name => 'drop_test',
		   cascade_to_materializations => FALSE);
\else
SELECT drop_chunks('drop_test', NOW() - INTERVAL '7 days');
\endif
