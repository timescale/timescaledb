-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

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
    CREATE VIEW mat_before
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_lag='-30 day', timescaledb.max_interval_per_job ='1000 day')
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity),
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
    CREATE MATERIALIZED VIEW IF NOT EXISTS mat_before
    WITH ( timescaledb.continuous, timescaledb.materialized_only=true)
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	min(allnull) as min_allnull,
	max(temperature) as max_temp,
	sum(temperature)+sum(humidity) as agg_sum_expr,
	avg(humidity),
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
  END IF;
END $$;

REFRESH MATERIALIZED VIEW mat_before;
    
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
	avg(humidity),
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
	avg(humidity),
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

REFRESH MATERIALIZED VIEW cagg.realtime_mat;

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

REFRESH MATERIALIZED VIEW mat_ignoreinval;

-- test new data beyond the invalidation threshold is properly handled --
CREATE TABLE inval_test (time TIMESTAMPTZ, location TEXT, temperature DOUBLE PRECISION);
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

REFRESH MATERIALIZED VIEW mat_inval;

INSERT INTO inval_test
SELECT generate_series('2118-12-01 00:00'::timestamp, '2118-12-20 00:00'::timestamp, '1 day'), 'POR', generate_series(135.25, 140.0, 0.25);
INSERT INTO inval_test
SELECT generate_series('2118-12-01 00:00'::timestamp, '2118-12-20 00:00'::timestamp, '1 day'), 'NYC', generate_series(131.0, 150.0, 1.0);
