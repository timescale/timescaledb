-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Collect information about different features so that we can pick
-- the right usage. Some of these are changed in the same version, but
-- we keep them separate anyway so that we can do additional checking
-- if necessary.
SELECT
	extversion < '2.0.0' AS has_refresh_mat_view
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

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

\if has_refresh_mat_view
    CREATE VIEW mat_before
    WITH ( timescaledb.continuous, timescaledb.refresh_lag='-30 day', timescaledb.max_interval_per_job ='1000 day')
\else
    CREATE MATERIALIZED VIEW IF NOT EXISTS mat_before
	WITH ( timescaledb.continuous)
\endif
    AS
      SELECT time_bucket('1week', timec) as bucket,
	location,
	round(min(allnull)) as min_allnull,
	round(max(temperature)) as max_temp,
	round(sum(temperature)+sum(humidity)) as agg_sum_expr,
	round(avg(humidity)) AS avg_humidity,
	round(stddev(humidity)) as stddev_humidity,
	bit_and(bit_int),
	bit_or(bit_int),
	bool_and(good_life),
	every(temperature > 0),
	bool_or(good_life),
	count(*) as count_rows,
	count(temperature) as count_temp,
	count(allnull) as count_zero,
	round(corr(temperature, humidity)) as corr,
	round(covar_pop(temperature, humidity)) as covar_pop,
	round(covar_samp(temperature, humidity)) as covar_samp,
	round(regr_avgx(temperature, humidity)) as regr_avgx,
	round(regr_avgy(temperature, humidity)) as regr_avgy,
	round(regr_count(temperature, humidity)) as regr_count,
	round(regr_intercept(temperature, humidity)) as regr_intercept,
	round(regr_r2(temperature, humidity)) as regr_r2,
	round(regr_slope(temperature, humidity)) as regr_slope,
	round(regr_sxx(temperature, humidity)) as regr_sxx,
	round(regr_sxy(temperature, humidity)) as regr_sxy,
	round(regr_syy(temperature, humidity)) as regr_syy,
	round(stddev(temperature)) as stddev_temp,
	round(stddev_pop(temperature)) as stddev_pop,
	round(stddev_samp(temperature)) as stddev_samp,
	round(variance(temperature)) as variance,
	round(var_pop(temperature)) as var_pop,
	round(var_samp(temperature)) as var_samp,
	last(temperature, timec) as last_temp,
	last(highlow, timec) as last_hl,
	first(highlow, timec) as first_hl,
	histogram(temperature, 0, 100, 5)
      FROM conditions_before
\if :has_refresh_mat_view
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2;

    ALTER VIEW mat_before SET (timescaledb.materialized_only=true);
\else
      GROUP BY bucket, location
      HAVING min(location) >= 'NYC' and avg(temperature) > 2 WITH NO DATA;
	SELECT add_continuous_aggregate_policy('mat_before', NULL, '-30 days'::interval, '336 h');
    ALTER MATERIALIZED VIEW mat_before SET (timescaledb.materialized_only=true);
\endif

GRANT SELECT ON mat_before TO cagg_user WITH GRANT OPTION;

-- have to use psql conditional here because the procedure call can't be in transaction
\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW mat_before;
\else
CALL refresh_continuous_aggregate('mat_before',NULL,NULL);
\endif

