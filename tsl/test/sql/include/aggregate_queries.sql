-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This files assumes the existence of some table with definition as seen in the aggregate_table.sql file.

-- All of these should be able to be pushed down if enabled
:PREFIX SELECT :GROUPING,
   min(allnull) as min_allnull,
   max(temperature) as max_temp,
   sum(temperature)+sum(humidity) as agg_sum_expr,
   avg(humidity),
   ROUND( CAST(stddev(humidity) AS NUMERIC), 5),
   bit_and(bit_int),
   bit_or(bit_int),
   bool_and(good_life),
   every(temperature > 0),
   bool_or(good_life),
   count(*) as count_rows,
   count(temperature) as count_temp,
   count(allnull) as count_zero,
   ROUND( CAST(corr(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(covar_pop(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(covar_samp(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_avgx(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_avgy(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_count(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_intercept(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_r2(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_slope(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_sxx(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_sxy(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(regr_syy(temperature, humidity) AS NUMERIC), 5),
   ROUND( CAST(stddev(temperature) AS NUMERIC), 5) as stddev_temp,
   ROUND( CAST(stddev_pop(temperature) AS NUMERIC), 5),
   ROUND( CAST(stddev_samp(temperature) AS NUMERIC), 5),
   ROUND( CAST(variance(temperature) AS NUMERIC), 5),
   ROUND( CAST(var_pop(temperature) AS NUMERIC), 5),
   ROUND( CAST(var_samp(temperature) AS NUMERIC), 5),
   last(temperature, timec) as last_temp,
   histogram(temperature, 0, 100, 5)
  FROM :TEST_TABLE
  GROUP BY :GROUPING
  ORDER BY :GROUPING;

-- Aggregates on custom types are not yet pushed down
:PREFIX SELECT :GROUPING,
   last(highlow, timec) as last_hl,
   first(highlow, timec) as first_hl
  FROM :TEST_TABLE
  GROUP BY :GROUPING
  ORDER BY :GROUPING;

-- Mix of aggregates that push down and those that don't
:PREFIX SELECT :GROUPING,
   min(allnull) as min_allnull,
   max(temperature) as max_temp,
   sum(temperature)+sum(humidity) as agg_sum_expr,
   avg(humidity),
   ROUND( CAST(stddev(humidity) AS NUMERIC), 5),
   bit_and(bit_int),
   bit_or(bit_int),
   bool_and(good_life),
   every(temperature > 0),
   bool_or(good_life),
   first(highlow, timec) as first_hl
  FROM :TEST_TABLE
  GROUP BY :GROUPING
  ORDER BY :GROUPING;
