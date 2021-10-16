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
   ROUND(stddev(CAST(humidity AS INT)), 5),
   bit_and(bit_int),
   bit_or(bit_int),
   bool_and(good_life),
   every(temperature > 0),
   bool_or(good_life),
   count(*) as count_rows,
   count(temperature) as count_temp,
   count(allnull) as count_zero,
   ROUND(CAST(corr(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(covar_pop(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(covar_samp(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_avgx(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_avgy(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_count(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_intercept(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_r2(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_slope(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_sxx(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_sxy(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(CAST(regr_syy(CAST(temperature AS INT), CAST(humidity AS INT)) AS NUMERIC), 5),
   ROUND(stddev(CAST(temperature AS INT)), 5) as stddev_temp,
   ROUND(stddev_pop(CAST(temperature AS INT)), 5),
   ROUND(stddev_samp(CAST(temperature AS INT)), 5),
   ROUND(variance(CAST(temperature AS INT)), 5),
   ROUND(var_pop(CAST(temperature AS INT)), 5),
   ROUND(var_samp(CAST(temperature AS INT)), 5),
   last(temperature, timec) as last_temp,
   histogram(temperature, 0, 100, 1)
  FROM :TEST_TABLE
  GROUP BY :GROUPING, timec
  ORDER BY :GROUPING, timec;

-- Aggregates on custom types are not yet pushed down
:PREFIX SELECT :GROUPING,
   last(highlow, timec) as last_hl,
   first(highlow, timec) as first_hl
  FROM :TEST_TABLE
  GROUP BY :GROUPING, timec
  ORDER BY :GROUPING, timec;

-- Mix of aggregates that push down and those that don't
:PREFIX SELECT :GROUPING,
   min(allnull) as min_allnull,
   max(temperature) as max_temp,
   sum(temperature)+sum(humidity) as agg_sum_expr,
   avg(humidity),
   ROUND(stddev(CAST(humidity AS INT)), 5),
   bit_and(bit_int),
   bit_or(bit_int),
   bool_and(good_life),
   every(temperature > 0),
   bool_or(good_life),
   first(highlow, timec) as first_hl
  FROM :TEST_TABLE
  GROUP BY :GROUPING, timec
  ORDER BY :GROUPING, timec;

-- Aggregates nested in expressions and no top-level aggregate #3672
:PREFIX SELECT :GROUPING,
  sum(temperature)+sum(humidity) as agg_sum_expr
  FROM :TEST_TABLE
  GROUP BY :GROUPING, timec
  ORDER BY :GROUPING, timec;

-- Aggregates with no aggregate reference in targetlist #3664
:PREFIX SELECT :GROUPING
  FROM :TEST_TABLE
  GROUP BY :GROUPING, timec
  HAVING avg(temperature) > 20
  ORDER BY :GROUPING, timec;

