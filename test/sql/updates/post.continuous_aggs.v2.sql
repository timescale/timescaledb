-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir post.continuous_aggs.sql

\d cagg.*

\x on
SELECT
    bucket,
    location,
    min_allnull,
    max_temp,
    agg_sum_expr,
    avg_humidity,
    stddev,
    bit_and,
    bit_or,
    bool_and,
    every,
    bool_or,
    count_rows,
    count_temp,
    count_zero,
    corr,
    covar_pop,
    covar_samp,
    regr_avgx,
    regr_avgy,
    regr_count,
    regr_intercept,
    regr_r2,
    regr_slope,
    regr_sxx,
    regr_sxy,
    round(regr_syy) AS regr_syy,
    stddev_temp,
    round(stddev_pop) AS stddev_pop,
    stddev_samp,
    round(variance) AS variance,
    round(var_pop) AS var_pop,
    round(var_samp) AS var_samp,
    last_temp,
    last_hl,
    first_hl,
    histogram
FROM
    cagg.realtime_mat
ORDER BY
    bucket, location;
\x off

CALL refresh_continuous_aggregate('cagg.realtime_mat',NULL,NULL);

\x on
SELECT
    bucket,
    location,
    min_allnull,
    max_temp,
    agg_sum_expr,
    avg_humidity,
    stddev,
    bit_and,
    bit_or,
    bool_and,
    every,
    bool_or,
    count_rows,
    count_temp,
    count_zero,
    corr,
    covar_pop,
    covar_samp,
    regr_avgx,
    regr_avgy,
    regr_count,
    regr_intercept,
    regr_r2,
    regr_slope,
    regr_sxx,
    regr_sxy,
    round(regr_syy) AS regr_syy,
    stddev_temp,
    round(stddev_pop) AS stddev_pop,
    stddev_samp,
    round(variance) AS variance,
    round(var_pop) AS var_pop,
    round(var_samp) AS var_samp,
    last_temp,
    last_hl,
    first_hl,
    histogram
FROM
    cagg.realtime_mat
ORDER BY
    bucket, location;
\x off

SELECT view_name, materialized_only, materialization_hypertable_name
FROM timescaledb_information.continuous_aggregates
ORDER BY view_name::text;

SELECT schedule_interval
FROM timescaledb_information.jobs
ORDER BY job_id;

SELECT maxtemp FROM mat_ignoreinval ORDER BY 1;

SELECT materialization_id FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE lowest_modified_value = -9223372036854775808 ORDER BY 1;

SELECT count(*) FROM mat_inval;
CALL refresh_continuous_aggregate('mat_inval',NULL,NULL);
SELECT count(*) FROM mat_inval;
