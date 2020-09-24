-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir post.continuous_aggs.sql

\d cagg.*

SELECT * FROM cagg.realtime_mat ORDER BY bucket, location;

REFRESH MATERIALIZED VIEW cagg.realtime_mat;

SELECT * FROM cagg.realtime_mat ORDER BY bucket, location;

SELECT view_name, schedule_interval, materialized_only, materialization_hypertable FROM timescaledb_information.continuous_aggregates ORDER BY view_name::text;

SELECT maxtemp FROM mat_ignoreinval ORDER BY 1;

SELECT count(*) FROM mat_inval;
REFRESH MATERIALIZED VIEW mat_inval;
SELECT count(*) FROM mat_inval;
