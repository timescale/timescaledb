-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir post.continuous_aggs.sql

\d cagg.*

SELECT * FROM cagg.realtime_mat ORDER BY bucket, location;

CALL refresh_continuous_aggregate('cagg.realtime_mat',NULL,NULL);

SELECT * FROM cagg.realtime_mat ORDER BY bucket, location;

SELECT view_name, schedule_interval, materialized_only, materialization_hypertable_name FROM timescaledb_information.continuous_aggregates ORDER BY view_name::text;

SELECT maxtemp FROM mat_ignoreinval ORDER BY 1;

SELECT materialization_id FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
WHERE lowest_modified_value = -9223372036854775808 ORDER BY 1;

SELECT count(*) FROM mat_inval;
CALL refresh_continuous_aggregate('mat_inval',NULL,NULL);
SELECT count(*) FROM mat_inval;
