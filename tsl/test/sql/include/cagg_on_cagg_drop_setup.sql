-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Create a 3 level hierarchy on top of drop_raw, materialized_only taken
-- from MAT_ONLY_1ST, MAT_ONLY_2TH and MAT_ONLY_3TH
CREATE MATERIALIZED VIEW drop_cagg_1
WITH (timescaledb.continuous, timescaledb.materialized_only=:MAT_ONLY_1ST) AS
SELECT time_bucket('1 hour', time) AS bucket, device_id, sum(value) AS value
FROM drop_raw
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW drop_cagg_2
WITH (timescaledb.continuous, timescaledb.materialized_only=:MAT_ONLY_2TH) AS
SELECT time_bucket('1 day', bucket) AS bucket, device_id, sum(value) AS value
FROM drop_cagg_1
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW drop_cagg_3
WITH (timescaledb.continuous, timescaledb.materialized_only=:MAT_ONLY_3TH) AS
SELECT time_bucket('1 week', bucket) AS bucket, device_id, sum(value) AS value
FROM drop_cagg_2
GROUP BY 1, 2
WITH NO DATA;

SELECT add_continuous_aggregate_policy('drop_cagg_1', NULL, INTERVAL '1 hour', INTERVAL '1 hour') AS job_1,
       add_continuous_aggregate_policy('drop_cagg_2', NULL, INTERVAL '1 day', INTERVAL '1 hour') AS job_2,
       add_continuous_aggregate_policy('drop_cagg_3', NULL, INTERVAL '1 week', INTERVAL '1 hour') AS job_3 \gset

-- Remember the materialization hypertables to check for leftovers after a drop
SELECT array_agg(mat_hypertable_id ORDER BY mat_hypertable_id) AS mat_ids
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name LIKE 'drop_cagg_%' \gset
