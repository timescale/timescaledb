-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE VIEW timescaledb_experimental.policies AS
SELECT ca.user_view_name AS relation_name,
  ca.user_view_schema AS relation_schema,
  j.schedule_interval,
  j.proc_schema,
  j.proc_name,
  j.config,
  ht.schema_name AS hypertable_schema,
  ht.table_name AS hypertable_name
FROM _timescaledb_config.bgw_job j
  JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = j.hypertable_id
  JOIN _timescaledb_catalog.hypertable ht ON ht.id = ca.mat_hypertable_id;

GRANT SELECT ON ALL TABLES IN SCHEMA timescaledb_experimental TO PUBLIC;
