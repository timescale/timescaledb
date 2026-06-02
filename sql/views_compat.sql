-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- temporary views for compatibility

CREATE OR REPLACE VIEW _timescaledb_config.bgw_job AS
SELECT * from _timescaledb_catalog.bgw_job;

CREATE OR REPLACE VIEW _timescaledb_catalog.chunk_constraint AS
SELECT
  chunk_id,
  id AS dimension_slice_id,
  format('constraint_%s',id) AS constraint_name,
  NULL::text AS hypertable_constraint_name from _timescaledb_catalog.dimension_slice;

GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_config TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO PUBLIC;
