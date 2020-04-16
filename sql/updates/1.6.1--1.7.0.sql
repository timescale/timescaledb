
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

ALTER TABLE IF EXISTS _timescaledb_catalog.continuous_agg ADD COLUMN IF NOT EXISTS  materialized_only BOOL NOT NULL DEFAULT false;

-- all continuous aggregrates created before this update had materialized only views
UPDATE _timescaledb_catalog.continuous_agg SET materialized_only = true;

-- rewrite catalog table to not break catalog scans on tables with missingval optimization
CLUSTER  _timescaledb_catalog.continuous_agg USING continuous_agg_pkey;
ALTER TABLE _timescaledb_catalog.continuous_agg SET WITHOUT CLUSTER;

