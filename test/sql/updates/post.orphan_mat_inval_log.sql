-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Materialization invalidation log must not reference dropped continuous aggregates.
SELECT count(*) AS orphan_mat_inval_log_entries
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log l
WHERE NOT EXISTS (SELECT FROM _timescaledb_catalog.continuous_agg c
                  WHERE c.mat_hypertable_id = l.materialization_id);
