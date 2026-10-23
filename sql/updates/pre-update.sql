-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file is always prepended to all upgrade scripts.

-- Remove any orphaned materialization invalidation log entries for dropped
-- continuous aggregates.
DELETE FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log l
WHERE l.materialization_id IS NOT NULL
  AND NOT EXISTS (
    SELECT FROM _timescaledb_catalog.continuous_agg c
    WHERE c.mat_hypertable_id = l.materialization_id);
