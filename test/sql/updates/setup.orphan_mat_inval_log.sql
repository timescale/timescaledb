-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Insert a materialization invalidation log entry for a continuous aggregate that does not exist.
BEGIN;
-- Using "replica" to force skip the foreign key check on
-- materialization_id so that an orphaned row can be inserted.
SET LOCAL session_replication_role = replica;
INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    (materialization_id, lowest_modified_value, greatest_modified_value)
VALUES (1000000, 0, 1000);
COMMIT;
