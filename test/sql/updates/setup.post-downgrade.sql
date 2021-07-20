-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- When running a downgrade tests, the extension is first updated to
-- the later version and then downgraded to the previous version. This
-- means that in some cases, changes done by the update is not
-- reversed by the downgrade. If these changes are harmless, we can
-- apply changes to the clean rerun to incorporate these changes
-- directly and prevent a diff between the clean-rerun version and the
-- upgrade-downgrade version of the database.

SELECT extversion >= '2.0.0' AS has_create_mat_view
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

-- For the renamed continuous aggregate "rename_cols", fix the column
-- name in the materialized hypertable and in the dimension
-- table. This will allow the views to be rebuilt with the new name
-- and prevent a diff when doing an upgrade-downgrade.
SELECT timescaledb_pre_restore();
ALTER TABLE _timescaledb_internal._materialized_hypertable_4 RENAME COLUMN bucket TO "time";
UPDATE _timescaledb_catalog.dimension SET column_name = 'time' WHERE hypertable_id = 4;
SELECT timescaledb_post_restore();

-- Rebuild the user views based on the renamed views
\if :has_create_mat_view
ALTER MATERIALIZED VIEW rename_cols SET (timescaledb.materialized_only = FALSE);
\else
ALTER VIEW rename_cols SET (timescaledb.materialized_only = FALSE);
\endif
