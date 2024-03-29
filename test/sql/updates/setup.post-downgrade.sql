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

SELECT
     split_part(extversion, '.', 1)::int * 100000 +
     split_part(extversion, '.', 2)::int *    100 AS extversion_num
FROM
     pg_extension WHERE extname = 'timescaledb' \gset

SELECT
     :extversion_num >= 200000 AS has_create_mat_view \gset

-- Rebuild the user views based on the renamed views
\if :has_create_mat_view
ALTER MATERIALIZED VIEW rename_cols SET (timescaledb.materialized_only = FALSE);
\else
ALTER VIEW rename_cols SET (timescaledb.materialized_only = FALSE);
\endif
