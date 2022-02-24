-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Collect information about different features so that we can pick
-- the right usage. Some of these are changed in the same version, but
-- we keep them separate anyway so that we can do additional checking
-- if necessary.
SELECT extversion < '2.0.0' AS has_refresh_mat_view,
       extversion < '2.0.0' AS has_drop_chunks_old_interface,
       extversion < '2.0.0' AS has_ignore_invalidations_older_than,
       extversion < '2.0.0' AS has_max_interval_per_job,
       extversion >= '2.0.0' AS has_create_mat_view,
       extversion >= '2.0.0' AS has_continuous_aggs_policy
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

\if :has_continuous_aggs_policy
SELECT remove_continuous_aggregate_policy('mat_drop');
\endif

\if :has_create_mat_view
DROP MATERIALIZED VIEW mat_drop;
\else
DROP VIEW mat_drop;
\endif

DROP TABLE drop_test;

\if :has_create_mat_view
DROP MATERIALIZED VIEW mat_conflict;
\else
DROP VIEW mat_conflict;
\endif

DROP TABLE conflict_test;

\if :has_create_mat_view
DROP MATERIALIZED VIEW mat_inttime;
DROP MATERIALIZED VIEW mat_inttime2;
\else
DROP VIEW mat_inttime;
DROP VIEW mat_inttime2;
\endif

DROP FUNCTION integer_now_test;
DROP TABLE IF EXISTS int_time_test;

\if :has_create_mat_view
DROP MATERIALIZED VIEW mat_inval;
\else
DROP VIEW mat_inval;
\endif

DROP TABLE inval_test;

\if :has_create_mat_view
DROP MATERIALIZED VIEW mat_ignoreinval;
\else
DROP VIEW mat_ignoreinval;
\endif

\if :has_create_mat_view
DROP MATERIALIZED VIEW cagg.realtime_mat;
\else
DROP VIEW cagg.realtime_mat;
\endif

DROP SCHEMA cagg;

\if :has_create_mat_view
DROP MATERIALIZED VIEW mat_before;
\else
DROP VIEW mat_before;
\endif

\if :has_create_mat_view
DROP MATERIALIZED VIEW rename_cols;
\else
DROP VIEW rename_cols;
\endif

DROP TABLE conditions_before;
DROP TYPE custom_type;


