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

\if :has_create_mat_view
\set :DROP_CAGG DROP MATERIALIZED VIEW
\else
\set :DROP_CAGG DROP VIEW
\endif

\if :has_continuous_aggs_policy
SELECT drop_continuous_aggregate_policy('mat_drop');
\endif

:DROP_CAGG mat_drop
DROP TABLE drop_test;
:DROP_CAGG mat_conflict;
DROP TABLE conflict_test;
:DROP_CAGG mat_inttime;
DROP FUNCTION integer_now_test;
DROP TABLE int_time_test;
:DROP_CAGG mat_inval;
DROP TABLE inval_test;
:DROP_CAGG mat_ignoreinval;
:DROP_CAGG cagg.realtime_mat;
DROP SCHEMA cagg;
:DROP_CAGG mat_before;
DROP TABLE conditions_before;
DROP TYPE custom_type;


