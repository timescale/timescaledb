-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT mat_hypertable_id,
       format('%I.%I', partial_view_schema, partial_view_name)::regclass AS partial_view,
       format('%I.%I', direct_view_schema, direct_view_name)::regclass AS direct_view
  FROM _timescaledb_catalog.continuous_agg where user_view_name = :'CAGG_NAME'
  \gset

SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function WHERE mat_hypertable_id = :mat_hypertable_id;
SELECT pg_get_viewdef(:'partial_view', true);
SELECT pg_get_viewdef(:'direct_view', true);
SELECT pg_get_viewdef(:'CAGG_NAME', true);

CALL _timescaledb_functions.cagg_migrate_to_time_bucket(:'CAGG_NAME');

SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function WHERE mat_hypertable_id = :mat_hypertable_id;
SELECT pg_get_viewdef(:'partial_view', true);
SELECT pg_get_viewdef(:'direct_view', true);
SELECT pg_get_viewdef(:'CAGG_NAME', true);

-- Test CAGG refresh
CALL refresh_continuous_aggregate(:'CAGG_NAME', NULL, NULL);
