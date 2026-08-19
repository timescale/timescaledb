-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_validate_query(
    query TEXT,
    OUT is_valid BOOLEAN,
    OUT error_level TEXT,
    OUT error_code TEXT,
    OUT error_message TEXT,
    OUT error_detail TEXT,
    OUT error_hint TEXT
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_continuous_agg_validate_query' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_bucket_function_info(
    mat_hypertable_id INTEGER,
    -- The bucket function
    OUT bucket_func REGPROCEDURE,
    -- `bucket_width` argument of the function, e.g. "1 month"
    OUT bucket_width TEXT,
    -- optional `origin` argument of the function provided by the user
    OUT bucket_origin TEXT,
    -- optional `offset` argument of the function provided by the user
    OUT bucket_offset TEXT,
    -- optional `timezone` argument of the function provided by the user
    OUT bucket_timezone TEXT,
    -- fixed or variable sized bucket
    OUT bucket_fixed_width BOOLEAN
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_continuous_agg_get_bucket_function_info' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_grouping_columns(
    cagg REGCLASS )
    RETURNS TEXT[] AS '@MODULE_PATHNAME@', 'ts_continuous_agg_get_grouping_columns'
LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_get_tenant_tracking_info(
    hypertable REGCLASS,
    OUT seq_num int4,
    OUT active_generation int4,
    OUT nentries int4,
    OUT status int4,
    OUT late_threshold_start int8,
    OUT late_threshold_end int8)
AS '@MODULE_PATHNAME@', 'ts_hypertable_get_tenant_tracking_info' LANGUAGE C VOLATILE;

-- Lists hypertables in the per-tenant invalidation tracker.  The tracker map is
-- process-global, so rows for other databases show up too (identified by
-- database_id, since their names are not readable from here).  is_tracked is false
-- when the map has an entry but its tracker could not be allocated (out of shared
-- memory): tracking is off for that hypertable until restart.  Superuser only,
-- since the listing spans databases.
CREATE OR REPLACE FUNCTION _timescaledb_functions.tenant_tracking_map()
RETURNS TABLE (
    database_id   oid,
    hypertable_id int4,
    is_tracked    boolean)
AS '@MODULE_PATHNAME@', 'ts_tenant_tracking_map' LANGUAGE C VOLATILE;
