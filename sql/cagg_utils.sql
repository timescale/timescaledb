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

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_parse_invalidation_record(
    invalidation_record BYTEA,
    OUT hypertable_relid REGCLASS,
    OUT lowest_modified_value BIGINT,
    OUT greatest_modified_value BIGINT)
RETURNS RECORD
AS '@MODULE_PATHNAME@', 'ts_continuous_agg_read_invalidation_record'
LANGUAGE C STRICT IMMUTABLE;
