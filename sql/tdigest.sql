-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

/*
 * T-Digest aggregates and associated functionality
 * 
 * Originally from PipelineDB 1.0.0
 * Original copyright (c) 2018, PipelineDB, Inc. and released under Apache License 2.0
 * Modifications copyright by Timescale, Inc. per NOTICE
 */

-- T-Digest aggregate support functions

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_sfunc(internal, float8)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_tdigest_sfunc_sql'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_sfunc(internal, float8, integer)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_tdigest_sfunc_comp_sql'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_finalfunc(internal)
RETURNS @extschema@.tdigest
AS '@MODULE_PATHNAME@', 'ts_tdigest_finalfunc_sql'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_serializefunc(internal)
RETURNS bytea
AS '@MODULE_PATHNAME@', 'ts_tdigest_serializefunc_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_deserializefunc(bytea, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_tdigest_deserializefunc_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_combinefunc(internal, internal)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_tdigest_combinefunc_sql'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

--- Public facing API
-- percentile_approx and cdf_approx are intended to support sketch types 
-- other than TDigest in the future

CREATE OR REPLACE FUNCTION percentile_approx(@extschema@.tdigest, float8)
RETURNS float8
AS '@MODULE_PATHNAME@', 'ts_tdigest_percentile_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION cdf_approx(@extschema@.tdigest, float8)
RETURNS float8
AS '@MODULE_PATHNAME@', 'ts_tdigest_cdf_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Exact count of number of data points added to the TDigest
CREATE OR REPLACE FUNCTION tdigest_count(@extschema@.tdigest)
RETURNS bigint
AS '@MODULE_PATHNAME@', 'ts_tdigest_count_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- User friendly output of key values in a T-Digest
CREATE OR REPLACE FUNCTION tdigest_print(@extschema@.tdigest)
RETURNS varchar
AS '@MODULE_PATHNAME@', 'ts_tdigest_print_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Comparison Functions

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_cmp(tdigest, tdigest)
RETURNS integer
AS '@MODULE_PATHNAME@', 'ts_tdigest_cmp_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_equal(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_equal_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_lt(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_lt_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_gt(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_gt_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_ge(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_ge_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tdigest_le(@extschema@.tdigest, @extschema@.tdigest)
RETURNS boolean
AS '@MODULE_PATHNAME@', 'ts_tdigest_le_sql'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;
