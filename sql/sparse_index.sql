-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.ts_bloom1_matches(bytea, anyelement)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_bloom1_matches'
LANGUAGE C IMMUTABLE PARALLEL SAFE;
