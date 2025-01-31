CREATE OR REPLACE FUNCTION _timescaledb_functions.ts_bloom1_matches(bytea, anyelement)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_bloom1_matches'
LANGUAGE C IMMUTABLE PARALLEL SAFE;
