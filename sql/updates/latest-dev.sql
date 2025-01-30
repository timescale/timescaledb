CREATE FUNCTION _timescaledb_functions.ts_bloom1_matches(anyelement, bytea)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_bloom1_matches'
LANGUAGE C IMMUTABLE STRICT;

