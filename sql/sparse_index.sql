CREATE FUNCTION _timescaledb_internal.ts_bloom1_matches(anyelement, bytea)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_bloom1_matches'
LANGUAGE C IMMUTABLE STRICT;
