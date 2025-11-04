CREATE FUNCTION _timescaledb_functions.bloom1_contains_any(_timescaledb_internal.bloom1, anyarray)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_stat_history_retention;
DROP VIEW IF EXISTS timescaledb_information.chunks;

