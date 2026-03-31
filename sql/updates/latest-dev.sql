
DROP PROCEDURE IF EXISTS _timescaledb_functions.repair_relation_acls();
DROP FUNCTION IF EXISTS _timescaledb_functions.makeaclitem(regrole, regrole, text, bool);

-- Takes pre-computed hash array and checks if ANY of the hashes
-- match the bloom1 parameter. This function doesn't hash, it only compares.
-- Handles both single equality (1-element array) and ANY (N-element array).
-- Returns true if the bloom maybe-contains ANY of the given hashes.
--
-- This function is intentionally not STRICT, because in case of NULL in the first
-- parameter, we can't decide and will return TRUE.
--
CREATE OR REPLACE FUNCTION _timescaledb_functions.bloom1_contains_any_hashes(_timescaledb_internal.bloom1, bigint[])
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.bloom1_hash(anyelement)
RETURNS bigint
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE PARALLEL SAFE;
