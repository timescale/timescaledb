-- Deprecated partition hash function
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_for_key(val anyelement)
    RETURNS int
    AS '@MODULE_PATHNAME@', 'get_partition_for_key' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_hash(val anyelement)
    RETURNS int
    AS '@MODULE_PATHNAME@', 'get_partition_hash' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

