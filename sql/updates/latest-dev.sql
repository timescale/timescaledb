-- Type for bloom filters used by the sparse indexes on compressed hypertables.
CREATE TYPE _timescaledb_internal.bloom1;
CREATE FUNCTION _timescaledb_functions.bloom1in(cstring) RETURNS _timescaledb_internal.bloom1 AS 'byteain' LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE;
CREATE FUNCTION _timescaledb_functions.bloom1out(_timescaledb_internal.bloom1) RETURNS cstring AS 'byteaout' LANGUAGE INTERNAL IMMUTABLE PARALLEL SAFE;
CREATE TYPE _timescaledb_internal.bloom1 (
    INPUT = _timescaledb_functions.bloom1in,
    OUTPUT = _timescaledb_functions.bloom1out,
    LIKE = bytea
);
CREATE OR REPLACE FUNCTION _timescaledb_functions.ts_bloom1_matches(_timescaledb_internal.bloom1, anyelement)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE STRICT;


DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_table;
DROP FUNCTION IF EXISTS _timescaledb_functions.create_chunk_table;

