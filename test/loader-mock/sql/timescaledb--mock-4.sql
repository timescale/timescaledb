CREATE SCHEMA IF NOT EXISTS _timescaledb_cache;
CREATE TABLE IF NOT EXISTS  _timescaledb_cache.cache_inval_extension();
CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
    AS '$libdir/timescaledb-mock-4', 'mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
