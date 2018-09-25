broken sql;
CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
    AS '$libdir/timescaledb-mock-3', 'mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
