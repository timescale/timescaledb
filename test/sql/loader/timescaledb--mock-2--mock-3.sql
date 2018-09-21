broken sql;
CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
    AS '$libdir/timescaledb-mock-3', 'ts_mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
