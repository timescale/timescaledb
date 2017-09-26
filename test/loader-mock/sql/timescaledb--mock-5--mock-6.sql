--test that the extension is deactivated during upgrade
SELECT 1;
SELECT 1;
SELECT 1;
SELECT 1;
--intentionally forget to updat func
--CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
--    AS '$libdir/timescaledb-mock-6', 'mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
