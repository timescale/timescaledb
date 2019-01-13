-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

broken sql;
CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
    AS '$libdir/timescaledb-mock-3', 'ts_mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
