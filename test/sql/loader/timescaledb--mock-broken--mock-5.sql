-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--test that the extension is deactivated during upgrade
SELECT 1;
SELECT 1;
SELECT 1;
SELECT 1;
CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
   AS '$libdir/timescaledb-mock-5', 'ts_mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
