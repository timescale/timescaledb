-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

--test that the extension is deactivated during upgrade
SELECT 1;
SELECT 1;
SELECT 1;
SELECT 1;
CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
    AS '$libdir/timescaledb-mock-2', 'ts_mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

