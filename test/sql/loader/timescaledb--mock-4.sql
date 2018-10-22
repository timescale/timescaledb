-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

CREATE SCHEMA IF NOT EXISTS _timescaledb_cache;
CREATE TABLE IF NOT EXISTS  _timescaledb_cache.cache_inval_extension();
CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
    AS '$libdir/timescaledb-mock-4', 'ts_mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
