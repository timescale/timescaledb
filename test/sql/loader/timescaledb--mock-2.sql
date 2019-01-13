-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--create function before proxy table
CREATE OR REPLACE FUNCTION mock_function() RETURNS VOID
    AS '$libdir/timescaledb-mock-2', 'ts_mock_function' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE SCHEMA IF NOT EXISTS _timescaledb_cache;
CREATE TABLE IF NOT EXISTS  _timescaledb_cache.cache_inval_extension();

