-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA _osm_catalog;
CREATE TABLE _osm_catalog.metadata();

CREATE OR REPLACE FUNCTION mock_osm() RETURNS VOID
    AS '$libdir/timescaledb_osm-mock-1', 'ts_mock_osm' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
