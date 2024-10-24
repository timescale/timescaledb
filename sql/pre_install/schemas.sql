-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SET LOCAL search_path TO pg_catalog, pg_temp;

CREATE SCHEMA _timescaledb_catalog;
CREATE SCHEMA _timescaledb_functions;
CREATE SCHEMA _timescaledb_internal;
CREATE SCHEMA _timescaledb_cache;
CREATE SCHEMA _timescaledb_config;
CREATE SCHEMA timescaledb_experimental;
CREATE SCHEMA timescaledb_information;
CREATE SCHEMA _timescaledb_debug;

GRANT USAGE ON SCHEMA
      _timescaledb_cache,
      _timescaledb_catalog,
      _timescaledb_config,
      _timescaledb_debug,
      _timescaledb_functions,
      _timescaledb_internal,
      timescaledb_experimental,
      timescaledb_information
TO PUBLIC;

