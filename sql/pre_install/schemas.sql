-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SET LOCAL search_path TO pg_catalog;

CREATE SCHEMA _timescaledb_catalog;
CREATE SCHEMA _timescaledb_internal;
CREATE SCHEMA _timescaledb_cache;
CREATE SCHEMA _timescaledb_config;
CREATE SCHEMA timescaledb_experimental;
CREATE SCHEMA timescaledb_information;

GRANT USAGE ON SCHEMA _timescaledb_cache, _timescaledb_catalog, _timescaledb_internal, _timescaledb_config, timescaledb_information, timescaledb_experimental TO PUBLIC;

