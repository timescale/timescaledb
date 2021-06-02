-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA IF NOT EXISTS _timescaledb_catalog;
CREATE SCHEMA IF NOT EXISTS _timescaledb_internal;
CREATE SCHEMA IF NOT EXISTS _timescaledb_cache;
CREATE SCHEMA IF NOT EXISTS _timescaledb_config;
CREATE SCHEMA IF NOT EXISTS timescaledb_experimental;
GRANT USAGE ON SCHEMA _timescaledb_cache, _timescaledb_catalog, _timescaledb_internal, _timescaledb_config TO PUBLIC;
GRANT USAGE ON SCHEMA timescaledb_experimental TO PUBLIC;

