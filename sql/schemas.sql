CREATE SCHEMA IF NOT EXISTS _timescaledb_catalog;
CREATE SCHEMA IF NOT EXISTS _timescaledb_internal;
CREATE SCHEMA IF NOT EXISTS _timescaledb_cache;
GRANT USAGE ON SCHEMA _timescaledb_catalog, _timescaledb_internal TO PUBLIC;
