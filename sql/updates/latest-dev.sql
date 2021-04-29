CREATE SCHEMA IF NOT EXISTS timescaledb_experimental;
GRANT USAGE ON SCHEMA timescaledb_experimental TO PUBLIC;

DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk;
