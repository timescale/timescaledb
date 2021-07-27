DROP SCHEMA IF EXISTS timescaledb_experimental CASCADE;
DROP FUNCTION IF EXISTS _timescaledb_internal.block_new_chunks;
DROP FUNCTION IF EXISTS _timescaledb_internal.allow_new_chunks;
DROP FUNCTION IF EXISTS _timescaledb_internal.refresh_continuous_aggregate;
