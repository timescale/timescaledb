-- Remove old compression policy procedures
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_interval(INTEGER, INTEGER, INTERVAL, INTEGER, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_integer(INTEGER, INTEGER, BIGINT, INTEGER, BOOLEAN, BOOLEAN);
