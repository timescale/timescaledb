-- Remove old compression policy procedures
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_interval(INTEGER, INTEGER, INTERVAL, INTEGER, BOOLEAN, BOOLEAN);
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_integer(INTEGER, INTEGER, BIGINT, INTEGER, BOOLEAN, BOOLEAN);

-- rewrite cagg to ensure selectedCols is set correctly in the view rules --
-- we do this in post-update.sql
