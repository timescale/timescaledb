-- Remove old compression policy procedures
DROP PROCEDURE IF EXISTS _timescaledb_internal.policy_compression_execute(INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN);
