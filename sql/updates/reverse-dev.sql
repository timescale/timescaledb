DROP FUNCTION _timescaledb_functions.bloom1_contains_any(_timescaledb_internal.bloom1, anyarray);

DROP FUNCTION IF EXISTS _timescaledb_functions.policy_job_stat_history_retention;
DROP VIEW IF EXISTS timescaledb_information.chunks;

-- Revert support for concurrent merge chunks()
DROP PROCEDURE IF EXISTS _timescaledb_functions.chunk_rewrite_cleanup();
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks_concurrently(REGCLASS[]);
DROP PROCEDURE IF EXISTS @extschema@.merge_chunks(REGCLASS, REGCLASS, BOOLEAN);
DROP TABLE IF EXISTS _timescaledb_catalog.chunk_rewrite;

-- Remove UUID time_bucket functions
DROP FUNCTION IF EXISTS @extschema@.time_bucket(INTERVAL, UUID);
DROP FUNCTION IF EXISTS @extschema@.time_bucket(INTERVAL, UUID, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS @extschema@.time_bucket(INTERVAL, UUID, INTERVAL);
DROP FUNCTION IF EXISTS @extschema@.time_bucket(INTERVAL, UUID, TEXT, TIMESTAMPTZ, INTERVAL);

