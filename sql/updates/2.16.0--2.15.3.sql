DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function_info(INTEGER);
-- remove chunk column statistics related objects
DROP FUNCTION IF EXISTS @extschema@.enable_column_stats(REGCLASS, NAME, BOOLEAN);
DROP FUNCTION IF EXISTS @extschema@.disable_column_stats(REGCLASS, NAME, BOOLEAN);
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.chunk_column_stats;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.chunk_column_stats_id_seq;
DROP TABLE IF EXISTS _timescaledb_catalog.chunk_column_stats;
