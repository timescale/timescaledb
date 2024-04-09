DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function_info(INTEGER);

-- Hyperstore AM
DROP ACCESS METHOD IF EXISTS hsproxy;
DROP FUNCTION IF EXISTS ts_hsproxy_handler;
DROP ACCESS METHOD IF EXISTS hyperstore;
DROP FUNCTION IF EXISTS ts_hyperstore_handler;
DROP FUNCTION IF EXISTS _timescaledb_debug.is_compressed_tid;
