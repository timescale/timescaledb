DROP FUNCTION IF EXISTS _timescaledb_functions.cagg_get_bucket_function_info(INTEGER);

-- Hyperstore AM
DROP ACCESS METHOD IF EXISTS tscompression;
DROP FUNCTION IF EXISTS ts_compressionam_handler;

