-- Hyperstore AM
DROP ACCESS METHOD IF EXISTS hyperstore;
DROP FUNCTION IF EXISTS ts_compressionam_handler;
DROP FUNCTION IF EXISTS _timescaledb_debug.is_compressed_tid;
