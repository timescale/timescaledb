-- Create and run the start_or_restart function here, see loader readme for more details. 
CREATE OR REPLACE FUNCTION _timescaledb_internal.start_or_restart_background_workers()
RETURNS BOOL
AS '@LOADER_PATHNAME@', 'ts_bgw_db_workers_start_or_restart'
LANGUAGE C VOLATILE;

SELECT _timescaledb_internal.start_or_restart_background_workers();
