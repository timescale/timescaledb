-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers()
RETURNS BOOL 
AS '@LOADER_PATHNAME@', 'ts_bgw_db_workers_restart'
LANGUAGE C VOLATILE;

SELECT _timescaledb_internal.restart_background_workers();
