-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

-- This file is always prepended to all upgrade scripts.

-- Triggers should be disabled during upgrades to avoid having them
-- invoke functions that might load an old version of the shared
-- library before those functions have been updated.
DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_command_end;
DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_sql_drop;

-- These are legacy triggers. They need to be disabled here even
-- though they don't exist in newer versions, because they might still
-- exist when upgrading from older versions. Thus we need to DROP all
-- triggers here that have ever been created.
DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.hypertable;
DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.chunk;
DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.chunk_constraint;
DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.dimension_slice;
DROP TRIGGER IF EXISTS "0_cache_inval" ON _timescaledb_catalog.dimension;

CREATE OR REPLACE FUNCTION _timescaledb_internal.restart_background_workers()
RETURNS BOOL
AS '@LOADER_PATHNAME@', 'ts_bgw_db_workers_restart'
LANGUAGE C VOLATILE;

SELECT _timescaledb_internal.restart_background_workers();
