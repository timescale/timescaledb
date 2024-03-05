-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file is always prepended to all upgrade and downgrade scripts.
-- This file must avoid referencing extension objects directly as that
-- would limit the things we can alter in extension update/downgrade
-- itself.
SET LOCAL search_path TO pg_catalog, pg_temp;

-- Disable parallel execution for the duration of the update process.
-- This avoids version mismatch errors that would have beeen triggered by the
-- parallel workers in ts_extension_check_version().
SET LOCAL max_parallel_workers = 0;

-- Triggers should be disabled during upgrades to avoid having them
-- invoke functions that might load an old version of the shared
-- library before those functions have been updated.
DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_command_end;
DROP EVENT TRIGGER IF EXISTS timescaledb_ddl_sql_drop;

-- Since we want to call the new version of restart_background_workers we
-- create a function that points to that version. The proper restart_background_workers
-- may either be in _timescaledb_internal or in _timescaledb_functions
-- depending on the version we are upgrading from and we can't make
-- the move in this location as the new schema might not have been set up.
CREATE FUNCTION _timescaledb_internal._tmp_restart_background_workers()
RETURNS BOOL
AS '@LOADER_PATHNAME@', 'ts_bgw_db_workers_restart'
LANGUAGE C VOLATILE;
SELECT _timescaledb_internal._tmp_restart_background_workers();
DROP FUNCTION _timescaledb_internal._tmp_restart_background_workers();

-- Table for ACL and initprivs of tables.
CREATE TABLE _timescaledb_internal.saved_privs(
       tmpnsp name,
       tmpname name,
       tmpacl aclitem[],
       tmpini aclitem[],
       UNIQUE (tmpnsp, tmpname));

-- We save away both the ACL and the initprivs for all tables and
-- views in the extension (but not for chunks and internal objects) so
-- that we can restore them to the proper state after the update.
INSERT INTO _timescaledb_internal.saved_privs
SELECT nspname, relname, relacl, initprivs
  FROM pg_class cl JOIN pg_namespace ns ON ns.oid = relnamespace
                   JOIN pg_init_privs ip ON ip.objoid = cl.oid AND ip.objsubid = 0 AND ip.classoid = 'pg_class'::regclass
WHERE
  nspname IN ('_timescaledb_catalog', '_timescaledb_config')
  OR (
    relname IN ('hypertable_chunk_local_size', 'compressed_chunk_stats', 'bgw_job_stat', 'bgw_policy_chunk_stats')
    AND nspname = '_timescaledb_internal'
  )
;

