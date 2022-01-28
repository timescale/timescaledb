-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_git_commit()
    RETURNS TABLE(commit_tag TEXT, commit_hash TEXT, commit_time TIMESTAMPTZ)
    AS '@MODULE_PATHNAME@', 'ts_get_git_commit' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_os_info()
    RETURNS TABLE(sysname TEXT, version TEXT, release TEXT, version_pretty TEXT)
    AS '@MODULE_PATHNAME@', 'ts_get_os_info' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.tsl_loaded() RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_tsl_loaded' LANGUAGE C;
