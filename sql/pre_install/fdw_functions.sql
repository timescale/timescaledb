-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.timescaledb_fdw_handler()
RETURNS fdw_handler
AS '@MODULE_PATHNAME@', 'ts_timescaledb_fdw_handler'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION @extschema@.timescaledb_fdw_validator(text[], oid)
RETURNS void
AS '@MODULE_PATHNAME@', 'ts_timescaledb_fdw_validator'
LANGUAGE C STRICT;

