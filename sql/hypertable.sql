-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.set_integer_now_func(hypertable REGCLASS, integer_now_func REGPROC, replace_if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_hypertable_set_integer_now_func'
LANGUAGE C VOLATILE STRICT;

-- Get the status of the hypertable
CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_status(REGCLASS) RETURNS INT
AS '@MODULE_PATHNAME@', 'ts_hypertable_status' LANGUAGE C;

-- Get the status of the hypertable as text array
CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_status_text(hypertable_status int) RETURNS TEXT[]
AS '@MODULE_PATHNAME@', 'ts_hypertable_status_text' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_status_text(hypertable regclass) RETURNS TEXT[]
AS $$ SELECT _timescaledb_functions.hypertable_status_text(_timescaledb_functions.hypertable_status($1)); $$ LANGUAGE SQL STRICT STABLE PARALLEL SAFE SET search_path TO pg_catalog, pg_temp;
