-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION @extschema@.set_integer_now_func(hypertable REGCLASS, integer_now_func REGPROC, replace_if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_hypertable_set_integer_now_func'
LANGUAGE C VOLATILE STRICT;

-- Look up a hypertable's relation Oid by its internal id, using the active
-- snapshot. Intended for logical decoding plugins, which install a
-- HistoricSnapshot via PushActiveSnapshot() before invoking. Returns NULL when
-- no hypertable matches or its underlying relation cannot be resolved.
CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_relid_by_id(hypertable_id INTEGER) RETURNS REGCLASS
AS '@MODULE_PATHNAME@', 'ts_hypertable_relid_by_id' LANGUAGE C STABLE STRICT PARALLEL SAFE;

