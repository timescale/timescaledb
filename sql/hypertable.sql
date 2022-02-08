-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Trigger that blocks INSERTs on the hypertable's root table
CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_blocker() RETURNS trigger
AS '@MODULE_PATHNAME@', 'ts_hypertable_insert_blocker' LANGUAGE C;

-- Records mutations or INSERTs which would invalidate a continuous aggregate
CREATE OR REPLACE FUNCTION _timescaledb_internal.continuous_agg_invalidation_trigger() RETURNS TRIGGER
AS '@MODULE_PATHNAME@', 'ts_continuous_agg_invalidation_trigger' LANGUAGE C;

CREATE OR REPLACE FUNCTION @extschema@.set_integer_now_func(hypertable REGCLASS, integer_now_func REGPROC, replace_if_exists BOOL = false) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_hypertable_set_integer_now_func'
LANGUAGE C VOLATILE STRICT;
