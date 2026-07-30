-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Referenced-side check trigger for foreign keys pointing at a hypertable,
-- installed in place of the builtin RI check on PostgreSQL 19+.
CREATE OR REPLACE FUNCTION _timescaledb_functions.fk_referenced_check() RETURNS trigger
AS '@MODULE_PATHNAME@', 'ts_fk_referenced_check' LANGUAGE C;

-- Reinstate the check trigger swap after a restore recreated the foreign keys
-- with the builtin triggers. No-op before PG19.
CREATE OR REPLACE FUNCTION _timescaledb_functions.restore_fk_check_triggers() RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_fk_restore_check_triggers' LANGUAGE C;
