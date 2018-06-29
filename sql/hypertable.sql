-- Trigger that blocks INSERTs on the hypertable's root table
CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_blocker() RETURNS trigger
AS '@MODULE_PATHNAME@', 'hypertable_insert_blocker' LANGUAGE C;
