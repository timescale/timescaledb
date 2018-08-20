-- Trigger that blocks INSERTs on the hypertable's root table
CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_blocker() RETURNS trigger
AS '@MODULE_PATHNAME@', 'hypertable_insert_blocker' LANGUAGE C;

-- Drop all pre-0.11.1 insert_blockers from hypertables and add the new, visible trigger
CREATE FUNCTION _timescaledb_internal.insert_blocker_trigger_add(relid REGCLASS) RETURNS OID
AS '@MODULE_PATHNAME@', 'hypertable_insert_blocker_trigger_add' LANGUAGE C VOLATILE STRICT;

SELECT _timescaledb_internal.insert_blocker_trigger_add(h.relid)
FROM (SELECT format('%I.%I', schema_name, table_name)::regclass AS relid FROM _timescaledb_catalog.hypertable) AS h;

DROP FUNCTION _timescaledb_internal.insert_blocker_trigger_add(REGCLASS);
