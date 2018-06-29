-- The trigger function must be defined so that we can add it to legacy tables
CREATE OR REPLACE FUNCTION _timescaledb_internal.insert_blocker() RETURNS trigger
AS '@MODULE_PATHNAME@', 'hypertable_insert_blocker' LANGUAGE C;

CREATE FUNCTION _timescaledb_internal.insert_blocker_trigger_add(relid REGCLASS) RETURNS OID
AS '@MODULE_PATHNAME@', 'hypertable_insert_blocker_trigger_add' LANGUAGE C VOLATILE STRICT;

SELECT _timescaledb_internal.insert_blocker_trigger_add(h.relid)
FROM (SELECT format('%I.%I', schema_name, table_name)::regclass AS relid FROM _timescaledb_catalog.hypertable) AS h;

DROP FUNCTION _timescaledb_internal.insert_blocker_trigger_add(REGCLASS);
