-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.generate_uuid() RETURNS UUID
AS '@MODULE_PATHNAME@', 'ts_uuid_generate' LANGUAGE C VOLATILE STRICT;

-- Trigger to change INSERT into UPDATE if key already exists.
--
-- During extension installation we create 3 entries in the metadata table which are
-- included in dumps. To allow loading logical dumps we need this trigger to turn INSERTs
-- into UPDATEs if the key already exists.
CREATE OR REPLACE FUNCTION _timescaledb_functions.metadata_insert_trigger() RETURNS TRIGGER LANGUAGE PLPGSQL
AS $$
BEGIN
  IF EXISTS (SELECT FROM _timescaledb_catalog.metadata WHERE key = NEW.key) THEN
    UPDATE _timescaledb_catalog.metadata SET value = NEW.value WHERE key = NEW.key;
    RETURN NULL;
  END IF;
  RETURN NEW;
END
$$ SET search_path TO pg_catalog, pg_temp;

-- CREATE OR REPLACE TRIGGER is PG14+ only
DROP TRIGGER IF EXISTS metadata_insert_trigger ON _timescaledb_catalog.metadata;
CREATE TRIGGER metadata_insert_trigger BEFORE INSERT ON _timescaledb_catalog.metadata FOR EACH ROW EXECUTE PROCEDURE _timescaledb_functions.metadata_insert_trigger();

-- Insert uuid and install_timestamp on database creation since the trigger
-- will turn these into UPDATEs on conflicts we can't use ON CONFLICT DO NOTHING.
DO $$
BEGIN
  IF (NOT EXISTS (SELECT FROM _timescaledb_catalog.metadata WHERE key = 'uuid')) THEN
    INSERT INTO _timescaledb_catalog.metadata SELECT 'uuid', _timescaledb_functions.generate_uuid(), TRUE;
  END IF;
  IF (NOT EXISTS (SELECT FROM _timescaledb_catalog.metadata WHERE key = 'install_timestamp')) THEN
    INSERT INTO _timescaledb_catalog.metadata SELECT 'install_timestamp', now(), TRUE;
  END IF;
END
$$;

-- Install catalog version on database installation and upgrade.
-- This allows us to detect catalog mismatches in dump/restore cycle.
INSERT INTO _timescaledb_catalog.metadata (key, value, include_in_telemetry) SELECT 'timescaledb_version', '@PROJECT_VERSION_MOD@', FALSE;

