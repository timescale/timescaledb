-- Insert uuid and install_timestamp on database creation.
-- Don't create exported_uuid because this shouldn't get called on a create
CREATE FUNCTION _timescaledb_internal.generate_uuid_external() RETURNS TEXT
AS '@MODULE_PATHNAME@', 'generate_uuid_external' LANGUAGE C VOLATILE STRICT;
INSERT INTO _timescaledb_catalog.installation_metadata SELECT 'uuid', _timescaledb_internal.generate_uuid_external();
INSERT INTO _timescaledb_catalog.installation_metadata SELECT 'install_timestamp', now();
DROP FUNCTION _timescaledb_internal.generate_uuid_external();
