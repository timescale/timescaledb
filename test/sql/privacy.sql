\c single :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.test_privacy() RETURNS VOID
    AS :MODULE_PATHNAME, 'test_privacy' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
\c single :ROLE_DEFAULT_PERM_USER

SHOW timescaledb.telemetry_level;
SET timescaledb.telemetry_level=off;
SHOW timescaledb.telemetry_level;
SELECT _timescaledb_internal.test_privacy();
-- To make sure nothing was sent, we check the UUID table to make sure no exported UUID row was created
SELECT COUNT(*) from _timescaledb_catalog.installation_metadata;

-- Now try to send something
RESET timescaledb.telemetry_level;
-- We don't care about the output of the version checker
SET client_min_messages=error;
SELECT _timescaledb_internal.test_privacy();
RESET client_min_messages;
-- We know it attempted to send if there generated UUIDs
SELECT COUNT(*) from _timescaledb_catalog.installation_metadata where key='uuid';
SELECT COUNT(*) from _timescaledb_catalog.installation_metadata where key='exported_uuid';
SELECT COUNT(*) from _timescaledb_catalog.installation_metadata where key='install_timestamp';
SELECT COUNT(*) from _timescaledb_catalog.installation_metadata;
