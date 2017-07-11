--NOTICE: UPGRADE-SCRIPT-NEEDED contents in this file are not auto-upgraded.
-- This sets up the permissions for entities created by this extension.

-- schema permisions
GRANT USAGE ON SCHEMA _timescaledb_catalog, _timescaledb_internal TO PUBLIC;

-- needed for working with hypertables
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO PUBLIC;




