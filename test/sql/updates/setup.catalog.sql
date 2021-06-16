-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Catalog tables are occationally rewritten as part of updates, so
-- this is to test that privileges are maintained over updates of the
-- extension. We could verify that other properties (e.g., comments)
-- are maintained here as well, but this is not something we use right
-- now.
--
-- We do not alter the privileges on _timescaledb_internal since this
-- affects both internal objects and two tables that are metadata
-- placed in the _timescaledb_internal schema.

GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO tsdbadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_config TO tsdbadmin;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog TO tsdbadmin;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_config TO tsdbadmin;

ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_catalog
      GRANT SELECT ON TABLES TO tsdbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_config
      GRANT SELECT ON TABLES TO tsdbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_catalog
    GRANT SELECT ON SEQUENCES TO tsdbadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA _timescaledb_config
    GRANT SELECT ON SEQUENCES TO tsdbadmin;
