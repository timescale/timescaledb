-- This sets up the permissions for entities created by this extension.

-- schema permisions
GRANT USAGE ON SCHEMA _timescaledb_catalog, _timescaledb_internal, _timescaledb_cache 
TO PUBLIC;

GRANT USAGE, CREATE ON SCHEMA _timescaledb_internal TO PUBLIC;

-- needed for working with hypertables
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO PUBLIC;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog TO PUBLIC;

-- Needed but dangerous. Anybody can mess up the _timescaledb_catalog.
-- MUST DOCUMENT TODO: remove these permissions. Have c-based workaround.
-- Everything below this line is suspect.
GRANT INSERT ON TABLE 
_timescaledb_catalog.hypertable, _timescaledb_catalog.chunk
TO PUBLIC;

-- needed for inserts to hypertable
GRANT UPDATE ON TABLE _timescaledb_catalog.hypertable, _timescaledb_catalog.chunk -- needed for lock
TO PUBLIC;

-- needed for ddl
GRANT INSERT, DELETE ON TABLE _timescaledb_catalog.hypertable_index, _timescaledb_catalog.chunk_index
TO PUBLIC;




