-- Add new function definitions, columns and tables for distributed hypertables
DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc,regproc);

ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN replication_factor SMALLINT NULL CHECK (replication_factor > 0);

-- Table for hypertable -> servers mappings
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_server (
    hypertable_id          INTEGER NOT NULL     REFERENCES _timescaledb_catalog.hypertable(id),
    server_hypertable_id   INTEGER NULL,
    server_name            NAME NOT NULL,
    UNIQUE(server_hypertable_id, server_name),
    UNIQUE(hypertable_id, server_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_server', '');

GRANT SELECT ON _timescaledb_catalog.hypertable_server TO PUBLIC;

-- Table for chunk -> servers mappings
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.chunk_server (
    chunk_id               INTEGER NOT NULL     REFERENCES _timescaledb_catalog.chunk(id),
    server_chunk_id        INTEGER NOT NULL,
    server_name            NAME NOT NULL,
    UNIQUE(server_chunk_id, server_name),
    UNIQUE(chunk_id, server_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_server', '');

GRANT SELECT ON _timescaledb_catalog.chunk_server TO PUBLIC;
