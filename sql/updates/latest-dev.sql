ALTER TABLE _timescaledb_catalog.telemetry_metadata ADD COLUMN include_in_telemetry BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE _timescaledb_catalog.telemetry_metadata ALTER COLUMN include_in_telemetry DROP DEFAULT;
ALTER TABLE _timescaledb_catalog.telemetry_metadata RENAME TO metadata;
ALTER INDEX _timescaledb_catalog.telemetry_metadata_pkey RENAME TO metadata_pkey;
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_aggs_materialization_invalidation_log(
    materialization_id INTEGER PRIMARY KEY
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id)
        ON DELETE CASCADE,
    lowest_modified_value BIGINT NOT NULL,
    greatest_modified_value BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_invalidation_log', '');

CREATE INDEX continuous_aggs_materialization_invalidation_log_idx
    ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value ASC);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log TO PUBLIC;
DROP FUNCTION IF EXISTS get_telemetry_report();

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

--placeholder to allow creation of functions below
CREATE TYPE rxid;

CREATE OR REPLACE FUNCTION _timescaledb_internal.rxid_in(cstring) RETURNS rxid
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_in' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.rxid_out(rxid) RETURNS cstring
    AS '@MODULE_PATHNAME@', 'ts_remote_txn_id_out' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE rxid (
   internallength = 16,
   input = _timescaledb_internal.rxid_in,
   output = _timescaledb_internal.rxid_out
);

CREATE TABLE _timescaledb_catalog.remote_txn (
    server_name              NAME, --this is really only to allow us to cleanup stuff on a per-server basis.
    remote_transaction_id    TEXT CHECK (remote_transaction_id::rxid is not null),
    PRIMARY KEY (remote_transaction_id)
);
CREATE INDEX IF NOT EXISTS remote_txn_server_name_idx
ON _timescaledb_catalog.remote_txn(server_name);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.remote_txn', '');


GRANT SELECT ON _timescaledb_catalog.remote_txn TO PUBLIC;
