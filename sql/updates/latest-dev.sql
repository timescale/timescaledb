
-- we add an addition optional argument to locf
DROP FUNCTION IF EXISTS locf(ANYELEMENT,ANYELEMENT);

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_agg (
    mat_hypertable_id INTEGER PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    raw_hypertable_id INTEGER NOT NULL REFERENCES  _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    user_view_schema NAME NOT NULL,
    user_view_name NAME NOT NULL,
    partial_view_schema NAME NOT NULL,
    partial_view_name NAME NOT NULL,
    bucket_width  BIGINT NOT NULL,
    job_id INTEGER UNIQUE NOT NULL REFERENCES _timescaledb_config.bgw_job(id) ON DELETE RESTRICT,
    refresh_lag BIGINT NOT NULL,
    direct_view_schema NAME NOT NULL,
    direct_view_name NAME NOT NULL,
    max_interval_per_job BIGINT NOT NULL,
    UNIQUE(user_view_schema, user_view_name),
    UNIQUE(partial_view_schema, partial_view_name)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

GRANT SELECT ON _timescaledb_catalog.continuous_agg TO PUBLIC;

CREATE OR REPLACE FUNCTION _timescaledb_internal.finalize_agg_sfunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS internal
AS '@MODULE_PATHNAME@', 'ts_finalize_agg_sfunc'
LANGUAGE C IMMUTABLE ;

CREATE OR REPLACE FUNCTION _timescaledb_internal.finalize_agg_ffunc(
tstate internal, aggfn TEXT, inner_agg_collation_schema NAME, inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val ANYELEMENT)
RETURNS anyelement
AS '@MODULE_PATHNAME@', 'ts_finalize_agg_ffunc'
LANGUAGE C IMMUTABLE ;

CREATE AGGREGATE _timescaledb_internal.finalize_agg(agg_name TEXT,  inner_agg_collation_schema NAME,  inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val anyelement) (
    SFUNC = _timescaledb_internal.finalize_agg_sfunc,
    STYPE = internal,
    FINALFUNC = _timescaledb_internal.finalize_agg_ffunc,
    FINALFUNC_EXTRA
);

ALTER TABLE _timescaledb_catalog.installation_metadata RENAME TO telemetry_metadata;
ALTER INDEX _timescaledb_catalog.installation_metadata_pkey RENAME TO telemetry_metadata_pkey;

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_aggs_invalidation_threshold(
    hypertable_id INTEGER PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    watermark BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_invalidation_threshold', '');

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_invalidation_threshold TO PUBLIC;

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_aggs_completed_threshold(
    materialization_id INTEGER PRIMARY KEY
        REFERENCES _timescaledb_catalog.continuous_agg(mat_hypertable_id)
        ON DELETE CASCADE,
    watermark BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_completed_threshold', '');

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_completed_threshold TO PUBLIC;

-- this does not have an FK on the materialization table since INSERTs to this
-- table are performance critical
CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log(
    hypertable_id INTEGER NOT NULL,
    lowest_modified_value BIGINT NOT NULL,
    greatest_modified_value BIGINT NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log', '');

CREATE INDEX continuous_aggs_hypertable_invalidation_log_idx
    ON _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (hypertable_id, lowest_modified_value ASC);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log TO PUBLIC;

DROP FUNCTION IF EXISTS drop_chunks(
    older_than "any",
    table_name  NAME,
    schema_name NAME,
    cascade  BOOLEAN,
    newer_than "any",
    verbose BOOLEAN
);

CREATE OR REPLACE FUNCTION drop_chunks(
    older_than "any" = NULL,
    table_name  NAME = NULL,
    schema_name NAME = NULL,
    cascade  BOOLEAN = FALSE,
    newer_than "any" = NULL,
    verbose BOOLEAN = FALSE,
    cascade_to_materializations BOOLEAN = NULL
) RETURNS SETOF REGCLASS AS '@MODULE_PATHNAME@', 'ts_chunk_drop_chunks'
LANGUAGE C STABLE PARALLEL SAFE;

ALTER TABLE  _timescaledb_config.bgw_job
DROP CONSTRAINT valid_job_type,
ADD CONSTRAINT valid_job_type CHECK (job_type IN ('telemetry_and_version_check_if_enabled', 'reorder', 'drop_chunks', 'continuous_aggregate'));

ALTER TABLE _timescaledb_config.bgw_policy_drop_chunks
  ADD COLUMN cascade_to_materializations BOOLEAN;
DROP FUNCTION IF EXISTS add_drop_chunks_policy(REGCLASS, INTERVAL, BOOL, BOOL);
CREATE OR REPLACE FUNCTION add_drop_chunks_policy(hypertable REGCLASS, older_than INTERVAL, cascade BOOL = FALSE, if_not_exists BOOL = false, cascade_to_materializations BOOL = false)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_add_drop_chunks_policy'
LANGUAGE C VOLATILE STRICT;
