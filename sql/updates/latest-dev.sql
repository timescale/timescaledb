
-- we add an addition optional argument to locf
DROP FUNCTION IF EXISTS locf(ANYELEMENT,ANYELEMENT);

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.continuous_agg (
    mat_hypertable_id INTEGER PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    raw_hypertable_id INTEGER NOT NULL REFERENCES  _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    user_view_schema NAME NOT NULL,
    user_view_name NAME NOT NULL,
    partial_view_schema NAME NOT NULL,
    partial_view_name    NAME NOT NULL,
    bucket_width  BIGINT NOT NULL
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
