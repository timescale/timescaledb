CREATE TABLE _timescaledb_catalog.continuous_aggs_watermark (
  mat_hypertable_id integer NOT NULL,
  watermark bigint NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_watermark_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_watermark TO PUBLIC;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_watermark', '');

CREATE FUNCTION _timescaledb_internal.cagg_watermark_materialized(hypertable_id INTEGER)
RETURNS INT8 AS '@MODULE_PATHNAME@', 'ts_continuous_agg_watermark_materialized' LANGUAGE C STABLE STRICT PARALLEL SAFE;
CREATE FUNCTION _timescaledb_internal.recompress_chunk_segmentwise(REGCLASS, BOOLEAN) RETURNS REGCLASS
AS '@MODULE_PATHNAME@', 'ts_recompress_chunk_segmentwise' LANGUAGE C STRICT VOLATILE;
CREATE FUNCTION _timescaledb_internal.get_compressed_chunk_index_for_recompression(REGCLASS) RETURNS REGCLASS
AS '@MODULE_PATHNAME@', 'ts_get_compressed_chunk_index_for_recompression' LANGUAGE C STRICT VOLATILE;

DROP FUNCTION _timescaledb_internal.dimension_is_finite;
DROP FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql;

CREATE SCHEMA _timescaledb_functions;
GRANT USAGE ON SCHEMA _timescaledb_functions TO PUBLIC;

-- migrate histogram support functions into _timescaledb_functions schema
ALTER FUNCTION _timescaledb_internal.hist_sfunc (state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_combinefunc(state1 INTERNAL, state2 INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_serializefunc(INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_deserializefunc(bytea, INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) SET SCHEMA _timescaledb_functions;

-- migrate first/last support functions into _timescaledb_functions schema
ALTER FUNCTION _timescaledb_internal.first_sfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.first_combinefunc(internal, internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.last_combinefunc(internal, internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_finalfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_serializefunc(internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_deserializefunc(bytea, internal) SET SCHEMA _timescaledb_functions;

DROP FUNCTION IF EXISTS _timescaledb_internal.is_main_table(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.is_main_table(name, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_from_main_table(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.main_table_from_hypertable(integer);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_literal_sql(bigint, regtype);

ALTER FUNCTION _timescaledb_internal.compressed_data_in(CSTRING) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_out(_timescaledb_internal.compressed_data) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_send(_timescaledb_internal.compressed_data) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_recv(internal) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.rxid_in(cstring) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.rxid_out(@extschema@.rxid) SET SCHEMA _timescaledb_functions;

ALTER TABLE _timescaledb_config.bgw_job
    ALTER COLUMN owner SET DEFAULT pg_catalog.quote_ident(current_role)::regrole;

ALTER TABLE _timescaledb_catalog.continuous_agg_migrate_plan
  ADD COLUMN user_view_definition TEXT,
  DROP CONSTRAINT continuous_agg_migrate_plan_mat_hypertable_id_fkey;

-- Log with events that will be sent out with the telemetry. The log
-- will be flushed after it has been sent out. We do not save it to
-- backups since it should not contain important data.
CREATE TABLE _timescaledb_catalog.telemetry_event (
       created timestamptz NOT NULL DEFAULT current_timestamp,
       tag name NOT NULL,
       body jsonb NOT NULL
);

GRANT SELECT ON _timescaledb_catalog.telemetry_event TO PUBLIC;
