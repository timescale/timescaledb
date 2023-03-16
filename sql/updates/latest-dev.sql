DROP FUNCTION _timescaledb_internal.ping_data_node(NAME);

CREATE FUNCTION _timescaledb_internal.ping_data_node(node_name NAME, timeout INTERVAL = NULL) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_data_node_ping' LANGUAGE C VOLATILE;

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
