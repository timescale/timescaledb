DROP FUNCTION _timescaledb_internal.ping_data_node(NAME, INTERVAL);

CREATE OR REPLACE FUNCTION _timescaledb_internal.ping_data_node(node_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_data_node_ping' LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS _timescaledb_internal.get_approx_row_count(REGCLASS);

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_aggs_watermark;

DROP TABLE IF EXISTS _timescaledb_catalog.continuous_aggs_watermark;

DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_watermark_materialized(hypertable_id INTEGER);
DROP FUNCTION _timescaledb_internal.recompress_chunk_segmentwise(REGCLASS, BOOLEAN);
DROP FUNCTION _timescaledb_internal.get_compressed_chunk_index_for_recompression(REGCLASS);
