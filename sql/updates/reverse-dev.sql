DROP FUNCTION _timescaledb_internal.ping_data_node(NAME, INTERVAL);

CREATE OR REPLACE FUNCTION _timescaledb_internal.ping_data_node(node_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_data_node_ping' LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS _timescaledb_internal.get_approx_row_count(REGCLASS);
