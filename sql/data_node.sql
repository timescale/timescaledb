-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Check if a data node is up
CREATE FUNCTION _timescaledb_internal.ping_data_node(node_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_data_node_ping' LANGUAGE C VOLATILE;

-- Change the default data node for a chunk
CREATE OR REPLACE FUNCTION _timescaledb_internal.set_chunk_default_data_node(schema_name NAME, chunk_table_name NAME, node_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_set_chunk_default_data_node' LANGUAGE C VOLATILE;
