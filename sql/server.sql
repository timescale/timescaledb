-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Check if server is up
CREATE FUNCTION _timescaledb_internal.server_ping(server_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_server_ping' LANGUAGE C VOLATILE;

-- change default server for a chunk
CREATE OR REPLACE FUNCTION _timescaledb_internal.set_chunk_default_server(schema_name NAME, chunk_table_name NAME, server_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_set_chunk_default_server' LANGUAGE C VOLATILE;
