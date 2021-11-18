DROP PROCEDURE IF EXISTS recompress_chunk;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_status;
-- revert changes to continuous aggregates view definition
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
