-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Built-in function for calculating the next chunk interval when
-- using adaptive chunking. The function can be replaced by a
-- user-defined function with the same signature.
--
-- The parameters passed to the function are as follows:
--
-- dimension_id: the ID of the dimension to calculate the interval for
-- dimension_coord: the coordinate / point on the dimensional axis
-- where the tuple that triggered this chunk creation falls.
-- chunk_target_size: the target size in bytes that the chunk should have.
--
-- The function should return the new interval in dimension-specific
-- time (ususally microseconds).
CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_chunk_interval(
        dimension_id INTEGER,
        dimension_coord BIGINT,
        chunk_target_size BIGINT
) RETURNS BIGINT AS '@MODULE_PATHNAME@', 'ts_calculate_chunk_interval' LANGUAGE C;

-- Function for explicit chunk exclusion. Supply a record and an array
-- of chunk ids as input.
-- Intended to be used in WHERE clause.
-- An example: SELECT * FROM hypertable WHERE _timescaledb_internal.chunks_in(hypertable, ARRAY[1,2]);
--
-- Use it with care as this function directly affects what chunks are being scanned for data.
-- Although this function is immutable (always returns true), we declare it here as volatile
-- so that the PostgreSQL optimizer does not try to evaluate/reduce it in the planner phase
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunks_in(record RECORD, chunks INTEGER[]) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_chunks_in' LANGUAGE C VOLATILE STRICT;

--given a chunk's relid, return the id. Error out if not a chunk relid.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_id_from_relid(relid OID) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_chunk_id_from_relid' LANGUAGE C STABLE STRICT PARALLEL SAFE;

--trigger to block dml on a chunk --
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_dml_blocker() RETURNS trigger
AS '@MODULE_PATHNAME@', 'ts_chunk_dml_blocker' LANGUAGE C;
