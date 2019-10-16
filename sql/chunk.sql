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
-- This is a marker function and should never be executed (we remove it from the plan)
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunks_in(record RECORD, chunks INTEGER[]) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_chunks_in' LANGUAGE C STABLE STRICT PARALLEL SAFE;

--given a chunk's relid, return the id. Error out if not a chunk relid.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_id_from_relid(relid OID) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_chunk_id_from_relid' LANGUAGE C STABLE STRICT PARALLEL SAFE;

--trigger to block dml on a chunk --
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_dml_blocker() RETURNS trigger
AS '@MODULE_PATHNAME@', 'ts_chunk_dml_blocker' LANGUAGE C;

-- Show the definition of a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_internal.show_chunk(chunk REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB)
AS '@MODULE_PATHNAME@', 'ts_chunk_show' LANGUAGE C VOLATILE;

-- Create a chunk with the given dimensional constraints (slices) as given in the JSONB.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk(
       hypertable REGCLASS,
       slices JSONB,
       schema_name NAME = NULL,
       table_name NAME = NULL)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB, created BOOLEAN)
AS '@MODULE_PATHNAME@', 'ts_chunk_create' LANGUAGE C VOLATILE;

-- change default data node for a chunk
CREATE OR REPLACE FUNCTION _timescaledb_internal.set_chunk_default_data_node(chunk REGCLASS, node_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_chunk_set_default_data_node' LANGUAGE C VOLATILE;

-- Get chunk stats.
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_chunk_relstats(relid REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, num_pages INTEGER, num_tuples REAL, num_allvisible INTEGER)
AS '@MODULE_PATHNAME@', 'ts_chunk_get_relstats' LANGUAGE C VOLATILE;
