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

-- Get the status of the chunk
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_status(REGCLASS) RETURNS INT
AS '@MODULE_PATHNAME@', 'ts_chunk_status' LANGUAGE C;

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

-- Show the definition of a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_internal.show_chunk(chunk REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB)
AS '@MODULE_PATHNAME@', 'ts_chunk_show' LANGUAGE C VOLATILE;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_detach(chunk REGCLASS) RETURNS JSONB
AS '@MODULE_PATHNAME@', 'ts_chunk_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_attach(hypertable REGCLASS, slices JSONB, chunk_table REGCLASS) RETURNS REGCLASS
AS '@MODULE_PATHNAME@', 'ts_chunk_attach' LANGUAGE C VOLATILE;


CREATE OR REPLACE FUNCTION _timescaledb_internal.slice_union(hypertable REGCLASS, slice1 JSONB,slice2 JSONB) RETURNS JSONB
AS '@MODULE_PATHNAME@', 'ts_slice_union' LANGUAGE C VOLATILE;

CREATE SEQUENCE _timescaledb_catalog.temp_table_seq;

CREATE OR REPLACE FUNCTION chunk_merge(hypertable REGCLASS, chunk1 REGCLASS,chunk2 REGCLASS) RETURNS REGCLASS
AS
$BODY$
DECLARE
    merged_slice  jsonb;
    tmp_table_name text;
BEGIN
    select slices into merged_slice
        from _timescaledb_internal.chunk_detach(chunk1) as t(slices);
    select _timescaledb_internal.slice_union(hypertable,slices,merged_slice) into merged_slice
        from _timescaledb_internal.chunk_detach(chunk2) as t(slices);


      -- FIXME: earlier check for collision
      -- alternate approach: create empty; insert into all other data

    SELECT 'merge_1' into tmp_table_name;

    EXECUTE format('create table %s ( like %s )',tmp_table_name,hypertable);
    EXECUTE format('insert into %s select * from %s union all select * from %s',tmp_table_name,chunk1,chunk2);
    
    RAISE NOTICE 'merged_slice: %',merged_slice;

    RETURN _timescaledb_internal.chunk_attach(hypertable,merged_slice, tmp_table_name);
END;
$BODY$
LANGUAGE PLPGSQL VOLATILE;



CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_merge1(hypertable REGCLASS, VARIADIC chunks REGCLASS[]) RETURNS REGCLASS
AS
$BODY$
DECLARE
BEGIN

END;
$BODY$
LANGUAGE PLPGSQL VOLATILE;

-- Create a chunk with the given dimensional constraints (slices) as
-- given in the JSONB. If chunk_table is a valid relation, it will be
-- attached to the hypertable and used as the data table for the new
-- chunk. Note that schema_name and table_name need not be the same as
-- the existing schema and name for chunk_table. The provided chunk
-- table will be renamed and/or moved as necessary.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk(
       hypertable REGCLASS,
       slices JSONB,
       schema_name NAME = NULL,
       table_name NAME = NULL,
	   chunk_table REGCLASS = NULL)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB, created BOOLEAN)
AS '@MODULE_PATHNAME@', 'ts_chunk_create' LANGUAGE C VOLATILE;

-- change default data node for a chunk
CREATE OR REPLACE FUNCTION _timescaledb_internal.set_chunk_default_data_node(chunk REGCLASS, node_name NAME) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_chunk_set_default_data_node' LANGUAGE C VOLATILE;

-- Get chunk stats.
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_chunk_relstats(relid REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, num_pages INTEGER, num_tuples REAL, num_allvisible INTEGER)
AS '@MODULE_PATHNAME@', 'ts_chunk_get_relstats' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.get_chunk_colstats(relid REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, att_num INTEGER, nullfrac REAL, width INTEGER, distinctval REAL, slotkind INTEGER[], slotopstrings CSTRING[], slotcollations OID[],
slot1numbers FLOAT4[], slot2numbers FLOAT4[], slot3numbers FLOAT4[], slot4numbers FLOAT4[], slot5numbers FLOAT4[],
slotvaluetypetrings CSTRING[], slot1values CSTRING[], slot2values CSTRING[], slot3values CSTRING[], slot4values CSTRING[], slot5values CSTRING[])
AS '@MODULE_PATHNAME@', 'ts_chunk_get_colstats' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_table(
       hypertable REGCLASS,
       slices JSONB,
       schema_name NAME,
       table_name NAME)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_create_empty_table' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.freeze_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_freeze_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.unfreeze_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_unfreeze_chunk' LANGUAGE C VOLATILE;

--wrapper for ts_chunk_drop
--drops the chunk table and its entry in the chunk catalog
CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_drop_single_chunk' LANGUAGE C VOLATILE;

-- internal API used by OSM extension to attach a table as a chunk of the hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.attach_osm_table_chunk(
   hypertable REGCLASS,
   chunk REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_attach_osm_table_chunk' LANGUAGE C VOLATILE;
