-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Get the status of the chunk
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_status(REGCLASS) RETURNS INT
AS '@MODULE_PATHNAME@', 'ts_chunk_status' LANGUAGE C;

-- Get the status of the chunk as text array
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_status_text(chunk_status int) RETURNS TEXT[]
AS '@MODULE_PATHNAME@', 'ts_chunk_status_text' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_status_text(chunk regclass) RETURNS TEXT[]
AS $$ SELECT _timescaledb_functions.chunk_status_text(_timescaledb_functions.chunk_status($1)); $$ LANGUAGE SQL STRICT IMMUTABLE PARALLEL SAFE SET search_path TO pg_catalog, pg_temp;;

--given a chunk's relid, return the id. Error out if not a chunk relid.
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_id_from_relid(relid OID) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_chunk_id_from_relid' LANGUAGE C STABLE STRICT PARALLEL SAFE;

-- Returns hypertable regclass if the relation is a chunk (for compressed chunk
-- the main hypertable is returned, not the internal compressed hypertable) and
-- `NULL` otherwise. The returned value of `is_compressed` is `true` if the
-- chunk is an internal compressed chunk, `false` if it is a regular or
-- uncompressed chunk (even if it has an associated with it compressed chunk)
-- and `NULL` if the relation is not a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_relid_from_chunk_relid(
      IN  relation      REGCLASS,
      OUT hypertable    REGCLASS,
      OUT is_compressed BOOLEAN)
RETURNS RECORD
AS $$
    SELECT
        pc.oid::regclass,
        cs.compress_relid IS NOT NULL
    FROM _timescaledb_catalog.hypertable h
    JOIN _timescaledb_catalog.chunk c
        ON c.hypertable_id = h.id
    JOIN pg_catalog.pg_namespace pn
        ON pn.nspname = h.schema_name
    JOIN pg_catalog.pg_class pc
        ON pc.relname = h.table_name AND pc.relnamespace = pn.oid
    LEFT JOIN _timescaledb_catalog.compression_settings cs
        ON cs.compress_relid = $1
    WHERE c.relid = COALESCE(cs.relid, $1)
$$ LANGUAGE SQL STABLE STRICT PARALLEL SAFE SET search_path TO pg_catalog, pg_temp;

-- Show the definition of a chunk.
CREATE OR REPLACE FUNCTION _timescaledb_functions.show_chunk(chunk REGCLASS)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB)
AS '@MODULE_PATHNAME@', 'ts_chunk_show' LANGUAGE C VOLATILE;

-- Create a chunk with the given dimensional constraints (slices) as
-- given in the JSONB. If chunk_table is a valid relation, it will be
-- attached to the hypertable and used as the data table for the new
-- chunk. Note that schema_name and table_name need not be the same as
-- the existing schema and name for chunk_table. The provided chunk
-- table will be renamed and/or moved as necessary.
CREATE OR REPLACE FUNCTION _timescaledb_functions.create_chunk(
       hypertable REGCLASS,
       slices JSONB,
       schema_name NAME = NULL,
       table_name NAME = NULL,
	   chunk_table REGCLASS = NULL)
RETURNS TABLE(chunk_id INTEGER, hypertable_id INTEGER, schema_name NAME, table_name NAME, relkind "char", slices JSONB, created BOOLEAN)
AS '@MODULE_PATHNAME@', 'ts_chunk_create' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.freeze_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_freeze_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.unfreeze_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_unfreeze_chunk' LANGUAGE C VOLATILE;

--wrapper for ts_chunk_drop
--drops the chunk table and its entry in the chunk catalog
CREATE OR REPLACE FUNCTION _timescaledb_functions.drop_chunk(
   chunk REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_drop_single_chunk' LANGUAGE C VOLATILE;

-- internal API used by OSM extension to attach a table as a chunk of the hypertable
CREATE OR REPLACE FUNCTION _timescaledb_functions.attach_osm_table_chunk(
   hypertable REGCLASS,
   chunk REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_attach_osm_table_chunk' LANGUAGE C VOLATILE;

-- internal API used by OSM extension to drop an OSM chunk table from the hypertable
CREATE OR REPLACE FUNCTION _timescaledb_functions.drop_osm_chunk(hypertable REGCLASS)
RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_chunk_drop_osm_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE PROCEDURE @extschema@.detach_chunk(chunk REGCLASS)
LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_detach_chunk';

CREATE OR REPLACE PROCEDURE @extschema@.attach_chunk(hypertable REGCLASS,
   chunk REGCLASS,
   slices JSONB)
LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_attach_chunk';
