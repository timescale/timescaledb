-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_replace' LANGUAGE C VOLATILE STRICT;
