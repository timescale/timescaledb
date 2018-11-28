-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

CREATE OR REPLACE FUNCTION _timescaledb_internal.drop_chunks_impl(
    hypertable  REGCLASS = NULL,
    older_than "any" = NULL,
    newer_than "any" = NULL,
	cascade BOOLEAN = FALSE
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_chunk_drop_chunks'
LANGUAGE C STABLE PARALLEL SAFE;

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_replace' LANGUAGE C VOLATILE STRICT;
