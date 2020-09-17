-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--documentation of these function located in chunk_index.h
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_clone(chunk_index_oid OID) RETURNS OID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_clone' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_index_replace(chunk_index_oid_old OID, chunk_index_oid_new OID) RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_chunk_index_replace' LANGUAGE C VOLATILE STRICT;

-- Block new chunk creation on a data node for a distributed
-- hypertable. NULL hypertable means it will block chunks for all
-- distributed hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.block_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL, force BOOLEAN = FALSE) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_block_new_chunks' LANGUAGE C VOLATILE;

-- Allow chunk creation on a blocked data node for a distributed
-- hypertable. NULL hypertable means it will allow chunks for all
-- distributed hypertables
CREATE OR REPLACE FUNCTION _timescaledb_internal.allow_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_allow_new_chunks' LANGUAGE C VOLATILE;

-- Freeze a continuous aggregate across the given window.
CREATE OR REPLACE PROCEDURE _timescaledb_internal.freeze_continuous_aggregate(
	cagg                     REGCLASS,
	window_start             "any" = NULL,
	window_end               "any" = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_continuous_agg_freeze';

-- Unfreeze a continuous aggregate across the given window.
CREATE OR REPLACE PROCEDURE _timescaledb_internal.unfreeze_continuous_aggregate(
	cagg                     REGCLASS,
	window_start             "any" = NULL,
	window_end               "any" = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_continuous_agg_unfreeze';
