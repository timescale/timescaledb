-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

------------------------------------------------------------------------
-- Experimental DDL functions and APIs.
--
-- Users should not rely on these functions unless they accept that
-- they can change and/or be removed at any time.
------------------------------------------------------------------------

-- Block new chunk creation on a data node for a distributed
-- hypertable. NULL hypertable means it will block chunks for all
-- distributed hypertables
CREATE OR REPLACE FUNCTION timescaledb_experimental.block_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL, force BOOLEAN = FALSE) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_block_new_chunks' LANGUAGE C VOLATILE;

-- Allow chunk creation on a blocked data node for a distributed
-- hypertable. NULL hypertable means it will allow chunks for all
-- distributed hypertables
CREATE OR REPLACE FUNCTION timescaledb_experimental.allow_new_chunks(data_node_name NAME, hypertable REGCLASS = NULL) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_allow_new_chunks' LANGUAGE C VOLATILE;

CREATE OR REPLACE PROCEDURE timescaledb_experimental.move_chunk(
    chunk REGCLASS,
    source_node NAME = NULL,
    destination_node NAME = NULL)
AS '@MODULE_PATHNAME@', 'ts_move_chunk_proc' LANGUAGE C;

CREATE OR REPLACE PROCEDURE timescaledb_experimental.copy_chunk(
    chunk REGCLASS,
    source_node NAME = NULL,
    destination_node NAME = NULL)
AS '@MODULE_PATHNAME@', 'ts_copy_chunk_proc' LANGUAGE C;

-- A copy_chunk or move_chunk procedure call involves multiple nodes and
-- depending on the data size can take a long time. Failures are possible
-- when this long running activity is ongoing. We need to be able to recover
-- and cleanup such failed chunk copy/move activities and it's done via this
-- procedure
CREATE OR REPLACE PROCEDURE timescaledb_experimental.cleanup_copy_chunk_operation(
    operation_id NAME)
AS '@MODULE_PATHNAME@', 'ts_copy_chunk_cleanup_proc' LANGUAGE C;
