-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE PROCEDURE timescaledb_experimental.move_chunk(
    chunk REGCLASS,
    source_node Name = NULL,
    destination_node Name = NULL)
AS '@MODULE_PATHNAME@', 'ts_move_chunk_proc' LANGUAGE C;

CREATE OR REPLACE PROCEDURE timescaledb_experimental.copy_chunk(
    chunk REGCLASS,
    source_node Name = NULL,
    destination_node Name = NULL)
AS '@MODULE_PATHNAME@', 'ts_copy_chunk_proc' LANGUAGE C;
