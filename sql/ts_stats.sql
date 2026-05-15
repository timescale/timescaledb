-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Chunk statistics. This function takes three optional parameters:
-- - compressed_relid: if provided, returns stats for the specified compressed chunk only; otherwise, returns stats for all chunks.
-- - uncompressed_relid: if provided, returns stats for the specified user chunk only; if compressed_relid is also provided, both must agree.
-- - since: if provided, returns stats for operations that occurred since the specified timestamp; otherwise, returns all stats.
--
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_statistics(
    compressed_relid   regclass    DEFAULT NULL,
    uncompressed_relid regclass    DEFAULT NULL,
    since              timestamptz DEFAULT NULL
)
RETURNS TABLE (
    compressed_relid                   oid,
    uncompressed_relid                 oid,
    -- Compression
    compressed_batch_count             bigint,
    compressed_block_count             bigint,
    compressed_batch_rows_min          bigint,
    compressed_batch_rows_max          bigint,
    compressed_batch_rows_sum          bigint,
    compressed_batch_rows_sqsum        double precision,
    compressed_batch_bytes_min         bigint,
    compressed_batch_bytes_max         bigint,
    compressed_batch_bytes_sum         bigint,
    compressed_batch_bytes_sqsum       double precision,
    compressed_block_bytes_min         bigint,
    compressed_block_bytes_max         bigint,
    compressed_block_bytes_sum         bigint,
    compressed_block_bytes_sqsum       double precision,
    -- DML totals (cumulative, from cmd_totals)
    total_batches_deleted              bigint,
    total_batches_decompressed         bigint,
    total_tuples_decompressed          bigint,
    total_batches_scanned              bigint,
    total_batches_checked_by_bloom     bigint,
    total_batches_pruned_by_bloom      bigint,
    total_batches_without_bloom        bigint,
    total_batches_bloom_false_positives bigint,
    total_batches_filtered_compressed  bigint,
    total_batches_filtered_decompressed bigint,
    -- DML last operation snapshot (from cmd_last_op)
    last_op_batches_deleted            bigint,
    last_op_batches_decompressed       bigint,
    last_op_tuples_decompressed        bigint,
    last_op_batches_scanned            bigint,
    last_op_batches_checked_by_bloom   bigint,
    last_op_batches_pruned_by_bloom    bigint,
    last_op_batches_without_bloom      bigint,
    last_op_batches_bloom_false_positives bigint,
    last_op_batches_filtered_compressed bigint,
    last_op_batches_filtered_decompressed bigint,
    -- Operation counts
    n_selects                          bigint,
    n_inserts                          bigint,
    n_updates                          bigint,
    n_deletes                          bigint,
    -- Timestamps
    first_update                       timestamptz,
    last_update                        timestamptz
)
AS '@MODULE_PATHNAME@', 'ts_stats_chunks'
LANGUAGE C STABLE;

-- Logical reset: clears all cached chunks under the segment lock.
CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_statistics_reset()
RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_stats_reset'
LANGUAGE C VOLATILE;
