/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/relcache.h>

#include "nodes/columnar_scan/compressed_batch.h"
#include "ts_catalog/compression_settings.h"

/*
 * Build the columnar scan's decompression state without a plan node, for
 * utility consumers: decompress_chunk, recompression, chunk split, and the
 * DML batch processing. The caller owns the relations and the settings for
 * the lifetime of the returned context.
 */
extern DecompressContext *decompress_context_create_utility(Relation uncompressed_rel,
															Relation compressed_rel,
															CompressionSettings *settings);
extern DecompressContext *decompress_context_create_utility_desc(TupleDesc uncompressed_desc,
																 TupleDesc compressed_desc);
extern void decompress_context_destroy_utility(DecompressContext *dcontext);

/*
 * Allocate a zeroed DecompressBatchState with room for the context's data
 * columns. The batch is lazily initialized by
 * compressed_batch_set_compressed_tuple(), same as in the scan.
 */
extern DecompressBatchState *decompress_batch_state_create_utility(DecompressContext *dcontext);
extern void decompress_batch_state_destroy_utility(DecompressBatchState *batch_state);

/*
 * Emission state for utility consumers: owns the decompression context, the
 * batch state, and a per-row heap slot array mirroring the RowDecompressor's
 * decompressed_slots (grown on demand, tuples owned by the batch context,
 * slots never own them).
 */
typedef struct UtilityEmitState
{
	DecompressContext *dcontext;
	DecompressBatchState *batch_state;
	TupleTableSlot **slots;
	int slots_capacity;
} UtilityEmitState;

extern UtilityEmitState *utility_emit_create(Relation uncompressed_rel, Relation compressed_rel,
											 CompressionSettings *settings);
extern UtilityEmitState *utility_emit_create_desc(TupleDesc uncompressed_desc,
												  TupleDesc compressed_desc);
extern void utility_emit_destroy(UtilityEmitState *state);

/*
 * Decompress the compressed tuple in the slot into state->slots and return
 * the number of rows. The batch is fully decompressed (no quals) and the
 * state is ready for the next batch on return.
 */
extern int utility_emit_batch(UtilityEmitState *state, TupleTableSlot *compressed_slot);
extern int utility_emit_rows(UtilityEmitState *state);
