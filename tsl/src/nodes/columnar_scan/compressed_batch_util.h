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
 * the lifetime of the returned context; the context keeps its own copy of
 * the uncompressed tuple descriptor. A segmentby column whose type differs
 * between the two descriptors is reported with an internal error code, or
 * with a user-facing one when internal_error is false (the record SRF takes
 * user input).
 */
extern DecompressContext *decompress_context_create_utility(Relation uncompressed_rel,
															Relation compressed_rel,
															CompressionSettings *settings);
extern DecompressContext *decompress_context_create_utility_desc(TupleDesc uncompressed_desc,
																 TupleDesc compressed_desc,
																 bool internal_error);
extern void decompress_context_destroy_utility(DecompressContext *dcontext);

/*
 * Allocate a zeroed DecompressBatchState with room for the context's data
 * columns. The batch is lazily initialized by the first prepare, same as in
 * the scan.
 */
extern DecompressBatchState *decompress_batch_state_create_utility(DecompressContext *dcontext);
extern void decompress_batch_state_destroy_utility(DecompressBatchState *batch_state);

/*
 * Emission state for utility consumers: owns the decompression context, the
 * batch state, and a per-row heap slot array mirroring the RowDecompressor's
 * decompressed_slots (grown on demand, tuples owned by the batch context,
 * slots never own them).
 *
 * All memory of the state hangs off mctx, the memory context that was current
 * when it was created: the per-batch and bulk scratch contexts are children
 * of it, and the output slots are allocated in it on first use. The state can
 * therefore be reused from a shorter-lived context (the DML path calls it
 * from the executor's per-tuple context).
 */
typedef struct UtilityEmitState
{
	DecompressContext *dcontext;
	DecompressBatchState *batch_state;
	TupleTableSlot **slots;
	int slots_capacity;
	MemoryContext mctx;
} UtilityEmitState;

extern UtilityEmitState *utility_emit_create(Relation uncompressed_rel, Relation compressed_rel,
											 CompressionSettings *settings);
extern UtilityEmitState *utility_emit_create_desc(TupleDesc uncompressed_desc,
												  TupleDesc compressed_desc, bool internal_error);
extern void utility_emit_destroy(UtilityEmitState *state);

/*
 * Prepare the batch state for the compressed tuple in the slot (count,
 * segmentby values, lazy per-column decompression state). Consumers that
 * decompress selectively (DML) call this, then decompress_column() and
 * compressed_batch_decode_remaining(), then utility_emit_rows().
 */
extern void utility_emit_prepare(UtilityEmitState *state, TupleTableSlot *compressed_slot);

/*
 * Decompress the compressed tuple in the slot into state->slots and return
 * the number of rows. The batch is fully decompressed (no quals) and the
 * state is ready for the next batch on return.
 */
extern int utility_emit_batch(UtilityEmitState *state, TupleTableSlot *compressed_slot);
extern int utility_emit_rows(UtilityEmitState *state);

/*
 * Drop the current batch and release the toast relation opened by the
 * Detoaster, keeping the state itself usable for the next batch. Callers that
 * keep a state across scans call this at the end of every scan.
 */
extern void utility_emit_reset(UtilityEmitState *state);
