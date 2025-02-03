/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/attnum.h>
#include <access/tupdesc.h>
#include <nodes/decompress_chunk/compressed_batch.h>

/*
 * Vector slot functions.
 *
 * These functions provide a common interface for arrow slots and compressed
 * batches.
 *
 */
static inline const uint64 *
vector_slot_get_qual_result(const TupleTableSlot *slot, uint16 *num_rows)
{
	const DecompressBatchState *batch_state = (const DecompressBatchState *) slot;
	*num_rows = batch_state->total_batch_rows;
	return batch_state->vector_qual_result;
}

/*
 * Return the arrow array or the datum (in case of single scalar value) for a
 * given attribute.
 *
 * This is essentially doing the same thing as the separate functions above,
 * but with a common return type.
 */
static inline const CompressedColumnValues *
vector_slot_get_compressed_column_values(TupleTableSlot *slot, const AttrNumber attnum)
{
	const uint16 offset = AttrNumberGetAttrOffset(attnum);
	const DecompressBatchState *batch_state = (const DecompressBatchState *) slot;
	const CompressedColumnValues *values = &batch_state->compressed_columns[offset];
	return values;
}
