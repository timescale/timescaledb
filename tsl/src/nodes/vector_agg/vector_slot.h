/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/attnum.h>
#include <access/tupdesc.h>

#include <hypercore/arrow_tts.h>
#include <nodes/decompress_chunk/compressed_batch.h>

/*
 * Vector slot functions.
 *
 * These functions provide a common interface for arrow slots and compressed
 * batches.
 *
 */

/*
 * Get the result vectorized filter bitmap.
 */
static inline const uint64 *
vector_slot_get_qual_result(const TupleTableSlot *slot, uint16 *num_rows)
{
	if (TTS_IS_ARROWTUPLE(slot))
	{
		*num_rows = arrow_slot_total_row_count(slot);
		return arrow_slot_get_qual_result(slot);
	}

	const DecompressBatchState *batch_state = (const DecompressBatchState *) slot;
	*num_rows = batch_state->total_batch_rows;
	return batch_state->vector_qual_result;
}

/*
 * Return the arrow array or the datum (in case of single scalar value) for a
 * given attribute as a CompressedColumnValues struct.
 */
static inline const CompressedColumnValues *
vector_slot_get_compressed_column_values(TupleTableSlot *slot, const AttrNumber attnum)
{
	const uint16 offset = AttrNumberGetAttrOffset(attnum);

	if (TTS_IS_ARROWTUPLE(slot))
	{
		ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
		const ArrowArray *arrow = arrow_slot_get_array(slot, attnum);
		CompressedColumnValues *values = &aslot->ccvalues;
		int16 attlen = TupleDescAttr(slot->tts_tupleDescriptor, offset)->attlen;

		MemSet(values, 0, sizeof(CompressedColumnValues));

		if (arrow == NULL)
		{
			bool isnull;

			slot_getattr(slot, attnum, &isnull);
			values->decompression_type = DT_Scalar;
			values->output_value = &slot->tts_values[offset];
			values->output_isnull = &slot->tts_isnull[offset];
		}
		else if (attlen > 0)
		{
			Assert(arrow->dictionary == NULL);
			values->decompression_type = attlen;
			values->arrow = (ArrowArray *) arrow;
			values->buffers[0] = arrow->buffers[0];
			values->buffers[1] = arrow->buffers[1];
		}
		else if (arrow->dictionary)
		{
			values->decompression_type = DT_ArrowTextDict;
			values->buffers[0] = arrow->buffers[0];
			values->buffers[1] = arrow->dictionary->buffers[1];
			values->buffers[2] = arrow->dictionary->buffers[2];
			values->buffers[3] = arrow->buffers[1];
		}
		else
		{
			values->decompression_type = DT_ArrowText;
			values->buffers[0] = arrow->buffers[0];
			values->buffers[1] = arrow->buffers[1];
			values->buffers[2] = arrow->buffers[2];
			values->buffers[3] = NULL;
		}

		return values;
	}

	const DecompressBatchState *batch_state = (const DecompressBatchState *) slot;
	const CompressedColumnValues *values = &batch_state->compressed_columns[offset];
	return values;
}
