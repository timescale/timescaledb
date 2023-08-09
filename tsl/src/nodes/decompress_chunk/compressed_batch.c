/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <nodes/bitmapset.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/timestamp.h>

#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "debug_assert.h"
#include "guc.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/exec.h"

void
compressed_batch_set_compressed_tuple(DecompressChunkState *chunk_state,
									  DecompressBatchState *batch_state, TupleTableSlot *subslot)
{
	Assert(TupIsNull(batch_state->decompressed_scan_slot));

	/*
	 * The batch states are initialized on demand, because creating the memory
	 * context and the tuple table slots is expensive.
	 */
	if (batch_state->per_batch_context == NULL)
	{
		/*
		 * We use custom size for the batch memory context page, calculated to
		 * fit the typical result of bulk decompression (if we use it).
		 * This allows us to save on expensive malloc/free calls, because the
		 * Postgres memory contexts reallocate all pages except the first one
		 * after earch reset.
		 */
		batch_state->per_batch_context =
			AllocSetContextCreate(CurrentMemoryContext,
								  "DecompressChunk per_batch",
								  0,
								  chunk_state->batch_memory_context_bytes,
								  chunk_state->batch_memory_context_bytes);

		Assert(batch_state->compressed_slot == NULL);

		/* Create a non ref-counted copy of the tuple descriptor */
		if (chunk_state->compressed_slot_tdesc == NULL)
			chunk_state->compressed_slot_tdesc =
				CreateTupleDescCopyConstr(subslot->tts_tupleDescriptor);
		Assert(chunk_state->compressed_slot_tdesc->tdrefcount == -1);

		batch_state->compressed_slot =
			MakeSingleTupleTableSlot(chunk_state->compressed_slot_tdesc, subslot->tts_ops);

		Assert(batch_state->decompressed_scan_slot == NULL);

		/* Get a reference the the output TupleTableSlot */
		TupleTableSlot *slot = chunk_state->csstate.ss.ss_ScanTupleSlot;

		/* Create a non ref-counted copy of the tuple descriptor */
		if (chunk_state->decompressed_slot_scan_tdesc == NULL)
			chunk_state->decompressed_slot_scan_tdesc =
				CreateTupleDescCopyConstr(slot->tts_tupleDescriptor);
		Assert(chunk_state->decompressed_slot_scan_tdesc->tdrefcount == -1);

		batch_state->decompressed_scan_slot =
			MakeSingleTupleTableSlot(chunk_state->decompressed_slot_scan_tdesc, slot->tts_ops);

		/* Ensure that all fields are empty. Calling ExecClearTuple is not enough
		 * because some attributes might not be populated (e.g., due to a dropped
		 * column) and these attributes need to be set to null. */
		ExecStoreAllNullTuple(batch_state->decompressed_scan_slot);
		ExecClearTuple(batch_state->decompressed_scan_slot);
	}
	else
	{
		Assert(batch_state->compressed_slot != NULL);
		Assert(batch_state->decompressed_scan_slot != NULL);
	}

	ExecCopySlot(batch_state->compressed_slot, subslot);
	Assert(!TupIsNull(batch_state->compressed_slot));

	batch_state->total_batch_rows = 0;
	batch_state->next_batch_row = 0;

	MemoryContext old_context = MemoryContextSwitchTo(batch_state->per_batch_context);
	MemoryContextReset(batch_state->per_batch_context);

	for (int i = 0; i < chunk_state->num_total_columns; i++)
	{
		DecompressChunkColumnDescription *column_description = &chunk_state->template_columns[i];

		switch (column_description->type)
		{
			case COMPRESSED_COLUMN:
			{
				Assert(i < chunk_state->num_compressed_columns);
				CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
				column_values->iterator = NULL;
				column_values->arrow = NULL;
				column_values->value_bytes = -1;
				column_values->arrow_values = NULL;
				column_values->arrow_validity = NULL;
				column_values->output_attno = column_description->output_attno;
				bool isnull;
				Datum value = slot_getattr(batch_state->compressed_slot,
										   column_description->compressed_scan_attno,
										   &isnull);
				if (isnull)
				{
					/*
					 * The column will have a default value for the entire batch,
					 * set it now.
					 */
					column_values->iterator = NULL;
					AttrNumber attr = AttrNumberGetAttrOffset(column_description->output_attno);

					batch_state->decompressed_scan_slot->tts_values[attr] =
						getmissingattr(batch_state->decompressed_scan_slot->tts_tupleDescriptor,
									   attr + 1,
									   &batch_state->decompressed_scan_slot->tts_isnull[attr]);
					break;
				}

				/* Decompress the entire batch if it is supported. */
				CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(value);
				ArrowArray *arrow = NULL;
				if (chunk_state->enable_bulk_decompression &&
					column_description->bulk_decompression_supported)
				{
					if (chunk_state->bulk_decompression_context == NULL)
					{
						chunk_state->bulk_decompression_context =
							AllocSetContextCreate(MemoryContextGetParent(
													  batch_state->per_batch_context),
												  "bulk decompression",
												  /* minContextSize = */ 0,
												  /* initBlockSize = */ 64 * 1024,
												  /* maxBlockSize = */ 64 * 1024);
					}

					DecompressAllFunction decompress_all =
						tsl_get_decompress_all_function(header->compression_algorithm);
					Assert(decompress_all != NULL);

					MemoryContext context_before_decompression =
						MemoryContextSwitchTo(chunk_state->bulk_decompression_context);

					arrow = decompress_all(PointerGetDatum(header),
										   column_description->typid,
										   batch_state->per_batch_context);

					MemoryContextReset(chunk_state->bulk_decompression_context);

					MemoryContextSwitchTo(context_before_decompression);
				}

				if (arrow)
				{
					if (batch_state->total_batch_rows == 0)
					{
						batch_state->total_batch_rows = arrow->length;
					}
					else if (batch_state->total_batch_rows != arrow->length)
					{
						elog(ERROR, "compressed column out of sync with batch counter");
					}

					column_values->arrow = arrow;
					column_values->arrow_values = arrow->buffers[1];
					column_values->arrow_validity = arrow->buffers[0];

					column_values->value_bytes = get_typlen(column_description->typid);

					break;
				}

				/* As a fallback, decompress row-by-row. */
				column_values->iterator =
					tsl_get_decompression_iterator_init(header->compression_algorithm,
														chunk_state
															->reverse)(PointerGetDatum(header),
																	   column_description->typid);

				break;
			}
			case SEGMENTBY_COLUMN:
			{
				/*
				 * A segmentby column is not going to change during one batch,
				 * and our output tuples are read-only, so it's enough to only
				 * save it once per batch, which we do here.
				 */
				AttrNumber attr = AttrNumberGetAttrOffset(column_description->output_attno);
				batch_state->decompressed_scan_slot->tts_values[attr] =
					slot_getattr(batch_state->compressed_slot,
								 column_description->compressed_scan_attno,
								 &batch_state->decompressed_scan_slot->tts_isnull[attr]);
				break;
			}
			case COUNT_COLUMN:
			{
				bool isnull;
				Datum value = slot_getattr(batch_state->compressed_slot,
										   column_description->compressed_scan_attno,
										   &isnull);
				/* count column should never be NULL */
				Assert(!isnull);
				int count_value = DatumGetInt32(value);
				if (count_value <= 0)
				{
					ereport(ERROR,
							(errmsg("the compressed data is corrupt: got a segment with length %d",
									count_value)));
				}

				if (batch_state->total_batch_rows == 0)
				{
					batch_state->total_batch_rows = count_value;
				}
				else if (batch_state->total_batch_rows != count_value)
				{
					elog(ERROR, "compressed column out of sync with batch counter");
				}

				break;
			}
			case SEQUENCE_NUM_COLUMN:
				/*
				 * nothing to do here for sequence number
				 * we only needed this for sorting in node below
				 */
				break;
		}
	}

	MemoryContextSwitchTo(old_context);
}

/*
 * Construct the next tuple in the decompressed scan slot.
 * Doesn't check the quals.
 */
static void
compressed_batch_make_next_tuple(DecompressChunkState *chunk_state,
								 DecompressBatchState *batch_state)
{
	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(decompressed_scan_slot != NULL);

	Assert(batch_state->total_batch_rows > 0);
	Assert(batch_state->next_batch_row < batch_state->total_batch_rows);

	const int output_row = batch_state->next_batch_row;
	const size_t arrow_row = unlikely(chunk_state->reverse) ?
								 batch_state->total_batch_rows - 1 - output_row :
								 output_row;

	const int num_compressed_columns = chunk_state->num_compressed_columns;
	for (int i = 0; i < num_compressed_columns; i++)
	{
		CompressedColumnValues column_values = batch_state->compressed_columns[i];

		if (column_values.iterator != NULL)
		{
			DecompressResult result = column_values.iterator->try_next(column_values.iterator);

			if (result.is_done)
			{
				elog(ERROR, "compressed column out of sync with batch counter");
			}

			const AttrNumber attr = AttrNumberGetAttrOffset(column_values.output_attno);
			decompressed_scan_slot->tts_isnull[attr] = result.is_null;
			decompressed_scan_slot->tts_values[attr] = result.val;
		}
		else if (column_values.arrow_values != NULL)
		{
			const char *restrict src = column_values.arrow_values;
			Assert(column_values.value_bytes > 0);

			/*
			 * The conversion of Datum to more narrow types will truncate
			 * the higher bytes, so we don't care if we read some garbage
			 * into them, and can always read 8 bytes. These are unaligned
			 * reads, so technically we have to do memcpy.
			 */
			uint64 value;
			memcpy(&value, &src[column_values.value_bytes * arrow_row], 8);

#ifdef USE_FLOAT8_BYVAL
			Datum datum = Int64GetDatum(value);
#else
			/*
			 * On 32-bit systems, the data larger than 4 bytes go by
			 * reference, so we have to jump through these hoops.
			 */
			Datum datum;
			if (column_values.value_bytes <= 4)
			{
				datum = Int32GetDatum((uint32) value);
			}
			else
			{
				datum = Int64GetDatum(value);
			}
#endif
			const AttrNumber attr = AttrNumberGetAttrOffset(column_values.output_attno);
			decompressed_scan_slot->tts_values[attr] = datum;
			decompressed_scan_slot->tts_isnull[attr] =
				!arrow_row_is_valid(column_values.arrow_validity, arrow_row);
		}
	}

	/*
	 * It's a virtual tuple slot, so no point in clearing/storing it
	 * per each row, we can just update the values in-place. This saves
	 * some CPU. We have to store it after ExecQual returns false (the tuple
	 * didn't pass the filter), or after a new batch. The standard protocol
	 * is to clear and set the tuple slot for each row, but our output tuple
	 * slots are read-only, and the memory is owned by this node, so it is
	 * safe to violate this protocol.
	 */
	Assert(TTS_IS_VIRTUAL(decompressed_scan_slot));
	if (TTS_EMPTY(decompressed_scan_slot))
	{
		ExecStoreVirtualTuple(decompressed_scan_slot);
	}
}

static bool
compressed_batch_postgres_qual(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(!TupIsNull(decompressed_scan_slot));

	if (!chunk_state->csstate.ss.ps.qual)
	{
		return true;
	}

	/* Perform the usual Postgres selection. */
	ExprContext *econtext = chunk_state->csstate.ss.ps.ps_ExprContext;
	econtext->ecxt_scantuple = decompressed_scan_slot;
	ResetExprContext(econtext);
	return ExecQual(chunk_state->csstate.ss.ps.qual, econtext);
}

/*
 * Decompress the next tuple from the batch indicated by batch state. The result is stored
 * in batch_state->decompressed_scan_slot. The slot will be empty if the batch
 * is entirely processed.
 */
void
compressed_batch_advance(DecompressChunkState *chunk_state, DecompressBatchState *batch_state)
{
	Assert(batch_state->total_batch_rows > 0);

	TupleTableSlot *decompressed_scan_slot = batch_state->decompressed_scan_slot;
	Assert(decompressed_scan_slot != NULL);

	const int num_compressed_columns = chunk_state->num_compressed_columns;

	for (; batch_state->next_batch_row < batch_state->total_batch_rows;
		 batch_state->next_batch_row++)
	{
		compressed_batch_make_next_tuple(chunk_state, batch_state);

		if (!compressed_batch_postgres_qual(chunk_state, batch_state))
		{
			/*
			 * The tuple didn't pass the qual, fetch the next one in the next
			 * iteration.
			 */
			InstrCountFiltered1(&chunk_state->csstate, 1);
			continue;
		}

		/* The tuple passed the qual. */
		batch_state->next_batch_row++;
		return;
	}

	/*
	 * Reached end of batch. Check that the columns that we're decompressing
	 * row-by-row have also ended.
	 */
	Assert(batch_state->next_batch_row == batch_state->total_batch_rows);
	for (int i = 0; i < num_compressed_columns; i++)
	{
		CompressedColumnValues *column_values = &batch_state->compressed_columns[i];
		if (column_values->iterator)
		{
			DecompressResult result = column_values->iterator->try_next(column_values->iterator);
			if (!result.is_done)
			{
				elog(ERROR, "compressed column out of sync with batch counter");
			}
		}
	}

	/* Clear old slot state */
	ExecClearTuple(decompressed_scan_slot);
}

/*
 * Before loading the first matching tuple from the batch, also save the very
 * first one into the given slot, even if it doesn't pass the quals. This is
 * needed for batch merge append.
 */
void
compressed_batch_save_first_tuple(DecompressChunkState *chunk_state,
								  DecompressBatchState *batch_state,
								  TupleTableSlot *first_tuple_slot)
{
	Assert(batch_state->next_batch_row == 0);
	Assert(batch_state->total_batch_rows > 0);
	Assert(TupIsNull(batch_state->decompressed_scan_slot));

	compressed_batch_make_next_tuple(chunk_state, batch_state);
	ExecCopySlot(first_tuple_slot, batch_state->decompressed_scan_slot);

	const bool qual_passed = compressed_batch_postgres_qual(chunk_state, batch_state);
	batch_state->next_batch_row++;

	if (!qual_passed)
	{
		InstrCountFiltered1(&chunk_state->csstate, 1);
		compressed_batch_advance(chunk_state, batch_state);
	}
}
